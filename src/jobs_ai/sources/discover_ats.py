from __future__ import annotations

from collections.abc import Iterable, Sequence
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass
from datetime import datetime, timezone
from importlib.resources import files
from pathlib import Path
import json
import re
import threading
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from ..collect.fetch import (
    DEFAULT_FETCH_USER_AGENT,
    FetchError,
    FetchRequest,
    FetchResponse,
    Fetcher,
)
from ..collect.harness import run_collection
from ..db import initialize_schema
from ..workspace import WorkspacePaths
from .models import (
    SourceRegistryATSDiscoveryItemResult,
    SourceRegistryATSDiscoveryResult,
    SourceRegistryATSProviderCount,
)
from .registry import normalize_registry_source_url, register_verified_source, upsert_registry_source

DEFAULT_DISCOVER_ATS_LIMIT = 500
DEFAULT_DISCOVER_ATS_TIMEOUT_SECONDS = 6.0
DEFAULT_DISCOVER_ATS_MAX_CONCURRENCY = 5
DEFAULT_DISCOVER_ATS_MAX_REQUESTS_PER_SECOND = 5.0
SUPPORTED_DISCOVER_ATS_PROVIDERS = ("greenhouse", "lever", "ashby")

_SLUG_RE = re.compile(r"[^a-z0-9-]+")
_INVALID_HTTP_STATUSES = frozenset({404, 410})
_TRANSIENT_HTTP_STATUSES = frozenset({401, 403, 408, 409, 423, 425, 429})
_GREENHOUSE_API_URL = "https://boards-api.greenhouse.io/v1/boards/{slug}/jobs"
_GREENHOUSE_BOARD_URL = "https://boards.greenhouse.io/{slug}"
_LEVER_API_URL = "https://api.lever.co/v0/postings/{slug}"
_LEVER_BOARD_URL = "https://jobs.lever.co/{slug}"
_ASHBY_BOARD_URL = "https://jobs.ashbyhq.com/{slug}"
_DISCOVERY_HEADERS = {"Accept": "application/json, text/html"}


@dataclass(frozen=True, slots=True)
class _ProviderProbeResult:
    slug: str
    portal_type: str
    source_url: str
    status: str
    reason_code: str
    reason: str
    company: str | None = None
    lead_count: int = 0


class _RateLimiter:
    def __init__(self, max_requests_per_second: float) -> None:
        self._interval = 1.0 / max_requests_per_second if max_requests_per_second > 0 else 0.0
        self._next_allowed_at = 0.0
        self._lock = threading.Lock()

    def acquire(self) -> None:
        if self._interval <= 0:
            return

        while True:
            with self._lock:
                now = time.monotonic()
                wait_seconds = self._next_allowed_at - now
                if wait_seconds <= 0:
                    self._next_allowed_at = max(self._next_allowed_at, now) + self._interval
                    return
            time.sleep(wait_seconds)


def discover_registry_ats_sources(
    paths: WorkspacePaths,
    *,
    limit: int,
    output_path: Path | None = None,
    providers: Sequence[str] | None = None,
    timeout_seconds: float = DEFAULT_DISCOVER_ATS_TIMEOUT_SECONDS,
    max_concurrency: int = DEFAULT_DISCOVER_ATS_MAX_CONCURRENCY,
    max_requests_per_second: float = DEFAULT_DISCOVER_ATS_MAX_REQUESTS_PER_SECOND,
    slug_candidates: Sequence[str] | None = None,
    created_at: datetime | None = None,
    fetcher: Fetcher | None = None,
) -> SourceRegistryATSDiscoveryResult:
    if limit < 1:
        raise ValueError("limit must be at least 1")
    if timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be greater than 0")
    if max_concurrency < 1:
        raise ValueError("max_concurrency must be at least 1")
    if max_requests_per_second <= 0:
        raise ValueError("max_requests_per_second must be greater than 0")

    initialize_schema(paths.database_path)
    selected_providers = _normalize_provider_filters(providers)
    normalized_slugs = (
        _normalize_slug_candidates(slug_candidates)
        if slug_candidates is not None
        else load_company_slug_candidates()
    )
    if not normalized_slugs:
        raise ValueError("no company slug candidates were available for ATS discovery")

    created_at_dt = _normalize_created_at(created_at)
    effective_output_path = _resolve_output_path(paths, output_path)
    rate_limiter = _RateLimiter(max_requests_per_second)
    effective_fetcher = _build_rate_limited_fetcher(
        _fetch_text_allow_http_errors if fetcher is None else fetcher,
        rate_limiter=rate_limiter,
    )

    worker_count = min(max_concurrency, len(normalized_slugs))
    slug_iter = iter(normalized_slugs)
    active_count = 0
    tested_slug_count = 0
    seen_source_urls: set[str] = set()
    item_results: list[SourceRegistryATSDiscoveryItemResult] = []
    provider_counts = {
        provider: {"active": 0, "manual_review": 0, "ignored": 0}
        for provider in selected_providers
    }

    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        future_map = _submit_discovery_tasks(
            executor,
            slug_iter=slug_iter,
            providers=selected_providers,
            effective_fetcher=effective_fetcher,
            timeout_seconds=timeout_seconds,
            task_count=worker_count,
        )

        while future_map and active_count < limit:
            completed, _ = wait(tuple(future_map), return_when=FIRST_COMPLETED)
            for future in completed:
                slug = future_map.pop(future)
                probe_results = future.result()
                tested_slug_count += 1

                for probe in probe_results:
                    status_key = "ignored" if probe.status == "ignore" else probe.status
                    provider_counts[probe.portal_type][status_key] += 1

                    if probe.status == "ignore":
                        continue

                    normalized_source_url, _ = normalize_registry_source_url(
                        probe.source_url,
                        portal_type=probe.portal_type,
                    )
                    if normalized_source_url in seen_source_urls:
                        continue
                    if probe.status == "active" and active_count >= limit:
                        continue

                    mutation = _upsert_discovered_source(
                        paths,
                        probe=probe,
                        created_at=created_at_dt,
                    )
                    seen_source_urls.add(normalized_source_url)
                    item_results.append(
                        SourceRegistryATSDiscoveryItemResult(
                            slug=probe.slug,
                            portal_type=probe.portal_type,
                            source_url=mutation.entry.source_url,
                            company=mutation.entry.company,
                            status=mutation.entry.status,
                            reason_code=probe.reason_code,
                            reason=probe.reason,
                            lead_count=probe.lead_count,
                            mutation=mutation,
                        )
                    )
                    if probe.status == "active":
                        active_count += 1
                        if active_count >= limit:
                            break

                if active_count >= limit:
                    break

                next_slug = next(slug_iter, None)
                if next_slug is not None:
                        future_map[
                            executor.submit(
                                _discover_slug_candidates,
                                next_slug,
                                providers=selected_providers,
                                timeout_seconds=timeout_seconds,
                                fetcher=effective_fetcher,
                            )
                        ] = next_slug

        for future in future_map:
            future.cancel()

    result = SourceRegistryATSDiscoveryResult(
        limit=limit,
        candidate_slug_count=len(normalized_slugs),
        tested_slug_count=tested_slug_count,
        item_results=tuple(item_results),
        provider_counts=tuple(
            SourceRegistryATSProviderCount(
                provider=provider,
                active=provider_counts[provider]["active"],
                manual_review=provider_counts[provider]["manual_review"],
                ignored=provider_counts[provider]["ignored"],
            )
            for provider in selected_providers
        ),
        providers=selected_providers,
        output_path=effective_output_path,
        max_concurrency=worker_count,
        max_requests_per_second=max_requests_per_second,
    )
    if effective_output_path is not None:
        _write_discovered_sources_output(effective_output_path, result.discovered_source_urls)
    return result


def load_company_slug_candidates(source_path: Path | None = None) -> tuple[str, ...]:
    if source_path is None:
        source_text = files("jobs_ai.sources").joinpath("data/company_slug_candidates.txt").read_text(
            encoding="utf-8"
        )
    else:
        source_text = source_path.read_text(encoding="utf-8")
    return _normalize_slug_candidates(source_text.splitlines())


def _discover_slug_candidates(
    slug: str,
    *,
    providers: Sequence[str],
    timeout_seconds: float,
    fetcher: Fetcher,
) -> tuple[_ProviderProbeResult, ...]:
    probe_results: list[_ProviderProbeResult] = []
    for provider in providers:
        if provider == "greenhouse":
            probe_results.append(
                _probe_greenhouse_board(slug, timeout_seconds=timeout_seconds, fetcher=fetcher)
            )
        elif provider == "lever":
            probe_results.append(
                _probe_lever_board(slug, timeout_seconds=timeout_seconds, fetcher=fetcher)
            )
        elif provider == "ashby":
            probe_results.append(
                _probe_ashby_board(slug, timeout_seconds=timeout_seconds, fetcher=fetcher)
            )
    return tuple(probe_results)


def _probe_greenhouse_board(
    slug: str,
    *,
    timeout_seconds: float,
    fetcher: Fetcher,
) -> _ProviderProbeResult:
    board_url = _GREENHOUSE_BOARD_URL.format(slug=slug)
    response = _fetch_provider_response(
        _GREENHOUSE_API_URL.format(slug=slug),
        timeout_seconds=timeout_seconds,
        fetcher=fetcher,
    )
    if response is None:
        return _ProviderProbeResult(
            slug=slug,
            portal_type="greenhouse",
            source_url=board_url,
            status="ignore",
            reason_code="greenhouse_fetch_failed",
            reason="Greenhouse jobs API could not be reached.",
        )
    if response.status_code in _INVALID_HTTP_STATUSES:
        return _ProviderProbeResult(
            slug=slug,
            portal_type="greenhouse",
            source_url=board_url,
            status="ignore",
            reason_code="greenhouse_not_found",
            reason="Greenhouse jobs API returned not found.",
        )
    if _status_requires_manual_review(response.status_code):
        return _ProviderProbeResult(
            slug=slug,
            portal_type="greenhouse",
            source_url=board_url,
            status="manual_review",
            reason_code="greenhouse_api_blocked",
            reason=f"Greenhouse jobs API returned HTTP {response.status_code}; manual review required.",
        )

    payload = _load_json_payload(response.text)
    if not isinstance(payload, dict):
        return _ProviderProbeResult(
            slug=slug,
            portal_type="greenhouse",
            source_url=board_url,
            status="manual_review",
            reason_code="greenhouse_api_ambiguous",
            reason="Greenhouse jobs API returned an unexpected payload; manual review required.",
        )

    jobs = payload.get("jobs")
    if not isinstance(jobs, list):
        return _ProviderProbeResult(
            slug=slug,
            portal_type="greenhouse",
            source_url=board_url,
            status="manual_review",
            reason_code="greenhouse_api_ambiguous",
            reason="Greenhouse jobs API payload was missing the jobs list; manual review required.",
        )
    if not jobs:
        return _ProviderProbeResult(
            slug=slug,
            portal_type="greenhouse",
            source_url=board_url,
            status="ignore",
            reason_code="greenhouse_no_open_jobs",
            reason="Greenhouse board exists but has no open jobs.",
        )

    return _ProviderProbeResult(
        slug=slug,
        portal_type="greenhouse",
        source_url=board_url,
        status="active",
        reason_code="greenhouse_jobs_api_discovered",
        reason=f"Greenhouse jobs API returned {len(jobs)} job(s).",
        company=_provider_company_name(payload, jobs, slug),
        lead_count=len(jobs),
    )


def _probe_lever_board(
    slug: str,
    *,
    timeout_seconds: float,
    fetcher: Fetcher,
) -> _ProviderProbeResult:
    board_url = _LEVER_BOARD_URL.format(slug=slug)
    response = _fetch_provider_response(
        _LEVER_API_URL.format(slug=slug),
        timeout_seconds=timeout_seconds,
        fetcher=fetcher,
    )
    if response is None:
        return _ProviderProbeResult(
            slug=slug,
            portal_type="lever",
            source_url=board_url,
            status="ignore",
            reason_code="lever_fetch_failed",
            reason="Lever postings API could not be reached.",
        )
    if response.status_code in _INVALID_HTTP_STATUSES:
        return _ProviderProbeResult(
            slug=slug,
            portal_type="lever",
            source_url=board_url,
            status="ignore",
            reason_code="lever_not_found",
            reason="Lever postings API returned not found.",
        )
    if _status_requires_manual_review(response.status_code):
        return _ProviderProbeResult(
            slug=slug,
            portal_type="lever",
            source_url=board_url,
            status="manual_review",
            reason_code="lever_api_blocked",
            reason=f"Lever postings API returned HTTP {response.status_code}; manual review required.",
        )

    payload = _load_json_payload(response.text)
    if not isinstance(payload, list):
        return _ProviderProbeResult(
            slug=slug,
            portal_type="lever",
            source_url=board_url,
            status="manual_review",
            reason_code="lever_api_ambiguous",
            reason="Lever postings API returned an unexpected payload; manual review required.",
        )
    if not payload:
        return _ProviderProbeResult(
            slug=slug,
            portal_type="lever",
            source_url=board_url,
            status="ignore",
            reason_code="lever_no_open_jobs",
            reason="Lever board exists but has no open jobs.",
        )

    return _ProviderProbeResult(
        slug=slug,
        portal_type="lever",
        source_url=board_url,
        status="active",
        reason_code="lever_postings_api_discovered",
        reason=f"Lever postings API returned {len(payload)} job(s).",
        company=_provider_company_name(None, payload, slug),
        lead_count=len(payload),
    )


def _probe_ashby_board(
    slug: str,
    *,
    timeout_seconds: float,
    fetcher: Fetcher,
) -> _ProviderProbeResult:
    board_url = _ASHBY_BOARD_URL.format(slug=slug)
    run = run_collection(
        [board_url],
        timeout_seconds=timeout_seconds,
        fetcher=fetcher,
    )
    result = run.report.source_results[0]
    if result.outcome == "collected":
        companies = tuple(dict.fromkeys(lead.company for lead in run.collected_leads if lead.company))
        company = companies[0] if len(companies) == 1 else _slug_to_company_name(slug)
        return _ProviderProbeResult(
            slug=slug,
            portal_type="ashby",
            source_url=board_url,
            status="active",
            reason_code="ashby_board_discovered",
            reason=f"Ashby board exposed {len(run.collected_leads)} job(s).",
            company=company,
            lead_count=len(run.collected_leads),
        )

    if result.outcome == "manual_review":
        return _ProviderProbeResult(
            slug=slug,
            portal_type="ashby",
            source_url=board_url,
            status="manual_review",
            reason_code=_prefix_reason_code("ashby", result.reason_code),
            reason=result.reason,
        )

    evidence = result.evidence
    if evidence is not None and evidence.status_code in _INVALID_HTTP_STATUSES:
        return _ProviderProbeResult(
            slug=slug,
            portal_type="ashby",
            source_url=board_url,
            status="ignore",
            reason_code="ashby_not_found",
            reason="Ashby board returned not found.",
        )
    if result.reason_code == "blocked_or_access_denied" or (
        evidence is not None and _status_requires_manual_review(evidence.status_code)
    ):
        return _ProviderProbeResult(
            slug=slug,
            portal_type="ashby",
            source_url=board_url,
            status="manual_review",
            reason_code=_prefix_reason_code("ashby", result.reason_code),
            reason=result.reason,
        )
    if result.reason_code == "non_html_content":
        return _ProviderProbeResult(
            slug=slug,
            portal_type="ashby",
            source_url=board_url,
            status="manual_review",
            reason_code="ashby_non_html_content",
            reason=result.reason,
        )
    return _ProviderProbeResult(
        slug=slug,
        portal_type="ashby",
        source_url=board_url,
        status="ignore",
        reason_code=_prefix_reason_code("ashby", result.reason_code),
        reason=result.reason,
    )


def _upsert_discovered_source(
    paths: WorkspacePaths,
    *,
    probe: _ProviderProbeResult,
    created_at: datetime,
):
    company = probe.company or _slug_to_company_name(probe.slug)
    provenance = f"sources discover-ats {probe.portal_type} {probe.slug}"
    if probe.status == "active":
        return register_verified_source(
            paths.database_path,
            source_url=probe.source_url,
            portal_type=probe.portal_type,
            company=company,
            provenance=provenance,
            verification_reason_code=probe.reason_code,
            verification_reason=probe.reason,
            created_at=created_at,
        )

    return upsert_registry_source(
        paths.database_path,
        source_url=probe.source_url,
        portal_type=probe.portal_type,
        company=company,
        status="manual_review",
        provenance=provenance,
        verification_reason_code=probe.reason_code,
        verification_reason=probe.reason,
        created_at=created_at,
        preserve_existing_active=True,
        mark_verified_at=True,
    )


def _submit_discovery_tasks(
    executor: ThreadPoolExecutor,
    *,
    slug_iter,
    providers: Sequence[str],
    effective_fetcher: Fetcher,
    timeout_seconds: float,
    task_count: int,
) -> dict[Future[tuple[_ProviderProbeResult, ...]], str]:
    future_map: dict[Future[tuple[_ProviderProbeResult, ...]], str] = {}
    for _ in range(task_count):
        slug = next(slug_iter, None)
        if slug is None:
            break
        future_map[
            executor.submit(
                _discover_slug_candidates,
                slug,
                providers=providers,
                timeout_seconds=timeout_seconds,
                fetcher=effective_fetcher,
            )
        ] = slug
    return future_map


def _build_rate_limited_fetcher(fetcher: Fetcher, *, rate_limiter: _RateLimiter) -> Fetcher:
    def wrapped(request: FetchRequest) -> FetchResponse:
        rate_limiter.acquire()
        return fetcher(request)

    return wrapped


def _fetch_provider_response(
    url: str,
    *,
    timeout_seconds: float,
    fetcher: Fetcher,
) -> FetchResponse | None:
    try:
        return fetcher(
            FetchRequest(
                url=url,
                timeout_seconds=timeout_seconds,
                headers=_DISCOVERY_HEADERS,
            )
        )
    except FetchError:
        return None


def _fetch_text_allow_http_errors(request: FetchRequest) -> FetchResponse:
    headers = {
        "Accept": "text/html,application/json",
        "User-Agent": DEFAULT_FETCH_USER_AGENT,
    }
    headers.update(dict(request.headers))
    url_request = Request(request.url, headers=headers)

    try:
        with urlopen(url_request, timeout=request.timeout_seconds) as response:
            charset = response.headers.get_content_charset() or "utf-8"
            content_type = response.headers.get("Content-Type")
            payload = response.read()
            text = payload.decode(charset, errors="replace")
            return FetchResponse(
                url=request.url,
                final_url=response.geturl(),
                status_code=response.getcode() or 200,
                content_type=content_type,
                text=text,
            )
    except HTTPError as exc:
        charset = exc.headers.get_content_charset() or "utf-8"
        content_type = exc.headers.get("Content-Type")
        payload = exc.read()
        text = payload.decode(charset, errors="replace")
        return FetchResponse(
            url=request.url,
            final_url=exc.geturl(),
            status_code=exc.code,
            content_type=content_type,
            text=text,
        )
    except URLError as exc:
        raise FetchError(f"unable to fetch {request.url}: {exc.reason}") from exc
    except TimeoutError as exc:
        raise FetchError(f"timed out after {request.timeout_seconds:.1f}s while fetching {request.url}") from exc


def _normalize_slug_candidates(values: Sequence[str] | Iterable[str]) -> tuple[str, ...]:
    normalized_values: list[str] = []
    seen: set[str] = set()
    for raw_value in values:
        normalized_value = _normalize_slug(raw_value)
        if normalized_value is None or normalized_value in seen:
            continue
        seen.add(normalized_value)
        normalized_values.append(normalized_value)
    return tuple(normalized_values)


def _normalize_slug(value: str) -> str | None:
    stripped = value.strip()
    if not stripped or stripped.startswith("#"):
        return None
    normalized = _SLUG_RE.sub("-", stripped.lower())
    normalized = re.sub(r"-{2,}", "-", normalized).strip("-")
    if not normalized:
        return None
    return normalized


def _provider_company_name(
    payload: dict[str, object] | None,
    jobs: Sequence[object],
    slug: str,
) -> str:
    if payload is not None:
        for key in ("company", "company_name", "companyName", "name"):
            company = _normalize_text(payload.get(key))
            if company is not None:
                return company

    for job in jobs:
        if not isinstance(job, dict):
            continue
        for key in ("company", "company_name", "companyName", "name"):
            company = _normalize_text(job.get(key))
            if company is not None:
                return company
    return _slug_to_company_name(slug)


def _load_json_payload(text: str) -> object | None:
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return None


def _status_requires_manual_review(status_code: int | None) -> bool:
    if status_code is None:
        return False
    return status_code in _TRANSIENT_HTTP_STATUSES or status_code >= 500


def _prefix_reason_code(portal_type: str, reason_code: str) -> str:
    if reason_code.startswith(f"{portal_type}_"):
        return reason_code
    return f"{portal_type}_{reason_code}"


def _slug_to_company_name(slug: str) -> str:
    return " ".join(part.capitalize() for part in slug.split("-"))


def _normalize_text(value: object) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        value = str(value)
    normalized_value = value.strip()
    return normalized_value or None


def _resolve_output_path(paths: WorkspacePaths, output_path: Path | None) -> Path | None:
    if output_path is None:
        return None
    if output_path.is_absolute():
        return output_path
    return (paths.project_root / output_path).resolve()


def _write_discovered_sources_output(output_path: Path, source_urls: Sequence[str]) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        "".join(f"{source_url}\n" for source_url in source_urls),
        encoding="utf-8",
    )


def _normalize_created_at(created_at: datetime | None) -> datetime:
    if created_at is None:
        return datetime.now(timezone.utc)
    if created_at.tzinfo is None:
        return created_at.replace(tzinfo=timezone.utc)
    return created_at.astimezone(timezone.utc)


def _normalize_provider_filters(providers: Sequence[str] | None) -> tuple[str, ...]:
    if providers is None or not providers:
        return SUPPORTED_DISCOVER_ATS_PROVIDERS

    normalized_providers = tuple(
        dict.fromkeys(provider.strip().lower() for provider in providers if provider.strip())
    )
    unknown_providers = [
        provider
        for provider in normalized_providers
        if provider not in SUPPORTED_DISCOVER_ATS_PROVIDERS
    ]
    if unknown_providers:
        allowed = ", ".join(SUPPORTED_DISCOVER_ATS_PROVIDERS)
        raise ValueError(
            f"unknown discover-ats provider(s): {', '.join(unknown_providers)}. Allowed values: {allowed}"
        )
    return normalized_providers
