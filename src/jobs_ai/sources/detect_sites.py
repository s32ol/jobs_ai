from __future__ import annotations

from collections import OrderedDict, deque
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from html import unescape
from html.parser import HTMLParser
from pathlib import Path
import re
from urllib.parse import urljoin, urlparse, urlunparse

from ..collect.fetch import FetchRequest, FetchResponse, Fetcher, fetch_text
from ..collect.harness import run_collection
from ..collect.models import SourceResult
from ..db import initialize_schema
from ..portal_support import build_portal_support
from ..source_seed.infer import primary_domain_label
from ..source_seed.models import CompanySeedInput, ManualReviewSourceHint, SourceCandidate
from ..source_seed.starter_lists import (
    available_starter_lists,
    resolve_starter_lists,
    starter_lists_help_text,
)
from ..source_seed.verify import discover_supported_source_candidate_from_url
from ..workspace import WorkspacePaths
from .intake import LoadedDiscoveryInput, load_discovery_inputs
from .models import (
    SourceRegistryDetectSitesResult,
    SourceRegistrySeedItemResult,
    SourceRegistrySiteDetectInputResult,
)
from .registry import (
    normalize_registry_source_url,
    register_verified_source,
    upsert_registry_source,
)
from .structured_clues import scan_structured_clues

_MAX_FETCH_PAGES_PER_INPUT = 5
_MAX_LIKELY_CAREERS_LINKS = 4
_MAX_STRUCTURED_FOLLOW_UP_URLS = 2
_ABSOLUTE_URL_RE = re.compile(r"https?://[^\s\"'<>]+", re.IGNORECASE)
_PROTOCOL_RELATIVE_URL_RE = re.compile(r"(?<![:\w])//[^\s\"'<>]+")
_NON_ALNUM_RE = re.compile(r"[^a-z0-9]+")
_CAREERS_KEYWORDS = (
    "careers",
    "jobs",
    "join us",
    "work with us",
    "join our team",
    "openings",
    "opportunities",
    "hiring",
)
_SKIPPED_FOLLOW_SUFFIXES = (
    ".css",
    ".gif",
    ".ico",
    ".jpeg",
    ".jpg",
    ".js",
    ".json",
    ".pdf",
    ".png",
    ".svg",
    ".xml",
    ".zip",
)

@dataclass(frozen=True, slots=True)
class _SiteScanResult:
    supported_candidates: tuple[SourceCandidate, ...]
    manual_review_sources: tuple[ManualReviewSourceHint, ...]
    fetched_page_count: int
    failure_reason_code: str | None = None
    failure_reason: str | None = None


@dataclass(frozen=True, slots=True)
class _AnchorLink:
    href: str
    text: str


def available_detect_sites_starter_lists() -> tuple[str, ...]:
    return available_starter_lists()


def detect_sites_starter_help_text() -> str:
    return starter_lists_help_text()


def detect_registry_sources_from_sites(
    paths: WorkspacePaths,
    *,
    companies: Sequence[str],
    from_file: Path | None,
    starter_lists: Sequence[str],
    timeout_seconds: float,
    use_structured_clues: bool = True,
    created_at: datetime | None = None,
    fetcher: Fetcher | None = None,
) -> SourceRegistryDetectSitesResult:
    resolved_starter_lists = resolve_starter_lists(tuple(starter_lists))
    loaded_inputs = load_discovery_inputs(
        command_label="sources detect-sites",
        companies=companies,
        from_file=from_file,
        starter_lists=resolved_starter_lists,
    )
    if not loaded_inputs:
        raise ValueError(
            "at least one company entry is required via arguments, --from-file, or --starter"
        )

    return detect_registry_sources_from_loaded_inputs(
        paths,
        loaded_inputs=loaded_inputs,
        starter_lists=resolved_starter_lists,
        timeout_seconds=timeout_seconds,
        use_structured_clues=use_structured_clues,
        created_at=created_at,
        fetcher=fetcher,
    )


def detect_registry_sources_from_loaded_inputs(
    paths: WorkspacePaths,
    *,
    loaded_inputs: Sequence[LoadedDiscoveryInput],
    starter_lists: Sequence[str],
    timeout_seconds: float,
    use_structured_clues: bool = True,
    created_at: datetime | None = None,
    fetcher: Fetcher | None = None,
) -> SourceRegistryDetectSitesResult:
    if not loaded_inputs:
        raise ValueError("at least one loaded discovery input is required")

    initialize_schema(paths.database_path)
    resolved_fetcher = fetch_text if fetcher is None else fetcher
    input_results: list[SourceRegistrySiteDetectInputResult] = []
    item_results: list[SourceRegistrySeedItemResult] = []

    for loaded_input in loaded_inputs:
        input_result, detected_items = _process_detection_input(
            paths,
            company_input=loaded_input.company_input,
            provenance=loaded_input.provenance,
            timeout_seconds=timeout_seconds,
            use_structured_clues=use_structured_clues,
            created_at=created_at,
            fetcher=resolved_fetcher,
        )
        input_results.append(input_result)
        item_results.extend(detected_items)

    return SourceRegistryDetectSitesResult(
        starter_lists=tuple(starter_lists),
        input_results=tuple(input_results),
        item_results=tuple(item_results),
    )


def _process_detection_input(
    paths: WorkspacePaths,
    *,
    company_input: CompanySeedInput,
    provenance: str,
    timeout_seconds: float,
    use_structured_clues: bool,
    created_at: datetime | None,
    fetcher: Fetcher,
) -> tuple[SourceRegistrySiteDetectInputResult, tuple[SourceRegistrySeedItemResult, ...]]:
    start_url = _resolve_start_url(company_input)
    if start_url is None:
        return (
            SourceRegistrySiteDetectInputResult(
                raw_input=company_input.raw_value,
                resolved_start_url=None,
                fetched_page_count=0,
                outcome="failed",
                reason_code="missing_domain_or_url",
                reason="detect-sites needs a domain, homepage URL, careers URL, or direct ATS URL; company-name-only inputs stay conservative",
            ),
            (),
        )

    scan_result = _scan_company_site(
        company_input,
        start_url=start_url,
        timeout_seconds=timeout_seconds,
        use_structured_clues=use_structured_clues,
        fetcher=fetcher,
    )

    item_results: list[SourceRegistrySeedItemResult] = []
    confirmed_source_urls: list[str] = []
    manual_review_source_urls: list[str] = []
    confirmed_normalized_urls: set[str] = set()
    seen_manual_review_urls: set[str] = set()
    primary_manual_review_reason: tuple[str, str] | None = None
    primary_skipped_source_result: SourceResult | None = None

    for candidate in scan_result.supported_candidates:
        source_result = _verify_supported_candidate(
            candidate,
            timeout_seconds=timeout_seconds,
            created_at=created_at,
            fetcher=fetcher,
        )
        resolved_source_url = source_result.source.normalized_url or candidate.url
        normalized_url = _normalized_registry_url(resolved_source_url)

        if source_result.outcome == "collected":
            if normalized_url is not None:
                confirmed_normalized_urls.add(normalized_url)
            mutation = register_verified_source(
                paths.database_path,
                source_url=resolved_source_url,
                portal_type=source_result.source.portal_type or candidate.portal_type,
                company=_verified_company(source_result) or company_input.company,
                label=_registry_label(company_input),
                provenance=provenance,
                verification_reason_code="confirmed_via_detect_sites",
                verification_reason=(
                    f'confirmed from company-site detection input "{company_input.raw_value}"'
                ),
                created_at=created_at,
            )
            item_results.append(
                SourceRegistrySeedItemResult(
                    raw_input=company_input.raw_value,
                    outcome="confirmed",
                    reason_code="confirmed_supported_source",
                    reason=f"confirmed reusable {candidate.portal_type} source from the company site",
                    source_url=mutation.entry.source_url,
                    portal_type=mutation.entry.portal_type,
                    mutation=mutation,
                )
            )
            confirmed_source_urls.append(mutation.entry.source_url)
            continue

        if source_result.outcome == "manual_review":
            if normalized_url is None or normalized_url in confirmed_normalized_urls:
                continue
            if normalized_url in seen_manual_review_urls:
                continue
            seen_manual_review_urls.add(normalized_url)
            mutation = upsert_registry_source(
                paths.database_path,
                source_url=resolved_source_url,
                portal_type=source_result.source.portal_type or candidate.portal_type,
                company=_verified_company(source_result) or company_input.company,
                label=_registry_label(company_input),
                status="manual_review",
                provenance=provenance,
                verification_reason_code=source_result.reason_code,
                verification_reason=source_result.reason,
                created_at=created_at,
                preserve_existing_active=True,
                mark_verified_at=True,
            )
            item_results.append(
                SourceRegistrySeedItemResult(
                    raw_input=company_input.raw_value,
                    outcome="manual_review",
                    reason_code=source_result.reason_code,
                    reason=source_result.reason,
                    source_url=mutation.entry.source_url,
                    portal_type=mutation.entry.portal_type,
                    mutation=mutation,
                )
            )
            manual_review_source_urls.append(mutation.entry.source_url)
            if primary_manual_review_reason is None:
                primary_manual_review_reason = (
                    source_result.reason_code,
                    source_result.reason,
                )
            continue

        if primary_skipped_source_result is None:
            primary_skipped_source_result = source_result

    for manual_review_source in scan_result.manual_review_sources:
        normalized_url = _normalized_registry_url(manual_review_source.source_url)
        if normalized_url is None or normalized_url in confirmed_normalized_urls:
            continue
        if normalized_url in seen_manual_review_urls:
            continue
        seen_manual_review_urls.add(normalized_url)
        mutation = upsert_registry_source(
            paths.database_path,
            source_url=manual_review_source.source_url,
            portal_type=manual_review_source.portal_type,
            company=manual_review_source.detected_company or company_input.company,
            label=_registry_label(company_input),
            status="manual_review",
            provenance=provenance,
            verification_reason_code=manual_review_source.reason_code,
            verification_reason=manual_review_source.reason,
            created_at=created_at,
            preserve_existing_active=True,
            mark_verified_at=True,
        )
        item_results.append(
            SourceRegistrySeedItemResult(
                raw_input=company_input.raw_value,
                outcome="manual_review",
                reason_code=manual_review_source.reason_code,
                reason=manual_review_source.reason,
                source_url=mutation.entry.source_url,
                portal_type=mutation.entry.portal_type,
                mutation=mutation,
            )
        )
        manual_review_source_urls.append(mutation.entry.source_url)
        if primary_manual_review_reason is None:
            primary_manual_review_reason = (
                manual_review_source.reason_code,
                manual_review_source.reason,
            )

    if confirmed_source_urls:
        confirmed_count = len(tuple(dict.fromkeys(confirmed_source_urls)))
        manual_review_count = len(tuple(dict.fromkeys(manual_review_source_urls)))
        reason = f"confirmed {confirmed_count} ATS source(s) from the company site"
        if manual_review_count:
            reason = (
                f"{reason}; {manual_review_count} additional source(s) need manual review"
            )
        return (
            SourceRegistrySiteDetectInputResult(
                raw_input=company_input.raw_value,
                resolved_start_url=start_url,
                fetched_page_count=scan_result.fetched_page_count,
                outcome="confirmed",
                reason_code="confirmed_site_sources_found",
                reason=reason,
                detected_source_urls=tuple(dict.fromkeys(confirmed_source_urls)),
                manual_review_source_urls=tuple(dict.fromkeys(manual_review_source_urls)),
            ),
            tuple(item_results),
        )

    if manual_review_source_urls:
        assert primary_manual_review_reason is not None
        return (
            SourceRegistrySiteDetectInputResult(
                raw_input=company_input.raw_value,
                resolved_start_url=start_url,
                fetched_page_count=scan_result.fetched_page_count,
                outcome="manual_review",
                reason_code=primary_manual_review_reason[0],
                reason=primary_manual_review_reason[1],
                detected_source_urls=(),
                manual_review_source_urls=tuple(dict.fromkeys(manual_review_source_urls)),
            ),
            tuple(item_results),
        )

    if primary_skipped_source_result is not None:
        return (
            SourceRegistrySiteDetectInputResult(
                raw_input=company_input.raw_value,
                resolved_start_url=start_url,
                fetched_page_count=scan_result.fetched_page_count,
                outcome="failed",
                reason_code=primary_skipped_source_result.reason_code,
                reason=primary_skipped_source_result.reason,
            ),
            tuple(item_results),
        )

    return (
        SourceRegistrySiteDetectInputResult(
            raw_input=company_input.raw_value,
            resolved_start_url=start_url,
            fetched_page_count=scan_result.fetched_page_count,
            outcome="failed",
            reason_code=scan_result.failure_reason_code or "no_ats_footprint_found",
            reason=scan_result.failure_reason
            or "no supported ATS footprint was found on the company site",
        ),
        tuple(item_results),
    )


def _scan_company_site(
    company_input: CompanySeedInput,
    *,
    start_url: str,
    timeout_seconds: float,
    use_structured_clues: bool,
    fetcher: Fetcher,
) -> _SiteScanResult:
    direct_candidate = discover_supported_source_candidate_from_url(
        start_url,
        slug_source="site_input",
    )
    if direct_candidate is not None:
        return _SiteScanResult(
            supported_candidates=(direct_candidate,),
            manual_review_sources=(),
            fetched_page_count=0,
        )

    direct_manual_review = _manual_review_hint_from_url(start_url)
    if direct_manual_review is not None:
        return _SiteScanResult(
            supported_candidates=(),
            manual_review_sources=(direct_manual_review,),
            fetched_page_count=0,
        )

    candidate_map: OrderedDict[str, SourceCandidate] = OrderedDict()
    manual_review_map: OrderedDict[str, ManualReviewSourceHint] = OrderedDict()
    queued_urls = deque([start_url])
    seen_fetch_urls: set[str] = set()
    fetched_page_count = 0
    saw_likely_careers_link = False
    start_url_looks_like_careers = _looks_like_careers_url(start_url)
    initial_fetch_error: Exception | None = None

    while queued_urls and fetched_page_count < _MAX_FETCH_PAGES_PER_INPUT:
        fetch_url = queued_urls.popleft()
        normalized_fetch_url = _normalize_url(fetch_url)
        if normalized_fetch_url is None or normalized_fetch_url in seen_fetch_urls:
            continue
        seen_fetch_urls.add(normalized_fetch_url)

        try:
            response = fetcher(
                FetchRequest(
                    url=fetch_url,
                    timeout_seconds=timeout_seconds,
                    headers={"Accept": _accept_header_for_url(fetch_url)},
                )
            )
        except Exception as exc:
            if fetched_page_count == 0 and initial_fetch_error is None:
                initial_fetch_error = exc
            continue

        fetched_page_count += 1
        final_url = response.final_url or fetch_url
        _record_detected_url(
            final_url,
            candidate_map=candidate_map,
            manual_review_map=manual_review_map,
        )

        for explicit_url in _extract_explicit_urls(response):
            _record_detected_url(
                explicit_url,
                candidate_map=candidate_map,
                manual_review_map=manual_review_map,
            )

        if use_structured_clues:
            structured_clues = scan_structured_clues(response)
            for candidate in structured_clues.supported_candidates:
                candidate_map.setdefault(candidate.url, candidate)
            for manual_review_source in structured_clues.manual_review_sources:
                manual_review_map.setdefault(
                    manual_review_source.source_url,
                    manual_review_source,
                )
            for follow_up_url in structured_clues.follow_up_urls[:_MAX_STRUCTURED_FOLLOW_UP_URLS]:
                if not _should_follow_structured_clue_url(
                    follow_up_url,
                    base_url=final_url,
                    company_domain=company_input.domain,
                ):
                    continue
                queued_urls.append(follow_up_url)

        likely_careers_links = _extract_likely_careers_links(
            response,
            company_domain=company_input.domain,
        )
        if likely_careers_links:
            saw_likely_careers_link = True
        for careers_link in likely_careers_links:
            _record_detected_url(
                careers_link,
                candidate_map=candidate_map,
                manual_review_map=manual_review_map,
            )
            if not _should_follow_careers_link(
                careers_link,
                base_url=final_url,
                company_domain=company_input.domain,
            ):
                continue
            queued_urls.append(careers_link)

    if candidate_map or manual_review_map:
        return _SiteScanResult(
            supported_candidates=tuple(candidate_map.values()),
            manual_review_sources=tuple(manual_review_map.values()),
            fetched_page_count=fetched_page_count,
        )

    if fetched_page_count == 0 and initial_fetch_error is not None:
        return _SiteScanResult(
            supported_candidates=(),
            manual_review_sources=(),
            fetched_page_count=0,
            failure_reason_code="fetch_failed",
            failure_reason=str(initial_fetch_error),
        )

    if saw_likely_careers_link or start_url_looks_like_careers:
        return _SiteScanResult(
            supported_candidates=(),
            manual_review_sources=(),
            fetched_page_count=fetched_page_count,
            failure_reason_code="no_ats_footprint_found",
            failure_reason=(
                "no supported Greenhouse, Lever, or Ashby source was found after inspecting the company site"
            ),
        )

    return _SiteScanResult(
        supported_candidates=(),
        manual_review_sources=(),
        fetched_page_count=fetched_page_count,
        failure_reason_code="no_careers_link_found",
        failure_reason="could not find a likely careers/jobs path or explicit ATS footprint on the company site",
    )


def _verify_supported_candidate(
    candidate: SourceCandidate,
    *,
    timeout_seconds: float,
    created_at: datetime | None,
    fetcher: Fetcher,
) -> SourceResult:
    run = run_collection(
        (candidate.url,),
        timeout_seconds=timeout_seconds,
        created_at=created_at,
        fetcher=fetcher,
    )
    return run.report.source_results[0]


def _record_detected_url(
    value: str | None,
    *,
    candidate_map: OrderedDict[str, SourceCandidate],
    manual_review_map: OrderedDict[str, ManualReviewSourceHint],
) -> None:
    candidate = discover_supported_source_candidate_from_url(
        value,
        index=len(candidate_map) + 1,
        slug_source="company_site",
    )
    if candidate is not None:
        candidate_map.setdefault(candidate.url, candidate)
        return

    manual_review_source = _manual_review_hint_from_url(value)
    if manual_review_source is not None:
        manual_review_map.setdefault(
            manual_review_source.source_url,
            manual_review_source,
        )


def _manual_review_hint_from_url(value: str | None) -> ManualReviewSourceHint | None:
    portal_support = build_portal_support(value)
    if portal_support is None or portal_support.portal_type != "workday":
        return None
    return ManualReviewSourceHint(
        source_url=portal_support.normalized_apply_url,
        portal_type=portal_support.portal_type,
        reason_code="workday_partial_support",
        reason="Workday portal detected from the company site; keep it in manual review only.",
        suggested_next_action=(
            "Keep the Workday URL visible in the registry for manual review, but do not rely on it as a structured collector source."
        ),
    )


def _extract_explicit_urls(response: FetchResponse) -> tuple[str, ...]:
    base_url = response.final_url or response.url
    parser = _AnchorExtractor()
    parser.feed(response.text)
    parser.close()

    discovered_urls: OrderedDict[str, None] = OrderedDict()
    for link in parser.links:
        absolute_url = _absolutize_url(link.href, base_url=base_url)
        if absolute_url is not None:
            discovered_urls.setdefault(absolute_url, None)

    normalized_html = unescape(response.text).replace("\\/", "/")
    for raw_url in _ABSOLUTE_URL_RE.findall(normalized_html):
        cleaned_url = _clean_extracted_url(raw_url)
        if cleaned_url is not None:
            discovered_urls.setdefault(cleaned_url, None)
    for raw_url in _PROTOCOL_RELATIVE_URL_RE.findall(normalized_html):
        cleaned_url = _clean_extracted_url(f"https:{raw_url}")
        if cleaned_url is not None:
            discovered_urls.setdefault(cleaned_url, None)
    return tuple(discovered_urls.keys())


def _extract_likely_careers_links(
    response: FetchResponse,
    *,
    company_domain: str | None,
) -> tuple[str, ...]:
    base_url = response.final_url or response.url
    parser = _AnchorExtractor()
    parser.feed(response.text)
    parser.close()

    scored_links: list[tuple[int, int, str]] = []
    for index, link in enumerate(parser.links):
        absolute_url = _absolutize_url(link.href, base_url=base_url)
        if absolute_url is None:
            continue
        score = _careers_link_score(
            link,
            absolute_url=absolute_url,
            company_domain=company_domain,
            base_url=base_url,
        )
        if score <= 0:
            continue
        scored_links.append((score, index, absolute_url))

    scored_links.sort(key=lambda item: (-item[0], item[1], item[2]))
    selected_links: list[str] = []
    seen_links: set[str] = set()
    for _, _, absolute_url in scored_links:
        if absolute_url in seen_links:
            continue
        seen_links.add(absolute_url)
        selected_links.append(absolute_url)
        if len(selected_links) >= _MAX_LIKELY_CAREERS_LINKS:
            break
    return tuple(selected_links)


def _careers_link_score(
    link: _AnchorLink,
    *,
    absolute_url: str,
    company_domain: str | None,
    base_url: str,
) -> int:
    normalized_text = _normalize_search_text(link.text)
    parsed = urlparse(absolute_url)
    host = (parsed.hostname or "").lower()
    path_text = _normalize_search_text(parsed.path)
    host_text = _normalize_search_text(host)

    score = 0
    for keyword in _CAREERS_KEYWORDS:
        if keyword in normalized_text:
            score = max(score, 5)
        if keyword in path_text:
            score = max(score, 4)
        if keyword in host_text:
            score = max(score, 3)

    if _looks_like_careers_url(absolute_url):
        score = max(score, 3)
    if build_portal_support(absolute_url) is not None:
        score = max(score, 6)

    if not _is_related_host(
        host,
        company_domain=company_domain,
        base_host=(urlparse(base_url).hostname or "").lower(),
    ):
        if build_portal_support(absolute_url) is None:
            return 0
        score = max(score - 2, 1)

    if absolute_url == base_url:
        return 0
    return score


def _should_follow_careers_link(
    absolute_url: str,
    *,
    base_url: str,
    company_domain: str | None,
) -> bool:
    if build_portal_support(absolute_url) is not None:
        return False

    parsed = urlparse(absolute_url)
    if parsed.scheme.lower() not in {"http", "https"} or not parsed.netloc:
        return False
    if parsed.path.lower().endswith(_SKIPPED_FOLLOW_SUFFIXES):
        return False
    return _is_related_host(
        (parsed.hostname or "").lower(),
        company_domain=company_domain,
        base_host=(urlparse(base_url).hostname or "").lower(),
    )


def _should_follow_structured_clue_url(
    absolute_url: str,
    *,
    base_url: str,
    company_domain: str | None,
) -> bool:
    if build_portal_support(absolute_url) is not None:
        return False

    parsed = urlparse(absolute_url)
    if parsed.scheme.lower() not in {"http", "https"} or not parsed.netloc:
        return False
    if parsed.path.lower().endswith((".css", ".gif", ".ico", ".jpeg", ".jpg", ".js", ".pdf", ".png", ".svg", ".zip")):
        return False
    if not _looks_like_machine_readable_clue_url(absolute_url):
        return False
    return _is_related_host(
        (parsed.hostname or "").lower(),
        company_domain=company_domain,
        base_host=(urlparse(base_url).hostname or "").lower(),
    )


def _is_related_host(
    host: str,
    *,
    company_domain: str | None,
    base_host: str,
) -> bool:
    if not host:
        return False
    normalized_company_domain = (company_domain or "").lower()
    if normalized_company_domain:
        if host == normalized_company_domain:
            return True
        if host.endswith(f".{normalized_company_domain}"):
            return True
        if normalized_company_domain.endswith(f".{host}"):
            return True

    if base_host:
        if host == base_host:
            return True
        if host.endswith(f".{base_host}") or base_host.endswith(f".{host}"):
            return True

    host_label = primary_domain_label(host)
    company_label = primary_domain_label(normalized_company_domain)
    base_label = primary_domain_label(base_host)
    if company_label is not None and host_label == company_label:
        return True
    if base_label is not None and host_label == base_label:
        return True
    return False


def _resolve_start_url(company_input: CompanySeedInput) -> str | None:
    if company_input.career_page_url is not None:
        return _normalize_url(company_input.career_page_url)
    if company_input.domain is None:
        return None
    return _normalize_url(f"https://{company_input.domain}")


def _normalize_url(value: str | None) -> str | None:
    if value is None:
        return None
    stripped_value = value.strip()
    if not stripped_value:
        return None
    parsed = urlparse(stripped_value)
    if parsed.scheme.lower() not in {"http", "https"} or not parsed.netloc:
        return None
    return urlunparse(parsed._replace(fragment=""))


def _absolutize_url(value: str | None, *, base_url: str) -> str | None:
    if value is None:
        return None
    stripped_value = value.strip()
    if not stripped_value or stripped_value.startswith(("#", "javascript:", "mailto:", "tel:")):
        return None
    if stripped_value.startswith("//"):
        stripped_value = f"https:{stripped_value}"
    return _normalize_url(urljoin(base_url, stripped_value))


def _clean_extracted_url(value: str) -> str | None:
    stripped_value = value.strip().rstrip("),.;")
    return _normalize_url(stripped_value)


def _normalize_search_text(value: str | None) -> str:
    if value is None:
        return ""
    return _NON_ALNUM_RE.sub(" ", value.lower()).strip()


def _looks_like_careers_url(value: str) -> bool:
    parsed = urlparse(value)
    searchable_text = _normalize_search_text(
        f"{parsed.hostname or ''} {parsed.path}"
    )
    return any(keyword in searchable_text for keyword in _CAREERS_KEYWORDS)


def _looks_like_machine_readable_clue_url(value: str) -> bool:
    parsed = urlparse(value)
    searchable_text = _normalize_search_text(f"{parsed.path} {parsed.query}")
    has_feed_shape = any(
        token in searchable_text
        for token in ("sitemap", "feed", "api", "json", "xml")
    )
    has_jobs_shape = any(
        token in searchable_text
        for token in ("job", "jobs", "career", "careers", "opening")
    )
    return has_feed_shape and has_jobs_shape


def _accept_header_for_url(value: str) -> str:
    if _looks_like_machine_readable_clue_url(value):
        return "text/html, application/json, application/xml, text/xml"
    return "text/html"


def _registry_label(company_input: CompanySeedInput) -> str:
    return company_input.company or company_input.domain or company_input.raw_value


def _verified_company(source_result: SourceResult) -> str | None:
    companies = tuple(
        dict.fromkeys(
            lead.company
            for lead in source_result.collected_leads
            if lead.company
        )
    )
    if len(companies) == 1:
        return companies[0]
    return None


def _normalized_registry_url(source_url: str | None) -> str | None:
    if source_url is None:
        return None
    try:
        normalized_url, _ = normalize_registry_source_url(source_url)
    except ValueError:
        return None
    return normalized_url


class _AnchorExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.links: list[_AnchorLink] = []
        self._current_href: str | None = None
        self._current_text_parts: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() != "a":
            return
        attr_map = {
            key.lower(): value
            for key, value in attrs
            if value is not None
        }
        href = attr_map.get("href")
        if href is None:
            return
        self._current_href = href
        self._current_text_parts = []

    def handle_data(self, data: str) -> None:
        if self._current_href is None:
            return
        self._current_text_parts.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() != "a" or self._current_href is None:
            return
        self.links.append(
            _AnchorLink(
                href=self._current_href,
                text="".join(self._current_text_parts).strip(),
            )
        )
        self._current_href = None
        self._current_text_parts = []
