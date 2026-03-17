from __future__ import annotations

from collections import OrderedDict
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from html import unescape
from html.parser import HTMLParser
from pathlib import Path
import json
import re
import threading
import time
from urllib.parse import urljoin, urlparse, urlunparse

from ..collect.fetch import FetchRequest, FetchResponse, Fetcher, fetch_text
from ..source_seed.infer import parse_company_input_line
from ..workspace import WorkspacePaths
from .detect_sites import detect_registry_sources_from_loaded_inputs
from .intake import LoadedDiscoveryInput
from .models import (
    CompanyHarvestArtifactPaths,
    CompanyHarvestPageResult,
    SourceRegistryHarvestCompaniesResult,
)

DEFAULT_COMPANY_HARVEST_TIMEOUT_SECONDS = 10.0
DEFAULT_COMPANY_HARVEST_MAX_REQUESTS_PER_SECOND = 2.0

_HTML_ACCEPT_HEADER = "text/html,application/xhtml+xml"
_LABEL_RE = re.compile(r"[^A-Za-z0-9._-]+")
_ABSOLUTE_URL_RE = re.compile(r"https?://[^\s\"'<>]+", re.IGNORECASE)
_PROTOCOL_RELATIVE_URL_RE = re.compile(r"(?<![:\w])//[^\s\"'<>]+")
_MULTI_PART_PUBLIC_SUFFIXES = frozenset(
    {
        "co.in",
        "co.jp",
        "co.uk",
        "com.au",
        "com.br",
        "com.hk",
        "com.mx",
        "com.sg",
    }
)
_SKIPPED_URL_SUFFIXES = (
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
_BLOCKED_ROOT_DOMAINS = frozenset(
    {
        "angel.co",
        "angellist.com",
        "crunchbase.com",
        "facebook.com",
        "github.com",
        "glassdoor.com",
        "instagram.com",
        "linkedin.com",
        "medium.com",
        "producthunt.com",
        "reddit.com",
        "substack.com",
        "twitter.com",
        "x.com",
        "youtube.com",
        "youtu.be",
    }
)
_BLOCKED_HOST_SUFFIXES = (
    ".myworkdayjobs.com",
    ".pages.dev",
)
_BLOCKED_EXACT_HOSTS = frozenset(
    {
        "api.lever.co",
        "boards.greenhouse.io",
        "boards.eu.greenhouse.io",
        "careers.jobvite.com",
        "companysites.workable.com",
        "jobs.ashbyhq.com",
        "jobs.gem.com",
        "jobs.lever.co",
        "jobs.smartrecruiters.com",
        "jobs.workable.com",
    }
)


@dataclass(frozen=True, slots=True)
class _DirectorySourceSpec:
    directory_urls: tuple[str, ...]
    aliases: tuple[str, ...] = ()


_DIRECTORY_SOURCE_SPECS = {
    "startup-list": _DirectorySourceSpec(
        directory_urls=("https://openstartuplist.com/",),
    ),
    "ai-startups": _DirectorySourceSpec(
        directory_urls=(
            "https://github.com/Yuan-ManX/AI-Startups",
            "https://github.com/athivvat/ai-startups",
        ),
        aliases=("ai-companies",),
    ),
    "remote-companies": _DirectorySourceSpec(
        directory_urls=(
            "https://github.com/yanirs/established-remote",
            "https://buildremote.co/companies/",
        ),
    ),
}

_SOURCE_ALIAS_MAP = {
    alias: name
    for name, spec in _DIRECTORY_SOURCE_SPECS.items()
    for alias in spec.aliases
}


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


class _AnchorExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.hrefs: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() != "a":
            return
        for key, value in attrs:
            if key.lower() == "href" and value is not None:
                self.hrefs.append(value)
                return


def available_company_harvest_sources() -> tuple[str, ...]:
    return tuple(_DIRECTORY_SOURCE_SPECS.keys())


def company_harvest_sources_help_text() -> str:
    available = ", ".join(available_company_harvest_sources())
    return (
        f"Curated public directory source bundle. Repeat to combine bundles. "
        f"Allowed values: {available}, all. Alias: ai-companies -> ai-startups."
    )


def harvest_companies_from_sources(
    paths: WorkspacePaths,
    *,
    sources: Sequence[str],
    out_dir: Path | None,
    label: str | None,
    timeout_seconds: float = DEFAULT_COMPANY_HARVEST_TIMEOUT_SECONDS,
    max_requests_per_second: float = DEFAULT_COMPANY_HARVEST_MAX_REQUESTS_PER_SECOND,
    created_at: datetime | None = None,
    fetcher: Fetcher | None = None,
) -> SourceRegistryHarvestCompaniesResult:
    if timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be greater than 0")
    if max_requests_per_second <= 0:
        raise ValueError("max_requests_per_second must be greater than 0")

    resolved_sources = resolve_company_harvest_sources(tuple(sources))
    if not resolved_sources:
        raise ValueError("at least one --source value is required")

    created_at_dt = _normalize_created_at(created_at)
    normalized_label = _normalize_label(label)
    run_id = _build_run_id(normalized_label, created_at_dt)
    output_dir = _resolve_output_dir(paths, out_dir=out_dir, run_id=run_id)

    rate_limiter = _RateLimiter(max_requests_per_second)
    effective_fetcher = _build_rate_limited_fetcher(
        fetch_text if fetcher is None else fetcher,
        rate_limiter=rate_limiter,
    )
    directory_hosts, directory_root_domains = _directory_source_domain_filters(resolved_sources)

    harvested_domain_map: OrderedDict[str, str] = OrderedDict()
    page_results: list[CompanyHarvestPageResult] = []
    for source_name in resolved_sources:
        spec = _DIRECTORY_SOURCE_SPECS[source_name]
        for directory_url in spec.directory_urls:
            page_result = _harvest_directory_page(
                source_name=source_name,
                directory_url=directory_url,
                timeout_seconds=timeout_seconds,
                fetcher=effective_fetcher,
                directory_hosts=directory_hosts,
                directory_root_domains=directory_root_domains,
            )
            page_results.append(page_result)
            for domain in page_result.harvested_domains:
                harvested_domain_map.setdefault(
                    domain,
                    f'sources harvest-companies source "{source_name}" directory "{directory_url}"',
                )

    if not harvested_domain_map:
        raise ValueError(
            "no company domains were discovered from the selected directory pages"
        )

    loaded_inputs = _build_loaded_inputs(harvested_domain_map)
    detect_sites_result = detect_registry_sources_from_loaded_inputs(
        paths,
        loaded_inputs=loaded_inputs,
        starter_lists=(),
        timeout_seconds=timeout_seconds,
        use_structured_clues=True,
        created_at=created_at_dt,
        fetcher=effective_fetcher,
    )

    finished_at_dt = created_at_dt if created_at is not None else _current_utc_datetime()
    finished_at = _format_created_at(finished_at_dt)
    artifact_paths = _write_company_harvest_artifacts(
        output_dir,
        run_id=run_id,
        source_names=tuple(resolved_sources),
        page_results=tuple(page_results),
        harvested_domains=tuple(harvested_domain_map.keys()),
        detect_sites_result=detect_sites_result,
        timeout_seconds=timeout_seconds,
        max_requests_per_second=max_requests_per_second,
        created_at=_format_created_at(created_at_dt),
        finished_at=finished_at,
    )

    return SourceRegistryHarvestCompaniesResult(
        source_names=tuple(resolved_sources),
        page_results=tuple(page_results),
        harvested_domains=tuple(harvested_domain_map.keys()),
        detect_sites_result=detect_sites_result,
        timeout_seconds=timeout_seconds,
        max_requests_per_second=max_requests_per_second,
        created_at=_format_created_at(created_at_dt),
        finished_at=finished_at,
        run_id=run_id,
        artifact_paths=artifact_paths,
    )


def resolve_company_harvest_sources(requested_sources: Sequence[str]) -> tuple[str, ...]:
    normalized_sources = tuple(
        dict.fromkeys(
            _normalize_source_name(source_name)
            for source_name in requested_sources
            if _normalize_source_name(source_name) is not None
        )
    )
    if not normalized_sources:
        return ()

    resolved_sources: list[str] = []
    for source_name in normalized_sources:
        assert source_name is not None
        if source_name == "all":
            resolved_sources.extend(available_company_harvest_sources())
            continue
        canonical_name = _SOURCE_ALIAS_MAP.get(source_name, source_name)
        if canonical_name not in _DIRECTORY_SOURCE_SPECS:
            allowed = ", ".join((*available_company_harvest_sources(), "all", *sorted(_SOURCE_ALIAS_MAP)))
            raise ValueError(
                f"unknown company directory source {source_name!r}. Allowed values: {allowed}"
            )
        resolved_sources.append(canonical_name)
    return tuple(dict.fromkeys(resolved_sources))


def _build_loaded_inputs(domain_provenance: OrderedDict[str, str]) -> tuple[LoadedDiscoveryInput, ...]:
    loaded_inputs: list[LoadedDiscoveryInput] = []
    for index, (domain, provenance) in enumerate(domain_provenance.items(), start=1):
        company_input = parse_company_input_line(index, domain)
        if company_input is None:
            continue
        loaded_inputs.append(
            LoadedDiscoveryInput(
                company_input=company_input,
                provenance=provenance,
            )
        )
    return tuple(loaded_inputs)


def _harvest_directory_page(
    *,
    source_name: str,
    directory_url: str,
    timeout_seconds: float,
    fetcher: Fetcher,
    directory_hosts: frozenset[str],
    directory_root_domains: frozenset[str],
) -> CompanyHarvestPageResult:
    try:
        response = fetcher(
            FetchRequest(
                url=directory_url,
                timeout_seconds=timeout_seconds,
                headers={"Accept": _HTML_ACCEPT_HEADER},
            )
        )
    except Exception as exc:
        return CompanyHarvestPageResult(
            source_name=source_name,
            directory_url=directory_url,
            candidate_url_count=0,
            harvested_domains=(),
            error=str(exc),
        )

    harvested_domains = _extract_company_domains(
        response,
        directory_hosts=directory_hosts,
        directory_root_domains=directory_root_domains,
    )
    candidate_urls = _extract_candidate_urls(response)
    return CompanyHarvestPageResult(
        source_name=source_name,
        directory_url=directory_url,
        candidate_url_count=len(candidate_urls),
        harvested_domains=harvested_domains,
    )


def _extract_company_domains(
    response: FetchResponse,
    *,
    directory_hosts: frozenset[str],
    directory_root_domains: frozenset[str],
) -> tuple[str, ...]:
    harvested_domains: OrderedDict[str, None] = OrderedDict()
    for candidate_url in _extract_candidate_urls(response):
        normalized_domain = _normalize_company_domain(
            candidate_url,
            directory_hosts=directory_hosts,
            directory_root_domains=directory_root_domains,
        )
        if normalized_domain is None:
            continue
        harvested_domains.setdefault(normalized_domain, None)
    return tuple(harvested_domains.keys())


def _extract_candidate_urls(response: FetchResponse) -> tuple[str, ...]:
    base_url = response.final_url or response.url
    parser = _AnchorExtractor()
    parser.feed(response.text)
    parser.close()

    discovered_urls: OrderedDict[str, None] = OrderedDict()
    for href in parser.hrefs:
        absolute_url = _absolutize_url(href, base_url=base_url)
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


def _normalize_company_domain(
    value: str,
    *,
    directory_hosts: frozenset[str],
    directory_root_domains: frozenset[str],
) -> str | None:
    parsed = urlparse(value)
    if parsed.scheme.lower() not in {"http", "https"} or not parsed.netloc:
        return None

    host = parsed.hostname.lower().strip(".") if parsed.hostname else ""
    if not host or host in directory_hosts:
        return None
    if parsed.path.lower().endswith(_SKIPPED_URL_SUFFIXES):
        return None
    if host in _BLOCKED_EXACT_HOSTS or any(host.endswith(suffix) for suffix in _BLOCKED_HOST_SUFFIXES):
        return None

    root_domain = _registrable_domain(host)
    if root_domain is None:
        return None
    if root_domain in directory_root_domains or root_domain in _BLOCKED_ROOT_DOMAINS:
        return None
    return root_domain


def _directory_source_domain_filters(
    source_names: Sequence[str],
) -> tuple[frozenset[str], frozenset[str]]:
    hosts: set[str] = set()
    root_domains: set[str] = set()
    for source_name in source_names:
        for directory_url in _DIRECTORY_SOURCE_SPECS[source_name].directory_urls:
            parsed = urlparse(directory_url)
            host = parsed.hostname.lower().strip(".") if parsed.hostname else ""
            if not host:
                continue
            hosts.add(host)
            root_domain = _registrable_domain(host)
            if root_domain is not None:
                root_domains.add(root_domain)
    return frozenset(hosts), frozenset(root_domains)


def _registrable_domain(host: str) -> str | None:
    normalized_host = host.lower().strip(".")
    if not normalized_host or "." not in normalized_host:
        return None

    labels = [label for label in normalized_host.split(".") if label]
    if len(labels) < 2:
        return None
    if len(labels) >= 3 and ".".join(labels[-2:]) in _MULTI_PART_PUBLIC_SUFFIXES:
        return ".".join(labels[-3:])
    return ".".join(labels[-2:])


def _build_rate_limited_fetcher(fetcher: Fetcher, *, rate_limiter: _RateLimiter) -> Fetcher:
    def wrapped(request: FetchRequest) -> FetchResponse:
        rate_limiter.acquire()
        return fetcher(request)

    return wrapped


def _write_company_harvest_artifacts(
    output_dir: Path,
    *,
    run_id: str,
    source_names: tuple[str, ...],
    page_results: tuple[CompanyHarvestPageResult, ...],
    harvested_domains: tuple[str, ...],
    detect_sites_result,
    timeout_seconds: float,
    max_requests_per_second: float,
    created_at: str,
    finished_at: str,
) -> CompanyHarvestArtifactPaths:
    output_dir.mkdir(parents=True, exist_ok=True)
    harvested_domains_path = output_dir / "harvested_domains.txt"
    harvest_report_path = output_dir / "harvest_report.json"

    harvested_domains_path.write_text(
        "".join(f"{domain}\n" for domain in harvested_domains),
        encoding="utf-8",
    )
    harvest_report_path.write_text(
        json.dumps(
            {
                "run_id": run_id,
                "source_names": list(source_names),
                "created_at": created_at,
                "finished_at": finished_at,
                "timeout_seconds": timeout_seconds,
                "max_requests_per_second": max_requests_per_second,
                "harvested_domain_count": len(harvested_domains),
                "harvested_domains_path": str(harvested_domains_path),
                "harvested_domains": list(harvested_domains),
                "directory_pages": [
                    {
                        "source_name": page_result.source_name,
                        "directory_url": page_result.directory_url,
                        "candidate_url_count": page_result.candidate_url_count,
                        "harvested_domain_count": page_result.harvested_domain_count,
                        "harvested_domains": list(page_result.harvested_domains),
                        "error": page_result.error,
                    }
                    for page_result in page_results
                ],
                "detect_sites": {
                    "input_count": detect_sites_result.input_count,
                    "confirmed_count": detect_sites_result.confirmed_count,
                    "manual_review_count": detect_sites_result.manual_review_count,
                    "failed_count": detect_sites_result.failed_count,
                    "created_count": detect_sites_result.created_count,
                    "updated_count": detect_sites_result.updated_count,
                    "unchanged_count": detect_sites_result.unchanged_count,
                },
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    return CompanyHarvestArtifactPaths(
        output_dir=output_dir,
        harvested_domains_path=harvested_domains_path,
        harvest_report_path=harvest_report_path,
    )


def _normalize_source_name(value: str | None) -> str | None:
    if value is None:
        return None
    normalized_value = value.strip().lower()
    return normalized_value or None


def _resolve_output_dir(
    paths: WorkspacePaths,
    *,
    out_dir: Path | None,
    run_id: str,
) -> Path:
    if out_dir is not None:
        if out_dir.is_absolute():
            return out_dir
        return (paths.project_root / out_dir).resolve()
    return paths.processed_dir / run_id


def _normalize_label(label: str | None) -> str | None:
    if label is None:
        return None
    normalized_label = _LABEL_RE.sub("-", label.strip()).strip("-.")
    if not normalized_label:
        raise ValueError("label must contain at least one letter or number")
    return normalized_label


def _normalize_created_at(created_at: datetime | None) -> datetime:
    if created_at is None:
        return _current_utc_datetime()
    if created_at.tzinfo is None:
        return created_at.replace(tzinfo=timezone.utc)
    return created_at.astimezone(timezone.utc)


def _current_utc_datetime() -> datetime:
    return datetime.now(timezone.utc)


def _build_run_id(label: str | None, created_at: datetime) -> str:
    stamp = created_at.strftime("%Y%m%dT%H%M%S%fZ")
    if label is None:
        return f"harvest-companies-{stamp}"
    return f"harvest-companies-{label}-{stamp}"


def _format_created_at(created_at: datetime) -> str:
    return created_at.isoformat(timespec="seconds").replace("+00:00", "Z")


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
