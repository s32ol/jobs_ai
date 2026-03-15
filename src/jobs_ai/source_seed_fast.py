from __future__ import annotations

import argparse
import json
import threading
from collections import OrderedDict
from collections.abc import Iterable, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path
import re
from urllib.parse import urljoin, urlparse, urlunparse

from .collect.adapters.base import detect_blocked_patterns, detect_generic_page_patterns, extract_title_text
from .collect.fetch import FetchError, FetchRequest, FetchResponse, Fetcher, fetch_text
from .collect.harness import run_collection
from .workspace import build_workspace_paths, ensure_workspace

DEFAULT_TIMEOUT_SECONDS = 6.0
DEFAULT_MAX_WORKERS = 8
DEFAULT_DATABASE_PATH = Path("data/jobs_ai.db")

_HTML_MARKER_RE = re.compile(r"<!doctype\s+html|<html\b|<head\b|<body\b|<meta\b|<script\b", re.IGNORECASE)
_TOKEN_RE = re.compile(r"[a-z0-9]+")
_SLUG_SUFFIX_TOKENS = frozenset(
    {
        "ai",
        "ag",
        "bv",
        "co",
        "company",
        "corp",
        "corporation",
        "gmbh",
        "inc",
        "incorporated",
        "io",
        "labs",
        "llc",
        "llp",
        "lp",
        "limited",
        "ltd",
        "oy",
        "plc",
        "pte",
        "pty",
        "sa",
        "systems",
        "tech",
    }
)
_MATCH_SUFFIX_TOKENS = frozenset(
    {
        "ag",
        "bv",
        "co",
        "company",
        "corp",
        "corporation",
        "gmbh",
        "inc",
        "incorporated",
        "llc",
        "llp",
        "lp",
        "limited",
        "ltd",
        "oy",
        "plc",
        "pte",
        "pty",
        "sa",
    }
)
_LEADING_ARTICLE_TOKENS = frozenset({"a", "an", "the"})
_COMMON_MULTI_LABEL_SUFFIXES = frozenset(
    {
        "ac.uk",
        "co.in",
        "co.jp",
        "co.nz",
        "co.uk",
        "com.au",
        "com.br",
        "com.hk",
        "com.mx",
        "com.sg",
        "gov.uk",
        "net.au",
        "org.au",
        "org.uk",
    }
)
_NON_COMPANY_HOSTS = frozenset(
    {
        "angel.co",
        "bit.ly",
        "crunchbase.com",
        "discord.com",
        "docs.google.com",
        "drive.google.com",
        "facebook.com",
        "github.com",
        "instagram.com",
        "linkedin.com",
        "linktr.ee",
        "medium.com",
        "substack.com",
        "t.co",
        "twitter.com",
        "x.com",
        "youtube.com",
    }
)
_ATS_HOSTS = frozenset(
    {
        "ashbyhq.com",
        "boards.greenhouse.io",
        "greenhouse.io",
        "job-boards.greenhouse.io",
        "jobs.ashbyhq.com",
        "jobs.lever.co",
        "lever.co",
    }
)
_SKIPPED_FILE_EXTENSIONS = frozenset(
    {
        ".csv",
        ".doc",
        ".docx",
        ".gif",
        ".jpeg",
        ".jpg",
        ".pdf",
        ".png",
        ".ppt",
        ".pptx",
        ".svg",
        ".txt",
        ".webp",
        ".xls",
        ".xlsx",
        ".zip",
    }
)
_GENERIC_LINK_TEXTS = frozenset(
    {
        "apply",
        "apply now",
        "company",
        "company profile",
        "details",
        "learn more",
        "more",
        "open roles",
        "portfolio",
        "profile",
        "read more",
        "site",
        "view company",
        "visit site",
        "visit website",
        "website",
    }
)
_PORTAL_PROBE_TEMPLATES: tuple[tuple[str, str], ...] = (
    ("greenhouse", "https://boards.greenhouse.io/{slug}"),
    ("greenhouse", "https://job-boards.greenhouse.io/{slug}"),
    ("lever", "https://jobs.lever.co/{slug}"),
    ("ashby", "https://jobs.ashbyhq.com/{slug}"),
)


@dataclass(frozen=True, slots=True)
class CandidateUrl:
    portal_type: str
    slug: str
    url: str


@dataclass(frozen=True, slots=True)
class ExtractedCompany:
    company_domain: str
    company_name: str | None
    seed_pages: tuple[str, ...]
    evidence_links: tuple[dict[str, str | None], ...]


@dataclass(frozen=True, slots=True)
class SeedPageResult:
    seed_page: str
    outcome: str
    reason_code: str
    reason: str
    final_url: str | None = None
    content_type: str | None = None
    page_title: str | None = None
    extracted_company_domains: tuple[str, ...] = ()
    evidence_links: tuple[dict[str, str | None], ...] = ()


@dataclass(frozen=True, slots=True)
class PortalProbeResult:
    candidate_url: str
    portal_type: str
    slug: str
    collector_outcome: str
    reason_code: str
    reason: str
    confirmed_root: str | None
    final_url: str | None
    content_type: str | None
    page_title: str | None
    detected_patterns: tuple[str, ...]
    board_companies: tuple[str, ...]
    lead_count: int
    error: str | None = None


@dataclass(frozen=True, slots=True)
class CompanyResult:
    seed_page: str | None
    company_name: str | None
    company_domain: str
    outcome: str
    reason_code: str
    reason: str
    suggested_next_action: str | None
    confirmed_root: str | None
    attempted_candidates: tuple[dict[str, object], ...]
    evidence: dict[str, object]


@dataclass(frozen=True, slots=True)
class SeedBuildResult:
    run_id: str
    created_at: str
    finished_at: str
    output_dir: Path
    ats_roots_path: Path
    manual_review_path: Path
    seed_report_path: Path
    confirmed_roots: tuple[str, ...]
    manual_review_items: tuple[dict[str, object], ...]
    company_results: tuple[CompanyResult, ...]
    seed_page_results: tuple[SeedPageResult, ...]


class _AnchorExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self._href: str | None = None
        self._text_parts: list[str] = []
        self.links: list[tuple[str, str]] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() != "a":
            return
        attr_map = {key.lower(): value for key, value in attrs}
        href = attr_map.get("href")
        if href is None:
            return
        self._href = href
        self._text_parts = []

    def handle_data(self, data: str) -> None:
        if self._href is None:
            return
        self._text_parts.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() != "a" or self._href is None:
            return
        text = " ".join(part.strip() for part in self._text_parts if part.strip()).strip()
        self.links.append((self._href, text))
        self._href = None
        self._text_parts = []


class FetchMemoizer:
    def __init__(self, delegate: Fetcher) -> None:
        self._delegate = delegate
        self._lock = threading.Lock()
        self._entries: dict[str, FetchResponse | Exception] = {}

    def fetch(self, request: FetchRequest) -> FetchResponse:
        with self._lock:
            cached = self._entries.get(request.url)
        if cached is not None:
            if isinstance(cached, Exception):
                raise cached
            return cached

        try:
            response = self._delegate(request)
        except Exception as exc:
            with self._lock:
                self._entries[request.url] = exc
            raise

        with self._lock:
            self._entries[request.url] = response
        return response

    def peek(self, url: str) -> FetchResponse | Exception | None:
        with self._lock:
            return self._entries.get(url)


def normalize_company_domain(value: str) -> str | None:
    if not value:
        return None
    parsed = _parse_httpish_value(value)
    host = parsed.hostname if parsed is not None else None
    if host is None:
        return None
    normalized_host = host.strip().lower().strip(".")
    if not normalized_host or "." not in normalized_host:
        return None
    if re.fullmatch(r"\d+\.\d+\.\d+\.\d+", normalized_host):
        return None
    if normalized_host.startswith("www.") and normalized_host.count(".") >= 2:
        normalized_host = normalized_host[4:]
    return _registrable_domain(normalized_host)


def infer_slug_candidates(company_domain: str, company_name: str | None = None) -> tuple[str, ...]:
    ordered: list[str] = []
    seen: set[str] = set()

    for token_group in (
        _domain_stem_tokens(company_domain, strip_suffixes=False),
        _domain_stem_tokens(company_domain, strip_suffixes=True),
        _normalized_name_tokens(company_name, strip_suffixes=True),
        _normalized_name_tokens(company_name, strip_suffixes=False),
    ):
        if not token_group:
            continue
        for candidate in (_join_tokens(token_group, "-"), _join_tokens(token_group, "")):
            if candidate is None or len(candidate) < 2 or candidate in seen:
                continue
            seen.add(candidate)
            ordered.append(candidate)
            if len(ordered) >= 6:
                return tuple(ordered)
    return tuple(ordered)


def build_ats_candidate_urls(slugs: Sequence[str]) -> tuple[CandidateUrl, ...]:
    ordered: list[CandidateUrl] = []
    seen: set[str] = set()
    for slug in slugs:
        normalized_slug = slug.strip().lower()
        if not normalized_slug:
            continue
        for portal_type, template in _PORTAL_PROBE_TEMPLATES:
            url = template.format(slug=normalized_slug)
            if url in seen:
                continue
            seen.add(url)
            ordered.append(CandidateUrl(portal_type=portal_type, slug=normalized_slug, url=url))
    return tuple(ordered)


def classify_company_result(
    company_domain: str,
    company_name: str | None,
    attempted_candidates: Sequence[dict[str, object]],
) -> tuple[str, str, str, str | None, str | None]:
    confirmed = next((item for item in attempted_candidates if item["outcome"] == "confirmed"), None)
    if confirmed is not None:
        return (
            "confirmed",
            "confirmed_board_root",
            f"confirmed {confirmed['portal_type']} board root",
            confirmed["confirmed_root"],
            None,
        )

    manual_review_candidates = [item for item in attempted_candidates if item["outcome"] == "manual_review"]
    if manual_review_candidates:
        best = manual_review_candidates[0]
        return (
            "manual_review",
            str(best["reason_code"]),
            str(best["reason"]),
            None,
            (
                "Open the company site or the most plausible ATS candidate, confirm the real board root, "
                "then append it to ats_roots.txt before rerunning collect."
            ),
        )

    if not attempted_candidates:
        return (
            "skipped",
            "no_candidate_urls",
            f"no ATS slug candidates were generated for {company_domain}",
            None,
            "Review the company name/domain manually and add a supported Greenhouse, Lever, or Ashby root if you find one.",
        )

    return (
        "skipped",
        "no_confirmed_candidates",
        (
            f"no supported ATS board root was confirmed from {len(attempted_candidates)} deterministic candidate URL(s) "
            f"for {company_name or company_domain}"
        ),
        None,
        "Skip for now or confirm the company's Greenhouse, Lever, or Ashby slug manually before collecting.",
    )


def build_seed_list(
    seed_pages_file: Path,
    *,
    domains_file: Path | None = None,
    out_dir: Path | None = None,
    label: str | None = None,
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
    max_workers: int = DEFAULT_MAX_WORKERS,
    created_at: datetime | None = None,
    fetcher: Fetcher = fetch_text,
) -> SeedBuildResult:
    if not seed_pages_file.exists():
        raise ValueError(f"seed pages file does not exist: {seed_pages_file}")
    if domains_file is not None and not domains_file.exists():
        raise ValueError(f"domains file does not exist: {domains_file}")

    created_at_dt = _normalize_created_at(created_at)
    run_id = _build_run_id(label=label, created_at=created_at_dt)
    workspace = build_workspace_paths(DEFAULT_DATABASE_PATH)
    ensure_workspace(workspace)
    output_dir = _resolve_output_dir(workspace.processed_dir, out_dir=out_dir, run_id=run_id)
    output_dir.mkdir(parents=True, exist_ok=True)

    seed_pages = _load_lines(seed_pages_file)
    if not seed_pages:
        raise ValueError("seed pages file did not contain any usable URLs")

    direct_domain_values = _load_lines(domains_file) if domains_file is not None else ()
    memoizer = FetchMemoizer(fetcher)

    seed_page_results: list[SeedPageResult] = []
    extracted_company_map: OrderedDict[str, dict[str, object]] = OrderedDict()
    immediate_company_results: list[CompanyResult] = []

    for seed_page in seed_pages:
        result, extracted_companies = _process_seed_page(
            seed_page,
            timeout_seconds=timeout_seconds,
            fetcher=memoizer.fetch,
        )
        seed_page_results.append(result)
        for company in extracted_companies:
            _merge_extracted_company(extracted_company_map, company)

    for direct_value in direct_domain_values:
        normalized_domain = normalize_company_domain(direct_value)
        if normalized_domain is None:
            immediate_company_results.append(
                CompanyResult(
                    seed_page=None,
                    company_name=None,
                    company_domain=direct_value,
                    outcome="skipped",
                    reason_code="invalid_company_domain",
                    reason=f"could not normalize company domain from {direct_value!r}",
                    suggested_next_action="Fix the domain input and rerun the seed builder.",
                    confirmed_root=None,
                    attempted_candidates=(),
                    evidence={"source": "domains_file", "raw_value": direct_value},
                )
            )
            continue
        _merge_extracted_company(
            extracted_company_map,
            ExtractedCompany(
                company_domain=normalized_domain,
                company_name=None,
                seed_pages=(),
                evidence_links=({"source": "domains_file", "raw_value": direct_value},),
            ),
        )

    extracted_companies = tuple(
        _materialize_extracted_company(domain, payload)
        for domain, payload in sorted(extracted_company_map.items())
    )
    probes = _probe_candidate_urls(
        extracted_companies,
        timeout_seconds=timeout_seconds,
        max_workers=max_workers,
        created_at=created_at_dt,
        memoizer=memoizer,
    )

    company_results = immediate_company_results + [
        _build_company_result(company, probes)
        for company in extracted_companies
    ]
    company_results_sorted = tuple(sorted(company_results, key=_company_result_sort_key))

    confirmed_roots = tuple(
        sorted(
            {
                result.confirmed_root
                for result in company_results_sorted
                if result.confirmed_root is not None
            }
        )
    )
    manual_review_items = tuple(
        _manual_review_payload(result)
        for result in company_results_sorted
        if result.outcome == "manual_review"
    )

    finished_at = _format_timestamp(datetime.now(timezone.utc) if created_at is None else created_at_dt)
    ats_roots_path = output_dir / "ats_roots.txt"
    manual_review_path = output_dir / "manual_review_sources.json"
    seed_report_path = output_dir / "seed_report.json"

    _write_text_lines(ats_roots_path, confirmed_roots)
    _write_json(manual_review_path, list(manual_review_items))
    _write_json(
        seed_report_path,
        _seed_report_payload(
            run_id=run_id,
            created_at=_format_timestamp(created_at_dt),
            finished_at=finished_at,
            seed_pages=seed_pages,
            direct_domains=direct_domain_values,
            seed_page_results=seed_page_results,
            company_results=company_results_sorted,
            ats_roots_path=ats_roots_path,
            manual_review_path=manual_review_path,
            seed_report_path=seed_report_path,
        ),
    )

    return SeedBuildResult(
        run_id=run_id,
        created_at=_format_timestamp(created_at_dt),
        finished_at=finished_at,
        output_dir=output_dir,
        ats_roots_path=ats_roots_path,
        manual_review_path=manual_review_path,
        seed_report_path=seed_report_path,
        confirmed_roots=confirmed_roots,
        manual_review_items=manual_review_items,
        company_results=company_results_sorted,
        seed_page_results=tuple(seed_page_results),
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Build a one-off list of confirmed Greenhouse, Lever, and Ashby board roots from seed pages "
            "and optional company domains."
        )
    )
    parser.add_argument("seed_pages_file", type=Path, help="Text file of public directory or portfolio URLs.")
    parser.add_argument(
        "--domains-file",
        type=Path,
        default=None,
        help="Optional text file of direct company domains or company URLs.",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=None,
        help="Optional output directory. Defaults to data/processed/seed-<timestamp>.",
    )
    parser.add_argument(
        "--label",
        default=None,
        help="Optional short label to include in the output run id.",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=DEFAULT_TIMEOUT_SECONDS,
        help="Per-request timeout in seconds. Default: 6.0",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=DEFAULT_MAX_WORKERS,
        help="Parallel ATS probe workers. Default: 8",
    )
    args = parser.parse_args(argv)

    try:
        result = build_seed_list(
            args.seed_pages_file,
            domains_file=args.domains_file,
            out_dir=args.out_dir,
            label=args.label,
            timeout_seconds=args.timeout,
            max_workers=max(args.max_workers, 1),
        )
    except ValueError as exc:
        parser.error(str(exc))

    confirmed_count = sum(1 for item in result.company_results if item.outcome == "confirmed")
    manual_review_count = sum(1 for item in result.company_results if item.outcome == "manual_review")
    skipped_count = sum(1 for item in result.company_results if item.outcome == "skipped")
    print(f"run_id: {result.run_id}")
    print(f"confirmed: {confirmed_count}")
    print(f"manual_review: {manual_review_count}")
    print(f"skipped: {skipped_count}")
    print(f"ats_roots.txt: {result.ats_roots_path}")
    print(f"manual_review_sources.json: {result.manual_review_path}")
    print(f"seed_report.json: {result.seed_report_path}")
    return 0


def _process_seed_page(
    seed_page: str,
    *,
    timeout_seconds: float,
    fetcher: Fetcher,
) -> tuple[SeedPageResult, tuple[ExtractedCompany, ...]]:
    try:
        response = fetcher(FetchRequest(url=seed_page, timeout_seconds=timeout_seconds))
    except FetchError as exc:
        return (
            SeedPageResult(
                seed_page=seed_page,
                outcome="skipped",
                reason_code="fetch_failed",
                reason=str(exc),
            ),
            (),
        )

    outcome, reason_code, reason = _classify_html_response(response)
    final_url = _normalize_url(response.final_url)
    content_type = _normalize_text(response.content_type)
    page_title = extract_title_text(response.text)
    if outcome != "extracted":
        return (
            SeedPageResult(
                seed_page=seed_page,
                outcome=outcome,
                reason_code=reason_code,
                reason=reason,
                final_url=final_url,
                content_type=content_type,
                page_title=page_title,
            ),
            (),
        )

    extracted_links = _extract_company_links(
        response.text,
        base_url=final_url or seed_page,
        seed_page=seed_page,
    )
    company_map: OrderedDict[str, dict[str, object]] = OrderedDict()
    for link in extracted_links:
        domain = str(link["company_domain"])
        payload = company_map.setdefault(
            domain,
            {"company_name": None, "seed_pages": OrderedDict(), "evidence_links": []},
        )
        company_name = payload["company_name"]
        if company_name is None and link.get("company_name") is not None:
            payload["company_name"] = link["company_name"]
        payload["seed_pages"][seed_page] = True
        payload["evidence_links"].append(link)

    extracted_companies = tuple(
        ExtractedCompany(
            company_domain=domain,
            company_name=_normalize_text(str(payload["company_name"])) if payload["company_name"] is not None else None,
            seed_pages=tuple(payload["seed_pages"].keys()),
            evidence_links=tuple(payload["evidence_links"]),
        )
        for domain, payload in company_map.items()
    )
    result = SeedPageResult(
        seed_page=seed_page,
        outcome="extracted",
        reason_code="seed_page_processed",
        reason=f"extracted {len(extracted_companies)} company domain(s)",
        final_url=final_url,
        content_type=content_type,
        page_title=page_title,
        extracted_company_domains=tuple(sorted(company_map.keys())),
        evidence_links=tuple(
            dict(link)
            for domain in sorted(company_map.keys())
            for link in company_map[domain]["evidence_links"]
        ),
    )
    return result, extracted_companies


def _classify_html_response(response: FetchResponse) -> tuple[str, str, str]:
    body = response.text or ""
    if not body.strip():
        return ("skipped", "empty_response_body", "empty response body returned")
    blocked_patterns = detect_blocked_patterns(body)
    if blocked_patterns:
        return (
            "skipped",
            "blocked_or_access_denied",
            f"page appears blocked or access denied: {', '.join(blocked_patterns)}",
        )
    if response.status_code >= 400:
        return ("skipped", "http_error_status", f"HTTP {response.status_code} returned while fetching seed page")
    if not _response_looks_html(response):
        content_type = _normalize_text(response.content_type) or "<unknown>"
        return ("skipped", "non_html_content", f"non-HTML content-type returned: {content_type}")
    return ("extracted", "seed_page_processed", "seed page fetched successfully")


def _response_looks_html(response: FetchResponse) -> bool:
    content_type = (response.content_type or "").lower()
    if "html" in content_type or "xhtml" in content_type:
        return True
    return bool(_HTML_MARKER_RE.search(response.text[:2000]))


def _extract_company_links(
    html_text: str,
    *,
    base_url: str,
    seed_page: str,
) -> tuple[dict[str, str | None], ...]:
    parser = _AnchorExtractor()
    parser.feed(html_text)
    seed_domain = normalize_company_domain(base_url)
    links: list[dict[str, str | None]] = []
    for href, raw_text in parser.links:
        absolute_url = _normalize_url(urljoin(base_url, href))
        if absolute_url is None:
            continue
        parsed = urlparse(absolute_url)
        if parsed.scheme.lower() not in {"http", "https"} or not parsed.hostname:
            continue
        if _should_skip_link(parsed, seed_domain=seed_domain):
            continue
        company_domain = normalize_company_domain(parsed.hostname)
        if company_domain is None:
            continue
        company_name = _clean_company_name(raw_text)
        links.append(
            {
                "seed_page": seed_page,
                "company_domain": company_domain,
                "company_name": company_name,
                "href": absolute_url,
                "anchor_text": _normalize_text(raw_text),
                "source": "seed_page",
            }
        )
    return tuple(links)


def _should_skip_link(parsed, *, seed_domain: str | None) -> bool:
    host = parsed.hostname.lower()
    normalized_host = host[4:] if host.startswith("www.") else host
    company_domain = normalize_company_domain(normalized_host)
    if company_domain is None:
        return True
    if seed_domain is not None and company_domain == seed_domain:
        return True
    if normalized_host in _ATS_HOSTS or company_domain in _ATS_HOSTS:
        return True
    if normalized_host in _NON_COMPANY_HOSTS or company_domain in _NON_COMPANY_HOSTS:
        return True
    if parsed.path:
        lower_path = parsed.path.lower()
        if any(lower_path.endswith(extension) for extension in _SKIPPED_FILE_EXTENSIONS):
            return True
    return False


def _clean_company_name(value: str | None) -> str | None:
    text = _normalize_text(value)
    if text is None:
        return None
    if text.lower() in _GENERIC_LINK_TEXTS:
        return None
    if len(text) > 80:
        return None
    return text


def _merge_extracted_company(
    company_map: OrderedDict[str, dict[str, object]],
    company: ExtractedCompany,
) -> None:
    payload = company_map.setdefault(
        company.company_domain,
        {"company_names": OrderedDict(), "seed_pages": OrderedDict(), "evidence_links": []},
    )
    if company.company_name is not None:
        payload["company_names"][company.company_name] = True
    for seed_page in company.seed_pages:
        payload["seed_pages"][seed_page] = True
    payload["evidence_links"].extend(company.evidence_links)


def _materialize_extracted_company(domain: str, payload: dict[str, object]) -> ExtractedCompany:
    company_names = list(payload["company_names"].keys())
    company_name = company_names[0] if company_names else None
    return ExtractedCompany(
        company_domain=domain,
        company_name=company_name,
        seed_pages=tuple(payload["seed_pages"].keys()),
        evidence_links=tuple(payload["evidence_links"]),
    )


def _probe_candidate_urls(
    companies: Sequence[ExtractedCompany],
    *,
    timeout_seconds: float,
    max_workers: int,
    created_at: datetime,
    memoizer: FetchMemoizer,
) -> dict[str, PortalProbeResult]:
    ordered_candidates: OrderedDict[str, CandidateUrl] = OrderedDict()
    for company in companies:
        for candidate in build_ats_candidate_urls(infer_slug_candidates(company.company_domain, company.company_name)):
            ordered_candidates.setdefault(candidate.url, candidate)

    if not ordered_candidates:
        return {}

    if max_workers <= 1:
        return {
            url: _probe_candidate_url(
                candidate,
                timeout_seconds=timeout_seconds,
                created_at=created_at,
                memoizer=memoizer,
            )
            for url, candidate in ordered_candidates.items()
        }

    results: dict[str, PortalProbeResult] = {}
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {
            executor.submit(
                _probe_candidate_url,
                candidate,
                timeout_seconds=timeout_seconds,
                created_at=created_at,
                memoizer=memoizer,
            ): url
            for url, candidate in ordered_candidates.items()
        }
        for future, url in ((future, future_map[future]) for future in future_map):
            results[url] = future.result()
    return results


def _probe_candidate_url(
    candidate: CandidateUrl,
    *,
    timeout_seconds: float,
    created_at: datetime,
    memoizer: FetchMemoizer,
) -> PortalProbeResult:
    run = run_collection(
        [candidate.url],
        timeout_seconds=timeout_seconds,
        created_at=created_at,
        fetcher=memoizer.fetch,
    )
    result = run.report.source_results[0]
    cached = memoizer.peek(candidate.url)
    cached_response = cached if isinstance(cached, FetchResponse) else None

    if cached_response is None:
        try:
            cached_response = memoizer.fetch(FetchRequest(url=candidate.url, timeout_seconds=timeout_seconds))
        except Exception:
            cached_response = None

    final_url = _normalize_url(cached_response.final_url) if cached_response is not None else None
    content_type = _normalize_text(cached_response.content_type) if cached_response is not None else None
    page_title = extract_title_text(cached_response.text) if cached_response is not None else None
    detected_patterns = ()
    error = None
    if result.evidence is not None:
        detected_patterns = tuple(result.evidence.detected_patterns)
        error = result.evidence.error
        if page_title is None:
            page_title = result.evidence.page_title
        if content_type is None:
            content_type = result.evidence.content_type
        if final_url is None:
            final_url = _normalize_text(result.evidence.final_url)
    elif cached_response is not None:
        detected_patterns = detect_generic_page_patterns(cached_response.text)

    board_companies = tuple(dict.fromkeys(lead.company for lead in run.collected_leads if lead.company))
    confirmed_root = None
    if result.outcome == "collected":
        confirmed_root = _canonical_board_root(final_url or candidate.url, candidate.portal_type)
    return PortalProbeResult(
        candidate_url=candidate.url,
        portal_type=candidate.portal_type,
        slug=candidate.slug,
        collector_outcome=result.outcome,
        reason_code=result.reason_code,
        reason=result.reason,
        confirmed_root=confirmed_root,
        final_url=final_url,
        content_type=content_type,
        page_title=page_title,
        detected_patterns=detected_patterns,
        board_companies=board_companies,
        lead_count=len(run.collected_leads),
        error=error,
    )


def _build_company_result(
    company: ExtractedCompany,
    probes: dict[str, PortalProbeResult],
) -> CompanyResult:
    attempted_candidates: list[dict[str, object]] = []
    for candidate in build_ats_candidate_urls(infer_slug_candidates(company.company_domain, company.company_name)):
        probe = probes.get(candidate.url)
        if probe is None:
            continue
        attempted_candidates.append(_assess_candidate_for_company(company, probe))
        if attempted_candidates[-1]["outcome"] == "confirmed":
            break

    outcome, reason_code, reason, confirmed_root, suggested_next_action = classify_company_result(
        company.company_domain,
        company.company_name,
        attempted_candidates,
    )
    evidence = {
        "seed_pages": list(company.seed_pages),
        "evidence_links": list(company.evidence_links),
        "slug_candidates": list(infer_slug_candidates(company.company_domain, company.company_name)),
    }
    return CompanyResult(
        seed_page=company.seed_pages[0] if company.seed_pages else None,
        company_name=company.company_name,
        company_domain=company.company_domain,
        outcome=outcome,
        reason_code=reason_code,
        reason=reason,
        suggested_next_action=suggested_next_action,
        confirmed_root=confirmed_root,
        attempted_candidates=tuple(attempted_candidates),
        evidence=evidence,
    )


def _assess_candidate_for_company(
    company: ExtractedCompany,
    probe: PortalProbeResult,
) -> dict[str, object]:
    outcome = "skipped"
    reason_code = probe.reason_code
    reason = probe.reason
    confirmed_root = None

    if probe.collector_outcome == "collected":
        match_outcome, match_reason_code, match_reason = _assess_company_match(
            company.company_domain,
            company.company_name,
            probe.board_companies,
        )
        outcome = match_outcome
        reason_code = match_reason_code
        reason = match_reason
        if outcome == "confirmed":
            confirmed_root = probe.confirmed_root
    elif probe.collector_outcome == "manual_review":
        outcome = "manual_review"
    else:
        outcome = "skipped"

    return {
        "candidate_url": probe.candidate_url,
        "portal_type": probe.portal_type,
        "slug": probe.slug,
        "outcome": outcome,
        "reason_code": reason_code,
        "reason": reason,
        "confirmed_root": confirmed_root,
        "evidence": {
            "board_companies": list(probe.board_companies),
            "collector_outcome": probe.collector_outcome,
            "content_type": probe.content_type,
            "detected_patterns": list(probe.detected_patterns),
            "error": probe.error,
            "final_url": probe.final_url,
            "lead_count": probe.lead_count,
            "page_title": probe.page_title,
        },
    }


def _assess_company_match(
    company_domain: str,
    company_name: str | None,
    board_companies: Sequence[str],
) -> tuple[str, str, str]:
    unique_companies = tuple(dict.fromkeys(company for company in board_companies if company))
    if not unique_companies:
        return (
            "manual_review",
            "board_company_missing",
            "collector found postings, but the board company name could not be determined confidently",
        )
    if len(unique_companies) != 1:
        return (
            "manual_review",
            "board_company_ambiguous",
            "collector found multiple company labels on the board; manual review required",
        )

    board_company = unique_companies[0]
    expected_compacts = _expected_match_compacts(company_domain, company_name)
    board_compacts = _name_compact_forms(board_company)
    if expected_compacts & board_compacts:
        return ("confirmed", "company_match_exact", f"board company matched {company_name or company_domain}")

    expected_tokens = _preferred_match_tokens(company_name) or _domain_stem_tokens(company_domain, strip_suffixes=True)
    board_tokens = _preferred_match_tokens(board_company)
    if expected_tokens and board_tokens:
        if not set(expected_tokens) & set(board_tokens):
            return (
                "skipped",
                "company_mismatch",
                f"board company {board_company!r} did not match expected company {company_name or company_domain!r}",
            )
        if expected_tokens[0] != board_tokens[0]:
            return (
                "skipped",
                "company_mismatch",
                f"board company {board_company!r} did not match expected company {company_name or company_domain!r}",
            )

    return (
        "manual_review",
        "company_match_ambiguous",
        f"board company {board_company!r} looked plausible but did not match {company_name or company_domain!r} strongly enough",
    )


def _expected_match_compacts(company_domain: str, company_name: str | None) -> set[str]:
    compacts = set(_name_compact_forms(_domain_stem(company_domain)))
    if company_name is not None:
        compacts.update(_name_compact_forms(company_name))
    compacts.discard("")
    return compacts


def _name_compact_forms(value: str | None) -> set[str]:
    tokens = _normalized_name_tokens(value, strip_suffixes=False, suffix_tokens=_MATCH_SUFFIX_TOKENS)
    stripped_tokens = _normalized_name_tokens(value, strip_suffixes=True, suffix_tokens=_MATCH_SUFFIX_TOKENS)
    forms = {
        _join_tokens(tokens, "") or "",
        _join_tokens(stripped_tokens, "") or "",
    }
    return {form for form in forms if form}


def _preferred_match_tokens(value: str | None) -> tuple[str, ...]:
    stripped_tokens = _normalized_name_tokens(value, strip_suffixes=True, suffix_tokens=_MATCH_SUFFIX_TOKENS)
    if stripped_tokens:
        return stripped_tokens
    return _normalized_name_tokens(value, strip_suffixes=False, suffix_tokens=_MATCH_SUFFIX_TOKENS)


def _domain_stem(company_domain: str) -> str:
    parts = company_domain.split(".")
    if len(parts) < 2:
        return company_domain
    suffix = ".".join(parts[-2:])
    if suffix in _COMMON_MULTI_LABEL_SUFFIXES and len(parts) >= 3:
        return parts[-3]
    return parts[-2]


def _domain_stem_tokens(company_domain: str, *, strip_suffixes: bool) -> tuple[str, ...]:
    tokens = _tokens_from_text(_domain_stem(company_domain))
    if strip_suffixes:
        tokens = _strip_suffix_tokens(tokens, suffix_tokens=_SLUG_SUFFIX_TOKENS)
    return _strip_leading_articles(tokens)


def _normalized_name_tokens(
    value: str | None,
    *,
    strip_suffixes: bool,
    suffix_tokens: frozenset[str] = _SLUG_SUFFIX_TOKENS,
) -> tuple[str, ...]:
    if value is None:
        return ()
    tokens = _tokens_from_text(value)
    if strip_suffixes:
        tokens = _strip_suffix_tokens(tokens, suffix_tokens=suffix_tokens)
    return _strip_leading_articles(tokens)


def _tokens_from_text(value: str) -> tuple[str, ...]:
    return tuple(_TOKEN_RE.findall(value.lower()))


def _strip_suffix_tokens(tokens: Sequence[str], *, suffix_tokens: frozenset[str]) -> tuple[str, ...]:
    normalized = tuple(tokens)
    while len(normalized) > 1 and normalized[-1] in suffix_tokens:
        normalized = normalized[:-1]
    return normalized


def _strip_leading_articles(tokens: Sequence[str]) -> tuple[str, ...]:
    normalized = tuple(tokens)
    while len(normalized) > 1 and normalized[0] in _LEADING_ARTICLE_TOKENS:
        normalized = normalized[1:]
    return normalized


def _join_tokens(tokens: Sequence[str], separator: str) -> str | None:
    if not tokens:
        return None
    return separator.join(tokens)


def _canonical_board_root(value: str, portal_type: str) -> str | None:
    parsed = urlparse(value)
    host = parsed.netloc.lower()
    segments = tuple(segment for segment in parsed.path.split("/") if segment)
    if portal_type == "greenhouse":
        if host not in {"boards.greenhouse.io", "job-boards.greenhouse.io"} or not segments:
            return None
        if len(segments) >= 2 and segments[1] == "jobs":
            return None
        path = f"/{segments[0]}"
    elif portal_type == "lever":
        if host != "jobs.lever.co" or len(segments) != 1:
            return None
        path = f"/{segments[0]}"
    elif portal_type == "ashby":
        if host != "jobs.ashbyhq.com" or len(segments) != 1:
            return None
        path = f"/{segments[0]}"
    else:
        return None
    return urlunparse(parsed._replace(path=path, query="", fragment=""))


def _manual_review_payload(result: CompanyResult) -> dict[str, object]:
    return {
        "seed_page": result.seed_page,
        "company_name": result.company_name,
        "company_domain": result.company_domain,
        "attempted_candidates": list(result.attempted_candidates),
        "reason_code": result.reason_code,
        "reason": result.reason,
        "suggested_next_action": result.suggested_next_action,
        "evidence": result.evidence,
    }


def _seed_report_payload(
    *,
    run_id: str,
    created_at: str,
    finished_at: str,
    seed_pages: Sequence[str],
    direct_domains: Sequence[str],
    seed_page_results: Sequence[SeedPageResult],
    company_results: Sequence[CompanyResult],
    ats_roots_path: Path,
    manual_review_path: Path,
    seed_report_path: Path,
) -> dict[str, object]:
    confirmed_count = sum(1 for result in company_results if result.outcome == "confirmed")
    manual_review_count = sum(1 for result in company_results if result.outcome == "manual_review")
    skipped_count = sum(1 for result in company_results if result.outcome == "skipped")
    return {
        "run_id": run_id,
        "created_at": created_at,
        "finished_at": finished_at,
        "input_seed_pages": list(seed_pages),
        "input_direct_domains": list(direct_domains),
        "totals": {
            "seed_pages": len(seed_pages),
            "seed_pages_skipped": sum(1 for result in seed_page_results if result.outcome == "skipped"),
            "company_results": len(company_results),
            "confirmed": confirmed_count,
            "manual_review": manual_review_count,
            "skipped": skipped_count,
        },
        "confirmed_count": confirmed_count,
        "manual_review_count": manual_review_count,
        "skipped_count": skipped_count,
        "artifacts": {
            "output_dir": str(seed_report_path.parent),
            "ats_roots_path": str(ats_roots_path),
            "manual_review_path": str(manual_review_path),
            "seed_report_path": str(seed_report_path),
        },
        "seed_page_results": [
            {
                "seed_page": result.seed_page,
                "outcome": result.outcome,
                "reason_code": result.reason_code,
                "reason": result.reason,
                "final_url": result.final_url,
                "content_type": result.content_type,
                "page_title": result.page_title,
                "extracted_company_domains": list(result.extracted_company_domains),
                "evidence_links": list(result.evidence_links),
            }
            for result in seed_page_results
        ],
        "company_results": [
            {
                "seed_page": result.seed_page,
                "company_name": result.company_name,
                "company_domain": result.company_domain,
                "outcome": result.outcome,
                "reason_code": result.reason_code,
                "reason": result.reason,
                "suggested_next_action": result.suggested_next_action,
                "confirmed_root": result.confirmed_root,
                "attempted_candidates": list(result.attempted_candidates),
                "evidence": result.evidence,
            }
            for result in company_results
        ],
    }


def _load_lines(input_path: Path | None) -> tuple[str, ...]:
    if input_path is None:
        return ()
    lines = input_path.read_text(encoding="utf-8").splitlines()
    values: list[str] = []
    for line in lines:
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        values.append(stripped)
    return tuple(values)


def _build_run_id(*, label: str | None, created_at: datetime) -> str:
    stamp = created_at.strftime("%Y%m%dT%H%M%S%fZ")
    if label is None:
        return f"seed-{stamp}"
    normalized_label = re.sub(r"[^A-Za-z0-9._-]+", "-", label.strip()).strip("-.")
    if not normalized_label:
        raise ValueError("label must contain at least one letter or number")
    return f"seed-{normalized_label}-{stamp}"


def _normalize_created_at(created_at: datetime | None) -> datetime:
    if created_at is None:
        return datetime.now(timezone.utc)
    if created_at.tzinfo is None:
        return created_at.replace(tzinfo=timezone.utc)
    return created_at.astimezone(timezone.utc)


def _resolve_output_dir(processed_dir: Path, *, out_dir: Path | None, run_id: str) -> Path:
    if out_dir is None:
        return processed_dir / run_id
    return out_dir if out_dir.is_absolute() else (processed_dir.parent.parent / out_dir).resolve()


def _format_timestamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _write_json(output_path: Path, payload: object) -> None:
    temp_path = output_path.with_name(f"{output_path.name}.tmp")
    with temp_path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, ensure_ascii=True, sort_keys=True)
        handle.write("\n")
    temp_path.replace(output_path)


def _write_text_lines(output_path: Path, values: Iterable[str]) -> None:
    temp_path = output_path.with_name(f"{output_path.name}.tmp")
    with temp_path.open("w", encoding="utf-8") as handle:
        for value in values:
            handle.write(f"{value}\n")
    temp_path.replace(output_path)


def _company_result_sort_key(result: CompanyResult) -> tuple[str, str]:
    return (result.company_domain, result.outcome)


def _normalize_url(value: str | None) -> str | None:
    if value is None:
        return None
    stripped = value.strip()
    if not stripped:
        return None
    parsed = urlparse(stripped)
    if not parsed.scheme or not parsed.netloc:
        return None
    return parsed._replace(fragment="").geturl()


def _normalize_text(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    text = re.sub(r"\s+", " ", value).strip()
    return text or None


def _parse_httpish_value(value: str):
    text = value.strip()
    if not text:
        return None
    parsed = urlparse(text)
    if parsed.scheme in {"http", "https"}:
        return parsed
    if "://" in text:
        return None
    return urlparse(f"https://{text}")


def _registrable_domain(host: str) -> str | None:
    parts = tuple(part for part in host.split(".") if part)
    if len(parts) < 2:
        return None
    suffix = ".".join(parts[-2:])
    if suffix in _COMMON_MULTI_LABEL_SUFFIXES and len(parts) >= 3:
        return ".".join(parts[-3:])
    return ".".join(parts[-2:])


if __name__ == "__main__":
    raise SystemExit(main())
