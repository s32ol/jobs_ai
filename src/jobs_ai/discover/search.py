from __future__ import annotations

from collections import OrderedDict
from collections.abc import Callable
from dataclasses import dataclass
from html.parser import HTMLParser
from pathlib import Path
import re
import time
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse

from ..collect.adapters.base import build_response_evidence, detect_blocked_patterns
from ..collect.fetch import FetchError, FetchRequest, FetchResponse, Fetcher
from ..collect.models import OutcomeEvidence
from .models import SearchAttempt, SearchExecutionResult, SearchHit, SearchPlan

SEARCH_ENDPOINT = "https://html.duckduckgo.com/html/"
SEARCH_SITE_FILTERS: tuple[tuple[str, str], ...] = (
    ("greenhouse", "boards.greenhouse.io"),
    ("greenhouse", "job-boards.greenhouse.io"),
    ("lever", "jobs.lever.co"),
    ("ashby", "jobs.ashbyhq.com"),
    ("workday", "myworkdayjobs.com"),
    ("workday", "workday.com"),
)
SEARCH_RETRY_DELAYS_SECONDS: tuple[float, ...] = (0.5, 1.0)
_HTML_MARKER_RE = re.compile(r"<!doctype\s+html|<html\b|<head\b|<body\b|<meta\b|<script\b", re.IGNORECASE)
_RESULT_LAYOUT_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("duckduckgo_result_link", re.compile(r"\bresult__a\b", re.IGNORECASE)),
    ("duckduckgo_result_title", re.compile(r"\bresult__title\b", re.IGNORECASE)),
    ("duckduckgo_result_url", re.compile(r"\bresult__url\b", re.IGNORECASE)),
    ("duckduckgo_result_body", re.compile(r"\bresult-link\b", re.IGNORECASE)),
)
_ZERO_RESULTS_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("duckduckgo_no_results", re.compile(r"\bno results found for\b", re.IGNORECASE)),
    ("duckduckgo_no_results", re.compile(r"\bno results\b", re.IGNORECASE)),
    ("duckduckgo_no_more_results", re.compile(r"\bno more results\b", re.IGNORECASE)),
)
_DUCKDUCKGO_ANOMALY_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("duckduckgo_challenge_form", re.compile(r"\bchallenge-form\b", re.IGNORECASE)),
    ("duckduckgo_anomaly_modal", re.compile(r"\banomaly-modal\b", re.IGNORECASE)),
    ("duckduckgo_anomaly_js", re.compile(r"/anomaly\.js\b", re.IGNORECASE)),
    ("duckduckgo_bot_challenge", re.compile(r"bots use duckduckgo too", re.IGNORECASE)),
    (
        "duckduckgo_image_challenge",
        re.compile(r"select all squares containing a duck", re.IGNORECASE),
    ),
)
_HTML_CONTENT_TYPES = frozenset({"text/html", "application/xhtml+xml"})
_ARTIFACT_NAME_RE = re.compile(r"[^a-z0-9]+")


@dataclass(frozen=True, slots=True)
class _ExtractedAnchor:
    href: str
    title: str
    attrs: dict[str, str]


@dataclass(frozen=True, slots=True)
class _SearchPageInspection:
    hits: tuple[SearchHit, ...]
    result_anchor_count: int
    detected_patterns: tuple[str, ...]
    has_zero_results_marker: bool
    has_result_layout_marker: bool


class _AnchorExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self._href: str | None = None
        self._text_parts: list[str] = []
        self._attrs: dict[str, str] = {}
        self.links: list[_ExtractedAnchor] = []

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
        self._href = href
        self._text_parts = []
        self._attrs = attr_map

    def handle_data(self, data: str) -> None:
        if self._href is None:
            return
        self._text_parts.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() != "a" or self._href is None:
            return
        title = " ".join(part.strip() for part in self._text_parts if part.strip()).strip()
        self.links.append(
            _ExtractedAnchor(
                href=self._href,
                title=title,
                attrs=dict(self._attrs),
            )
        )
        self._href = None
        self._text_parts = []
        self._attrs = {}


def build_search_plans(query: str) -> tuple[SearchPlan, ...]:
    normalized_query = query.strip()
    if not normalized_query:
        return ()

    plans: list[SearchPlan] = []
    for portal_type, site_filter in SEARCH_SITE_FILTERS:
        search_text = f"{normalized_query} site:{site_filter}"
        search_url = f"{SEARCH_ENDPOINT}?{urlencode({'q': search_text})}"
        plans.append(
            SearchPlan(
                portal_type=portal_type,
                site_filter=site_filter,
                search_text=search_text,
                search_url=search_url,
            )
        )
    return tuple(plans)


def execute_search_plan(
    plan: SearchPlan,
    *,
    timeout_seconds: float,
    fetcher: Fetcher,
    artifact_dir: Path | None = None,
    sleep_fn: Callable[[float], None] = time.sleep,
) -> tuple[SearchExecutionResult, tuple[SearchHit, ...]]:
    attempts: list[SearchAttempt] = []
    final_hits: tuple[SearchHit, ...] = ()
    max_attempts = len(SEARCH_RETRY_DELAYS_SECONDS) + 1

    for attempt_number in range(1, max_attempts + 1):
        attempt, hits = _execute_search_attempt(
            plan,
            timeout_seconds=timeout_seconds,
            fetcher=fetcher,
            attempt_number=attempt_number,
            artifact_dir=artifact_dir,
        )
        attempts.append(attempt)
        final_hits = hits
        if attempt.status not in {"fetch_failure", "provider_anomaly", "parse_failure"}:
            break
        if attempt_number >= max_attempts:
            break
        sleep_fn(SEARCH_RETRY_DELAYS_SECONDS[attempt_number - 1])

    final_attempt = attempts[-1]
    raw_artifact_paths = tuple(
        attempt.raw_artifact_path
        for attempt in attempts
        if attempt.raw_artifact_path is not None
    )
    return (
        SearchExecutionResult(
            plan=plan,
            status=final_attempt.status,
            hit_count=len(final_hits),
            attempt_count=len(attempts),
            error=final_attempt.error,
            evidence=final_attempt.evidence,
            attempts=tuple(attempts),
            raw_artifact_paths=raw_artifact_paths,
        ),
        final_hits,
    )


def _execute_search_attempt(
    plan: SearchPlan,
    *,
    timeout_seconds: float,
    fetcher: Fetcher,
    attempt_number: int,
    artifact_dir: Path | None,
) -> tuple[SearchAttempt, tuple[SearchHit, ...]]:
    try:
        response = fetcher(
            FetchRequest(
                url=plan.search_url,
                timeout_seconds=timeout_seconds,
                headers={"Accept": "text/html"},
            )
        )
    except FetchError as exc:
        error = str(exc)
        return (
            SearchAttempt(
                attempt_number=attempt_number,
                status="fetch_failure",
                hit_count=0,
                error=error,
                evidence=OutcomeEvidence(error=error),
            ),
            (),
        )

    status, hits, error, evidence = _classify_search_response(
        response,
        search_text=plan.search_text,
        search_url=plan.search_url,
    )
    raw_artifact_path = None
    if artifact_dir is not None and status in {"provider_anomaly", "parse_failure"}:
        raw_artifact_path = _write_search_artifact(
            artifact_dir,
            plan=plan,
            attempt_number=attempt_number,
            status=status,
            response=response,
        )
    return (
        SearchAttempt(
            attempt_number=attempt_number,
            status=status,
            hit_count=len(hits),
            error=error,
            evidence=evidence,
            raw_artifact_path=raw_artifact_path,
        ),
        hits,
    )


def _classify_search_response(
    response: FetchResponse,
    *,
    search_text: str,
    search_url: str,
) -> tuple[str, tuple[SearchHit, ...], str | None, OutcomeEvidence | None]:
    text = response.text
    if not text.strip():
        error = "search provider returned an empty response body"
        evidence = build_response_evidence(
            response,
            detected_patterns=("empty_response_body",),
            error=error,
        )
        return "provider_anomaly", (), error, evidence

    if not _response_looks_html(response):
        error = "search provider returned non-HTML content"
        evidence = build_response_evidence(
            response,
            detected_patterns=("non_html_search_response",),
            error=error,
        )
        return "provider_anomaly", (), error, evidence

    anomaly_patterns = detect_search_anomaly_patterns(text)
    if anomaly_patterns:
        error = _search_anomaly_error(anomaly_patterns)
        evidence = build_response_evidence(
            response,
            detected_patterns=anomaly_patterns,
            error=error,
        )
        return "provider_anomaly", (), error, evidence

    inspection = inspect_search_response(
        response,
        search_text=search_text,
        search_url=search_url,
    )
    if inspection.hits:
        evidence = build_response_evidence(
            response,
            detected_patterns=inspection.detected_patterns,
        )
        return "success", inspection.hits, None, evidence

    if inspection.has_zero_results_marker:
        evidence = build_response_evidence(
            response,
            detected_patterns=inspection.detected_patterns,
        )
        return "zero_results", (), None, evidence

    if inspection.has_result_layout_marker or inspection.result_anchor_count:
        error = "search page looked like a result page, but no usable result URLs were extracted"
        evidence = build_response_evidence(
            response,
            detected_patterns=inspection.detected_patterns + ("search_result_parse_miss",),
            error=error,
        )
        return "parse_failure", (), error, evidence

    error = "search provider returned HTML without results or an explicit no-results marker"
    evidence = build_response_evidence(
        response,
        detected_patterns=inspection.detected_patterns + ("search_unclassified_html",),
        error=error,
    )
    return "provider_anomaly", (), error, evidence


def inspect_search_response(
    response: FetchResponse,
    *,
    search_text: str,
    search_url: str,
) -> _SearchPageInspection:
    parser = _AnchorExtractor()
    parser.feed(response.text)

    search_host = urlparse(search_url).netloc.lower()
    hits_by_target: OrderedDict[str, SearchHit] = OrderedDict()
    result_anchor_count = 0
    for link in parser.links:
        if _anchor_looks_like_search_result(link):
            result_anchor_count += 1
        target_url = decode_search_target_url(link.href, search_url=search_url)
        if target_url is None:
            continue
        target_host = urlparse(target_url).netloc.lower()
        if target_host == search_host or target_host.endswith(".duckduckgo.com") or target_host == "duckduckgo.com":
            continue
        hits_by_target.setdefault(
            target_url,
            SearchHit(
                search_text=search_text,
                search_url=search_url,
                target_url=target_url,
                title=link.title or None,
            ),
        )
    detected_patterns = _detect_result_layout_patterns(response.text)
    has_zero_results_marker = any(name.startswith("duckduckgo_no_") for name in detected_patterns)
    has_result_layout_marker = any(name.startswith("duckduckgo_result_") for name in detected_patterns)
    return _SearchPageInspection(
        hits=tuple(hits_by_target.values()),
        result_anchor_count=result_anchor_count,
        detected_patterns=detected_patterns,
        has_zero_results_marker=has_zero_results_marker,
        has_result_layout_marker=has_result_layout_marker,
    )


def extract_search_hits(
    response: FetchResponse,
    *,
    search_text: str,
    search_url: str,
) -> tuple[SearchHit, ...]:
    return inspect_search_response(
        response,
        search_text=search_text,
        search_url=search_url,
    ).hits


def decode_search_target_url(value: str, *, search_url: str) -> str | None:
    absolute_url = urljoin(search_url, value)
    parsed = urlparse(absolute_url)
    if parsed.scheme.lower() not in {"http", "https"} or not parsed.netloc:
        return None

    query_map = dict(parse_qsl(parsed.query, keep_blank_values=True))
    redirect_target = query_map.get("uddg") or query_map.get("rut")
    if redirect_target:
        decoded = urlparse(redirect_target)
        if decoded.scheme.lower() in {"http", "https"} and decoded.netloc:
            return decoded._replace(fragment="").geturl()

    return parsed._replace(fragment="").geturl()


def detect_search_anomaly_patterns(html_text: str) -> tuple[str, ...]:
    sample = html_text[:12000]
    detected_patterns = list(detect_blocked_patterns(sample))
    detected_patterns.extend(
        name
        for name, pattern in _DUCKDUCKGO_ANOMALY_PATTERNS
        if pattern.search(sample)
    )
    return tuple(dict.fromkeys(detected_patterns))


def _anchor_looks_like_search_result(link: _ExtractedAnchor) -> bool:
    attr_text = " ".join(
        value
        for key, value in link.attrs.items()
        if key in {"class", "id", "data-testid"}
    ).lower()
    href = link.href.lower()
    if "result" in attr_text:
        return True
    if "uddg=" in href or "rut=" in href:
        return True
    return href.startswith("/l/?") or href.startswith("https://duckduckgo.com/l/?")


def _detect_result_layout_patterns(html_text: str) -> tuple[str, ...]:
    sample = html_text[:24000]
    detected_patterns = [
        name
        for name, pattern in _RESULT_LAYOUT_PATTERNS + _ZERO_RESULTS_PATTERNS
        if pattern.search(sample)
    ]
    return tuple(dict.fromkeys(detected_patterns))


def _response_looks_html(response: FetchResponse) -> bool:
    content_type = (response.content_type or "").split(";", 1)[0].strip().lower()
    if content_type in _HTML_CONTENT_TYPES:
        return True
    return bool(_HTML_MARKER_RE.search(response.text[:2000]))


def _search_anomaly_error(detected_patterns: tuple[str, ...]) -> str:
    if any(pattern.startswith("duckduckgo_") for pattern in detected_patterns):
        return "search provider returned a DuckDuckGo challenge or anomaly page"
    if detected_patterns:
        return "search provider returned blocked or access-denied HTML"
    return "search provider returned anomalous HTML"


def _write_search_artifact(
    artifact_dir: Path,
    *,
    plan: SearchPlan,
    attempt_number: int,
    status: str,
    response: FetchResponse,
) -> Path:
    artifact_dir.mkdir(parents=True, exist_ok=True)
    site_filter = _ARTIFACT_NAME_RE.sub("-", plan.site_filter.lower()).strip("-")
    filename = f"{site_filter}-attempt-{attempt_number}-{status}.html"
    output_path = artifact_dir / filename
    temp_path = output_path.with_name(f"{output_path.name}.tmp")
    temp_path.write_text(response.text, encoding="utf-8")
    temp_path.replace(output_path)
    return output_path
