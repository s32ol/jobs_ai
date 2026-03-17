from __future__ import annotations

from dataclasses import dataclass
from collections.abc import Iterator, Sequence
from html import unescape
import json
import re
from typing import Protocol, runtime_checkable
from urllib.parse import urljoin, urlparse

from ..fetch import FetchRequest, FetchResponse, Fetcher
from ..models import (
    CensusSourceResult,
    CollectedLead,
    ManualReviewItem,
    OutcomeEvidence,
    SourceInput,
    SourceResult,
)

_ATTR_RE = re.compile(r'([:\w-]+)\s*=\s*(["\'])(.*?)\2', re.IGNORECASE | re.DOTALL)
_META_RE = re.compile(r"<meta(?P<attrs>[^>]*)>", re.IGNORECASE)
_SCRIPT_RE = re.compile(
    r"<script(?P<attrs>[^>]*)>(?P<body>.*?)</script>",
    re.IGNORECASE | re.DOTALL,
)
_TAG_RE = re.compile(r"<[^>]+>")
_TITLE_RE = re.compile(r"<title[^>]*>(?P<body>.*?)</title>", re.IGNORECASE | re.DOTALL)
_WHITESPACE_RE = re.compile(r"\s+")
_HTML_MARKER_RE = re.compile(r"<!doctype\s+html|<html\b|<head\b|<body\b|<meta\b|<script\b|<div\b", re.IGNORECASE)
_BLOCKED_HTML_PATTERNS = (
    ("access_denied", re.compile(r"\baccess denied\b", re.IGNORECASE)),
    ("captcha", re.compile(r"\bcaptcha\b", re.IGNORECASE)),
    ("human_verification", re.compile(r"\bverify you are human\b", re.IGNORECASE)),
    ("security_check", re.compile(r"\bsecurity check\b", re.IGNORECASE)),
    ("javascript_or_cookies_required", re.compile(r"\benable (?:javascript|cookies)\b", re.IGNORECASE)),
    ("interstitial_wait_room", re.compile(r"\bjust a moment\b", re.IGNORECASE)),
)
_HTML_CONTENT_TYPES = frozenset({"text/html", "application/xhtml+xml"})
_CAREERS_KEYWORD_RE = re.compile(r"\bcareers?\b|\bjoin our team\b|\bopen roles?\b|\bopen positions?\b", re.IGNORECASE)
_APPLY_KEYWORD_RE = re.compile(r"\bapply now\b|\bsubmit application\b|\bapply for\b", re.IGNORECASE)
_JOB_DETAIL_KEYWORD_RE = re.compile(
    r"\bjob description\b|\bresponsibilit(?:y|ies)\b|\brequirements?\b|\bqualifications?\b",
    re.IGNORECASE,
)
_WORKDAY_MARKER_RE = re.compile(r"\bworkday\b", re.IGNORECASE)


@runtime_checkable
class CollectionAdapter(Protocol):
    adapter_key: str

    def collect(
        self,
        source: SourceInput,
        *,
        timeout_seconds: float,
        fetcher: Fetcher,
    ) -> SourceResult:
        """Collect one source into collected leads, manual review, or skipped."""


@dataclass(frozen=True, slots=True)
class ParseAttempt:
    leads: tuple[CollectedLead, ...] = ()
    ambiguous_reason: str | None = None
    recognized_empty: bool = False


def fetch_source(
    source: SourceInput,
    *,
    timeout_seconds: float,
    fetcher: Fetcher,
) -> FetchResponse:
    assert source.normalized_url is not None
    return fetcher(
        FetchRequest(
            url=source.normalized_url,
            timeout_seconds=timeout_seconds,
        )
    )


def inspect_response_for_skip(
    source: SourceInput,
    *,
    adapter_key: str,
    response: FetchResponse,
) -> SourceResult | None:
    blocked_patterns = detect_blocked_patterns(response.text)

    if not response.text.strip():
        return build_skipped_result(
            source,
            adapter_key=adapter_key,
            reason_code="empty_response_body",
            reason="empty response body returned; skipped",
            evidence=build_response_evidence(response),
        )

    if blocked_patterns:
        return build_skipped_result(
            source,
            adapter_key=adapter_key,
            reason_code="blocked_or_access_denied",
            reason="page appears blocked or access denied; skipped",
            evidence=build_response_evidence(response, detected_patterns=blocked_patterns),
        )

    if response.status_code >= 400:
        return build_skipped_result(
            source,
            adapter_key=adapter_key,
            reason_code="http_error_status",
            reason=f"HTTP {response.status_code} returned while fetching source; skipped",
            evidence=build_response_evidence(response),
        )

    if not _response_looks_html(response):
        content_type = normalize_text(response.content_type)
        reason = "response body did not look like HTML; skipped"
        if content_type is not None:
            reason = f"non-HTML content-type returned: {content_type}"
        return build_skipped_result(
            source,
            adapter_key=adapter_key,
            reason_code="non_html_content",
            reason=reason,
            evidence=build_response_evidence(response),
        )

    return None


def finalize_supported_collection(
    source: SourceInput,
    *,
    adapter_key: str,
    portal_label: str,
    parse_attempts: Sequence[ParseAttempt],
    direct_job_url: bool,
    ambiguous_reason_code: str,
    default_manual_review_reason: str,
    direct_job_reason_code: str | None = None,
) -> SourceResult:
    for attempt in parse_attempts:
        if attempt.ambiguous_reason is None:
            continue
        return build_manual_review_result(
            source,
            adapter_key=adapter_key,
            reason_code=ambiguous_reason_code,
            reason=attempt.ambiguous_reason,
        )

    leads = next((attempt.leads for attempt in parse_attempts if attempt.leads), ())
    if not leads:
        return build_manual_review_result(
            source,
            adapter_key=adapter_key,
            reason_code=ambiguous_reason_code,
            reason=default_manual_review_reason,
        )
    if direct_job_url and len(leads) != 1:
        return build_manual_review_result(
            source,
            adapter_key=adapter_key,
            reason_code=direct_job_reason_code or ambiguous_reason_code,
            reason=f"expected exactly one {portal_label} posting for direct job URL, found {len(leads)}",
        )
    return build_collected_result(source, adapter_key=adapter_key, leads=leads)


def finalize_supported_census(
    source: SourceInput,
    *,
    adapter_key: str,
    parse_attempts: Sequence[ParseAttempt],
    default_failure_reason: str,
    evidence: OutcomeEvidence | None = None,
) -> CensusSourceResult:
    for attempt in parse_attempts:
        if attempt.ambiguous_reason is None:
            continue
        return build_failed_census_result(
            source,
            adapter_key=adapter_key,
            reason_code=f"{adapter_key}_parse_ambiguous",
            reason=attempt.ambiguous_reason,
            evidence=evidence,
        )

    leads = next((attempt.leads for attempt in parse_attempts if attempt.leads), ())
    if leads:
        return build_counted_census_result(
            source,
            adapter_key=adapter_key,
            available_job_count=len(leads),
            evidence=evidence,
        )

    if any(attempt.recognized_empty for attempt in parse_attempts):
        return build_counted_census_result(
            source,
            adapter_key=adapter_key,
            available_job_count=0,
            evidence=evidence,
        )

    return build_failed_census_result(
        source,
        adapter_key=adapter_key,
        reason_code=f"{adapter_key}_parse_ambiguous",
        reason=default_failure_reason,
        evidence=evidence,
    )


def normalize_text(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    text = _WHITESPACE_RE.sub(" ", unescape(value)).strip()
    return text or None


def strip_html_tags(value: str) -> str | None:
    return normalize_text(_TAG_RE.sub(" ", value))


def parse_attrs(attr_text: str) -> dict[str, str]:
    return {
        key.lower(): value
        for key, _, value in _ATTR_RE.findall(attr_text)
    }


def extract_meta_content(
    html_text: str,
    *,
    property_name: str | None = None,
    name: str | None = None,
) -> str | None:
    for match in _META_RE.finditer(html_text):
        attrs = parse_attrs(match.group("attrs"))
        if property_name is not None and attrs.get("property", "").lower() == property_name.lower():
            return normalize_text(attrs.get("content"))
        if name is not None and attrs.get("name", "").lower() == name.lower():
            return normalize_text(attrs.get("content"))
    return None


def extract_title_text(html_text: str) -> str | None:
    match = _TITLE_RE.search(html_text)
    if match is None:
        return None
    return strip_html_tags(match.group("body"))


def extract_script_body(
    html_text: str,
    *,
    script_id: str | None = None,
    script_type: str | None = None,
) -> str | None:
    for match in _SCRIPT_RE.finditer(html_text):
        attrs = parse_attrs(match.group("attrs"))
        if script_id is not None and attrs.get("id") != script_id:
            continue
        if script_type is not None and attrs.get("type", "").lower() != script_type.lower():
            continue
        return match.group("body").strip()
    return None


def extract_json_ld_blocks(html_text: str) -> tuple[object, ...]:
    payloads: list[object] = []
    for match in _SCRIPT_RE.finditer(html_text):
        attrs = parse_attrs(match.group("attrs"))
        if attrs.get("type", "").lower() != "application/ld+json":
            continue
        body = match.group("body").strip()
        if not body:
            continue
        try:
            payloads.append(json.loads(body))
        except json.JSONDecodeError:
            continue
    return tuple(payloads)


def extract_json_after_marker(text: str, marker: str) -> object | None:
    start_index = text.find(marker)
    if start_index < 0:
        return None
    json_start = _find_json_start(text, start_index + len(marker))
    if json_start is None:
        return None
    json_end = _find_json_end(text, json_start)
    if json_end is None:
        return None
    try:
        return json.loads(text[json_start:json_end])
    except json.JSONDecodeError:
        return None


def company_from_page_metadata(html_text: str, *, portal_label: str) -> str | None:
    for candidate in (
        extract_meta_content(html_text, property_name="og:site_name"),
        extract_meta_content(html_text, name="application-name"),
    ):
        if candidate is not None:
            return candidate

    title = extract_title_text(html_text)
    if title is None:
        return None

    normalized_title = title
    for separator in (" | ", " - "):
        if separator in normalized_title:
            normalized_title = normalized_title.split(separator, 1)[0]
            break

    normalized_title = re.sub(
        rf"\bjobs?\b(?:\s+at)?|\bcareers?\b|\bjob board\b|\b{re.escape(portal_label)}\b",
        " ",
        normalized_title,
        flags=re.IGNORECASE,
    )
    return normalize_text(normalized_title)


def build_absolute_url(base_url: str, value: object) -> str | None:
    if not isinstance(value, str):
        return None
    href = value.strip()
    if not href:
        return None
    return normalize_url(urljoin(base_url, href))


def normalize_url(value: str) -> str:
    parsed = urlparse(value.strip())
    return parsed._replace(fragment="").geturl()


def url_path_segments(value: str) -> tuple[str, ...]:
    return tuple(segment for segment in urlparse(value).path.split("/") if segment)


def extract_job_posting_nodes(html_text: str) -> tuple[dict[str, object], ...]:
    results: list[dict[str, object]] = []

    def walk(value: object) -> None:
        if isinstance(value, dict):
            node_type = value.get("@type") or value.get("type")
            if _is_job_posting_type(node_type):
                results.append(value)
            for child in value.values():
                walk(child)
            return
        if isinstance(value, list):
            for child in value:
                walk(child)

    for payload in extract_json_ld_blocks(html_text):
        walk(payload)
    return tuple(results)


def extract_identifier_value(value: object) -> str | None:
    if isinstance(value, str):
        return normalize_text(value)
    if isinstance(value, dict):
        for key in ("value", "name", "@value"):
            candidate = normalize_text(value.get(key))
            if candidate is not None:
                return candidate
    return None


def extract_location_text(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return normalize_text(value)
    if isinstance(value, list):
        locations = tuple(
            dict.fromkeys(
                location
                for item in value
                for location in (extract_location_text(item),)
                if location is not None
            )
        )
        if len(locations) == 1:
            return locations[0]
        return None
    if not isinstance(value, dict):
        return None

    if _dict_is_remote(value):
        return "Remote"

    for key in ("name", "location", "locationName", "displayName", "label", "text"):
        location = normalize_text(value.get(key))
        if location is not None:
            return location

    address = value.get("address")
    if isinstance(address, dict):
        location = _join_location_parts(
            normalize_text(address.get("addressLocality")),
            normalize_text(address.get("addressRegion")),
            normalize_text(address.get("addressCountry")),
        )
        if location is not None:
            return location

    return _join_location_parts(
        normalize_text(value.get("city")),
        normalize_text(value.get("state")),
        normalize_text(value.get("country")),
    )


def build_collected_result(
    source: SourceInput,
    *,
    adapter_key: str,
    leads: Sequence[CollectedLead],
    reason_code: str = "collected",
    reason: str | None = None,
    evidence: OutcomeEvidence | None = None,
) -> SourceResult:
    deduped_leads = tuple(_dedupe_leads(leads))
    if not deduped_leads:
        raise ValueError("build_collected_result requires at least one lead")
    result_reason = reason or f"collected {len(deduped_leads)} lead(s)"
    return SourceResult(
        source=source,
        adapter_key=adapter_key,
        outcome="collected",
        reason_code=reason_code,
        reason=result_reason,
        evidence=evidence,
        collected_leads=deduped_leads,
    )


def build_manual_review_result(
    source: SourceInput,
    *,
    adapter_key: str,
    reason_code: str,
    reason: str,
    suggested_next_action: str | None = None,
    evidence: OutcomeEvidence | None = None,
) -> SourceResult:
    manual_review_item = ManualReviewItem(
        source_url=source.source_url,
        normalized_url=source.normalized_url,
        portal_type=source.portal_type,
        adapter_key=adapter_key,
        reason_code=reason_code,
        reason=reason,
        suggested_next_action=suggested_next_action,
        company_apply_url=(
            source.portal_support.company_apply_url
            if source.portal_support is not None
            else None
        ),
        hints=source.portal_support.hints if source.portal_support is not None else (),
        evidence=evidence,
    )
    return SourceResult(
        source=source,
        adapter_key=adapter_key,
        outcome="manual_review",
        reason_code=reason_code,
        reason=reason,
        suggested_next_action=suggested_next_action,
        evidence=evidence,
        manual_review_item=manual_review_item,
    )


def build_skipped_result(
    source: SourceInput,
    *,
    adapter_key: str,
    reason_code: str,
    reason: str,
    suggested_next_action: str | None = None,
    evidence: OutcomeEvidence | None = None,
) -> SourceResult:
    return SourceResult(
        source=source,
        adapter_key=adapter_key,
        outcome="skipped",
        reason_code=reason_code,
        reason=reason,
        suggested_next_action=suggested_next_action,
        evidence=evidence,
    )


def build_counted_census_result(
    source: SourceInput,
    *,
    adapter_key: str,
    available_job_count: int,
    reason_code: str = "counted",
    reason: str | None = None,
    evidence: OutcomeEvidence | None = None,
) -> CensusSourceResult:
    result_reason = reason or f"counted {available_job_count} posting(s)"
    return CensusSourceResult(
        source=source,
        adapter_key=adapter_key,
        outcome="counted",
        available_job_count=available_job_count,
        reason_code=reason_code,
        reason=result_reason,
        evidence=evidence,
    )


def build_failed_census_result(
    source: SourceInput,
    *,
    adapter_key: str,
    reason_code: str,
    reason: str,
    evidence: OutcomeEvidence | None = None,
) -> CensusSourceResult:
    return CensusSourceResult(
        source=source,
        adapter_key=adapter_key,
        outcome="failed",
        available_job_count=None,
        reason_code=reason_code,
        reason=reason,
        evidence=evidence,
    )


def build_response_evidence(
    response: FetchResponse,
    *,
    detected_patterns: Sequence[str] = (),
    error: str | None = None,
) -> OutcomeEvidence:
    return OutcomeEvidence(
        final_url=normalize_text(response.final_url) or normalize_text(response.url),
        status_code=response.status_code,
        content_type=normalize_text(response.content_type),
        page_title=extract_title_text(response.text),
        detected_patterns=tuple(detected_patterns),
        error=normalize_text(error),
    )


def detect_blocked_patterns(html_text: str) -> tuple[str, ...]:
    sample = _blocked_detection_sample(html_text)
    return tuple(name for name, pattern in _BLOCKED_HTML_PATTERNS if pattern.search(sample))


def detect_generic_page_patterns(html_text: str) -> tuple[str, ...]:
    detected_patterns: list[str] = []
    if extract_job_posting_nodes(html_text):
        detected_patterns.append("job_posting_json_ld")
    if _CAREERS_KEYWORD_RE.search(html_text):
        detected_patterns.append("careers_keyword")
    if _APPLY_KEYWORD_RE.search(html_text):
        detected_patterns.append("apply_keyword")
    if _JOB_DETAIL_KEYWORD_RE.search(html_text):
        detected_patterns.append("job_detail_keyword")
    if _WORKDAY_MARKER_RE.search(html_text):
        detected_patterns.append("workday_marker")
    return tuple(detected_patterns)


def choose_first_text(*values: object) -> str | None:
    for value in values:
        text = normalize_text(value)
        if text is not None:
            return text
    return None


def _dedupe_leads(leads: Sequence[CollectedLead]) -> Iterator[CollectedLead]:
    seen: set[tuple[str | None, str | None, str, str]] = set()
    for lead in leads:
        key = (
            lead.apply_url,
            lead.source_job_id,
            lead.company,
            lead.title,
        )
        if key in seen:
            continue
        seen.add(key)
        yield lead


def _find_json_start(text: str, index: int) -> int | None:
    for position in range(index, len(text)):
        if text[position] in "{[":
            return position
    return None


def _find_json_end(text: str, start: int) -> int | None:
    stack: list[str] = [text[start]]
    in_string = False
    escaped = False
    for position in range(start + 1, len(text)):
        char = text[position]
        if in_string:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == '"':
                in_string = False
            continue
        if char == '"':
            in_string = True
            continue
        if char in "{[":
            stack.append(char)
            continue
        if char in "}]":
            if not stack:
                return None
            opener = stack.pop()
            if (opener, char) not in {("{", "}"), ("[", "]")}:
                return None
            if not stack:
                return position + 1
    return None


def _is_job_posting_type(value: object) -> bool:
    if isinstance(value, str):
        return value.lower() == "jobposting"
    if isinstance(value, list):
        return any(_is_job_posting_type(item) for item in value)
    return False


def _dict_is_remote(value: dict[str, object]) -> bool:
    remote_values = (
        normalize_text(value.get("jobLocationType")),
        normalize_text(value.get("workplaceType")),
        normalize_text(value.get("type")),
    )
    if any(item is not None and item.lower() == "telecommute" for item in remote_values):
        return True
    if value.get("remote") is True:
        return True
    return any(item is not None and "remote" in item.lower() for item in remote_values)


def _join_location_parts(*parts: str | None) -> str | None:
    values = tuple(dict.fromkeys(part for part in parts if part is not None))
    if not values:
        return None
    return ", ".join(values)


def _response_looks_html(response: FetchResponse) -> bool:
    content_type = normalize_text(response.content_type)
    if content_type is not None:
        normalized_content_type = content_type.split(";", 1)[0].strip().lower()
        if normalized_content_type not in _HTML_CONTENT_TYPES:
            return False
    return _HTML_MARKER_RE.search(response.text) is not None


def _blocked_detection_sample(html_text: str) -> str:
    title = extract_title_text(html_text) or ""
    page_text = strip_html_tags(html_text) or html_text
    return f"{title} {page_text[:4000]}"
