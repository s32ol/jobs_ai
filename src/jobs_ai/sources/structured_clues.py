from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass
from html import unescape
import json
import re
from urllib.parse import urljoin, urlparse

from ..collect.fetch import FetchResponse
from ..portal_support import build_portal_support
from ..source_seed.models import ManualReviewSourceHint, SourceCandidate
from ..source_seed.verify import discover_supported_source_candidate_from_url

_JSON_LD_SCRIPT_RE = re.compile(
    r"<script[^>]*type=[\"']application/ld\+json[\"'][^>]*>(.*?)</script>",
    re.IGNORECASE | re.DOTALL,
)
_ABSOLUTE_URL_RE = re.compile(r"https?://[^\s\"'<>]+", re.IGNORECASE)
_PROTOCOL_RELATIVE_URL_RE = re.compile(r"(?<![:\w])//[^\s\"'<>]+")
_KNOWN_PROVIDER_URL_RE = re.compile(
    r"(?<![A-Za-z0-9./:@-])("
    r"(?:boards(?:-api)?(?:\.[a-z]{2})?\.greenhouse\.io|"
    r"job-boards(?:\.[a-z]{2})?\.greenhouse\.io|"
    r"api\.lever\.co|jobs\.lever\.co|jobs\.ashbyhq\.com|"
    r"(?:[a-z0-9-]+\.)?myworkdayjobs\.com|"
    r"(?:[a-z0-9-]+\.)?myworkdaysite\.com|"
    r"(?:[a-z0-9-]+\.)*workday\.com)"
    r"/[^\s\"'<>]+)",
    re.IGNORECASE,
)
_QUOTED_URL_RE = re.compile(
    r"(?:href|src|content|data-url|data-feed|data-sitemap)=[\"']([^\"']+)[\"']",
    re.IGNORECASE,
)
_GREENHOUSE_API_RE = re.compile(
    r"https?://boards-api\.greenhouse\.io/v1/boards/([a-z0-9-]+)/jobs(?:\?[^\"'<>]*)?",
    re.IGNORECASE,
)
_LEVER_API_RE = re.compile(
    r"https?://api\.lever\.co/v0/postings/([a-z0-9-]+)(?:\?[^\"'<>]*)?",
    re.IGNORECASE,
)


@dataclass(frozen=True, slots=True)
class StructuredClueScanResult:
    supported_candidates: tuple[SourceCandidate, ...]
    manual_review_sources: tuple[ManualReviewSourceHint, ...]
    follow_up_urls: tuple[str, ...]
    discovered_urls: tuple[str, ...]


def scan_structured_clues(response: FetchResponse) -> StructuredClueScanResult:
    base_url = response.final_url or response.url
    normalized_text = unescape(response.text).replace("\\/", "/")
    discovered_urls: OrderedDict[str, None] = OrderedDict()
    follow_up_urls: OrderedDict[str, None] = OrderedDict()
    candidate_map: OrderedDict[str, SourceCandidate] = OrderedDict()
    manual_review_map: OrderedDict[str, ManualReviewSourceHint] = OrderedDict()

    for url in _extract_urls_from_text(normalized_text):
        discovered_urls.setdefault(url, None)
    for url in _extract_provider_api_urls(normalized_text):
        discovered_urls.setdefault(url, None)
    for url in _extract_urls_from_json_ld(normalized_text):
        discovered_urls.setdefault(url, None)
    for url in _extract_quoted_urls(normalized_text, base_url=base_url):
        discovered_urls.setdefault(url, None)
    if _looks_like_json_payload(response):
        for url in _extract_urls_from_json_text(response.text):
            discovered_urls.setdefault(url, None)

    for discovered_url in discovered_urls:
        candidate = discover_supported_source_candidate_from_url(
            discovered_url,
            index=len(candidate_map) + 1,
            slug_source="structured_clue",
        )
        if candidate is not None:
            candidate_map.setdefault(candidate.url, candidate)
            continue

        manual_review_hint = _manual_review_hint_from_url(discovered_url)
        if manual_review_hint is not None:
            manual_review_map.setdefault(
                manual_review_hint.source_url,
                manual_review_hint,
            )
            continue

        if _looks_like_machine_readable_url(discovered_url):
            follow_up_urls.setdefault(discovered_url, None)

    return StructuredClueScanResult(
        supported_candidates=tuple(candidate_map.values()),
        manual_review_sources=tuple(manual_review_map.values()),
        follow_up_urls=tuple(follow_up_urls.keys()),
        discovered_urls=tuple(discovered_urls.keys()),
    )


def _extract_urls_from_text(text: str) -> tuple[str, ...]:
    discovered_urls: OrderedDict[str, None] = OrderedDict()
    for raw_url in _ABSOLUTE_URL_RE.findall(text):
        cleaned_url = _clean_url(raw_url)
        if cleaned_url is not None:
            discovered_urls.setdefault(cleaned_url, None)
    for raw_url in _PROTOCOL_RELATIVE_URL_RE.findall(text):
        cleaned_url = _clean_url(f"https:{raw_url}")
        if cleaned_url is not None:
            discovered_urls.setdefault(cleaned_url, None)
    for raw_url in _KNOWN_PROVIDER_URL_RE.findall(text):
        cleaned_url = _clean_url(f"https://{raw_url}")
        if cleaned_url is not None:
            discovered_urls.setdefault(cleaned_url, None)
    return tuple(discovered_urls.keys())


def _extract_provider_api_urls(text: str) -> tuple[str, ...]:
    discovered_urls: OrderedDict[str, None] = OrderedDict()

    for slug in _GREENHOUSE_API_RE.findall(text):
        discovered_urls.setdefault(f"https://boards.greenhouse.io/{slug.lower()}", None)
    for slug in _LEVER_API_RE.findall(text):
        discovered_urls.setdefault(f"https://jobs.lever.co/{slug.lower()}", None)

    return tuple(discovered_urls.keys())


def _extract_urls_from_json_ld(text: str) -> tuple[str, ...]:
    discovered_urls: OrderedDict[str, None] = OrderedDict()
    for payload_text in _JSON_LD_SCRIPT_RE.findall(text):
        stripped_payload = payload_text.strip()
        if not stripped_payload:
            continue
        for url in _extract_urls_from_json_text(stripped_payload):
            discovered_urls.setdefault(url, None)
    return tuple(discovered_urls.keys())


def _extract_urls_from_json_text(text: str) -> tuple[str, ...]:
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        return _extract_urls_from_text(text)

    discovered_urls: OrderedDict[str, None] = OrderedDict()
    for value in _walk_json_strings(payload):
        for url in _extract_urls_from_text(value):
            discovered_urls.setdefault(url, None)
    return tuple(discovered_urls.keys())


def _extract_quoted_urls(text: str, *, base_url: str) -> tuple[str, ...]:
    discovered_urls: OrderedDict[str, None] = OrderedDict()
    for raw_url in _QUOTED_URL_RE.findall(text):
        cleaned_url = _absolutize_url(raw_url, base_url=base_url)
        if cleaned_url is None:
            continue
        if build_portal_support(cleaned_url) is not None or _looks_like_machine_readable_url(cleaned_url):
            discovered_urls.setdefault(cleaned_url, None)
    return tuple(discovered_urls.keys())


def _walk_json_strings(value: object):
    if isinstance(value, str):
        yield value
        return
    if isinstance(value, list):
        for item in value:
            yield from _walk_json_strings(item)
        return
    if isinstance(value, dict):
        for item in value.values():
            yield from _walk_json_strings(item)


def _manual_review_hint_from_url(value: str | None) -> ManualReviewSourceHint | None:
    portal_support = build_portal_support(value)
    if portal_support is None or portal_support.portal_type != "workday":
        return None
    return ManualReviewSourceHint(
        source_url=portal_support.normalized_apply_url,
        portal_type=portal_support.portal_type,
        reason_code="workday_partial_support",
        reason="Workday portal detected from machine-readable clues; keep it in manual review only.",
        suggested_next_action=(
            "Keep the Workday URL visible in the registry for manual review, but do not rely on it as a structured collector source."
        ),
    )


def _absolutize_url(value: str | None, *, base_url: str) -> str | None:
    if value is None:
        return None
    stripped_value = value.strip()
    if not stripped_value or stripped_value.startswith(("#", "javascript:", "mailto:", "tel:")):
        return None
    if stripped_value.startswith("//"):
        stripped_value = f"https:{stripped_value}"
    return _clean_url(urljoin(base_url, stripped_value))


def _clean_url(value: str | None) -> str | None:
    if value is None:
        return None
    stripped_value = value.strip().rstrip("),.;")
    parsed = urlparse(stripped_value)
    if parsed.scheme.lower() not in {"http", "https"} or not parsed.netloc:
        return None
    return parsed._replace(fragment="").geturl()


def _looks_like_json_payload(response: FetchResponse) -> bool:
    content_type = (response.content_type or "").lower()
    return "json" in content_type or response.text.lstrip().startswith(("{", "["))


def _looks_like_machine_readable_url(value: str) -> bool:
    parsed = urlparse(value)
    searchable_text = f"{parsed.path} {parsed.query}".lower()
    has_feed_shape = any(token in searchable_text for token in ("sitemap", "feed", "/api/", ".json", ".xml"))
    has_jobs_shape = any(token in searchable_text for token in ("job", "jobs", "career", "careers", "opening"))
    return has_feed_shape and has_jobs_shape
