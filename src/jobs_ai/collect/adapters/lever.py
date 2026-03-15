from __future__ import annotations

import re

from ..fetch import FetchError, Fetcher
from ..models import CollectedLead, OutcomeEvidence, SourceInput, SourceResult
from .base import (
    ParseAttempt,
    build_absolute_url,
    build_skipped_result,
    choose_first_text,
    company_from_page_metadata,
    extract_identifier_value,
    extract_job_posting_nodes,
    extract_json_after_marker,
    extract_location_text,
    fetch_source,
    finalize_supported_collection,
    inspect_response_for_skip,
    normalize_text,
    strip_html_tags,
    url_path_segments,
)

_LEVER_POSTING_RE = re.compile(
    r'<div[^>]*class="[^"]*\bposting\b[^"]*"[^>]*>(?P<body>.*?)</div>',
    re.IGNORECASE | re.DOTALL,
)
_LEVER_LINK_RE = re.compile(
    r'<a[^>]*href="(?P<href>[^"]+)"[^>]*>(?P<body>.*?)</a>',
    re.IGNORECASE | re.DOTALL,
)
_LEVER_TITLE_RE = re.compile(
    r'<(?:h5|span|div)[^>]*class="[^"]*(?:posting-name|posting-title)[^"]*"[^>]*>(?P<title>.*?)</(?:h5|span|div)>',
    re.IGNORECASE | re.DOTALL,
)
_LEVER_LOCATION_RE = re.compile(
    r'<(?:span|div)[^>]*class="[^"]*(?:sort-by-location|posting-categories|location)[^"]*"[^>]*>(?P<location>.*?)</(?:span|div)>',
    re.IGNORECASE | re.DOTALL,
)
_LEVER_PAYLOAD_MARKERS = (
    "window.__LEVER_POSTINGS__ =",
    "window.__LEVER_JOB__ =",
    "window.__INITIAL_STATE__ =",
)


class LeverAdapter:
    adapter_key = "lever"

    def collect(
        self,
        source: SourceInput,
        *,
        timeout_seconds: float,
        fetcher: Fetcher,
    ) -> SourceResult:
        try:
            response = fetch_source(source, timeout_seconds=timeout_seconds, fetcher=fetcher)
        except FetchError as exc:
            return build_skipped_result(
                source,
                adapter_key=self.adapter_key,
                reason_code="fetch_failed",
                reason=str(exc),
                evidence=OutcomeEvidence(error=str(exc)),
            )

        skip_result = inspect_response_for_skip(
            source,
            adapter_key=self.adapter_key,
            response=response,
        )
        if skip_result is not None:
            return skip_result

        fetch_url = response.final_url or source.normalized_url or source.source_url
        direct_job_url = _is_direct_lever_job_url(fetch_url)
        parse_attempts = [
            _parse_lever_embedded_json(response.text, fetch_url),
            _parse_lever_json_ld(response.text, fetch_url),
        ]
        if not direct_job_url:
            parse_attempts.append(_parse_lever_board_html(response.text, fetch_url))

        return finalize_supported_collection(
            source,
            adapter_key=self.adapter_key,
            portal_label="Lever",
            parse_attempts=parse_attempts,
            direct_job_url=direct_job_url,
            ambiguous_reason_code="lever_parse_ambiguous",
            default_manual_review_reason="Lever page did not expose complete importer fields; manual review required.",
            direct_job_reason_code="lever_direct_job_ambiguous",
        )


def _parse_lever_embedded_json(html_text: str, base_url: str) -> ParseAttempt:
    marker = next((candidate for candidate in _LEVER_PAYLOAD_MARKERS if candidate in html_text), None)
    if marker is None:
        return ParseAttempt()
    payload = extract_json_after_marker(html_text, marker)
    if payload is None:
        return ParseAttempt(
            ambiguous_reason="Lever embedded job data was present but could not be parsed; manual review required."
        )

    leads: list[CollectedLead] = []
    if isinstance(payload, dict) and "posting" in payload:
        postings = [payload.get("posting")]
        company = normalize_text(payload.get("company") or payload.get("companyName"))
    elif isinstance(payload, dict):
        postings = payload.get("postings")
        company = normalize_text(payload.get("company") or payload.get("companyName"))
    elif isinstance(payload, list):
        postings = payload
        company = None
    else:
        return ParseAttempt(
            ambiguous_reason="Lever embedded job data used an unsupported structure; manual review required."
        )

    if not isinstance(postings, list):
        return ParseAttempt(
            ambiguous_reason="Lever embedded job data was missing the postings list; manual review required."
        )
    if not postings:
        return ParseAttempt(
            ambiguous_reason="Lever embedded job data exposed no postings; manual review required."
        )

    incomplete_count = 0
    for posting in postings:
        if not isinstance(posting, dict):
            incomplete_count += 1
            continue
        lead = _lead_from_lever_posting(posting, company=company, base_url=base_url)
        if lead is not None:
            leads.append(lead)
            continue
        incomplete_count += 1
    if incomplete_count:
        return ParseAttempt(
            ambiguous_reason=(
                "Lever embedded job data was missing title, company, location, or URL for "
                f"{incomplete_count} posting(s); manual review required."
            )
        )
    if not leads:
        return ParseAttempt(
            ambiguous_reason="Lever embedded job data was present but no complete postings were parseable; manual review required."
        )
    return ParseAttempt(leads=tuple(leads))


def _parse_lever_json_ld(html_text: str, base_url: str) -> ParseAttempt:
    page_company = company_from_page_metadata(html_text, portal_label="Lever")
    payloads = extract_job_posting_nodes(html_text)
    if not payloads:
        return ParseAttempt()

    leads: list[CollectedLead] = []
    incomplete_count = 0
    for payload in payloads:
        title = choose_first_text(payload.get("title"), payload.get("name"))
        company = _lever_company_from_payload(payload) or page_company
        location = _lever_location_from_payload(payload)
        apply_url = build_absolute_url(base_url, payload.get("url")) or base_url
        source_job_id = extract_identifier_value(payload.get("identifier")) or _lever_job_id(apply_url)
        if None in (title, company, location):
            incomplete_count += 1
            continue
        leads.append(
            CollectedLead(
                source="lever",
                company=company,
                title=title,
                location=location,
                apply_url=apply_url,
                source_job_id=source_job_id,
                portal_type="lever",
            )
        )
    if incomplete_count:
        return ParseAttempt(
            ambiguous_reason=(
                "Lever JobPosting data was missing title, company, or location for "
                f"{incomplete_count} posting(s); manual review required."
            )
        )
    if not leads:
        return ParseAttempt(
            ambiguous_reason="Lever JobPosting data was present but no complete postings were parseable; manual review required."
        )
    return ParseAttempt(leads=tuple(leads))


def _parse_lever_board_html(html_text: str, base_url: str) -> ParseAttempt:
    company = company_from_page_metadata(html_text, portal_label="Lever")
    posting_blocks = tuple(_LEVER_POSTING_RE.finditer(html_text))
    if not posting_blocks:
        return ParseAttempt()
    if company is None:
        return ParseAttempt(
            ambiguous_reason="Lever board markup was detected but company metadata was missing; manual review required."
        )

    leads: list[CollectedLead] = []
    incomplete_count = 0
    for match in posting_blocks:
        block = match.group("body")
        link_match = _LEVER_LINK_RE.search(block)
        if link_match is None:
            incomplete_count += 1
            continue
        title_match = _LEVER_TITLE_RE.search(link_match.group("body"))
        location_match = _LEVER_LOCATION_RE.search(link_match.group("body"))
        if title_match is None or location_match is None:
            incomplete_count += 1
            continue
        title = strip_html_tags(title_match.group("title"))
        location = strip_html_tags(location_match.group("location"))
        apply_url = build_absolute_url(base_url, link_match.group("href"))
        if None in (title, location, apply_url):
            incomplete_count += 1
            continue
        leads.append(
            CollectedLead(
                source="lever",
                company=company,
                title=title,
                location=location,
                apply_url=apply_url,
                source_job_id=_lever_job_id(apply_url),
                portal_type="lever",
            )
        )
    if incomplete_count:
        return ParseAttempt(
            ambiguous_reason=(
                "Lever board markup was missing title, location, or URL for "
                f"{incomplete_count} posting(s); manual review required."
            )
        )
    if not leads:
        return ParseAttempt(
            ambiguous_reason="Lever board markup was detected but no complete postings were parseable; manual review required."
        )
    return ParseAttempt(leads=tuple(leads))


def _lead_from_lever_posting(
    posting: dict[str, object],
    *,
    company: str | None,
    base_url: str,
) -> CollectedLead | None:
    title = choose_first_text(posting.get("text"), posting.get("title"), posting.get("name"))
    company_name = company or normalize_text(posting.get("company"))
    location = _lever_location_from_payload(posting)
    apply_url = (
        build_absolute_url(base_url, posting.get("hostedUrl"))
        or build_absolute_url(base_url, posting.get("absolute_url"))
        or build_absolute_url(base_url, posting.get("applyUrl"))
        or build_absolute_url(base_url, posting.get("url"))
    )
    source_job_id = choose_first_text(posting.get("id")) or _lever_job_id(apply_url or base_url)
    if None in (title, company_name, location, apply_url):
        return None
    return CollectedLead(
        source="lever",
        company=company_name,
        title=title,
        location=location,
        apply_url=apply_url,
        source_job_id=source_job_id,
        portal_type="lever",
    )


def _lever_company_from_payload(payload: dict[str, object]) -> str | None:
    hiring_organization = payload.get("hiringOrganization")
    if isinstance(hiring_organization, dict):
        company = normalize_text(hiring_organization.get("name"))
        if company is not None:
            return company
    return normalize_text(payload.get("company"))


def _lever_location_from_payload(payload: dict[str, object]) -> str | None:
    location = extract_location_text(payload.get("jobLocation"))
    if location is not None:
        return location
    categories = payload.get("categories")
    if isinstance(categories, dict):
        location = choose_first_text(
            categories.get("location"),
            categories.get("team"),
        )
        if location is not None:
            return location
    return choose_first_text(payload.get("location"), payload.get("locationName"))


def _lever_job_id(value: str) -> str | None:
    segments = url_path_segments(value)
    if len(segments) >= 2:
        return normalize_text(segments[-1])
    return None


def _is_direct_lever_job_url(value: str) -> bool:
    return len(url_path_segments(value)) >= 2
