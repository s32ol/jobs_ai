from __future__ import annotations

import re
from urllib.parse import urlparse

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
    extract_location_text,
    fetch_source,
    finalize_supported_collection,
    inspect_response_for_skip,
    normalize_text,
    strip_html_tags,
    url_path_segments,
)

_GREENHOUSE_OPENING_RE = re.compile(
    r'<div[^>]*class="[^"]*\bopening\b[^"]*"[^>]*>'
    r'(?P<body>.*?)'
    r"</div>",
    re.IGNORECASE | re.DOTALL,
)
_GREENHOUSE_LINK_RE = re.compile(
    r'<a[^>]*href="(?P<href>[^"]+)"[^>]*>(?P<title>.*?)</a>',
    re.IGNORECASE | re.DOTALL,
)
_GREENHOUSE_LOCATION_RE = re.compile(
    r'<(?:span|div)[^>]*class="[^"]*\blocation\b[^"]*"[^>]*>(?P<location>.*?)</(?:span|div)>',
    re.IGNORECASE | re.DOTALL,
)


class GreenhouseAdapter:
    adapter_key = "greenhouse"

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
        direct_job_url = _is_direct_greenhouse_job_url(fetch_url)
        parse_attempts = [_parse_greenhouse_json_ld(response.text, fetch_url)]
        if not direct_job_url:
            parse_attempts.append(_parse_greenhouse_board_html(response.text, fetch_url))

        return finalize_supported_collection(
            source,
            adapter_key=self.adapter_key,
            portal_label="Greenhouse",
            parse_attempts=parse_attempts,
            direct_job_url=direct_job_url,
            ambiguous_reason_code="greenhouse_parse_ambiguous",
            default_manual_review_reason=(
                "Greenhouse page did not expose complete importer fields; manual review required."
            ),
            direct_job_reason_code="greenhouse_direct_job_ambiguous",
        )


def _parse_greenhouse_json_ld(html_text: str, base_url: str) -> ParseAttempt:
    page_company = company_from_page_metadata(html_text, portal_label="Greenhouse")
    payloads = extract_job_posting_nodes(html_text)
    if not payloads:
        return ParseAttempt()

    leads: list[CollectedLead] = []
    incomplete_count = 0
    for payload in payloads:
        title = choose_first_text(payload.get("title"), payload.get("name"))
        company = _greenhouse_company_from_payload(payload) or page_company
        location = _greenhouse_location_from_payload(payload)
        apply_url = build_absolute_url(base_url, payload.get("url")) or base_url
        source_job_id = extract_identifier_value(payload.get("identifier")) or _greenhouse_job_id(apply_url)
        if None in (title, company, location):
            incomplete_count += 1
            continue
        leads.append(
            CollectedLead(
                source="greenhouse",
                company=company,
                title=title,
                location=location,
                apply_url=apply_url,
                source_job_id=source_job_id,
                portal_type="greenhouse",
            )
        )
    if incomplete_count:
        return ParseAttempt(
            ambiguous_reason=(
                "Greenhouse JobPosting data was missing title, company, or location for "
                f"{incomplete_count} posting(s); manual review required."
            )
        )
    if not leads:
        return ParseAttempt(
            ambiguous_reason="Greenhouse JobPosting data was present but no complete postings were parseable; manual review required."
        )
    return ParseAttempt(leads=tuple(leads))


def _parse_greenhouse_board_html(html_text: str, base_url: str) -> ParseAttempt:
    company = company_from_page_metadata(html_text, portal_label="Greenhouse")
    opening_blocks = tuple(_GREENHOUSE_OPENING_RE.finditer(html_text))
    if not opening_blocks:
        return ParseAttempt()
    if company is None:
        return ParseAttempt(
            ambiguous_reason="Greenhouse board markup was detected but company metadata was missing; manual review required."
        )

    leads: list[CollectedLead] = []
    incomplete_count = 0
    for match in opening_blocks:
        block = match.group("body")
        link_match = _GREENHOUSE_LINK_RE.search(block)
        location_match = _GREENHOUSE_LOCATION_RE.search(block)
        if link_match is None or location_match is None:
            incomplete_count += 1
            continue
        title = strip_html_tags(link_match.group("title"))
        location = strip_html_tags(location_match.group("location"))
        apply_url = build_absolute_url(base_url, link_match.group("href"))
        if None in (title, location, apply_url):
            incomplete_count += 1
            continue
        leads.append(
            CollectedLead(
                source="greenhouse",
                company=company,
                title=title,
                location=location,
                apply_url=apply_url,
                source_job_id=_greenhouse_job_id(apply_url),
                portal_type="greenhouse",
            )
        )
    if incomplete_count:
        return ParseAttempt(
            ambiguous_reason=(
                "Greenhouse opening markup was missing title, location, or URL for "
                f"{incomplete_count} opening(s); manual review required."
            )
        )
    if not leads:
        return ParseAttempt(
            ambiguous_reason="Greenhouse board markup was detected but no complete openings were parseable; manual review required."
        )
    return ParseAttempt(leads=tuple(leads))


def _greenhouse_company_from_payload(payload: dict[str, object]) -> str | None:
    hiring_organization = payload.get("hiringOrganization")
    if isinstance(hiring_organization, dict):
        company = normalize_text(hiring_organization.get("name"))
        if company is not None:
            return company
    return normalize_text(payload.get("company"))


def _greenhouse_location_from_payload(payload: dict[str, object]) -> str | None:
    location = extract_location_text(payload.get("jobLocation"))
    if location is not None:
        return location
    if normalize_text(payload.get("jobLocationType")) == "TELECOMMUTE":
        return "Remote"
    applicant_location_requirements = payload.get("applicantLocationRequirements")
    location = extract_location_text(applicant_location_requirements)
    if location is not None:
        return location
    if applicant_location_requirements is not None:
        return "Remote"
    return None


def _greenhouse_job_id(value: str) -> str | None:
    segments = url_path_segments(value)
    if len(segments) >= 3 and segments[1] == "jobs":
        return normalize_text(segments[2])
    parsed_url = urlparse(value)
    query_match = re.search(r"(?:^|&)gh_jid=(?P<job_id>\d+)(?:&|$)", parsed_url.query)
    if query_match is None:
        return None
    return normalize_text(query_match.group("job_id"))


def _is_direct_greenhouse_job_url(value: str) -> bool:
    segments = url_path_segments(value)
    return len(segments) >= 3 and segments[1] == "jobs"
