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
    extract_json_after_marker,
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
_GREENHOUSE_STATE_MARKERS = (
    "window.__remixContext =",
    "window.__remixContext=",
    "window.__initialState__ =",
    "window.__initialState__=",
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
        parse_attempts = [
            _parse_greenhouse_remix(response.text, fetch_url, direct_job_url=direct_job_url),
            _parse_greenhouse_json_ld(response.text, fetch_url),
        ]
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


def _parse_greenhouse_remix(
    html_text: str,
    base_url: str,
    *,
    direct_job_url: bool,
) -> ParseAttempt:
    payload = _extract_greenhouse_state_payload(html_text)
    if payload is None:
        return ParseAttempt()

    loader_data = _greenhouse_loader_data(payload)
    if loader_data is None:
        return ParseAttempt(
            ambiguous_reason="Greenhouse embedded page data was missing loader data; manual review required."
        )

    if direct_job_url:
        return _parse_greenhouse_remix_direct_job(loader_data, base_url)
    return _parse_greenhouse_remix_board(loader_data, base_url)


def _extract_greenhouse_state_payload(html_text: str) -> dict[str, object] | None:
    for marker in _GREENHOUSE_STATE_MARKERS:
        if marker not in html_text:
            continue
        payload = extract_json_after_marker(html_text, marker)
        if isinstance(payload, dict):
            return payload
        return None
    return None


def _parse_greenhouse_remix_board(loader_data: dict[str, object], base_url: str) -> ParseAttempt:
    company = None
    posting_records: list[object] = []
    for route_payload in loader_data.values():
        if not isinstance(route_payload, dict):
            continue
        company = company or _greenhouse_company_from_board_route(route_payload)
        posting_records.extend(_greenhouse_postings_from_route(route_payload))

    if not posting_records:
        return ParseAttempt()
    if company is None:
        return ParseAttempt(
            ambiguous_reason="Greenhouse embedded board data was present but company metadata was missing; manual review required."
        )

    leads: list[CollectedLead] = []
    incomplete_count = 0
    for posting in posting_records:
        if not isinstance(posting, dict):
            incomplete_count += 1
            continue
        lead = _lead_from_greenhouse_board_posting(posting, company=company, base_url=base_url)
        if lead is not None:
            leads.append(lead)
            continue
        incomplete_count += 1
    if incomplete_count:
        return ParseAttempt(
            ambiguous_reason=(
                "Greenhouse embedded board data was missing title, location, or URL for "
                f"{incomplete_count} posting(s); manual review required."
            )
        )
    if not leads:
        return ParseAttempt(
            ambiguous_reason="Greenhouse embedded board data was present but no complete postings were parseable; manual review required."
        )
    return ParseAttempt(leads=tuple(leads))


def _parse_greenhouse_remix_direct_job(loader_data: dict[str, object], base_url: str) -> ParseAttempt:
    for route_payload in loader_data.values():
        if not isinstance(route_payload, dict):
            continue
        job_post = route_payload.get("jobPost")
        if not isinstance(job_post, dict):
            continue

        company = choose_first_text(
            job_post.get("company_name"),
            job_post.get("companyName"),
            job_post.get("company"),
        ) or _greenhouse_company_from_board_route(route_payload)
        lead = _lead_from_greenhouse_direct_job(
            job_post,
            route_payload=route_payload,
            company=company,
            base_url=base_url,
        )
        if lead is None:
            return ParseAttempt(
                ambiguous_reason="Greenhouse direct-job page data was missing title, company, location, or URL; manual review required."
            )
        return ParseAttempt(leads=(lead,))
    return ParseAttempt()


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


def _greenhouse_loader_data(payload: dict[str, object]) -> dict[str, object] | None:
    state = payload.get("state")
    if not isinstance(state, dict):
        return None
    loader_data = state.get("loaderData")
    if not isinstance(loader_data, dict):
        return None
    return loader_data


def _greenhouse_company_from_board_route(route_payload: dict[str, object]) -> str | None:
    board = route_payload.get("board")
    if not isinstance(board, dict):
        return None
    return choose_first_text(board.get("name"), board.get("company_name"), board.get("companyName"))


def _greenhouse_postings_from_route(route_payload: dict[str, object]) -> list[object]:
    postings: list[object] = []
    for key in ("jobPosts", "featuredPosts"):
        payload = route_payload.get(key)
        if isinstance(payload, dict):
            data = payload.get("data")
            if isinstance(data, list):
                postings.extend(data)
        elif isinstance(payload, list):
            postings.extend(payload)
    return postings


def _lead_from_greenhouse_board_posting(
    posting: dict[str, object],
    *,
    company: str,
    base_url: str,
) -> CollectedLead | None:
    title = choose_first_text(posting.get("title"), posting.get("text"), posting.get("name"))
    location = choose_first_text(posting.get("location"), posting.get("job_post_location")) or _greenhouse_location_from_payload(
        posting
    )
    apply_url = (
        build_absolute_url(base_url, posting.get("absolute_url"))
        or build_absolute_url(base_url, posting.get("public_url"))
        or build_absolute_url(base_url, posting.get("url"))
    )
    if None in (title, location, apply_url):
        return None
    return CollectedLead(
        source="greenhouse",
        company=company,
        title=title,
        location=location,
        apply_url=apply_url,
        source_job_id=(
            _greenhouse_job_id(apply_url)
            or _greenhouse_scalar_text(
                posting.get("id"),
                posting.get("job_post_id"),
                posting.get("internal_job_id"),
            )
        ),
        portal_type="greenhouse",
        posted_at=choose_first_text(posting.get("published_at"), posting.get("posted_at")),
    )


def _lead_from_greenhouse_direct_job(
    payload: dict[str, object],
    *,
    route_payload: dict[str, object],
    company: str | None,
    base_url: str,
) -> CollectedLead | None:
    title = choose_first_text(payload.get("title"), payload.get("name"))
    location = choose_first_text(payload.get("job_post_location"), payload.get("location")) or _greenhouse_location_from_payload(
        payload
    )
    apply_url = (
        build_absolute_url(base_url, payload.get("public_url"))
        or build_absolute_url(base_url, payload.get("absolute_url"))
        or build_absolute_url(base_url, route_payload.get("submitPath"))
        or base_url
    )
    if None in (title, company, location):
        return None
    return CollectedLead(
        source="greenhouse",
        company=company,
        title=title,
        location=location,
        apply_url=apply_url,
        source_job_id=(
            _greenhouse_job_id(apply_url)
            or _greenhouse_scalar_text(
                route_payload.get("jobPostId"),
                payload.get("id"),
                payload.get("job_post_id"),
                payload.get("hiring_plan_id"),
            )
        ),
        portal_type="greenhouse",
        salary_text=_greenhouse_salary_text(payload.get("pay_ranges")),
        posted_at=choose_first_text(payload.get("published_at"), payload.get("posted_at")),
    )


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


def _greenhouse_scalar_text(*values: object) -> str | None:
    for value in values:
        text = normalize_text(value)
        if text is not None:
            return text
        if isinstance(value, bool):
            continue
        if isinstance(value, int):
            return str(value)
        if isinstance(value, float) and value.is_integer():
            return str(int(value))
    return None


def _greenhouse_salary_text(value: object) -> str | None:
    if not isinstance(value, list):
        return None

    parts: list[str] = []
    for entry in value:
        if not isinstance(entry, dict):
            continue
        label = choose_first_text(entry.get("title"))
        bounds = [
            bound
            for bound in (
                choose_first_text(entry.get("min")),
                choose_first_text(entry.get("max")),
            )
            if bound is not None
        ]
        amount_text = " - ".join(bounds)
        currency = choose_first_text(entry.get("currency_type"))
        if currency is not None:
            amount_text = f"{amount_text} {currency}".strip()
        description = choose_first_text(entry.get("description"))

        segment = amount_text or description
        if segment is None:
            continue
        if label is not None:
            segment = f"{label}: {segment}"
        parts.append(segment)

    if not parts:
        return None
    return "; ".join(parts)


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
