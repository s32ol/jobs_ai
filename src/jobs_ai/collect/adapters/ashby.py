from __future__ import annotations

from ..fetch import FetchError, Fetcher
from ..models import CensusSourceResult, CollectedLead, OutcomeEvidence, SourceInput, SourceResult
from .base import (
    ParseAttempt,
    build_absolute_url,
    build_failed_census_result,
    build_response_evidence,
    build_skipped_result,
    choose_first_text,
    company_from_page_metadata,
    extract_identifier_value,
    extract_job_posting_nodes,
    extract_json_after_marker,
    extract_location_text,
    extract_script_body,
    fetch_source,
    finalize_supported_census,
    finalize_supported_collection,
    inspect_response_for_skip,
    normalize_text,
    url_path_segments,
)


class AshbyAdapter:
    adapter_key = "ashby"

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
        direct_job_url = _is_direct_ashby_job_url(fetch_url)
        return finalize_supported_collection(
            source,
            adapter_key=self.adapter_key,
            portal_label="Ashby",
            parse_attempts=(
                _parse_ashby_next_data(response.text, fetch_url),
                _parse_ashby_json_ld(response.text, fetch_url),
            ),
            direct_job_url=direct_job_url,
            ambiguous_reason_code="ashby_parse_ambiguous",
            default_manual_review_reason="Ashby page did not expose complete importer fields; manual review required.",
            direct_job_reason_code="ashby_direct_job_ambiguous",
        )

    def census(
        self,
        source: SourceInput,
        *,
        timeout_seconds: float,
        fetcher: Fetcher,
    ) -> CensusSourceResult:
        try:
            response = fetch_source(source, timeout_seconds=timeout_seconds, fetcher=fetcher)
        except FetchError as exc:
            return build_failed_census_result(
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
            return build_failed_census_result(
                source,
                adapter_key=self.adapter_key,
                reason_code=skip_result.reason_code,
                reason=skip_result.reason,
                evidence=skip_result.evidence,
            )

        fetch_url = response.final_url or source.normalized_url or source.source_url
        return finalize_supported_census(
            source,
            adapter_key=self.adapter_key,
            parse_attempts=(
                _parse_ashby_next_data(response.text, fetch_url),
                _parse_ashby_json_ld(response.text, fetch_url),
            ),
            default_failure_reason="Ashby board did not expose a reliable posting count; census failed.",
            evidence=build_response_evidence(response),
        )


def _parse_ashby_next_data(html_text: str, base_url: str) -> ParseAttempt:
    has_next_data_marker = "window.__NEXT_DATA__ =" in html_text or "__NEXT_DATA__" in html_text
    if not has_next_data_marker:
        return ParseAttempt()

    payload = extract_json_after_marker(html_text, "window.__NEXT_DATA__ =")
    if payload is None:
        script_body = extract_script_body(html_text, script_id="__NEXT_DATA__", script_type="application/json")
        if script_body is not None:
            payload = extract_json_after_marker(f"window.__NEXT_DATA__ = {script_body}", "window.__NEXT_DATA__ =")
    if not isinstance(payload, dict):
        return ParseAttempt(
            ambiguous_reason="Ashby __NEXT_DATA__ payload was present but could not be parsed; manual review required."
        )

    page_props = payload.get("props", {})
    if not isinstance(page_props, dict):
        return ParseAttempt(
            ambiguous_reason="Ashby page data was missing the props payload; manual review required."
        )
    page_props = page_props.get("pageProps", {})
    if not isinstance(page_props, dict):
        return ParseAttempt(
            ambiguous_reason="Ashby page data was missing pageProps; manual review required."
        )

    company = _ashby_company_from_page_props(page_props) or company_from_page_metadata(
        html_text,
        portal_label="Ashby",
    )
    if company is None:
        return ParseAttempt(
            ambiguous_reason="Ashby page data was present but company metadata was missing; manual review required."
        )

    if "job" in page_props:
        direct_job = page_props.get("job")
        if not isinstance(direct_job, dict):
            return ParseAttempt(
                ambiguous_reason="Ashby direct-job payload used an unsupported structure; manual review required."
            )
        lead = _lead_from_ashby_payload(direct_job, company=company, base_url=base_url, direct_url=base_url)
        if lead is None:
            return ParseAttempt(
                ambiguous_reason="Ashby direct-job payload was missing title, location, or URL; manual review required."
            )
        return ParseAttempt(leads=(lead,))

    postings = None
    for key in ("jobs", "jobPostings", "openings"):
        if key in page_props:
            postings = page_props.get(key)
            break
    if postings is None:
        return ParseAttempt(
            ambiguous_reason="Ashby page data was present but no supported jobs collection was found; manual review required."
        )
    if not isinstance(postings, list):
        return ParseAttempt(
            ambiguous_reason="Ashby jobs collection used an unsupported structure; manual review required."
        )
    if not postings:
        return ParseAttempt(recognized_empty=True)
    leads: list[CollectedLead] = []
    incomplete_count = 0
    for posting in postings:
        if not isinstance(posting, dict):
            incomplete_count += 1
            continue
        lead = _lead_from_ashby_payload(posting, company=company, base_url=base_url, direct_url=None)
        if lead is not None:
            leads.append(lead)
            continue
        incomplete_count += 1
    if incomplete_count:
        return ParseAttempt(
            ambiguous_reason=(
                "Ashby jobs collection was missing title, location, or URL for "
                f"{incomplete_count} posting(s); manual review required."
            )
        )
    if not leads:
        return ParseAttempt(
            ambiguous_reason="Ashby jobs collection was present but no complete postings were parseable; manual review required."
        )
    return ParseAttempt(leads=tuple(leads))


def _parse_ashby_json_ld(html_text: str, base_url: str) -> ParseAttempt:
    page_company = company_from_page_metadata(html_text, portal_label="Ashby")
    payloads = extract_job_posting_nodes(html_text)
    if not payloads:
        return ParseAttempt()

    leads: list[CollectedLead] = []
    incomplete_count = 0
    for payload in payloads:
        title = choose_first_text(payload.get("title"), payload.get("name"))
        company = _ashby_company_from_payload(payload) or page_company
        location = _ashby_location_from_payload(payload)
        apply_url = build_absolute_url(base_url, payload.get("url")) or base_url
        source_job_id = extract_identifier_value(payload.get("identifier")) or _ashby_job_id(apply_url)
        if None in (title, company, location):
            incomplete_count += 1
            continue
        leads.append(
            CollectedLead(
                source="ashby",
                company=company,
                title=title,
                location=location,
                apply_url=apply_url,
                source_job_id=source_job_id,
                portal_type="ashby",
            )
        )
    if incomplete_count:
        return ParseAttempt(
            ambiguous_reason=(
                "Ashby JobPosting data was missing title, company, or location for "
                f"{incomplete_count} posting(s); manual review required."
            )
        )
    if not leads:
        return ParseAttempt(
            ambiguous_reason="Ashby JobPosting data was present but no complete postings were parseable; manual review required."
        )
    return ParseAttempt(leads=tuple(leads))


def _lead_from_ashby_payload(
    posting: dict[str, object],
    *,
    company: str,
    base_url: str,
    direct_url: str | None,
) -> CollectedLead | None:
    title = choose_first_text(posting.get("title"), posting.get("name"))
    location = _ashby_location_from_payload(posting)
    apply_url = (
        build_absolute_url(base_url, posting.get("applyUrl"))
        or build_absolute_url(base_url, posting.get("hostedUrl"))
        or build_absolute_url(base_url, posting.get("jobUrl"))
        or build_absolute_url(base_url, posting.get("url"))
        or build_absolute_url(base_url, posting.get("jobPostingUrl"))
        or direct_url
    )
    source_job_id = choose_first_text(posting.get("id"), posting.get("jobId")) or _ashby_job_id(apply_url or base_url)
    if None in (title, location, apply_url):
        return None
    return CollectedLead(
        source="ashby",
        company=company,
        title=title,
        location=location,
        apply_url=apply_url,
        source_job_id=source_job_id,
        portal_type="ashby",
    )


def _ashby_company_from_page_props(payload: dict[str, object]) -> str | None:
    for key in ("organization", "company"):
        value = payload.get(key)
        if isinstance(value, dict):
            company = normalize_text(value.get("name"))
            if company is not None:
                return company
    return normalize_text(payload.get("companyName"))


def _ashby_company_from_payload(payload: dict[str, object]) -> str | None:
    for key in ("organization", "company", "hiringOrganization"):
        value = payload.get(key)
        if isinstance(value, dict):
            company = normalize_text(value.get("name"))
            if company is not None:
                return company
    return normalize_text(payload.get("company"))


def _ashby_location_from_payload(payload: dict[str, object]) -> str | None:
    location = extract_location_text(payload.get("jobLocation"))
    if location is not None:
        return location
    location = extract_location_text(payload.get("location"))
    if location is not None:
        return location
    if payload.get("isRemote") is True:
        return "Remote"
    return choose_first_text(payload.get("locationName"), payload.get("workplaceType"))


def _ashby_job_id(value: str) -> str | None:
    segments = url_path_segments(value)
    if len(segments) >= 2:
        return normalize_text(segments[-1])
    return None


def _is_direct_ashby_job_url(value: str) -> bool:
    return len(url_path_segments(value)) >= 2
