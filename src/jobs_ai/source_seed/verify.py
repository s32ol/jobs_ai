from __future__ import annotations

from urllib.parse import urlparse

from ..collect.adapters import DEFAULT_ADAPTERS
from ..collect.adapters.base import build_response_evidence, detect_generic_page_patterns
from ..collect.fetch import FetchResponse, Fetcher
from ..collect.models import SourceInput
from ..portal_support import build_portal_support
from .infer import company_identity_key
from .models import CandidateResult, CompanySeedInput, SourceCandidate


def verify_source_candidate(
    company_input: CompanySeedInput,
    candidate: SourceCandidate,
    *,
    timeout_seconds: float,
    fetcher: Fetcher,
) -> CandidateResult:
    if company_input.company is None:
        return CandidateResult(
            candidate=candidate,
            outcome="skipped",
            reason_code="missing_company_name",
            reason="company name is required to seed ATS sources",
            suggested_next_action="Provide a company name, optionally with a domain, and rerun seed-sources.",
        )

    portal_support = build_portal_support(candidate.url, portal_type=candidate.portal_type)
    source = SourceInput(
        index=candidate.index,
        source_url=candidate.url,
        normalized_url=(
            portal_support.normalized_apply_url
            if portal_support is not None
            else candidate.url
        ),
        portal_type=candidate.portal_type,
        portal_support=portal_support,
    )

    recording_fetcher = _RecordingFetcher(fetcher)
    adapter = DEFAULT_ADAPTERS[candidate.portal_type]
    source_result = adapter.collect(
        source,
        timeout_seconds=timeout_seconds,
        fetcher=recording_fetcher,
    )
    response = recording_fetcher.last_response
    evidence = source_result.evidence or _candidate_evidence(response)

    if response is not None and _is_direct_job_url(candidate.portal_type, response.final_url):
        return CandidateResult(
            candidate=candidate,
            outcome="manual_review",
            reason_code="redirected_to_direct_job",
            reason="candidate redirected to a direct job page instead of staying on a board root",
            suggested_next_action="Review the candidate manually and keep only board-root URLs in reusable source files.",
            evidence=evidence,
        )

    if source_result.outcome == "skipped":
        return CandidateResult(
            candidate=candidate,
            outcome="skipped",
            reason_code=source_result.reason_code,
            reason=source_result.reason,
            suggested_next_action=source_result.suggested_next_action,
            evidence=evidence,
        )

    if source_result.outcome == "manual_review":
        return CandidateResult(
            candidate=candidate,
            outcome="manual_review",
            reason_code=source_result.reason_code,
            reason=source_result.reason,
            suggested_next_action=(
                source_result.suggested_next_action
                or "Review the candidate manually before adding it to a reusable sources file."
            ),
            evidence=evidence,
        )

    detected_companies = tuple(
        dict.fromkeys(
            lead.company
            for lead in source_result.collected_leads
            if lead.company
        )
    )
    if not detected_companies:
        return CandidateResult(
            candidate=candidate,
            outcome="manual_review",
            reason_code="missing_detected_company",
            reason="portal parsing succeeded but no company name could be confirmed from collected postings",
            suggested_next_action="Open the candidate manually and confirm the company name before reusing this board root.",
            evidence=evidence,
        )
    if len(detected_companies) > 1:
        return CandidateResult(
            candidate=candidate,
            outcome="manual_review",
            reason_code="multiple_detected_companies",
            reason=(
                "portal parsing returned postings for multiple company names; unable to confirm one board root safely"
            ),
            suggested_next_action="Review the candidate manually and keep only the board root that clearly matches the target company.",
            evidence=evidence,
        )

    detected_company = detected_companies[0]
    if not company_names_match(company_input.company, detected_company):
        return CandidateResult(
            candidate=candidate,
            outcome="manual_review",
            reason_code="company_name_mismatch",
            reason=(
                f"candidate resolved to {detected_company!r}, which does not match the input company "
                f"{company_input.company!r} strongly enough for auto-confirmation"
            ),
            suggested_next_action="Review the candidate manually and confirm the hosted company name before reusing it.",
            detected_company=detected_company,
            evidence=evidence,
        )

    confirmed_url = _confirmed_board_root_url(candidate, response)
    return CandidateResult(
        candidate=candidate,
        outcome="confirmed",
        reason_code="confirmed_board_root",
        reason=f"confirmed {candidate.portal_type} board root for {detected_company}",
        detected_company=detected_company,
        confirmed_url=confirmed_url,
        evidence=evidence,
    )


def company_names_match(expected_company: str | None, detected_company: str | None) -> bool:
    expected_key = company_identity_key(expected_company)
    detected_key = company_identity_key(detected_company)
    if expected_key is None or detected_key is None:
        return False
    return expected_key == detected_key


def _candidate_evidence(response: FetchResponse | None):
    if response is None:
        return None
    return build_response_evidence(
        response,
        detected_patterns=detect_generic_page_patterns(response.text),
    )


def _confirmed_board_root_url(candidate: SourceCandidate, response: FetchResponse | None) -> str:
    if response is None:
        return candidate.url
    portal_support = build_portal_support(response.final_url, portal_type=candidate.portal_type)
    if portal_support is None:
        return candidate.url
    if _is_direct_job_url(candidate.portal_type, portal_support.normalized_apply_url):
        return candidate.url
    return portal_support.normalized_apply_url


def _is_direct_job_url(portal_type: str, value: str | None) -> bool:
    if value is None:
        return False
    path_segments = tuple(segment for segment in urlparse(value).path.split("/") if segment)
    if not path_segments:
        return False
    if portal_type == "greenhouse":
        return len(path_segments) >= 3 and path_segments[-2] == "jobs"
    if portal_type in {"lever", "ashby"}:
        return len(path_segments) >= 2
    return False


class _RecordingFetcher:
    def __init__(self, wrapped: Fetcher) -> None:
        self._wrapped = wrapped
        self.last_response: FetchResponse | None = None

    def __call__(self, request):
        response = self._wrapped(request)
        self.last_response = response
        return response
