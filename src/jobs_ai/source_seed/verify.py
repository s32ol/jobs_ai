from __future__ import annotations

from html.parser import HTMLParser
from urllib.parse import urljoin
from urllib.parse import urlparse

from ..collect.adapters import DEFAULT_ADAPTERS
from ..collect.adapters.base import build_response_evidence, detect_generic_page_patterns
from ..collect.fetch import FetchRequest, FetchResponse, Fetcher
from ..collect.models import SourceInput
from ..portal_support import build_portal_support, extract_portal_board_root_url
from .infer import company_identity_key
from .models import CandidateResult, CompanySeedInput, ManualReviewSourceHint, SourceCandidate

_ASHBY_INCONCLUSIVE_HTTP_STATUS_CODES = frozenset({401, 403, 429, 500, 502, 503, 504})


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
        ashby_manual_review = _manual_review_for_direct_ashby_inconclusive_result(
            company_input,
            candidate,
            source_result,
            response=response,
            evidence=evidence,
        )
        if ashby_manual_review is not None:
            return ashby_manual_review
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


def discover_source_candidates_from_career_page(
    company_input: CompanySeedInput,
    *,
    timeout_seconds: float,
    fetcher: Fetcher,
) -> tuple[SourceCandidate, ...]:
    if company_input.career_page_url is None:
        return ()

    direct_candidate = discover_supported_source_candidate_from_url(
        company_input.career_page_url,
    )
    if direct_candidate is not None:
        return (direct_candidate,)

    try:
        response = fetcher(
            FetchRequest(
                url=company_input.career_page_url,
                timeout_seconds=timeout_seconds,
                headers={"Accept": "text/html"},
            )
        )
    except Exception:
        return ()

    candidates = []
    seen_urls: set[str] = set()

    direct_response_candidate = _supported_source_candidate_from_url(
        response.final_url or company_input.career_page_url,
        index=1,
        slug_source="career_page",
    )
    if direct_response_candidate is not None:
        seen_urls.add(direct_response_candidate.url)
        candidates.append(direct_response_candidate)

    parser = _AnchorExtractor()
    parser.feed(response.text)
    for href in parser.links:
        absolute_url = urljoin(response.final_url or company_input.career_page_url, href)
        board_root_candidate = _supported_source_candidate_from_url(
            absolute_url,
            index=len(candidates) + 1,
            slug_source="career_page",
        )
        if board_root_candidate is None or board_root_candidate.url in seen_urls:
            continue
        seen_urls.add(board_root_candidate.url)
        candidates.append(board_root_candidate)
    return tuple(candidates)


def discover_supported_source_candidate_from_url(
    value: str | None,
    *,
    index: int = 1,
    slug_source: str = "career_page",
) -> SourceCandidate | None:
    return _supported_source_candidate_from_url(
        value,
        index=index,
        slug_source=slug_source,
    )


def discover_manual_review_sources_from_career_page(
    company_input: CompanySeedInput,
    *,
    timeout_seconds: float,
    fetcher: Fetcher,
) -> tuple[ManualReviewSourceHint, ...]:
    if company_input.career_page_url is None:
        return ()

    hints: list[ManualReviewSourceHint] = []
    seen_urls: set[str] = set()

    direct_hint = _manual_review_hint_from_url(
        company_input.career_page_url,
        reason_code="workday_partial_support",
        reason="Workday portal detected during source seeding; keep it in manual review only.",
    )
    if direct_hint is not None:
        seen_urls.add(direct_hint.source_url)
        hints.append(direct_hint)
        return tuple(hints)

    try:
        response = fetcher(
            FetchRequest(
                url=company_input.career_page_url,
                timeout_seconds=timeout_seconds,
                headers={"Accept": "text/html"},
            )
        )
    except Exception:
        return ()

    response_hint = _manual_review_hint_from_url(
        response.final_url or company_input.career_page_url,
        reason_code="workday_partial_support",
        reason="Workday portal detected during source seeding; keep it in manual review only.",
        evidence=_candidate_evidence(response),
    )
    if response_hint is not None:
        seen_urls.add(response_hint.source_url)
        hints.append(response_hint)

    parser = _AnchorExtractor()
    parser.feed(response.text)
    for href in parser.links:
        absolute_url = urljoin(response.final_url or company_input.career_page_url, href)
        hint = _manual_review_hint_from_url(
            absolute_url,
            reason_code="workday_partial_support",
            reason="Workday portal detected during source seeding; keep it in manual review only.",
        )
        if hint is None or hint.source_url in seen_urls:
            continue
        seen_urls.add(hint.source_url)
        hints.append(hint)
    return tuple(hints)


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


class _AnchorExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.links: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() != "a":
            return
        attr_map = {
            key.lower(): value
            for key, value in attrs
            if value is not None
        }
        href = attr_map.get("href")
        if href:
            self.links.append(href)


def _slug_from_board_root(board_root_url: str) -> str:
    path_segments = tuple(segment for segment in urlparse(board_root_url).path.split("/") if segment)
    if not path_segments:
        return "career-page"
    return path_segments[0]


def _supported_source_candidate_from_url(
    value: str | None,
    *,
    index: int,
    slug_source: str,
) -> SourceCandidate | None:
    portal_support = build_portal_support(value)
    if portal_support is None or portal_support.portal_type == "workday":
        return None

    board_root_url = extract_portal_board_root_url(
        portal_support.normalized_apply_url,
        portal_type=portal_support.portal_type,
    )
    if board_root_url is None:
        return None

    return SourceCandidate(
        index=index,
        portal_type=portal_support.portal_type,
        slug=_slug_from_board_root(board_root_url),
        url=board_root_url,
        slug_source=slug_source,
        confidence="high",
    )


def _manual_review_hint_from_url(
    value: str | None,
    *,
    reason_code: str,
    reason: str,
    evidence=None,
) -> ManualReviewSourceHint | None:
    portal_support = build_portal_support(value)
    if portal_support is None or portal_support.portal_type != "workday":
        return None
    return ManualReviewSourceHint(
        source_url=portal_support.normalized_apply_url,
        portal_type=portal_support.portal_type,
        reason_code=reason_code,
        reason=reason,
        suggested_next_action=(
            "Keep the Workday URL in the registry for manual review, but do not rely on it as a structured collector source."
        ),
        evidence=evidence,
    )


def _manual_review_for_direct_ashby_inconclusive_result(
    company_input: CompanySeedInput,
    candidate: SourceCandidate,
    source_result,
    *,
    response: FetchResponse | None,
    evidence,
) -> CandidateResult | None:
    if not _has_strong_direct_ashby_evidence(company_input, candidate):
        return None

    if source_result.reason_code == "blocked_or_access_denied":
        mapped_reason_code = "ashby_blocked_or_access_denied"
    elif source_result.reason_code == "fetch_failed":
        mapped_reason_code = "ashby_fetch_inconclusive"
    elif (
        source_result.reason_code == "http_error_status"
        and response is not None
        and response.status_code in _ASHBY_INCONCLUSIVE_HTTP_STATUS_CODES
    ):
        mapped_reason_code = "ashby_http_inconclusive"
    else:
        return None

    return CandidateResult(
        candidate=candidate,
        outcome="manual_review",
        reason_code=mapped_reason_code,
        reason=(
            "direct Ashby board root looked structurally valid, but verification was "
            "inconclusive; keep it in manual review instead of discarding it"
        ),
        suggested_next_action=(
            "Keep the direct Ashby URL in manual review and verify it in a browser "
            "before discarding it."
        ),
        evidence=evidence,
    )


def _has_strong_direct_ashby_evidence(
    company_input: CompanySeedInput,
    candidate: SourceCandidate,
) -> bool:
    if candidate.portal_type != "ashby" or candidate.slug_source != "career_page":
        return False
    if company_input.career_page_url is None:
        return False
    board_root_url = extract_portal_board_root_url(
        candidate.url,
        portal_type=candidate.portal_type,
    )
    if board_root_url != candidate.url:
        return False
    parsed_url = urlparse(candidate.url)
    return parsed_url.netloc.lower() == "jobs.ashbyhq.com"
