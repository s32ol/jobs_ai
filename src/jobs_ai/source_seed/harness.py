from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime, timezone

from ..collect.fetch import Fetcher, fetch_text
from .infer import build_source_candidates
from .models import CompanySeedInput, CompanySeedResult, SourceSeedRun, SourceSeedRunReport
from .verify import (
    discover_manual_review_sources_from_career_page,
    discover_supported_source_candidate_from_url,
    discover_source_candidates_from_career_page,
    verify_source_candidate,
)


def run_source_seeding(
    company_inputs: Sequence[CompanySeedInput],
    *,
    timeout_seconds: float,
    label: str | None = None,
    report_only: bool = False,
    created_at: datetime | None = None,
    fetcher: Fetcher = fetch_text,
) -> SourceSeedRun:
    created_at_dt = _normalize_created_at(created_at)
    company_results: list[CompanySeedResult] = []
    confirmed_sources: list[str] = []
    seen_confirmed_sources: set[str] = set()

    for company_input in company_inputs:
        result = _seed_company(
            company_input,
            timeout_seconds=timeout_seconds,
            fetcher=fetcher,
        )
        company_results.append(result)
        for confirmed_source in result.confirmed_sources:
            if confirmed_source in seen_confirmed_sources:
                continue
            seen_confirmed_sources.add(confirmed_source)
            confirmed_sources.append(confirmed_source)

    report = SourceSeedRunReport(
        created_at=_format_created_at(created_at_dt),
        finished_at=None,
        run_id=None,
        label=label,
        timeout_seconds=timeout_seconds,
        report_only=report_only,
        input_companies=tuple(company_inputs),
        input_company_count=len(company_inputs),
        confirmed_count=sum(1 for result in company_results if result.outcome == "confirmed"),
        manual_review_count=sum(1 for result in company_results if result.outcome == "manual_review"),
        skipped_count=sum(1 for result in company_results if result.outcome == "skipped"),
        confirmed_source_count=len(confirmed_sources),
        company_results=tuple(company_results),
    )
    return SourceSeedRun(
        report=report,
        confirmed_sources=tuple(confirmed_sources),
    )


def _seed_company(
    company_input: CompanySeedInput,
    *,
    timeout_seconds: float,
    fetcher: Fetcher,
) -> CompanySeedResult:
    manual_review_sources = discover_manual_review_sources_from_career_page(
        company_input,
        timeout_seconds=timeout_seconds,
        fetcher=fetcher,
    )
    if company_input.company is None:
        if manual_review_sources:
            primary_manual_review = manual_review_sources[0]
            return CompanySeedResult(
                company_input=company_input,
                outcome="manual_review",
                reason_code=primary_manual_review.reason_code,
                reason=primary_manual_review.reason,
                manual_review_sources=manual_review_sources,
                suggested_next_action=primary_manual_review.suggested_next_action,
                evidence=primary_manual_review.evidence,
            )
        return CompanySeedResult(
            company_input=company_input,
            outcome="skipped",
            reason_code="missing_company_name",
            reason="company name is required to seed ATS sources",
            suggested_next_action="Add a company name, optionally with a domain, and rerun seed-sources.",
        )

    direct_candidates = discover_source_candidates_from_career_page(
        company_input,
        timeout_seconds=timeout_seconds,
        fetcher=fetcher,
    )
    direct_input_candidate = discover_supported_source_candidate_from_url(
        company_input.career_page_url,
    )
    candidates = _merge_source_candidates(
        direct_candidates,
        ()
        if direct_input_candidate is not None
        else build_source_candidates(company_input),
    )
    if not candidates:
        return CompanySeedResult(
            company_input=company_input,
            outcome="skipped",
            reason_code="no_slug_candidates",
            reason="could not infer any conservative ATS slug candidates from the provided company input",
            suggested_next_action="Provide a cleaner company name or a company domain and rerun seed-sources.",
        )

    attempted_candidates = []
    confirmed_sources: list[str] = []
    confirmed_portals: set[str] = set()
    for candidate in candidates:
        if candidate.portal_type in confirmed_portals:
            continue
        candidate_result = verify_source_candidate(
            company_input,
            candidate,
            timeout_seconds=timeout_seconds,
            fetcher=fetcher,
        )
        attempted_candidates.append(candidate_result)
        if candidate_result.outcome != "confirmed":
            continue
        confirmed_portals.add(candidate.portal_type)
        confirmed_source = candidate_result.confirmed_url or candidate.url
        if confirmed_source not in confirmed_sources:
            confirmed_sources.append(confirmed_source)

    if confirmed_sources:
        confirmed_evidence = next(
            (
                candidate_result.evidence
                for candidate_result in attempted_candidates
                if candidate_result.outcome == "confirmed"
            ),
            None,
        )
        return CompanySeedResult(
            company_input=company_input,
            outcome="confirmed",
            reason_code="confirmed_sources_found",
            reason=f"confirmed {len(confirmed_sources)} ATS board root(s)",
            confirmed_sources=tuple(confirmed_sources),
            manual_review_sources=manual_review_sources,
            attempted_candidates=tuple(attempted_candidates),
            evidence=confirmed_evidence,
        )

    manual_review_attempts = [
        candidate_result
        for candidate_result in attempted_candidates
        if candidate_result.outcome == "manual_review"
    ]
    if manual_review_attempts:
        primary = manual_review_attempts[0]
        return CompanySeedResult(
            company_input=company_input,
            outcome="manual_review",
            reason_code=primary.reason_code,
            reason=_primary_reason(primary.reason, len(manual_review_attempts)),
            confirmed_sources=(),
            manual_review_sources=manual_review_sources,
            attempted_candidates=tuple(attempted_candidates),
            suggested_next_action=(
                primary.suggested_next_action
                or "Review the attempted candidates manually before adding a reusable board-root source."
            ),
            evidence=primary.evidence,
        )

    if manual_review_sources:
        primary_manual_review = manual_review_sources[0]
        return CompanySeedResult(
            company_input=company_input,
            outcome="manual_review",
            reason_code=primary_manual_review.reason_code,
            reason=_primary_reason(primary_manual_review.reason, len(manual_review_sources)),
            confirmed_sources=(),
            manual_review_sources=manual_review_sources,
            attempted_candidates=tuple(attempted_candidates),
            suggested_next_action=primary_manual_review.suggested_next_action,
            evidence=primary_manual_review.evidence,
        )

    skipped_attempts = [
        candidate_result
        for candidate_result in attempted_candidates
        if candidate_result.outcome == "skipped"
    ]
    if skipped_attempts:
        primary = skipped_attempts[0]
        return CompanySeedResult(
            company_input=company_input,
            outcome="skipped",
            reason_code=primary.reason_code,
            reason=_primary_reason(primary.reason, len(skipped_attempts)),
            manual_review_sources=manual_review_sources,
            attempted_candidates=tuple(attempted_candidates),
            suggested_next_action=primary.suggested_next_action,
            evidence=primary.evidence,
        )

    return CompanySeedResult(
        company_input=company_input,
        outcome="skipped",
        reason_code="no_candidates_attempted",
        reason="no ATS candidates were attempted for this company input",
        manual_review_sources=manual_review_sources,
        attempted_candidates=tuple(attempted_candidates),
    )


def _primary_reason(reason: str, candidate_count: int) -> str:
    if candidate_count <= 1:
        return reason
    return f"{candidate_count} attempted candidates shared the same primary outcome; first detail: {reason}"


def _normalize_created_at(created_at: datetime | None) -> datetime:
    if created_at is None:
        return datetime.now(timezone.utc)
    if created_at.tzinfo is None:
        return created_at.replace(tzinfo=timezone.utc)
    return created_at.astimezone(timezone.utc)


def _format_created_at(created_at: datetime) -> str:
    return created_at.isoformat(timespec="seconds").replace("+00:00", "Z")


def _merge_source_candidates(
    primary_candidates,
    fallback_candidates,
):
    merged = []
    seen_urls = set()
    for candidate in primary_candidates + fallback_candidates:
        if candidate.url in seen_urls:
            continue
        seen_urls.add(candidate.url)
        merged.append(candidate)
    return tuple(merged)
