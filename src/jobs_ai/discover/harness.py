from __future__ import annotations

from collections import OrderedDict
from dataclasses import replace
from datetime import datetime, timezone

from ..collect.fetch import FetchError, Fetcher, fetch_text
from ..collect.harness import run_collection
from ..portal_support import build_portal_support, extract_portal_board_root_url
from .models import DiscoverCandidate, DiscoverCandidateResult, DiscoverRun, DiscoverRunReport, SearchExecutionResult, SearchHit
from .search import build_search_plans, execute_search_plan

SUPPORTED_DISCOVER_PORTALS = frozenset({"greenhouse", "lever", "ashby"})


def run_discovery(
    query: str,
    *,
    limit: int,
    timeout_seconds: float,
    label: str | None = None,
    report_only: bool = False,
    collect_requested: bool = False,
    import_requested: bool = False,
    created_at: datetime | None = None,
    fetcher: Fetcher = fetch_text,
) -> DiscoverRun:
    normalized_query = query.strip()
    if not normalized_query:
        raise ValueError("query must not be blank")
    if limit < 1:
        raise ValueError("limit must be at least 1")

    created_at_dt = _normalize_created_at(created_at)
    search_results: list[SearchExecutionResult] = []
    raw_hit_count = 0
    candidate_map: OrderedDict[str, DiscoverCandidate] = OrderedDict()
    manual_review_map: OrderedDict[str, DiscoverCandidateResult] = OrderedDict()

    for plan in build_search_plans(normalized_query):
        try:
            search_result, hits = execute_search_plan(
                plan,
                timeout_seconds=timeout_seconds,
                fetcher=fetcher,
            )
        except FetchError as exc:
            search_results.append(
                SearchExecutionResult(
                    plan=plan,
                    hit_count=0,
                    error=str(exc),
                )
            )
            continue

        search_results.append(search_result)
        raw_hit_count += len(hits)
        for hit in hits:
            candidate, review_result = _classify_search_hit(hit)
            if review_result is not None:
                review_key = _manual_review_key(review_result)
                existing_review = manual_review_map.get(review_key)
                if existing_review is None:
                    manual_review_map[review_key] = review_result
                else:
                    manual_review_map[review_key] = replace(
                        existing_review,
                        candidate=DiscoverCandidate(
                            portal_type=existing_review.candidate.portal_type,
                            source_url=existing_review.candidate.source_url,
                            normalized_url=existing_review.candidate.normalized_url,
                            supporting_results=_merge_hits(
                                existing_review.candidate.supporting_results,
                                review_result.candidate.supporting_results,
                            ),
                        ),
                    )
                continue
            assert candidate is not None
            assert candidate.source_url is not None
            existing = candidate_map.get(candidate.source_url)
            if existing is None:
                candidate_map[candidate.source_url] = candidate
                continue
            candidate_map[candidate.source_url] = DiscoverCandidate(
                portal_type=existing.portal_type,
                source_url=existing.source_url,
                normalized_url=existing.normalized_url,
                supporting_results=_merge_hits(existing.supporting_results, candidate.supporting_results),
            )

    candidate_results: list[DiscoverCandidateResult] = []
    confirmed_sources: list[str] = []

    for index, candidate in enumerate(candidate_map.values(), start=1):
        if index > limit:
            candidate_results.append(
                DiscoverCandidateResult(
                    candidate=candidate,
                    outcome="skipped",
                    reason_code="candidate_limit_reached",
                    reason=(
                        f"candidate was discovered but not verified because --limit {limit} was reached"
                    ),
                    suggested_next_action=(
                        "Rerun discover with a higher --limit to verify more ATS sources from the same query."
                    ),
                )
            )
            continue
        result = _verify_candidate(
            candidate,
            timeout_seconds=timeout_seconds,
            created_at=created_at_dt,
            fetcher=fetcher,
        )
        candidate_results.append(result)
        if result.confirmed_source is not None and result.confirmed_source not in confirmed_sources:
            confirmed_sources.append(result.confirmed_source)

    candidate_results.extend(manual_review_map.values())

    report = DiscoverRunReport(
        created_at=_format_created_at(created_at_dt),
        finished_at=None,
        run_id=None,
        label=label,
        query=normalized_query,
        limit=limit,
        timeout_seconds=timeout_seconds,
        report_only=report_only,
        collect_requested=collect_requested,
        import_requested=import_requested,
        search_results=tuple(search_results),
        raw_hit_count=raw_hit_count,
        candidate_source_count=len(candidate_map),
        verified_candidate_count=min(len(candidate_map), limit),
        confirmed_count=sum(1 for result in candidate_results if result.outcome == "confirmed"),
        manual_review_count=sum(1 for result in candidate_results if result.outcome == "manual_review"),
        skipped_count=sum(1 for result in candidate_results if result.outcome == "skipped"),
        candidate_results=tuple(candidate_results),
    )
    return DiscoverRun(
        report=report,
        confirmed_sources=tuple(confirmed_sources),
    )


def _classify_search_hit(
    hit: SearchHit,
) -> tuple[DiscoverCandidate | None, DiscoverCandidateResult | None]:
    portal_support = build_portal_support(hit.target_url)
    normalized_url = (
        portal_support.normalized_apply_url
        if portal_support is not None
        else hit.target_url
    )
    portal_type = portal_support.portal_type if portal_support is not None else None
    board_root = extract_portal_board_root_url(hit.target_url, portal_type=portal_type)

    if portal_support is None:
        return (
            None,
            DiscoverCandidateResult(
                candidate=DiscoverCandidate(
                    portal_type=None,
                    source_url=None,
                    normalized_url=normalized_url,
                    supporting_results=(hit,),
                ),
                outcome="manual_review",
                reason_code="unsupported_search_result",
                reason="search result was not a supported Greenhouse, Lever, or Ashby URL",
                suggested_next_action=(
                    "Review the result manually and keep only confirmed Greenhouse, Lever, or Ashby sources."
                ),
            ),
        )

    if portal_type == "workday":
        return (
            None,
            DiscoverCandidateResult(
                candidate=DiscoverCandidate(
                    portal_type=portal_type,
                    source_url=None,
                    normalized_url=normalized_url,
                    supporting_results=(hit,),
                ),
                outcome="manual_review",
                reason_code="workday_manual_review",
                reason="Workday portal detected during discovery; surfaced for manual review only.",
                suggested_next_action=(
                    "Open the normalized Workday job URL manually and capture any leads outside the supported Greenhouse, Lever, and Ashby flow."
                ),
            ),
        )

    if portal_type not in SUPPORTED_DISCOVER_PORTALS:
        return (
            None,
            DiscoverCandidateResult(
                candidate=DiscoverCandidate(
                    portal_type=portal_type,
                    source_url=None,
                    normalized_url=normalized_url,
                    supporting_results=(hit,),
                ),
                outcome="manual_review",
                reason_code="unsupported_portal_type",
                reason=f"{portal_type} results stay manual-review only in discover v1",
                suggested_next_action=(
                    "Review the result manually or source a supported Greenhouse, Lever, or Ashby board root."
                ),
            ),
        )

    if board_root is None:
        return (
            None,
            DiscoverCandidateResult(
                candidate=DiscoverCandidate(
                    portal_type=portal_type,
                    source_url=None,
                    normalized_url=normalized_url,
                    supporting_results=(hit,),
                ),
                outcome="manual_review",
                reason_code="unable_to_normalize_board_root",
                reason="supported portal result could not be normalized to a reusable board-root source",
                suggested_next_action=(
                    "Review the result manually and keep only reusable board-root URLs in confirmed sources."
                ),
            ),
        )

    return (
        DiscoverCandidate(
            portal_type=portal_type,
            source_url=board_root,
            normalized_url=board_root,
            supporting_results=(hit,),
        ),
        None,
    )


def _verify_candidate(
    candidate: DiscoverCandidate,
    *,
    timeout_seconds: float,
    created_at: datetime,
    fetcher: Fetcher,
) -> DiscoverCandidateResult:
    assert candidate.source_url is not None

    run = run_collection(
        [candidate.source_url],
        timeout_seconds=timeout_seconds,
        created_at=created_at,
        fetcher=fetcher,
    )
    source_result = run.report.source_results[0]
    if source_result.outcome == "collected":
        return DiscoverCandidateResult(
            candidate=candidate,
            outcome="confirmed",
            reason_code="confirmed_supported_source",
            reason=f"verified reusable {candidate.portal_type} source",
            confirmed_source=source_result.source.normalized_url or candidate.source_url,
            collected_lead_count=len(source_result.collected_leads),
            evidence=source_result.evidence,
        )

    reason_code = f"verification_{source_result.reason_code}"
    suggested_next_action = source_result.suggested_next_action
    if suggested_next_action is None:
        suggested_next_action = (
            "Review the discovered source manually before adding it to a reusable sources file."
        )
    return DiscoverCandidateResult(
        candidate=candidate,
        outcome="manual_review",
        reason_code=reason_code,
        reason=source_result.reason,
        suggested_next_action=suggested_next_action,
        collected_lead_count=len(source_result.collected_leads),
        evidence=source_result.evidence,
    )


def _manual_review_key(result: DiscoverCandidateResult) -> str:
    candidate = result.candidate
    target_url = candidate.normalized_url or candidate.source_url or "<missing>"
    return f"{result.reason_code}|{target_url}"


def _merge_hits(
    existing_hits: tuple[SearchHit, ...],
    new_hits: tuple[SearchHit, ...],
) -> tuple[SearchHit, ...]:
    merged: OrderedDict[str, SearchHit] = OrderedDict()
    for hit in existing_hits + new_hits:
        merged.setdefault(hit.target_url, hit)
    return tuple(merged.values())


def _normalize_created_at(created_at: datetime | None) -> datetime:
    if created_at is None:
        return datetime.now(timezone.utc)
    if created_at.tzinfo is None:
        return created_at.replace(tzinfo=timezone.utc)
    return created_at.astimezone(timezone.utc)


def _format_created_at(created_at: datetime) -> str:
    return created_at.isoformat(timespec="seconds").replace("+00:00", "Z")
