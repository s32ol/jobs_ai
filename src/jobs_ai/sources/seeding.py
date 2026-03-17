from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from pathlib import Path

from ..collect.fetch import Fetcher, fetch_text
from ..db import initialize_schema
from ..source_seed.harness import run_source_seeding
from ..source_seed.models import CompanySeedResult, ManualReviewSourceHint
from ..source_seed.starter_lists import (
    available_starter_lists,
    resolve_starter_lists,
    starter_lists_help_text,
)
from ..workspace import WorkspacePaths
from .intake import LoadedDiscoveryInput, load_discovery_inputs
from .models import SourceRegistrySeedBulkResult, SourceRegistrySeedItemResult
from .registry import (
    normalize_registry_source_url,
    register_verified_source,
    upsert_registry_source,
)


def available_seed_bulk_starter_lists() -> tuple[str, ...]:
    return available_starter_lists()


def seed_bulk_starter_help_text() -> str:
    return starter_lists_help_text()


def seed_registry_bulk(
    paths: WorkspacePaths,
    *,
    companies: Sequence[str],
    from_file: Path | None,
    starter_lists: Sequence[str],
    timeout_seconds: float,
    created_at: datetime | None = None,
    fetcher: Fetcher | None = None,
) -> SourceRegistrySeedBulkResult:
    resolved_starter_lists = resolve_starter_lists(tuple(starter_lists))
    loaded_inputs = load_discovery_inputs(
        command_label="sources seed-bulk",
        companies=companies,
        from_file=from_file,
        starter_lists=resolved_starter_lists,
    )
    if not loaded_inputs:
        raise ValueError(
            "at least one company entry is required via arguments, --from-file, or --starter"
        )

    return seed_registry_loaded_inputs(
        paths,
        loaded_inputs=loaded_inputs,
        starter_lists=resolved_starter_lists,
        timeout_seconds=timeout_seconds,
        created_at=created_at,
        fetcher=fetcher,
    )


def seed_registry_loaded_inputs(
    paths: WorkspacePaths,
    *,
    loaded_inputs: Sequence[LoadedDiscoveryInput],
    starter_lists: Sequence[str],
    timeout_seconds: float,
    created_at: datetime | None = None,
    fetcher: Fetcher | None = None,
) -> SourceRegistrySeedBulkResult:
    if not loaded_inputs:
        raise ValueError("at least one loaded discovery input is required")

    initialize_schema(paths.database_path)
    seed_run = run_source_seeding(
        tuple(item.company_input for item in loaded_inputs),
        timeout_seconds=timeout_seconds,
        created_at=created_at,
        fetcher=fetch_text if fetcher is None else fetcher,
    )
    provenance_by_index = {
        item.company_input.index: item.provenance
        for item in loaded_inputs
    }

    item_results: list[SourceRegistrySeedItemResult] = []
    for company_result in seed_run.report.company_results:
        item_results.extend(
            _item_results_for_company_result(
                paths,
                company_result=company_result,
                provenance=provenance_by_index[company_result.company_input.index],
                created_at=created_at,
            )
        )

    return SourceRegistrySeedBulkResult(
        seed_run=seed_run,
        starter_lists=tuple(starter_lists),
        item_results=tuple(item_results),
    )


def _item_results_for_company_result(
    paths: WorkspacePaths,
    *,
    company_result: CompanySeedResult,
    provenance: str,
    created_at: datetime | None,
) -> list[SourceRegistrySeedItemResult]:
    item_results: list[SourceRegistrySeedItemResult] = []
    seen_manual_review_urls: set[str] = set()
    confirmed_urls = {
        normalized_url
        for normalized_url in (
            _normalized_registry_url(source_url)
            for source_url in company_result.confirmed_sources
        )
        if normalized_url is not None
    }

    confirmed_candidate_results = [
        candidate_result
        for candidate_result in company_result.attempted_candidates
        if candidate_result.outcome == "confirmed"
    ]
    for candidate_result in confirmed_candidate_results:
        source_url = candidate_result.confirmed_url or candidate_result.candidate.url
        mutation = register_verified_source(
            paths.database_path,
            source_url=source_url,
            portal_type=candidate_result.candidate.portal_type,
            company=candidate_result.detected_company or company_result.company_input.company,
            label=_registry_label(company_result),
            provenance=provenance,
            verification_reason_code="confirmed_via_seed_bulk",
            verification_reason=(
                f'confirmed from bulk seed input "{company_result.company_input.raw_value}"'
            ),
            created_at=created_at,
        )
        item_results.append(
            SourceRegistrySeedItemResult(
                raw_input=company_result.company_input.raw_value,
                outcome="confirmed",
                reason_code=candidate_result.reason_code,
                reason=candidate_result.reason,
                source_url=mutation.entry.source_url,
                portal_type=mutation.entry.portal_type,
                mutation=mutation,
            )
        )

    if company_result.outcome != "confirmed":
        for candidate_result in company_result.attempted_candidates:
            if candidate_result.outcome != "manual_review":
                continue
            source_url = candidate_result.confirmed_url or candidate_result.candidate.url
            normalized_url = _normalized_registry_url(source_url)
            if normalized_url is None or normalized_url in seen_manual_review_urls:
                continue
            seen_manual_review_urls.add(normalized_url)
            if normalized_url in confirmed_urls:
                continue
            mutation = upsert_registry_source(
                paths.database_path,
                source_url=source_url,
                portal_type=candidate_result.candidate.portal_type,
                company=candidate_result.detected_company or company_result.company_input.company,
                label=_registry_label(company_result),
                status="manual_review",
                provenance=provenance,
                verification_reason_code=candidate_result.reason_code,
                verification_reason=candidate_result.reason,
                created_at=created_at,
                preserve_existing_active=True,
                mark_verified_at=True,
            )
            item_results.append(
                SourceRegistrySeedItemResult(
                    raw_input=company_result.company_input.raw_value,
                    outcome="manual_review",
                    reason_code=candidate_result.reason_code,
                    reason=candidate_result.reason,
                    source_url=mutation.entry.source_url,
                    portal_type=mutation.entry.portal_type,
                    mutation=mutation,
                )
            )

        for manual_review_source in company_result.manual_review_sources:
            normalized_url = _normalized_registry_url(manual_review_source.source_url)
            if normalized_url is None or normalized_url in seen_manual_review_urls:
                continue
            seen_manual_review_urls.add(normalized_url)
            if normalized_url in confirmed_urls:
                continue
            mutation = _register_manual_review_source_hint(
                paths,
                company_result=company_result,
                manual_review_source=manual_review_source,
                provenance=provenance,
                created_at=created_at,
            )
            item_results.append(
                SourceRegistrySeedItemResult(
                    raw_input=company_result.company_input.raw_value,
                    outcome="manual_review",
                    reason_code=manual_review_source.reason_code,
                    reason=manual_review_source.reason,
                    source_url=mutation.entry.source_url,
                    portal_type=mutation.entry.portal_type,
                    mutation=mutation,
                )
            )

    if item_results:
        return item_results

    return [
        SourceRegistrySeedItemResult(
            raw_input=company_result.company_input.raw_value,
            outcome="failed",
            reason_code=company_result.reason_code,
            reason=company_result.reason,
        )
    ]


def _register_manual_review_source_hint(
    paths: WorkspacePaths,
    *,
    company_result: CompanySeedResult,
    manual_review_source: ManualReviewSourceHint,
    provenance: str,
    created_at: datetime | None,
):
    return upsert_registry_source(
        paths.database_path,
        source_url=manual_review_source.source_url,
        portal_type=manual_review_source.portal_type,
        company=manual_review_source.detected_company or company_result.company_input.company,
        label=_registry_label(company_result),
        status="manual_review",
        provenance=provenance,
        verification_reason_code=manual_review_source.reason_code,
        verification_reason=manual_review_source.reason,
        created_at=created_at,
        preserve_existing_active=True,
        mark_verified_at=True,
    )


def _normalized_registry_url(source_url: str | None) -> str | None:
    if source_url is None:
        return None
    try:
        normalized_url, _ = normalize_registry_source_url(source_url)
    except ValueError:
        return None
    return normalized_url


def _registry_label(company_result: CompanySeedResult) -> str:
    return company_result.company_input.company or company_result.company_input.raw_value
