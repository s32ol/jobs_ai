from __future__ import annotations

from dataclasses import dataclass, replace
from pathlib import Path
from typing import Literal

from ..collect.models import OutcomeEvidence

DiscoverOutcome = Literal["confirmed", "manual_review", "skipped"]
SearchExecutionStatus = Literal[
    "success",
    "zero_results",
    "provider_anomaly",
    "parse_failure",
    "fetch_failure",
]
SEARCH_FAILURE_STATUSES = frozenset({"provider_anomaly", "parse_failure", "fetch_failure"})


@dataclass(frozen=True, slots=True)
class SearchPlan:
    portal_type: str
    site_filter: str
    search_text: str
    search_url: str


@dataclass(frozen=True, slots=True)
class SearchExecutionResult:
    plan: SearchPlan
    status: SearchExecutionStatus
    hit_count: int
    attempt_count: int = 1
    error: str | None = None
    evidence: OutcomeEvidence | None = None
    attempts: tuple["SearchAttempt", ...] = ()
    raw_artifact_paths: tuple[Path, ...] = ()


@dataclass(frozen=True, slots=True)
class SearchAttempt:
    attempt_number: int
    status: SearchExecutionStatus
    hit_count: int
    error: str | None = None
    evidence: OutcomeEvidence | None = None
    raw_artifact_path: Path | None = None


@dataclass(frozen=True, slots=True)
class SearchHit:
    search_text: str
    search_url: str
    target_url: str
    title: str | None = None


@dataclass(frozen=True, slots=True)
class DiscoverCandidate:
    portal_type: str | None
    source_url: str | None
    normalized_url: str | None
    supporting_results: tuple[SearchHit, ...]


@dataclass(frozen=True, slots=True)
class DiscoverCandidateResult:
    candidate: DiscoverCandidate
    outcome: DiscoverOutcome
    reason_code: str
    reason: str
    suggested_next_action: str | None = None
    confirmed_source: str | None = None
    collected_lead_count: int = 0
    evidence: OutcomeEvidence | None = None


@dataclass(frozen=True, slots=True)
class DiscoverCollectSummary:
    requested: bool
    executed: bool
    status: str
    output_dir: Path | None = None
    run_report_path: Path | None = None
    leads_path: Path | None = None
    manual_review_path: Path | None = None
    collected_count: int = 0
    manual_review_count: int = 0
    skipped_count: int = 0


@dataclass(frozen=True, slots=True)
class DiscoverImportSummary:
    requested: bool
    executed: bool
    status: str
    input_path: Path | None = None
    batch_id: str | None = None
    source_query: str | None = None
    inserted_count: int = 0
    skipped_count: int = 0
    duplicate_count: int = 0
    skipped: tuple[str, ...] = ()
    errors: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class DiscoverArtifactPaths:
    output_dir: Path
    confirmed_sources_path: Path | None
    manual_review_sources_path: Path | None
    discover_report_path: Path
    search_artifact_dir: Path | None = None


@dataclass(frozen=True, slots=True)
class DiscoverRunReport:
    created_at: str
    finished_at: str | None
    run_id: str | None
    label: str | None
    query: str
    limit: int
    timeout_seconds: float
    report_only: bool
    collect_requested: bool
    import_requested: bool
    search_results: tuple[SearchExecutionResult, ...]
    raw_hit_count: int
    candidate_source_count: int
    verified_candidate_count: int
    confirmed_count: int
    manual_review_count: int
    skipped_count: int
    candidate_results: tuple[DiscoverCandidateResult, ...]
    artifact_paths: DiscoverArtifactPaths | None = None
    collect_summary: DiscoverCollectSummary | None = None
    import_summary: DiscoverImportSummary | None = None

    @property
    def has_search_failures(self) -> bool:
        return any(result.status in SEARCH_FAILURE_STATUSES for result in self.search_results)

    @property
    def has_fatal_search_failure(self) -> bool:
        return self.confirmed_count == 0 and self.has_search_failures


@dataclass(frozen=True, slots=True)
class DiscoverRun:
    report: DiscoverRunReport
    confirmed_sources: tuple[str, ...]

    def with_finalization(
        self,
        *,
        artifact_paths: DiscoverArtifactPaths,
        run_id: str,
        finished_at: str,
        collect_summary: DiscoverCollectSummary | None = None,
        import_summary: DiscoverImportSummary | None = None,
    ) -> DiscoverRun:
        return replace(
            self,
            report=replace(
                self.report,
                artifact_paths=artifact_paths,
                run_id=run_id,
                finished_at=finished_at,
                collect_summary=collect_summary,
                import_summary=import_summary,
            ),
        )
