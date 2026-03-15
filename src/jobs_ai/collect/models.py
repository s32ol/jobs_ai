from __future__ import annotations

from dataclasses import dataclass, replace
from pathlib import Path
from typing import Literal

from ..portal_support import PortalSupport

CollectionOutcome = Literal["collected", "manual_review", "skipped"]


@dataclass(frozen=True, slots=True)
class SourceInput:
    index: int
    source_url: str
    normalized_url: str | None
    portal_type: str | None
    portal_support: PortalSupport | None


@dataclass(frozen=True, slots=True)
class CollectedLead:
    source: str
    company: str
    title: str
    location: str
    apply_url: str | None = None
    source_job_id: str | None = None
    portal_type: str | None = None
    salary_text: str | None = None
    posted_at: str | None = None
    found_at: str | None = None

    def to_import_record(self) -> dict[str, str | None]:
        return {
            "source": self.source,
            "company": self.company,
            "title": self.title,
            "location": self.location,
            "apply_url": self.apply_url,
            "source_job_id": self.source_job_id,
            "portal_type": self.portal_type,
            "salary_text": self.salary_text,
            "posted_at": self.posted_at,
            "found_at": self.found_at,
        }


@dataclass(frozen=True, slots=True)
class ManualReviewItem:
    source_url: str
    normalized_url: str | None
    portal_type: str | None
    adapter_key: str
    reason_code: str
    reason: str
    suggested_next_action: str | None = None
    company_apply_url: str | None = None
    hints: tuple[str, ...] = ()
    evidence: "OutcomeEvidence | None" = None


@dataclass(frozen=True, slots=True)
class OutcomeEvidence:
    final_url: str | None = None
    status_code: int | None = None
    content_type: str | None = None
    page_title: str | None = None
    detected_patterns: tuple[str, ...] = ()
    error: str | None = None


@dataclass(frozen=True, slots=True)
class SourceResult:
    source: SourceInput
    adapter_key: str
    outcome: CollectionOutcome
    reason_code: str
    reason: str
    suggested_next_action: str | None = None
    evidence: OutcomeEvidence | None = None
    collected_leads: tuple[CollectedLead, ...] = ()
    manual_review_item: ManualReviewItem | None = None


@dataclass(frozen=True, slots=True)
class CollectArtifactPaths:
    output_dir: Path
    leads_path: Path | None
    manual_review_path: Path | None
    run_report_path: Path


@dataclass(frozen=True, slots=True)
class CollectRunReport:
    created_at: str
    finished_at: str | None
    run_id: str | None
    label: str | None
    timeout_seconds: float
    report_only: bool
    input_sources: tuple[str, ...]
    input_source_count: int
    collected_count: int
    manual_review_count: int
    skipped_count: int
    source_results: tuple[SourceResult, ...]
    artifact_paths: CollectArtifactPaths | None = None


@dataclass(frozen=True, slots=True)
class CollectRun:
    report: CollectRunReport
    collected_leads: tuple[CollectedLead, ...]
    manual_review_items: tuple[ManualReviewItem, ...]

    def with_finalization(
        self,
        *,
        artifact_paths: CollectArtifactPaths,
        run_id: str,
        finished_at: str,
    ) -> CollectRun:
        return replace(
            self,
            report=replace(
                self.report,
                artifact_paths=artifact_paths,
                run_id=run_id,
                finished_at=finished_at,
            ),
        )
