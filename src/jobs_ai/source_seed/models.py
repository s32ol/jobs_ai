from __future__ import annotations

from dataclasses import dataclass, replace
from pathlib import Path
from typing import Literal

from ..collect.models import OutcomeEvidence

SeedOutcome = Literal["confirmed", "manual_review", "skipped"]
CandidateConfidence = Literal["high", "medium", "low"]


@dataclass(frozen=True, slots=True)
class CompanySeedInput:
    index: int
    raw_value: str
    company: str | None
    domain: str | None
    notes: str | None


@dataclass(frozen=True, slots=True)
class SlugCandidate:
    slug: str
    slug_source: str
    confidence: CandidateConfidence


@dataclass(frozen=True, slots=True)
class SourceCandidate:
    index: int
    portal_type: str
    slug: str
    url: str
    slug_source: str
    confidence: CandidateConfidence


@dataclass(frozen=True, slots=True)
class CandidateResult:
    candidate: SourceCandidate
    outcome: SeedOutcome
    reason_code: str
    reason: str
    suggested_next_action: str | None = None
    detected_company: str | None = None
    confirmed_url: str | None = None
    evidence: OutcomeEvidence | None = None


@dataclass(frozen=True, slots=True)
class CompanySeedResult:
    company_input: CompanySeedInput
    outcome: SeedOutcome
    reason_code: str
    reason: str
    confirmed_sources: tuple[str, ...] = ()
    attempted_candidates: tuple[CandidateResult, ...] = ()
    suggested_next_action: str | None = None
    evidence: OutcomeEvidence | None = None


@dataclass(frozen=True, slots=True)
class SourceSeedArtifactPaths:
    output_dir: Path
    confirmed_sources_path: Path | None
    manual_review_sources_path: Path | None
    seed_report_path: Path


@dataclass(frozen=True, slots=True)
class SourceSeedRunReport:
    created_at: str
    finished_at: str | None
    run_id: str | None
    label: str | None
    timeout_seconds: float
    report_only: bool
    input_companies: tuple[CompanySeedInput, ...]
    input_company_count: int
    confirmed_count: int
    manual_review_count: int
    skipped_count: int
    confirmed_source_count: int
    company_results: tuple[CompanySeedResult, ...]
    artifact_paths: SourceSeedArtifactPaths | None = None


@dataclass(frozen=True, slots=True)
class SourceSeedRun:
    report: SourceSeedRunReport
    confirmed_sources: tuple[str, ...]

    def with_finalization(
        self,
        *,
        artifact_paths: SourceSeedArtifactPaths,
        run_id: str,
        finished_at: str,
    ) -> SourceSeedRun:
        return replace(
            self,
            report=replace(
                self.report,
                artifact_paths=artifact_paths,
                run_id=run_id,
                finished_at=finished_at,
            ),
        )
