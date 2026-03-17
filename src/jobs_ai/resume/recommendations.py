from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from ..config import TARGET_ROLES
from ..jobs.queue import RankedQueuedJob, select_ranked_apply_queue
from ..jobs.scoring import ScoredJob
from .config import get_profile_snippet, get_resume_variant

TELEMETRY_KEYWORDS = ("telemetry", "observability")
ANALYTICS_KEYWORDS = ("analytics engineer", "analytics engineering")
DATA_ENGINEERING_KEYWORDS = ("platform data engineer", "data engineer")


@dataclass(frozen=True, slots=True)
class QueueRecommendation:
    rank: int
    job_id: int
    company: str
    title: str
    location: str | None
    apply_url: str | None
    portal_type: str | None
    source: str
    score: int
    resume_variant_key: str
    resume_variant_label: str
    snippet_key: str
    snippet_label: str
    snippet_text: str
    explanation: str


@dataclass(frozen=True, slots=True)
class _RecommendationDecision:
    resume_variant_key: str
    snippet_key: str
    explanation_prefix: str


def select_queue_recommendations(
    database_path: Path,
    *,
    limit: int | None = None,
    ingest_batch_id: str | None = None,
    query_text: str | None = None,
) -> tuple[QueueRecommendation, ...]:
    queued_jobs = select_ranked_apply_queue(
        database_path,
        limit=limit,
        ingest_batch_id=ingest_batch_id,
        query_text=query_text,
    )
    return tuple(recommend_queued_job(job) for job in queued_jobs)


def recommend_queued_job(queued_job: RankedQueuedJob) -> QueueRecommendation:
    decision = _decide_recommendation(queued_job.scored_job)
    resume_variant = get_resume_variant(decision.resume_variant_key)
    snippet = get_profile_snippet(decision.snippet_key)
    return QueueRecommendation(
        rank=queued_job.rank,
        job_id=queued_job.scored_job.job_id,
        company=queued_job.scored_job.company,
        title=queued_job.scored_job.title,
        location=queued_job.scored_job.location,
        apply_url=queued_job.scored_job.apply_url,
        portal_type=queued_job.scored_job.portal_type,
        source=queued_job.scored_job.source,
        score=queued_job.scored_job.total_score,
        resume_variant_key=resume_variant.key,
        resume_variant_label=resume_variant.label,
        snippet_key=snippet.key,
        snippet_label=snippet.label,
        snippet_text=snippet.text,
        explanation=_build_explanation(decision.explanation_prefix, queued_job),
    )


def _decide_recommendation(job: ScoredJob) -> _RecommendationDecision:
    normalized_title = job.title.strip().lower()
    matched_stack_keywords = set(job.matched_stack_keywords)

    if (
        job.matched_target_role == TARGET_ROLES[2]
        or _contains_keyword(normalized_title, TELEMETRY_KEYWORDS)
        or "telemetry/observability" in matched_stack_keywords
    ):
        return _RecommendationDecision(
            resume_variant_key="telemetry-observability",
            snippet_key="observability-signals",
            explanation_prefix="matched telemetry / observability signals from title or stack",
        )

    if (
        job.matched_target_role == TARGET_ROLES[1]
        or _contains_keyword(normalized_title, ANALYTICS_KEYWORDS)
        or "Looker" in matched_stack_keywords
    ):
        return _RecommendationDecision(
            resume_variant_key="analytics-engineering",
            snippet_key="analytics-modeling",
            explanation_prefix="matched analytics engineering signals from title or stack",
        )

    if (
        job.matched_target_role in {TARGET_ROLES[0], TARGET_ROLES[3], TARGET_ROLES[4]}
        or _contains_keyword(normalized_title, DATA_ENGINEERING_KEYWORDS)
        or bool({"Python", "BigQuery", "GCP"} & matched_stack_keywords)
    ):
        snippet_key = "pipeline-delivery" if {"Python", "BigQuery", "GCP"} & matched_stack_keywords else "general-data-platform"
        return _RecommendationDecision(
            resume_variant_key="data-engineering",
            snippet_key=snippet_key,
            explanation_prefix="matched data engineering signals from title or stack",
        )

    return _RecommendationDecision(
        resume_variant_key="general-data",
        snippet_key="general-data-platform",
        explanation_prefix="no strong specialization signal matched, using the general data profile",
    )


def _build_explanation(explanation_prefix: str, queued_job: RankedQueuedJob) -> str:
    if queued_job.reason_summary == "no strong score signals yet":
        return explanation_prefix
    return f"{explanation_prefix}; queue signals: {queued_job.reason_summary}"


def _contains_keyword(text: str, keywords: tuple[str, ...]) -> bool:
    return any(keyword in text for keyword in keywords)
