from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from .resume.recommendations import QueueRecommendation, select_queue_recommendations


@dataclass(frozen=True, slots=True)
class LaunchPreview:
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


def select_launch_preview(
    database_path: Path,
    *,
    limit: int | None = None,
    ingest_batch_id: str | None = None,
    query_text: str | None = None,
    us_only: bool = False,
) -> tuple[LaunchPreview, ...]:
    recommendations = select_queue_recommendations(
        database_path,
        limit=limit,
        ingest_batch_id=ingest_batch_id,
        query_text=query_text,
        us_only=us_only,
    )
    return tuple(_preview_from_recommendation(recommendation) for recommendation in recommendations)


def _preview_from_recommendation(recommendation: QueueRecommendation) -> LaunchPreview:
    return LaunchPreview(
        rank=recommendation.rank,
        job_id=recommendation.job_id,
        company=recommendation.company,
        title=recommendation.title,
        location=recommendation.location,
        apply_url=recommendation.apply_url,
        portal_type=recommendation.portal_type,
        source=recommendation.source,
        score=recommendation.score,
        resume_variant_key=recommendation.resume_variant_key,
        resume_variant_label=recommendation.resume_variant_label,
        snippet_key=recommendation.snippet_key,
        snippet_label=recommendation.snippet_label,
        snippet_text=recommendation.snippet_text,
        explanation=recommendation.explanation,
    )
