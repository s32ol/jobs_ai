from __future__ import annotations

from contextlib import closing
from dataclasses import dataclass
from pathlib import Path

from ..db import connect_database
from .scoring import ScoredJob, rank_jobs

QUEUEABLE_JOBS_SQL = """
SELECT
    id,
    source,
    company,
    title,
    location,
    apply_url,
    portal_type,
    raw_json
FROM jobs
WHERE status = 'new'
ORDER BY id
"""


@dataclass(frozen=True, slots=True)
class QueuedJob:
    rank: int
    job_id: int
    company: str
    title: str
    location: str | None
    source: str
    score: int
    reason_summary: str


@dataclass(frozen=True, slots=True)
class RankedQueuedJob:
    rank: int
    scored_job: ScoredJob
    reason_summary: str


def select_ranked_apply_queue(
    database_path: Path,
    *,
    limit: int | None = None,
) -> tuple[RankedQueuedJob, ...]:
    with closing(connect_database(database_path)) as connection:
        rows = connection.execute(QUEUEABLE_JOBS_SQL).fetchall()

    ranked_jobs = rank_jobs(rows)
    if limit is not None:
        ranked_jobs = ranked_jobs[:limit]

    return tuple(
        RankedQueuedJob(
            rank=index,
            scored_job=job,
            reason_summary=_queue_reason_summary(job),
        )
        for index, job in enumerate(ranked_jobs, start=1)
    )


def select_apply_queue(database_path: Path, *, limit: int | None = None) -> tuple[QueuedJob, ...]:
    ranked_queued_jobs = select_ranked_apply_queue(database_path, limit=limit)
    return tuple(
        QueuedJob(
            rank=queued_job.rank,
            job_id=queued_job.scored_job.job_id,
            company=queued_job.scored_job.company,
            title=queued_job.scored_job.title,
            location=queued_job.scored_job.location,
            source=queued_job.scored_job.source,
            score=queued_job.scored_job.total_score,
            reason_summary=queued_job.reason_summary,
        )
        for queued_job in ranked_queued_jobs
    )


def _queue_reason_summary(job: ScoredJob) -> str:
    parts: list[str] = []
    if job.matched_target_role is not None:
        parts.append(f"role={job.matched_target_role}")
    if job.matched_stack_keywords:
        parts.append(f"stack={', '.join(job.matched_stack_keywords)}")
    if job.geography_bucket is not None:
        parts.append(f"geo={job.geography_bucket}")
    if job.source_score > 0 and job.source_category != "unclassified":
        parts.append(f"source={job.source_category}")
    if parts:
        return "; ".join(parts)
    return "no strong score signals yet"
