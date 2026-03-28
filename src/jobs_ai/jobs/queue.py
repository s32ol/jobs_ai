from __future__ import annotations

from contextlib import closing
from dataclasses import dataclass
from pathlib import Path

from ..db import connect_database
from .location_guard import location_allowed_in_us_only_mode
from .query_filter import job_matches_query
from .scoring import ScoredJob, rank_jobs

QUEUEABLE_JOBS_BASE_SQL = """
SELECT
    id,
    source,
    company,
    title,
    location,
    apply_url,
    portal_type,
    posted_at,
    found_at,
    raw_json
FROM jobs
WHERE status = 'new'
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
    ingest_batch_id: str | None = None,
    query_text: str | None = None,
    us_only: bool = False,
) -> tuple[RankedQueuedJob, ...]:
    queueable_jobs_sql, params = _queueable_jobs_query(ingest_batch_id=ingest_batch_id)
    with closing(connect_database(database_path)) as connection:
        rows = connection.execute(
            queueable_jobs_sql,
            params,
        ).fetchall()

    if query_text is not None:
        rows = [row for row in rows if job_matches_query(row, query_text)]
    if us_only:
        rows = [row for row in rows if location_allowed_in_us_only_mode(_location_value(row))]

    ranked_jobs = rank_jobs(rows)
    if limit is not None:
        ranked_jobs = ranked_jobs[:limit]

    return tuple(
        RankedQueuedJob(
            rank=index,
            scored_job=job,
            reason_summary=summarize_queue_reason(job),
        )
        for index, job in enumerate(ranked_jobs, start=1)
    )


def select_apply_queue(
    database_path: Path,
    *,
    limit: int | None = None,
    ingest_batch_id: str | None = None,
    query_text: str | None = None,
    us_only: bool = False,
) -> tuple[QueuedJob, ...]:
    ranked_queued_jobs = select_ranked_apply_queue(
        database_path,
        limit=limit,
        ingest_batch_id=ingest_batch_id,
        query_text=query_text,
        us_only=us_only,
    )
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


def summarize_queue_reason(job: ScoredJob) -> str:
    parts: list[str] = []
    if job.matched_target_role is not None:
        parts.append(f"role={job.matched_target_role}")
    if job.matched_stack_keywords:
        parts.append(f"stack={', '.join(job.matched_stack_keywords)}")
    if job.geography_bucket is not None:
        parts.append(f"geo={job.geography_bucket}")
    if job.source_score > 0 and job.source_category != "unclassified":
        parts.append(f"source={job.source_category}")
    if job.actionability_score < 0:
        parts.append("launch=missing apply_url")
    if parts:
        return "; ".join(parts)
    return "no strong score signals yet"


def _queueable_jobs_query(*, ingest_batch_id: str | None) -> tuple[str, tuple[object, ...]]:
    if ingest_batch_id is None:
        return (
            f"{QUEUEABLE_JOBS_BASE_SQL}\nORDER BY id\n",
            (),
        )
    return (
        f"{QUEUEABLE_JOBS_BASE_SQL}\n  AND ingest_batch_id = ?\nORDER BY id\n",
        (ingest_batch_id,),
    )


def _location_value(row) -> str | None:
    try:
        value = row["location"]
    except (KeyError, TypeError):
        return None
    if value is None:
        return None
    if isinstance(value, str):
        normalized_value = value.strip()
        return normalized_value or None
    return str(value)
