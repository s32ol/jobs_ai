from __future__ import annotations

from contextlib import closing
from dataclasses import dataclass
from pathlib import Path

from .db import connect_database

APPLICATION_STATUSES = ("new", "opened", "applied", "skipped")

LIST_APPLICATION_STATUSES_SQL = """
SELECT
    jobs.id,
    jobs.company,
    jobs.title,
    jobs.location,
    jobs.status,
    (
        SELECT application_tracking.created_at
        FROM application_tracking
        WHERE application_tracking.job_id = jobs.id
        ORDER BY application_tracking.created_at DESC, application_tracking.id DESC
        LIMIT 1
    ) AS latest_timestamp
FROM jobs
WHERE (? IS NULL OR jobs.status = ?)
ORDER BY jobs.id
"""

GET_JOB_STATUS_SQL = """
SELECT
    id,
    company,
    title,
    location,
    status
FROM jobs
WHERE id = ?
"""

GET_TRACKING_HISTORY_SQL = """
SELECT
    status,
    created_at
FROM application_tracking
WHERE job_id = ?
ORDER BY created_at, id
"""


@dataclass(frozen=True, slots=True)
class ApplicationTrackingEntry:
    status: str
    timestamp: str


@dataclass(frozen=True, slots=True)
class ApplicationStatusSnapshot:
    job_id: int
    company: str
    title: str
    location: str | None
    current_status: str
    latest_timestamp: str | None


@dataclass(frozen=True, slots=True)
class ApplicationStatusDetail:
    snapshot: ApplicationStatusSnapshot
    history: tuple[ApplicationTrackingEntry, ...]


def normalize_application_status(value: str) -> str:
    normalized_value = value.strip().lower()
    if normalized_value not in APPLICATION_STATUSES:
        supported = ", ".join(APPLICATION_STATUSES)
        raise ValueError(f"invalid status '{value}'; expected one of: {supported}")
    return normalized_value


def record_application_status(
    database_path: Path,
    *,
    job_id: int,
    status: str,
) -> ApplicationStatusSnapshot:
    normalized_status = normalize_application_status(status)
    with closing(connect_database(database_path)) as connection:
        job_row = connection.execute(GET_JOB_STATUS_SQL, (job_id,)).fetchone()
        if job_row is None:
            raise ValueError(f"job id {job_id} was not found")

        cursor = connection.execute(
            "INSERT INTO application_tracking (job_id, status) VALUES (?, ?)",
            (job_id, normalized_status),
        )
        tracking_row = connection.execute(
            "SELECT created_at FROM application_tracking WHERE id = ?",
            (cursor.lastrowid,),
        ).fetchone()
        assert tracking_row is not None
        timestamp = str(tracking_row["created_at"])
        connection.execute(
            "UPDATE jobs SET status = ?, updated_at = ? WHERE id = ?",
            (normalized_status, timestamp, job_id),
        )
        connection.commit()

    return ApplicationStatusSnapshot(
        job_id=int(job_row["id"]),
        company=str(job_row["company"]),
        title=str(job_row["title"]),
        location=_nullable_text(job_row["location"]),
        current_status=normalized_status,
        latest_timestamp=timestamp,
    )


def list_application_statuses(
    database_path: Path,
    *,
    status: str | None = None,
) -> tuple[ApplicationStatusSnapshot, ...]:
    normalized_status = normalize_application_status(status) if status is not None else None
    with closing(connect_database(database_path)) as connection:
        rows = connection.execute(
            LIST_APPLICATION_STATUSES_SQL,
            (normalized_status, normalized_status),
        ).fetchall()

    return tuple(
        ApplicationStatusSnapshot(
            job_id=int(row["id"]),
            company=str(row["company"]),
            title=str(row["title"]),
            location=_nullable_text(row["location"]),
            current_status=str(row["status"]),
            latest_timestamp=_nullable_text(row["latest_timestamp"]),
        )
        for row in rows
    )


def get_application_status(database_path: Path, *, job_id: int) -> ApplicationStatusDetail:
    with closing(connect_database(database_path)) as connection:
        job_row = connection.execute(GET_JOB_STATUS_SQL, (job_id,)).fetchone()
        if job_row is None:
            raise ValueError(f"job id {job_id} was not found")

        history_rows = connection.execute(GET_TRACKING_HISTORY_SQL, (job_id,)).fetchall()

    history = tuple(
        ApplicationTrackingEntry(
            status=str(row["status"]),
            timestamp=str(row["created_at"]),
        )
        for row in history_rows
    )

    return ApplicationStatusDetail(
        snapshot=ApplicationStatusSnapshot(
            job_id=int(job_row["id"]),
            company=str(job_row["company"]),
            title=str(job_row["title"]),
            location=_nullable_text(job_row["location"]),
            current_status=str(job_row["status"]),
            latest_timestamp=history[-1].timestamp if history else None,
        ),
        history=history,
    )


def _nullable_text(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    return str(value)
