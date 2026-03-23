from __future__ import annotations

from collections.abc import Sequence
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
import sqlite3

from .db import connect_database

APPLICATION_STATUSES = (
    "new",
    "opened",
    "applied",
    "recruiter_screen",
    "assessment",
    "interview",
    "offer",
    "rejected",
    "skipped",
)
SESSION_MARK_APPLICATION_STATUSES = tuple(
    status
    for status in APPLICATION_STATUSES
    if status != "new"
)

_ALLOWED_STATUS_TRANSITIONS = {
    "new": frozenset(SESSION_MARK_APPLICATION_STATUSES),
    "opened": frozenset(
        {
            "applied",
            "skipped",
            "recruiter_screen",
            "assessment",
            "interview",
            "offer",
            "rejected",
        }
    ),
    "applied": frozenset(
        {
            "recruiter_screen",
            "assessment",
            "interview",
            "offer",
            "rejected",
        }
    ),
    "recruiter_screen": frozenset({"assessment", "interview", "offer", "rejected"}),
    "assessment": frozenset({"interview", "offer", "rejected"}),
    "interview": frozenset({"offer", "rejected"}),
    "offer": frozenset(),
    "rejected": frozenset(),
    "skipped": frozenset({"opened", "applied"}),
}

LIST_APPLICATION_STATUSES_BASE_SQL = """
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
"""

LIST_APPLICATION_STATUSES_SQL = f"{LIST_APPLICATION_STATUSES_BASE_SQL}\nORDER BY jobs.id"

LIST_APPLICATION_STATUSES_FILTERED_SQL = (
    f"{LIST_APPLICATION_STATUSES_BASE_SQL}\nWHERE jobs.status = ?\nORDER BY jobs.id"
)

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


@dataclass(frozen=True, slots=True)
class ApplicationStatusIssue:
    job_id: int
    reason: str


@dataclass(frozen=True, slots=True)
class ApplicationStatusBatchResult:
    requested_status: str
    updated: tuple[ApplicationStatusSnapshot, ...]
    skipped: tuple[ApplicationStatusIssue, ...]


def normalize_application_status(value: str) -> str:
    normalized_value = value.strip().lower()
    if normalized_value not in APPLICATION_STATUSES:
        supported = ", ".join(APPLICATION_STATUSES)
        raise ValueError(f"invalid status '{value}'; expected one of: {supported}")
    return normalized_value


def normalize_session_mark_status(value: str) -> str:
    normalized_value = normalize_application_status(value)
    if normalized_value not in SESSION_MARK_APPLICATION_STATUSES:
        supported = ", ".join(SESSION_MARK_APPLICATION_STATUSES)
        raise ValueError(
            f"invalid session mark status '{value}'; expected one of: {supported}"
        )
    return normalized_value


def record_application_status(
    database_path: Path,
    *,
    job_id: int,
    status: str,
) -> ApplicationStatusSnapshot:
    normalized_status = normalize_application_status(status)
    with closing(connect_database(database_path)) as connection:
        job_row = _get_job_status_row(connection, job_id)
        if job_row is None:
            raise ValueError(f"job id {job_id} was not found")
        transition_error = _validate_transition(
            current_status=str(job_row["status"]),
            requested_status=normalized_status,
        )
        if transition_error is not None:
            raise ValueError(transition_error)
        snapshot = _record_application_status_for_row(
            connection,
            job_row=job_row,
            normalized_status=normalized_status,
        )
        connection.commit()

    return snapshot


def record_application_statuses(
    database_path: Path,
    *,
    job_ids: Sequence[int],
    status: str,
) -> ApplicationStatusBatchResult:
    normalized_status = normalize_application_status(status)
    updated: list[ApplicationStatusSnapshot] = []
    skipped: list[ApplicationStatusIssue] = []
    seen_job_ids: set[int] = set()

    with closing(connect_database(database_path)) as connection:
        for requested_job_id in job_ids:
            job_id = int(requested_job_id)
            if job_id in seen_job_ids:
                skipped.append(
                    ApplicationStatusIssue(job_id=job_id, reason="duplicate target ignored")
                )
                continue
            seen_job_ids.add(job_id)

            job_row = _get_job_status_row(connection, job_id)
            if job_row is None:
                skipped.append(
                    ApplicationStatusIssue(job_id=job_id, reason="job id was not found")
                )
                continue

            current_status = str(job_row["status"])
            if current_status == normalized_status:
                skipped.append(
                    ApplicationStatusIssue(job_id=job_id, reason=f"already {normalized_status}")
                )
                continue
            transition_error = _validate_transition(
                current_status=current_status,
                requested_status=normalized_status,
            )
            if transition_error is not None:
                skipped.append(
                    ApplicationStatusIssue(job_id=job_id, reason=transition_error)
                )
                continue

            updated.append(
                _record_application_status_for_row(
                    connection,
                    job_row=job_row,
                    normalized_status=normalized_status,
                )
            )

        if updated:
            connection.commit()

    return ApplicationStatusBatchResult(
        requested_status=normalized_status,
        updated=tuple(updated),
        skipped=tuple(skipped),
    )


def list_application_statuses(
    database_path: Path,
    *,
    status: str | None = None,
) -> tuple[ApplicationStatusSnapshot, ...]:
    normalized_status = normalize_application_status(status) if status is not None else None
    with closing(connect_database(database_path)) as connection:
        if normalized_status is None:
            rows = connection.execute(LIST_APPLICATION_STATUSES_SQL).fetchall()
        else:
            rows = connection.execute(
                LIST_APPLICATION_STATUSES_FILTERED_SQL,
                (normalized_status,),
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
        job_row = _get_job_status_row(connection, job_id)
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


def _get_job_status_row(connection: sqlite3.Connection, job_id: int) -> sqlite3.Row | None:
    return connection.execute(GET_JOB_STATUS_SQL, (job_id,)).fetchone()


def _validate_transition(
    *,
    current_status: str,
    requested_status: str,
) -> str | None:
    if current_status == requested_status:
        return None

    supported_current_status = current_status.strip().lower()
    allowed_targets = _ALLOWED_STATUS_TRANSITIONS.get(supported_current_status)
    if allowed_targets is None:
        supported = ", ".join(APPLICATION_STATUSES)
        return (
            f"job has unsupported current status '{current_status}'; "
            f"expected one of: {supported}"
        )
    if requested_status in allowed_targets:
        return None
    if not allowed_targets:
        return f"cannot move from {supported_current_status} to {requested_status}"
    allowed_label = ", ".join(sorted(allowed_targets))
    return (
        f"cannot move from {supported_current_status} to {requested_status}; "
        f"allowed next statuses: {allowed_label}"
    )


def _record_application_status_for_row(
    connection: sqlite3.Connection,
    *,
    job_row: sqlite3.Row,
    normalized_status: str,
) -> ApplicationStatusSnapshot:
    job_id = int(job_row["id"])
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
    return ApplicationStatusSnapshot(
        job_id=job_id,
        company=str(job_row["company"]),
        title=str(job_row["title"]),
        location=_nullable_text(job_row["location"]),
        current_status=normalized_status,
        latest_timestamp=timestamp,
    )
