from __future__ import annotations

from contextlib import closing
from dataclasses import dataclass
from pathlib import Path

from .db import connect_database
from .db_runtime import backend_name_for_connection

_DEFAULT_TOP_COMPANIES_LIMIT = 5

_TOTAL_JOBS_SQL = "SELECT COUNT(*) AS count FROM jobs"
_JOB_STATUS_COUNTS_SQL = """
SELECT status, COUNT(*) AS count
FROM jobs
GROUP BY status
ORDER BY status
"""
_RECENT_IMPORTED_SQL = """
SELECT
    COUNT(*) AS job_count,
    COUNT(DISTINCT ingest_batch_id) AS batch_count
FROM jobs
WHERE datetime(created_at) >= datetime('now', ?)
"""
_RECENT_IMPORTED_POSTGRES_SQL = """
SELECT
    COUNT(*) AS job_count,
    COUNT(DISTINCT ingest_batch_id) AS batch_count
FROM jobs
WHERE created_at::timestamptz >= CURRENT_TIMESTAMP - (?::interval)
"""
_TRACKING_COUNTS_SQL = """
SELECT COUNT(*) AS count
FROM application_tracking
"""
_RECENT_TRACKING_COUNTS_SQL = """
SELECT COUNT(*) AS count
FROM application_tracking
WHERE datetime(created_at) >= datetime('now', ?)
"""
_RECENT_TRACKING_COUNTS_POSTGRES_SQL = """
SELECT COUNT(*) AS count
FROM application_tracking
WHERE created_at::timestamptz >= CURRENT_TIMESTAMP - (?::interval)
"""
_SESSION_COUNTS_SQL = """
SELECT COUNT(*) AS count
FROM session_history
"""
_RECENT_SESSION_COUNTS_SQL = """
SELECT COUNT(*) AS count
FROM session_history
WHERE datetime(created_at) >= datetime('now', ?)
"""
_RECENT_SESSION_COUNTS_POSTGRES_SQL = """
SELECT COUNT(*) AS count
FROM session_history
WHERE created_at::timestamptz >= CURRENT_TIMESTAMP - (?::interval)
"""
_PORTAL_COUNTS_SQL = """
SELECT COALESCE(portal_type, 'unknown') AS portal_type, COUNT(*) AS count
FROM jobs
GROUP BY COALESCE(portal_type, 'unknown')
ORDER BY count DESC, portal_type
"""
_TOP_COMPANIES_SQL = """
SELECT company, COUNT(*) AS count
FROM jobs
GROUP BY company
ORDER BY count DESC, company
LIMIT ?
"""


@dataclass(frozen=True, slots=True)
class StatsCount:
    label: str
    count: int


@dataclass(frozen=True, slots=True)
class OperatorStats:
    days: int
    total_jobs: int
    status_counts: tuple[StatsCount, ...]
    recent_imported_jobs: int
    recent_import_batches: int
    total_tracking_events: int
    recent_tracking_events: int
    total_sessions_started: int
    recent_sessions_started: int
    portal_counts: tuple[StatsCount, ...]
    top_companies: tuple[StatsCount, ...]

    def status_count(self, status: str) -> int:
        return next((entry.count for entry in self.status_counts if entry.label == status), 0)


def gather_operator_stats(
    database_path: Path,
    *,
    days: int,
    top_companies_limit: int = _DEFAULT_TOP_COMPANIES_LIMIT,
) -> OperatorStats:
    if days < 1:
        raise ValueError("days must be at least 1")

    window = f"-{days} days"
    with closing(connect_database(database_path)) as connection:
        recent_import_sql = (
            _RECENT_IMPORTED_POSTGRES_SQL
            if backend_name_for_connection(connection) == "postgres"
            else _RECENT_IMPORTED_SQL
        )
        recent_tracking_sql = (
            _RECENT_TRACKING_COUNTS_POSTGRES_SQL
            if backend_name_for_connection(connection) == "postgres"
            else _RECENT_TRACKING_COUNTS_SQL
        )
        recent_session_sql = (
            _RECENT_SESSION_COUNTS_POSTGRES_SQL
            if backend_name_for_connection(connection) == "postgres"
            else _RECENT_SESSION_COUNTS_SQL
        )
        total_jobs = int(connection.execute(_TOTAL_JOBS_SQL).fetchone()["count"])
        status_counts = tuple(
            StatsCount(label=str(row["status"]), count=int(row["count"]))
            for row in connection.execute(_JOB_STATUS_COUNTS_SQL).fetchall()
        )
        recent_import_row = connection.execute(recent_import_sql, (window,)).fetchone()
        total_tracking_events = int(connection.execute(_TRACKING_COUNTS_SQL).fetchone()["count"])
        recent_tracking_events = int(
            connection.execute(recent_tracking_sql, (window,)).fetchone()["count"]
        )
        total_sessions_started = int(connection.execute(_SESSION_COUNTS_SQL).fetchone()["count"])
        recent_sessions_started = int(
            connection.execute(recent_session_sql, (window,)).fetchone()["count"]
        )
        portal_counts = tuple(
            StatsCount(label=str(row["portal_type"]), count=int(row["count"]))
            for row in connection.execute(_PORTAL_COUNTS_SQL).fetchall()
        )
        top_companies = tuple(
            StatsCount(label=str(row["company"]), count=int(row["count"]))
            for row in connection.execute(_TOP_COMPANIES_SQL, (top_companies_limit,)).fetchall()
        )

    return OperatorStats(
        days=days,
        total_jobs=total_jobs,
        status_counts=status_counts,
        recent_imported_jobs=int(recent_import_row["job_count"]),
        recent_import_batches=int(recent_import_row["batch_count"] or 0),
        total_tracking_events=total_tracking_events,
        recent_tracking_events=recent_tracking_events,
        total_sessions_started=total_sessions_started,
        recent_sessions_started=recent_sessions_started,
        portal_counts=portal_counts,
        top_companies=top_companies,
    )
