from __future__ import annotations

from contextlib import closing
from collections.abc import Mapping
from pathlib import Path
import sqlite3

REQUIRED_TABLES = ("jobs", "applications", "application_tracking")

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS jobs (
    id INTEGER PRIMARY KEY,
    source TEXT NOT NULL,
    source_job_id TEXT,
    company TEXT NOT NULL,
    title TEXT NOT NULL,
    location TEXT,
    apply_url TEXT,
    portal_type TEXT,
    salary_text TEXT,
    posted_at TEXT,
    found_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    status TEXT NOT NULL DEFAULT 'new',
    raw_json TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS applications (
    id INTEGER PRIMARY KEY,
    job_id INTEGER NOT NULL,
    state TEXT NOT NULL DEFAULT 'draft',
    resume_variant TEXT,
    notes TEXT,
    last_attempted_at TEXT,
    applied_at TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (job_id) REFERENCES jobs(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS application_tracking (
    id INTEGER PRIMARY KEY,
    job_id INTEGER NOT NULL,
    status TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (job_id) REFERENCES jobs(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_jobs_apply_url ON jobs(apply_url);
CREATE INDEX IF NOT EXISTS idx_jobs_source_company_title_location
ON jobs(source, company, title, location);
CREATE INDEX IF NOT EXISTS idx_applications_job_id ON applications(job_id);
CREATE INDEX IF NOT EXISTS idx_application_tracking_job_id ON application_tracking(job_id);
"""

JOB_INSERT_SQL = """
INSERT INTO jobs (
    source,
    source_job_id,
    company,
    title,
    location,
    apply_url,
    portal_type,
    salary_text,
    posted_at,
    found_at,
    raw_json
) VALUES (
    ?,
    ?,
    ?,
    ?,
    ?,
    ?,
    ?,
    ?,
    ?,
    COALESCE(?, CURRENT_TIMESTAMP),
    ?
)
"""

EXACT_APPLY_URL_MATCH_SQL = """
SELECT id
FROM jobs
WHERE apply_url = ?
LIMIT 1
"""

EXACT_COMPOSITE_MATCH_SQL = """
SELECT id
FROM jobs
WHERE source = ?
  AND company = ?
  AND title = ?
  AND location = ?
LIMIT 1
"""


def connect_database(database_path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(database_path)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    return connection


def initialize_schema(database_path: Path) -> None:
    database_path.parent.mkdir(parents=True, exist_ok=True)
    with closing(connect_database(database_path)) as connection:
        connection.executescript(SCHEMA_SQL)
        connection.commit()


def existing_tables(database_path: Path) -> set[str]:
    if not database_path.exists():
        return set()

    with closing(connect_database(database_path)) as connection:
        rows = connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        ).fetchall()
    return {str(row["name"]) for row in rows}


def missing_required_tables(database_path: Path) -> list[str]:
    return sorted(set(REQUIRED_TABLES) - existing_tables(database_path))


def schema_exists(database_path: Path) -> bool:
    return database_path.exists() and not missing_required_tables(database_path)


def insert_job(connection: sqlite3.Connection, job_record: Mapping[str, str | None]) -> int:
    cursor = connection.execute(
        JOB_INSERT_SQL,
        (
            job_record["source"],
            job_record.get("source_job_id"),
            job_record["company"],
            job_record["title"],
            job_record["location"],
            job_record["apply_url"],
            job_record.get("portal_type"),
            job_record.get("salary_text"),
            job_record.get("posted_at"),
            job_record.get("found_at"),
            job_record["raw_json"],
        ),
    )
    return int(cursor.lastrowid)


def find_duplicate_job_id(
    connection: sqlite3.Connection,
    job_record: Mapping[str, str | None],
) -> int | None:
    apply_url = job_record.get("apply_url")
    if apply_url is not None:
        row = connection.execute(EXACT_APPLY_URL_MATCH_SQL, (apply_url,)).fetchone()
    else:
        row = connection.execute(
            EXACT_COMPOSITE_MATCH_SQL,
            (
                job_record["source"],
                job_record["company"],
                job_record["title"],
                job_record["location"],
            ),
        ).fetchone()

    if row is None:
        return None
    return int(row["id"])
