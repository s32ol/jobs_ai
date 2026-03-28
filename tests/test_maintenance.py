from __future__ import annotations

from contextlib import closing
import json
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest

from typer.testing import CliRunner

SRC_PATH = Path(__file__).resolve().parents[1] / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

from jobs_ai.cli import app
from jobs_ai.db import connect_database, initialize_schema, insert_job
from jobs_ai.maintenance import backfill_jobs_metadata, mark_invalid_location_jobs

RUNNER = CliRunner()

_OLD_SCHEMA_SQL = """
CREATE TABLE jobs (
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

CREATE TABLE applications (
    id INTEGER PRIMARY KEY,
    job_id INTEGER NOT NULL,
    state TEXT NOT NULL DEFAULT 'draft',
    resume_variant TEXT,
    notes TEXT,
    last_attempted_at TEXT,
    applied_at TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE application_tracking (
    id INTEGER PRIMARY KEY,
    job_id INTEGER NOT NULL,
    status TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
"""


def _create_old_schema_database(database_path: Path) -> None:
    database_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(database_path) as connection:
        connection.executescript(_OLD_SCHEMA_SQL)
        connection.execute(
            """
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
                status,
                raw_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "manual",
                None,
                "Acme Data",
                "Data Engineer",
                "Remote",
                "https://boards.greenhouse.io/acme?gh_jid=12345&utm_source=test",
                None,
                None,
                "2026-03-10",
                "2026-03-13T08:00:00Z",
                "new",
                json.dumps({"description": "python pipelines"}, ensure_ascii=True),
            ),
        )
        connection.execute(
            """
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
                status,
                raw_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "referral",
                None,
                "Bright Metrics",
                "Analytics Engineer",
                "San Jose, CA",
                None,
                None,
                None,
                None,
                "2026-03-12T08:00:00Z",
                "opened",
                json.dumps({"description": "sql modeling"}, ensure_ascii=True),
            ),
        )
        connection.commit()


def _job_record(
    *,
    source: str,
    company: str,
    title: str,
    location: str,
    apply_url: str | None = None,
    portal_type: str | None = None,
    raw_payload: dict[str, object] | None = None,
) -> dict[str, object]:
    return {
        "source": source,
        "source_job_id": None,
        "company": company,
        "title": title,
        "location": location,
        "apply_url": apply_url,
        "portal_type": portal_type,
        "salary_text": None,
        "posted_at": None,
        "found_at": "2026-03-13T08:00:00Z",
        "raw_json": json.dumps(raw_payload or {}, ensure_ascii=True),
    }


def _insert_job_with_status(
    connection,
    *,
    status: str = "new",
    source: str,
    company: str,
    title: str,
    location: str,
    apply_url: str | None = None,
    portal_type: str | None = None,
    raw_payload: dict[str, object] | None = None,
) -> int:
    job_id = insert_job(
        connection,
        _job_record(
            source=source,
            company=company,
            title=title,
            location=location,
            apply_url=apply_url,
            portal_type=portal_type,
            raw_payload=raw_payload,
        ),
    )
    if status != "new":
        connection.execute(
            "UPDATE jobs SET status = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
            (status, job_id),
        )
    return job_id


class MaintenanceBackfillTest(unittest.TestCase):
    def test_backfill_dry_run_reports_old_schema_gaps(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            database_path = Path(tmp_dir) / "runtime" / "jobs_ai.db"
            _create_old_schema_database(database_path)

            result = backfill_jobs_metadata(database_path, dry_run=True)

            self.assertTrue(result.dry_run)
            self.assertEqual(result.total_jobs, 2)
            self.assertEqual(result.candidate_jobs, 2)
            self.assertEqual(result.updated_jobs, 2)
            self.assertEqual(result.skipped_jobs, 0)
            self.assertEqual(result.missing_tables, ("session_history", "source_registry"))
            self.assertIn("canonical_apply_url", result.missing_job_columns)
            self.assertIn("identity_key", result.missing_job_columns)
            field_counts = {entry.field_name: entry.count for entry in result.field_counts}
            self.assertEqual(field_counts["identity_key"], 2)
            self.assertEqual(field_counts["canonical_apply_url"], 1)
            self.assertEqual(field_counts["portal_type"], 1)

    def test_backfill_updates_missing_metadata_without_inventing_batch_history_and_is_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            database_path = Path(tmp_dir) / "runtime" / "jobs_ai.db"
            _create_old_schema_database(database_path)

            first_result = backfill_jobs_metadata(database_path)
            second_result = backfill_jobs_metadata(database_path)

            self.assertFalse(first_result.dry_run)
            self.assertEqual(first_result.updated_jobs, 2)
            self.assertEqual(first_result.deferred_jobs, 0)
            self.assertEqual(second_result.updated_jobs, 0)
            self.assertEqual(second_result.candidate_jobs, 0)

            with closing(connect_database(database_path)) as connection:
                columns = {
                    row["name"]
                    for row in connection.execute("PRAGMA table_info(jobs)").fetchall()
                }
                tables = {
                    row["name"]
                    for row in connection.execute(
                        "SELECT name FROM sqlite_master WHERE type = 'table'"
                    ).fetchall()
                }
                first_row = connection.execute(
                    """
                    SELECT
                        portal_type,
                        canonical_apply_url,
                        identity_key,
                        ingest_batch_id,
                        source_query
                    FROM jobs
                    WHERE id = 1
                    """
                ).fetchone()
                second_row = connection.execute(
                    """
                    SELECT
                        portal_type,
                        canonical_apply_url,
                        identity_key,
                        ingest_batch_id,
                        source_query
                    FROM jobs
                    WHERE id = 2
                    """
                ).fetchone()

            self.assertIn("session_history", tables)
            self.assertTrue(
                {
                    "ingest_batch_id",
                    "source_query",
                    "import_source",
                    "canonical_apply_url",
                    "identity_key",
                }.issubset(columns)
            )
            self.assertEqual(first_row["portal_type"], "greenhouse")
            self.assertEqual(
                first_row["canonical_apply_url"],
                "https://boards.greenhouse.io/acme/jobs/12345",
            )
            self.assertTrue(first_row["identity_key"])
            self.assertIsNone(first_row["ingest_batch_id"])
            self.assertIsNone(first_row["source_query"])
            self.assertIsNone(second_row["portal_type"])
            self.assertIsNone(second_row["canonical_apply_url"])
            self.assertTrue(second_row["identity_key"])

    def test_cli_maintenance_backfill_reports_preview(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            database_path = Path(tmp_dir) / "runtime" / "jobs_ai.db"
            env = {"JOBS_AI_DB_PATH": str(database_path)}
            _create_old_schema_database(database_path)

            result = RUNNER.invoke(app, ["maintenance", "backfill", "--dry-run"], env=env)

            self.assertEqual(result.exit_code, 0)
            self.assertIn("jobs_ai maintenance backfill", result.stdout)
            self.assertIn("dry run: yes", result.stdout)
            self.assertIn("candidate jobs: 2", result.stdout)
            self.assertIn("would update jobs: 2", result.stdout)
            self.assertIn("missing tables before run:", result.stdout)
            self.assertIn("- session_history", result.stdout)

    def test_mark_invalid_location_jobs_marks_obvious_non_us_and_leaves_ambiguous_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            database_path = Path(tmp_dir) / "runtime" / "jobs_ai.db"
            initialize_schema(database_path)

            with closing(connect_database(database_path)) as connection:
                india_job_id = _insert_job_with_status(
                    connection,
                    source="manual",
                    company="Northwind Talent",
                    title="Analytics Engineer",
                    location="India",
                    apply_url="https://example.com/jobs/india",
                )
                canada_job_id = _insert_job_with_status(
                    connection,
                    status="opened",
                    source="manual",
                    company="Remote North",
                    title="Analytics Engineer",
                    location="Toronto, Canada",
                    apply_url="https://example.com/jobs/canada",
                )
                remote_job_id = _insert_job_with_status(
                    connection,
                    source="manual",
                    company="Remote Data",
                    title="Analytics Engineer",
                    location="Remote",
                    apply_url="https://example.com/jobs/remote",
                )
                us_job_id = _insert_job_with_status(
                    connection,
                    source="manual",
                    company="Acme Data",
                    title="Analytics Engineer",
                    location="Sacramento, California, United States",
                    apply_url="https://example.com/jobs/us",
                )
                applied_job_id = _insert_job_with_status(
                    connection,
                    status="applied",
                    source="manual",
                    company="Past Application",
                    title="Analytics Engineer",
                    location="India",
                    apply_url="https://example.com/jobs/applied",
                )
                connection.commit()

            result = mark_invalid_location_jobs(database_path, us_only=True)

            self.assertEqual(result.total_jobs, 5)
            self.assertEqual(result.candidate_jobs, 2)
            self.assertEqual(result.marked_jobs, 2)
            self.assertEqual(result.ambiguous_jobs, 1)
            self.assertEqual(result.us_allowed_jobs, 1)
            self.assertEqual(result.skipped_jobs, 1)
            self.assertEqual({entry.job_id for entry in result.job_updates}, {india_job_id, canada_job_id})

            with closing(connect_database(database_path)) as connection:
                rows = connection.execute(
                    "SELECT id, status FROM jobs WHERE id IN (?, ?, ?, ?, ?) ORDER BY id",
                    (india_job_id, canada_job_id, remote_job_id, us_job_id, applied_job_id),
                ).fetchall()
                tracking_rows = connection.execute(
                    """
                    SELECT job_id, status
                    FROM application_tracking
                    WHERE job_id IN (?, ?)
                    ORDER BY job_id, id
                    """,
                    (india_job_id, canada_job_id),
                ).fetchall()

            self.assertEqual(
                {int(row["id"]): str(row["status"]) for row in rows},
                {
                    india_job_id: "invalid_location",
                    canada_job_id: "invalid_location",
                    remote_job_id: "new",
                    us_job_id: "new",
                    applied_job_id: "applied",
                },
            )
            self.assertEqual(
                [(int(row["job_id"]), str(row["status"])) for row in tracking_rows],
                [
                    (india_job_id, "invalid_location"),
                    (canada_job_id, "invalid_location"),
                ],
            )

    def test_cli_maintenance_mark_invalid_location_supports_dry_run_limit(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            database_path = Path(tmp_dir) / "runtime" / "jobs_ai.db"
            env = {"JOBS_AI_DB_PATH": str(database_path)}
            initialize_schema(database_path)

            with closing(connect_database(database_path)) as connection:
                _insert_job_with_status(
                    connection,
                    source="manual",
                    company="Northwind Talent",
                    title="Analytics Engineer",
                    location="India",
                    apply_url="https://example.com/jobs/india",
                )
                _insert_job_with_status(
                    connection,
                    source="manual",
                    company="Remote North",
                    title="Analytics Engineer",
                    location="Toronto, Canada",
                    apply_url="https://example.com/jobs/canada",
                )
                connection.commit()

            result = RUNNER.invoke(
                app,
                ["maintenance", "mark-invalid-location", "--us-only", "--dry-run", "--limit", "1"],
                env=env,
            )

            self.assertEqual(result.exit_code, 0)
            self.assertIn("jobs_ai maintenance mark-invalid-location", result.stdout)
            self.assertIn("dry run: yes", result.stdout)
            self.assertIn("limit: 1", result.stdout)
            self.assertIn("obvious non-US jobs: 2", result.stdout)
            self.assertIn("would mark jobs: 1", result.stdout)
            self.assertIn("deferred by limit: 1", result.stdout)

    def test_cli_maintenance_supersede_duplicates_dry_run_leaves_rows_unchanged(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            database_path = Path(tmp_dir) / "runtime" / "jobs_ai.db"
            env = {"JOBS_AI_DB_PATH": str(database_path)}
            initialize_schema(database_path)

            with closing(connect_database(database_path)) as connection:
                first_job_id = _insert_job_with_status(
                    connection,
                    source="manual",
                    company="Acme Data",
                    title="Senior Data Engineer",
                    location="Remote",
                    apply_url="https://example.com/jobs/data-engineer",
                    raw_payload={"description": "python pipelines"},
                )
                second_job_id = _insert_job_with_status(
                    connection,
                    status="opened",
                    source="manual",
                    company="Acme Data",
                    title="Data Engineer",
                    location="Remote",
                    apply_url="https://example.com/jobs/data-engineer",
                    raw_payload={"description": "python pipelines"},
                )
                connection.commit()

            result = RUNNER.invoke(
                app,
                ["maintenance", "supersede-duplicates", "--dry-run"],
                env=env,
            )

            self.assertEqual(result.exit_code, 0)
            self.assertIn("jobs_ai maintenance supersede-duplicates", result.stdout)
            self.assertIn("dry run: yes", result.stdout)
            self.assertIn("would repair groups: 1", result.stdout)
            self.assertIn("status: success", result.stdout)

            with closing(connect_database(database_path)) as connection:
                rows = connection.execute(
                    "SELECT id, status FROM jobs ORDER BY id"
                ).fetchall()

            self.assertEqual(
                [(row["id"], row["status"]) for row in rows],
                [
                    (first_job_id, "new"),
                    (second_job_id, "opened"),
                ],
            )

    def test_cli_maintenance_supersede_duplicates_keeps_applied_row_canonical(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            database_path = Path(tmp_dir) / "runtime" / "jobs_ai.db"
            env = {"JOBS_AI_DB_PATH": str(database_path)}
            initialize_schema(database_path)

            with closing(connect_database(database_path)) as connection:
                first_job_id = _insert_job_with_status(
                    connection,
                    source="manual",
                    company="Acme Data",
                    title="Senior Data Engineer",
                    location="Remote",
                    apply_url="https://example.com/jobs/data-engineer",
                    raw_payload={"description": "python pipelines"},
                )
                second_job_id = _insert_job_with_status(
                    connection,
                    status="opened",
                    source="manual",
                    company="Acme Data",
                    title="Data Engineer",
                    location="Remote",
                    apply_url="https://example.com/jobs/data-engineer",
                    raw_payload={"description": "python pipelines"},
                )
                third_job_id = _insert_job_with_status(
                    connection,
                    status="applied",
                    source="manual",
                    company="Acme Data",
                    title="Data Engineer",
                    location="Remote",
                    apply_url="https://example.com/jobs/data-engineer",
                    raw_payload={"description": "python pipelines"},
                )
                connection.commit()

            result = RUNNER.invoke(
                app,
                ["maintenance", "supersede-duplicates"],
                env=env,
            )

            self.assertEqual(result.exit_code, 0)
            self.assertIn("jobs_ai maintenance supersede-duplicates", result.stdout)
            self.assertIn("dry run: no", result.stdout)
            self.assertIn("repaired groups: 1", result.stdout)
            self.assertIn("rows marked superseded: 2", result.stdout)
            self.assertIn("canonical rows restored: 0", result.stdout)

            with closing(connect_database(database_path)) as connection:
                rows = connection.execute(
                    "SELECT id, status FROM jobs ORDER BY id"
                ).fetchall()

            self.assertEqual(
                [(row["id"], row["status"]) for row in rows],
                [
                    (first_job_id, "superseded"),
                    (second_job_id, "superseded"),
                    (third_job_id, "applied"),
                ],
            )

    def test_cli_maintenance_supersede_duplicates_keeps_one_applied_winner_when_multiple_applied_rows_exist(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            database_path = Path(tmp_dir) / "runtime" / "jobs_ai.db"
            env = {"JOBS_AI_DB_PATH": str(database_path)}
            initialize_schema(database_path)

            with closing(connect_database(database_path)) as connection:
                first_job_id = _insert_job_with_status(
                    connection,
                    status="applied",
                    source="manual",
                    company="Qualified Health",
                    title="Data Engineer",
                    location="Remote",
                    apply_url="https://example.com/jobs/data-engineer",
                    raw_payload={"description": "python pipelines"},
                )
                second_job_id = _insert_job_with_status(
                    connection,
                    status="applied",
                    source="manual",
                    company="Qualified Health",
                    title="Sr. Data Engineer - Healthcare Data Infrastructure",
                    location="Remote",
                    apply_url="https://example.com/jobs/data-engineer",
                    raw_payload={"description": "python pipelines"},
                )
                third_job_id = _insert_job_with_status(
                    connection,
                    source="manual",
                    company="Qualified Health",
                    title="Platform Data Engineer",
                    location="Remote",
                    apply_url="https://example.com/jobs/data-engineer",
                    raw_payload={"description": "python pipelines"},
                )
                connection.commit()

            result = RUNNER.invoke(
                app,
                ["maintenance", "supersede-duplicates"],
                env=env,
            )

            self.assertEqual(result.exit_code, 0)
            self.assertIn("repaired groups: 1", result.stdout)
            self.assertIn("rows marked superseded: 2", result.stdout)

            with closing(connect_database(database_path)) as connection:
                rows = connection.execute(
                    "SELECT id, title, status FROM jobs ORDER BY id"
                ).fetchall()

            self.assertEqual(
                [(row["id"], row["title"], row["status"]) for row in rows],
                [
                    (first_job_id, "Data Engineer", "superseded"),
                    (
                        second_job_id,
                        "Sr. Data Engineer - Healthcare Data Infrastructure",
                        "applied",
                    ),
                    (third_job_id, "Platform Data Engineer", "superseded"),
                ],
            )


if __name__ == "__main__":
    unittest.main()
