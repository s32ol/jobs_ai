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
from jobs_ai.db import connect_database
from jobs_ai.maintenance import backfill_jobs_metadata

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
            self.assertEqual(result.missing_tables, ("session_history",))
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


if __name__ == "__main__":
    unittest.main()
