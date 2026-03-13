from __future__ import annotations

from contextlib import closing
import json
from pathlib import Path
import sys
import tempfile
import unittest

from typer.testing import CliRunner

SRC_PATH = Path(__file__).resolve().parents[1] / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

from jobs_ai.application_tracking import (
    get_application_status,
    list_application_statuses,
    record_application_status,
)
from jobs_ai.cli import app
from jobs_ai.db import connect_database, initialize_schema, insert_job

RUNNER = CliRunner()


def _job_record(
    *,
    source: str,
    company: str,
    title: str,
    location: str,
    apply_url: str | None = None,
) -> dict[str, object]:
    return {
        "source": source,
        "source_job_id": None,
        "company": company,
        "title": title,
        "location": location,
        "apply_url": apply_url,
        "portal_type": None,
        "salary_text": None,
        "posted_at": None,
        "found_at": "2026-03-13T08:00:00Z",
        "raw_json": json.dumps({}, ensure_ascii=True),
    }


class ApplicationTrackingTest(unittest.TestCase):
    def test_record_application_status_allows_manual_updates_and_records_timestamps(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            database_path = Path(tmp_dir) / "runtime" / "jobs_ai.db"
            initialize_schema(database_path)

            with closing(connect_database(database_path)) as connection:
                job_id = insert_job(
                    connection,
                    _job_record(
                        source="manual",
                        company="Acme Data",
                        title="Data Engineer",
                        location="Remote",
                    ),
                )
                connection.commit()

            opened_snapshot = record_application_status(database_path, job_id=job_id, status="opened")
            applied_snapshot = record_application_status(database_path, job_id=job_id, status="applied")

            self.assertEqual(opened_snapshot.current_status, "opened")
            self.assertEqual(applied_snapshot.current_status, "applied")
            self.assertRegex(opened_snapshot.latest_timestamp or "", r"^\d{4}-\d{2}-\d{2} ")
            self.assertRegex(applied_snapshot.latest_timestamp or "", r"^\d{4}-\d{2}-\d{2} ")

            detail = get_application_status(database_path, job_id=job_id)
            self.assertEqual(detail.snapshot.current_status, "applied")
            self.assertEqual([entry.status for entry in detail.history], ["opened", "applied"])
            self.assertTrue(all(entry.timestamp for entry in detail.history))

    def test_record_application_status_rejects_invalid_status(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            database_path = Path(tmp_dir) / "runtime" / "jobs_ai.db"
            initialize_schema(database_path)

            with closing(connect_database(database_path)) as connection:
                job_id = insert_job(
                    connection,
                    _job_record(
                        source="manual",
                        company="Acme Data",
                        title="Data Engineer",
                        location="Remote",
                    ),
                )
                connection.commit()

            with self.assertRaisesRegex(ValueError, "invalid status 'submitted'"):
                record_application_status(database_path, job_id=job_id, status="submitted")

    def test_cli_track_mark_records_status_update(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            database_path = Path(tmp_dir) / "runtime" / "jobs_ai.db"
            env = {"JOBS_AI_DB_PATH": str(database_path)}
            initialize_schema(database_path)

            with closing(connect_database(database_path)) as connection:
                job_id = insert_job(
                    connection,
                    _job_record(
                        source="manual",
                        company="Northwind Talent",
                        title="Platform Data Engineer",
                        location="Remote",
                    ),
                )
                connection.commit()

            result = RUNNER.invoke(app, ["track", "mark", str(job_id), "opened"], env=env)

            self.assertEqual(result.exit_code, 0)
            self.assertIn("jobs_ai track mark", result.stdout)
            self.assertIn(f"job id: {job_id}", result.stdout)
            self.assertIn("recorded status: opened", result.stdout)
            self.assertRegex(result.stdout, r"timestamp: \d{4}-\d{2}-\d{2} ")

    def test_cli_track_mark_rejects_invalid_status(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            database_path = Path(tmp_dir) / "runtime" / "jobs_ai.db"
            env = {"JOBS_AI_DB_PATH": str(database_path)}
            initialize_schema(database_path)

            with closing(connect_database(database_path)) as connection:
                job_id = insert_job(
                    connection,
                    _job_record(
                        source="manual",
                        company="Northwind Talent",
                        title="Platform Data Engineer",
                        location="Remote",
                    ),
                )
                connection.commit()

            result = RUNNER.invoke(app, ["track", "mark", str(job_id), "submitted"], env=env)

            self.assertEqual(result.exit_code, 1)
            self.assertIn("jobs_ai track mark", result.stdout)
            self.assertIn("status: failed", result.stdout)
            self.assertIn("invalid status 'submitted'", result.stdout)

    def test_cli_track_list_and_status_display_current_status_and_history(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            database_path = Path(tmp_dir) / "runtime" / "jobs_ai.db"
            env = {"JOBS_AI_DB_PATH": str(database_path)}
            initialize_schema(database_path)

            with closing(connect_database(database_path)) as connection:
                first_job_id = insert_job(
                    connection,
                    _job_record(
                        source="manual",
                        company="Acme Data",
                        title="Data Engineer",
                        location="Remote",
                        apply_url="https://example.com/jobs/1",
                    ),
                )
                second_job_id = insert_job(
                    connection,
                    _job_record(
                        source="manual",
                        company="Bright Metrics",
                        title="Analytics Engineer",
                        location="Sacramento, CA",
                        apply_url="https://example.com/jobs/2",
                    ),
                )
                connection.commit()

            record_application_status(database_path, job_id=second_job_id, status="opened")
            record_application_status(database_path, job_id=second_job_id, status="applied")

            listed_jobs = list_application_statuses(database_path)
            self.assertEqual([snapshot.job_id for snapshot in listed_jobs], [first_job_id, second_job_id])

            list_result = RUNNER.invoke(app, ["track", "list"], env=env)
            status_result = RUNNER.invoke(app, ["track", "status", str(second_job_id)], env=env)

            self.assertEqual(list_result.exit_code, 0)
            self.assertIn("jobs_ai track list", list_result.stdout)
            self.assertIn(f"[job {first_job_id}] Acme Data | Data Engineer | Remote", list_result.stdout)
            self.assertIn("current status: new", list_result.stdout)
            self.assertIn(f"[job {second_job_id}] Bright Metrics | Analytics Engineer | Sacramento, CA", list_result.stdout)
            self.assertIn("current status: applied", list_result.stdout)
            self.assertRegex(list_result.stdout, r"latest timestamp: \d{4}-\d{2}-\d{2} ")

            self.assertEqual(status_result.exit_code, 0)
            self.assertIn("jobs_ai track status", status_result.stdout)
            self.assertIn(f"job id: {second_job_id}", status_result.stdout)
            self.assertIn("current status: applied", status_result.stdout)
            self.assertIn("tracking entries: 2", status_result.stdout)
            self.assertRegex(status_result.stdout, r"- \d{4}-\d{2}-\d{2} .* \| opened")
            self.assertRegex(status_result.stdout, r"- \d{4}-\d{2}-\d{2} .* \| applied")


if __name__ == "__main__":
    unittest.main()
