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

from jobs_ai.application_tracking import record_application_status
from jobs_ai.cli import app
from jobs_ai.db import connect_database, initialize_schema, insert_job, record_session_history

RUNNER = CliRunner()


def _job_record(
    *,
    source: str,
    company: str,
    title: str,
    location: str,
    apply_url: str | None = None,
    portal_type: str | None = None,
    ingest_batch_id: str | None = None,
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
        "ingest_batch_id": ingest_batch_id,
        "source_query": None,
        "import_source": None,
        "raw_json": json.dumps({}, ensure_ascii=True),
    }


class StatsTest(unittest.TestCase):
    def test_cli_stats_reports_operator_counts_and_recent_activity(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            project_root = Path(tmp_dir) / "workspace"
            database_path = project_root / "runtime" / "jobs_ai.db"
            env = {"JOBS_AI_DB_PATH": str(database_path)}
            initialize_schema(database_path)

            with closing(connect_database(database_path)) as connection:
                old_job_id = insert_job(
                    connection,
                    _job_record(
                        source="manual",
                        company="Legacy Co",
                        title="Data Engineer",
                        location="Remote",
                        apply_url="https://example.com/jobs/legacy",
                        ingest_batch_id="old-batch",
                    ),
                )
                opened_job_id = insert_job(
                    connection,
                    _job_record(
                        source="manual",
                        company="Acme Data",
                        title="Platform Data Engineer",
                        location="Remote",
                        apply_url="https://boards.greenhouse.io/acme/jobs/12345",
                        portal_type="greenhouse",
                        ingest_batch_id="recent-batch-a",
                    ),
                )
                applied_job_id = insert_job(
                    connection,
                    _job_record(
                        source="manual",
                        company="Bright Metrics",
                        title="Analytics Engineer",
                        location="Remote",
                        apply_url="https://jobs.lever.co/bright/analytics-1",
                        portal_type="lever",
                        ingest_batch_id="recent-batch-b",
                    ),
                )
                connection.execute(
                    "UPDATE jobs SET created_at = datetime('now', '-40 days') WHERE id = ?",
                    (old_job_id,),
                )
                connection.commit()

            record_application_status(database_path, job_id=opened_job_id, status="opened")
            record_application_status(database_path, job_id=applied_job_id, status="applied")
            record_session_history(
                database_path,
                manifest_path=project_root / "data" / "exports" / "launch-preview-session-test.json",
                item_count=2,
                launchable_count=2,
                batch_id="recent-batch-a",
                source_query="python backend engineer remote",
            )

            result = RUNNER.invoke(app, ["stats", "--days", "7"], env=env)

            self.assertEqual(result.exit_code, 0)
            self.assertIn("jobs_ai stats", result.stdout)
            self.assertIn("total jobs: 3", result.stdout)
            self.assertIn("new: 1", result.stdout)
            self.assertIn("opened: 1", result.stdout)
            self.assertIn("applied: 1", result.stdout)
            self.assertIn("recent imports (7d): 2", result.stdout)
            self.assertIn("recent import batches (7d): 2", result.stdout)
            self.assertIn("total sessions started: 1", result.stdout)
            self.assertIn("recent sessions started (7d): 1", result.stdout)
            self.assertIn("total tracking events: 2", result.stdout)
            self.assertIn("portal counts:", result.stdout)
            self.assertIn("- greenhouse: 1", result.stdout)
            self.assertIn("- lever: 1", result.stdout)
            self.assertIn("- unknown: 1", result.stdout)

    def test_cli_stats_json_reports_machine_readable_summary(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            project_root = Path(tmp_dir) / "workspace"
            database_path = project_root / "runtime" / "jobs_ai.db"
            env = {"JOBS_AI_DB_PATH": str(database_path)}
            initialize_schema(database_path)

            with closing(connect_database(database_path)) as connection:
                insert_job(
                    connection,
                    _job_record(
                        source="manual",
                        company="Acme Data",
                        title="Data Engineer",
                        location="Remote",
                        ingest_batch_id="recent-batch-a",
                    ),
                )
                connection.commit()

            record_session_history(
                database_path,
                manifest_path=project_root / "data" / "exports" / "launch-preview-session-test.json",
                item_count=1,
                launchable_count=1,
                batch_id="recent-batch-a",
                source_query="python backend engineer remote",
            )

            result = RUNNER.invoke(app, ["stats", "--json"], env=env)

            self.assertEqual(result.exit_code, 0)
            payload = json.loads(result.stdout)
            self.assertEqual(payload["command"], "jobs_ai stats")
            self.assertEqual(payload["total_jobs"], 1)
            self.assertEqual(payload["recent_imported_jobs"], 1)
            self.assertEqual(payload["total_sessions_started"], 1)
            self.assertEqual(payload["status_counts"]["new"], 1)

    def test_cli_stats_surfaces_expanded_outcome_statuses(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            project_root = Path(tmp_dir) / "workspace"
            database_path = project_root / "runtime" / "jobs_ai.db"
            env = {"JOBS_AI_DB_PATH": str(database_path)}
            initialize_schema(database_path)

            with closing(connect_database(database_path)) as connection:
                interview_job_id = insert_job(
                    connection,
                    _job_record(
                        source="manual",
                        company="Acme Data",
                        title="Data Engineer",
                        location="Remote",
                    ),
                )
                rejected_job_id = insert_job(
                    connection,
                    _job_record(
                        source="manual",
                        company="Bright Metrics",
                        title="Analytics Engineer",
                        location="Remote",
                    ),
                )
                connection.commit()

            record_application_status(database_path, job_id=interview_job_id, status="applied")
            record_application_status(database_path, job_id=interview_job_id, status="interview")
            record_application_status(database_path, job_id=rejected_job_id, status="rejected")

            result = RUNNER.invoke(app, ["stats", "--days", "7"], env=env)

            self.assertEqual(result.exit_code, 0)
            self.assertIn("interview: 1", result.stdout)
            self.assertIn("rejected: 1", result.stdout)


if __name__ == "__main__":
    unittest.main()
