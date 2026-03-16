from __future__ import annotations

from contextlib import closing
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

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


def _write_manifest(
    manifest_path: Path,
    *,
    launchable_job_id: int,
    skipped_job_id: int,
) -> Path:
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(
        json.dumps(
            {
                "created_at": "2026-03-15T12:00:00Z",
                "label": "retro-session",
                "selection_scope": {
                    "batch_id": "discover-batch-1",
                    "source_query": "platform data engineer remote",
                    "import_source": "collect/leads.import.json",
                },
                "item_count": 2,
                "items": [
                    {
                        "rank": 1,
                        "job_id": launchable_job_id,
                        "company": "Northwind Talent",
                        "title": "Platform Data Engineer",
                        "location": "Remote",
                        "source": "manual",
                        "apply_url": "https://example.com/jobs/1",
                        "score": 20,
                        "recommended_resume_variant": {
                            "key": "data-engineering",
                            "label": "Data Engineering Resume",
                        },
                        "recommended_profile_snippet": {
                            "key": "pipeline-delivery",
                            "label": "Pipeline Delivery",
                            "text": "Python-first pipeline delivery across SQL warehouses.",
                        },
                        "explanation": "matched data engineering signals",
                    },
                    {
                        "rank": 2,
                        "job_id": skipped_job_id,
                        "company": "Contoso",
                        "title": "Analytics Engineer",
                        "location": "Remote",
                        "source": "manual",
                        "apply_url": None,
                        "score": 12,
                        "recommended_resume_variant": {
                            "key": "analytics-engineering",
                            "label": "Analytics Engineering Resume",
                        },
                        "recommended_profile_snippet": {
                            "key": "analytics-modeling",
                            "label": "Analytics Modeling",
                            "text": "Trusted metric definitions and semantic layers.",
                        },
                        "explanation": "matched analytics signals",
                    },
                ],
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    return manifest_path


class SessionHistoryCommandsTest(unittest.TestCase):
    def test_cli_session_recent_lists_recorded_sessions(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            project_root = Path(tmp_dir) / "workspace"
            database_path = project_root / "runtime" / "jobs_ai.db"
            env = {"JOBS_AI_DB_PATH": str(database_path)}
            initialize_schema(database_path)

            first_manifest = project_root / "data" / "exports" / "session-a.json"
            second_manifest = project_root / "data" / "exports" / "session-b.json"
            first_manifest.parent.mkdir(parents=True, exist_ok=True)
            first_manifest.write_text("{}\n", encoding="utf-8")
            second_manifest.write_text("{}\n", encoding="utf-8")

            record_session_history(
                database_path,
                manifest_path=first_manifest,
                item_count=3,
                launchable_count=2,
                batch_id="batch-a",
                source_query="analytics engineer remote",
                created_at="2026-03-15T10:00:00Z",
            )
            record_session_history(
                database_path,
                manifest_path=second_manifest,
                item_count=5,
                launchable_count=4,
                batch_id="batch-b",
                source_query="platform data engineer remote",
                created_at="2026-03-15T11:00:00Z",
            )

            result = RUNNER.invoke(app, ["session", "recent", "--limit", "2"], env=env)

            self.assertEqual(result.exit_code, 0)
            self.assertIn("jobs_ai session recent", result.stdout)
            self.assertIn("sessions listed: 2", result.stdout)
            self.assertIn("batch id: batch-b", result.stdout)
            self.assertIn("query: platform data engineer remote", result.stdout)
            self.assertIn(f"manifest path: {second_manifest}", result.stdout)

    def test_cli_session_inspect_shows_manifest_summary_and_current_statuses(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            project_root = Path(tmp_dir) / "workspace"
            database_path = project_root / "runtime" / "jobs_ai.db"
            env = {"JOBS_AI_DB_PATH": str(database_path)}
            initialize_schema(database_path)

            with closing(connect_database(database_path)) as connection:
                launchable_job_id = insert_job(
                    connection,
                    _job_record(
                        source="manual",
                        company="Northwind Talent",
                        title="Platform Data Engineer",
                        location="Remote",
                        apply_url="https://example.com/jobs/1",
                    ),
                )
                skipped_job_id = insert_job(
                    connection,
                    _job_record(
                        source="manual",
                        company="Contoso",
                        title="Analytics Engineer",
                        location="Remote",
                        apply_url="https://example.com/jobs/2",
                    ),
                )
                connection.commit()

            record_application_status(database_path, job_id=launchable_job_id, status="interview")
            record_application_status(database_path, job_id=skipped_job_id, status="rejected")

            manifest_path = _write_manifest(
                project_root / "data" / "exports" / "retro-session.json",
                launchable_job_id=launchable_job_id,
                skipped_job_id=skipped_job_id,
            )
            session_id = record_session_history(
                database_path,
                manifest_path=manifest_path,
                item_count=2,
                launchable_count=1,
                batch_id="discover-batch-1",
                source_query="platform data engineer remote",
                created_at="2026-03-15T12:00:00Z",
            )

            result = RUNNER.invoke(app, ["session", "inspect", str(session_id)], env=env)

            self.assertEqual(result.exit_code, 0)
            self.assertIn("jobs_ai session inspect", result.stdout)
            self.assertIn(f"session id: {session_id}", result.stdout)
            self.assertIn("launchable items: 1", result.stdout)
            self.assertIn("skipped items: 1", result.stdout)
            self.assertIn("current tracked statuses:", result.stdout)
            self.assertIn("- interview: 1", result.stdout)
            self.assertIn("- rejected: 1", result.stdout)
            self.assertIn(f"1. [job {launchable_job_id}] Northwind Talent | Platform Data Engineer", result.stdout)
            self.assertIn("manifest status: launchable", result.stdout)
            self.assertIn("current tracking status: interview", result.stdout)
            self.assertIn("warnings: apply_url missing", result.stdout)

    def test_cli_session_reopen_reuses_browser_stub_executor_for_launchable_items(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            project_root = Path(tmp_dir) / "workspace"
            database_path = project_root / "runtime" / "jobs_ai.db"
            env = {"JOBS_AI_DB_PATH": str(database_path)}
            initialize_schema(database_path)

            with closing(connect_database(database_path)) as connection:
                launchable_job_id = insert_job(
                    connection,
                    _job_record(
                        source="manual",
                        company="Northwind Talent",
                        title="Platform Data Engineer",
                        location="Remote",
                        apply_url="https://example.com/jobs/1",
                    ),
                )
                skipped_job_id = insert_job(
                    connection,
                    _job_record(
                        source="manual",
                        company="Contoso",
                        title="Analytics Engineer",
                        location="Remote",
                        apply_url="https://example.com/jobs/2",
                    ),
                )
                connection.commit()

            manifest_path = _write_manifest(
                project_root / "data" / "exports" / "retro-session.json",
                launchable_job_id=launchable_job_id,
                skipped_job_id=skipped_job_id,
            )
            session_id = record_session_history(
                database_path,
                manifest_path=manifest_path,
                item_count=2,
                launchable_count=1,
                batch_id="discover-batch-1",
                source_query="platform data engineer remote",
                created_at="2026-03-15T12:00:00Z",
            )

            with patch("jobs_ai.launch_executor.webbrowser.open", return_value=True) as open_browser:
                result = RUNNER.invoke(app, ["session", "reopen", str(session_id)], env=env)

            self.assertEqual(result.exit_code, 0)
            self.assertIn("jobs_ai session reopen", result.stdout)
            self.assertIn(f"session id: {session_id}", result.stdout)
            self.assertIn("executor mode: browser_stub", result.stdout)
            self.assertIn("reopen actions: 1", result.stdout)
            self.assertIn("opened in browser: 1", result.stdout)
            open_browser.assert_called_once_with("https://example.com/jobs/1", new=2)


if __name__ == "__main__":
    unittest.main()
