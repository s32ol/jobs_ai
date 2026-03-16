from __future__ import annotations

from contextlib import closing
from datetime import datetime, timezone
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

from jobs_ai.cli import app
from jobs_ai.db import connect_database, initialize_schema, insert_job
from jobs_ai.launch_preview import select_launch_preview
from jobs_ai.session_export import export_launch_preview_session, export_launch_previews_session

RUNNER = CliRunner()


def _job_record(
    *,
    source: str,
    company: str,
    title: str,
    location: str,
    raw_payload: dict[str, object] | None = None,
    apply_url: str | None = None,
    portal_type: str | None = None,
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
    raw_payload: dict[str, object] | None = None,
    apply_url: str | None = None,
    portal_type: str | None = None,
) -> int:
    job_id = insert_job(
        connection,
        _job_record(
            source=source,
            company=company,
            title=title,
            location=location,
            raw_payload=raw_payload,
            apply_url=apply_url,
            portal_type=portal_type,
        ),
    )
    if status != "new":
        connection.execute(
            "UPDATE jobs SET status = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
            (status, job_id),
        )
    return job_id


class SessionExportTest(unittest.TestCase):
    def test_cli_export_session_creates_json_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            project_root = Path(tmp_dir) / "workspace"
            database_path = project_root / "runtime" / "jobs_ai.db"
            env = {"JOBS_AI_DB_PATH": str(database_path)}
            initialize_schema(database_path)

            with closing(connect_database(database_path)) as connection:
                insert_job(
                    connection,
                    _job_record(
                        source="staffing recruiter",
                        company="Northwind Talent",
                        title="Senior Data Engineer",
                        location="Remote",
                        apply_url="https://agency.example/jobs/2",
                        raw_payload={"description": "Python BigQuery GCP pipelines"},
                    ),
                )
                connection.commit()

            with patch("jobs_ai.workspace.discover_project_root", return_value=project_root):
                result = RUNNER.invoke(app, ["export-session"], env=env)

            self.assertEqual(result.exit_code, 0)
            self.assertIn("jobs_ai export-session", result.stdout)

            export_files = sorted((project_root / "data" / "exports").glob("launch-preview-session-*.json"))
            self.assertEqual(len(export_files), 1)
            self.assertIn(f"export path: {export_files[0]}", result.stdout)

    def test_exported_content_includes_expected_fields(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            database_path = Path(tmp_dir) / "runtime" / "jobs_ai.db"
            exports_dir = Path(tmp_dir) / "data" / "exports"
            initialize_schema(database_path)

            with closing(connect_database(database_path)) as connection:
                job_id = insert_job(
                    connection,
                    _job_record(
                        source="staffing recruiter",
                        company="Northwind Talent",
                        title="Senior Data Engineer",
                        location="Remote",
                        apply_url="https://agency.example/jobs/2",
                        raw_payload={"description": "Python BigQuery GCP pipelines"},
                    ),
                )
                connection.commit()

            result = export_launch_preview_session(
                database_path,
                exports_dir,
                created_at=datetime(2026, 3, 13, 17, 30, 45, tzinfo=timezone.utc),
            )
            payload = json.loads(result.export_path.read_text(encoding="utf-8"))

            self.assertEqual(payload["created_at"], "2026-03-13T17:30:45Z")
            self.assertEqual(payload["item_count"], 1)

            item = payload["items"][0]
            self.assertEqual(item["job_id"], job_id)
            self.assertEqual(item["company"], "Northwind Talent")
            self.assertEqual(item["title"], "Senior Data Engineer")
            self.assertEqual(item["location"], "Remote")
            self.assertEqual(item["source"], "staffing recruiter")
            self.assertEqual(item["apply_url"], "https://agency.example/jobs/2")
            self.assertIn("matched data engineering signals", item["explanation"])
            self.assertEqual(
                item["recommended_resume_variant"],
                {
                    "key": "data-engineering",
                    "label": "Data Engineering Resume",
                },
            )
            self.assertEqual(
                item["recommended_profile_snippet"],
                {
                    "key": "pipeline-delivery",
                    "label": "Pipeline Delivery",
                    "text": "Python-first pipeline delivery across SQL warehouses, BigQuery/GCP, and production data systems.",
                },
            )

    def test_export_session_limit_applies_after_ranking(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            database_path = Path(tmp_dir) / "runtime" / "jobs_ai.db"
            exports_dir = Path(tmp_dir) / "data" / "exports"
            initialize_schema(database_path)

            with closing(connect_database(database_path)) as connection:
                _insert_job_with_status(
                    connection,
                    source="manual",
                    company="Acme Data",
                    title="Analytics Engineer",
                    location="Sacramento, CA",
                    apply_url="https://boards.greenhouse.io/acme/jobs/1",
                    portal_type="greenhouse",
                    raw_payload={"description": "Looker dashboards"},
                )
                top_job_id = _insert_job_with_status(
                    connection,
                    source="staffing recruiter",
                    company="Northwind Talent",
                    title="Platform Data Engineer",
                    location="Remote",
                    apply_url="https://agency.example/jobs/2",
                    raw_payload={"description": "Python BigQuery GCP contract"},
                )
                second_job_id = _insert_job_with_status(
                    connection,
                    source="manual",
                    company="Bright Metrics",
                    title="Data Engineer",
                    location="Remote",
                    apply_url="https://jobs.example.com/3",
                    raw_payload={"description": "Python pipelines"},
                )
                connection.commit()

            result = export_launch_preview_session(
                database_path,
                exports_dir,
                limit=2,
                created_at=datetime(2026, 3, 13, 18, 0, 0, tzinfo=timezone.utc),
            )
            payload = json.loads(result.export_path.read_text(encoding="utf-8"))

            self.assertEqual(result.limit, 2)
            self.assertEqual(payload["item_count"], 2)
            self.assertEqual([item["job_id"] for item in payload["items"]], [top_job_id, second_job_id])

    def test_export_launch_previews_session_accepts_prebuilt_batch_and_label(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            database_path = Path(tmp_dir) / "runtime" / "jobs_ai.db"
            exports_dir = Path(tmp_dir) / "data" / "exports"
            initialize_schema(database_path)

            with closing(connect_database(database_path)) as connection:
                insert_job(
                    connection,
                    _job_record(
                        source="staffing recruiter",
                        company="Northwind Talent",
                        title="Senior Data Engineer",
                        location="Remote",
                        apply_url="https://agency.example/jobs/2",
                        raw_payload={"description": "Python BigQuery GCP pipelines"},
                    ),
                )
                connection.commit()

            previews = select_launch_preview(database_path)
            result = export_launch_previews_session(
                previews,
                exports_dir,
                created_at=datetime(2026, 3, 13, 19, 15, 0, tzinfo=timezone.utc),
                label="morning batch",
            )
            payload = json.loads(result.export_path.read_text(encoding="utf-8"))

            self.assertEqual(result.label, "morning-batch")
            self.assertIn("launch-preview-session-morning-batch-", result.export_path.name)
            self.assertEqual(payload["label"], "morning-batch")
            self.assertEqual(payload["item_count"], 1)


if __name__ == "__main__":
    unittest.main()
