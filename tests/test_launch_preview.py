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

from jobs_ai.cli import app
from jobs_ai.db import connect_database, initialize_schema, insert_job
from jobs_ai.launch_preview import select_launch_preview

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


class LaunchPreviewTest(unittest.TestCase):
    def test_select_launch_preview_only_includes_new_jobs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            database_path = Path(tmp_dir) / "runtime" / "jobs_ai.db"
            initialize_schema(database_path)

            with closing(connect_database(database_path)) as connection:
                new_job_id = _insert_job_with_status(
                    connection,
                    source="manual",
                    company="Acme Data",
                    title="Analytics Engineer",
                    location="Remote",
                )
                _insert_job_with_status(
                    connection,
                    source="staffing recruiter",
                    company="Northwind Talent",
                    title="Platform Data Engineer",
                    location="Remote",
                    status="applied",
                    raw_payload={"description": "Python BigQuery GCP contract"},
                )
                connection.commit()

            previews = select_launch_preview(database_path)

            self.assertEqual([preview.job_id for preview in previews], [new_job_id])
            self.assertTrue(all(preview.company != "Northwind Talent" for preview in previews))

    def test_cli_launch_preview_reports_apply_url_and_recommendation_info(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            database_path = Path(tmp_dir) / "runtime" / "jobs_ai.db"
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

            result = RUNNER.invoke(app, ["launch-preview"], env=env)

            self.assertEqual(result.exit_code, 0)
            self.assertIn("jobs_ai launch-preview", result.stdout)
            self.assertIn("apply_url: https://agency.example/jobs/2", result.stdout)
            self.assertIn(
                "recommended resume variant: data-engineering (Data Engineering Resume)",
                result.stdout,
            )
            self.assertIn(
                "recommended profile snippet: pipeline-delivery (Pipeline Delivery)",
                result.stdout,
            )
            self.assertIn(
                "explanation: matched data engineering signals from title or stack",
                result.stdout,
            )
            self.assertIn("tip: rerun with --portal-hints when links look portal-hosted", result.stdout)
            self.assertIn("python -m jobs_ai export-session", result.stdout)

    def test_select_launch_preview_applies_limit_after_ranking(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            database_path = Path(tmp_dir) / "runtime" / "jobs_ai.db"
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

            previews = select_launch_preview(database_path, limit=2)

            self.assertEqual(len(previews), 2)
            self.assertEqual([preview.job_id for preview in previews], [top_job_id, second_job_id])

    def test_cli_launch_preview_portal_hints_show_detected_portal_details(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            database_path = Path(tmp_dir) / "runtime" / "jobs_ai.db"
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
                        apply_url="https://boards.greenhouse.io/acme?gh_jid=12345&gh_src=linkedin",
                        raw_payload={"description": "Python BigQuery GCP pipelines"},
                    ),
                )
                connection.commit()

            result = RUNNER.invoke(app, ["launch-preview", "--portal-hints"], env=env)

            self.assertEqual(result.exit_code, 0)
            self.assertIn("portal: Greenhouse", result.stdout)
            self.assertIn(
                "company apply_url: https://boards.greenhouse.io/acme/jobs/12345",
                result.stdout,
            )
            self.assertIn("portal hints: Prefer company-scoped Greenhouse links", result.stdout)


if __name__ == "__main__":
    unittest.main()
