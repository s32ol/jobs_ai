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
from jobs_ai.resume.recommendations import select_queue_recommendations

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


class RecommendationTest(unittest.TestCase):
    def test_data_engineer_recommendation_prefers_pipeline_resume_and_snippet(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            database_path = Path(tmp_dir) / "runtime" / "jobs_ai.db"
            initialize_schema(database_path)

            with closing(connect_database(database_path)) as connection:
                insert_job(
                    connection,
                    _job_record(
                        source="staffing recruiter",
                        company="Northwind Talent",
                        title="Senior Data Engineer",
                        location="Remote",
                        raw_payload={"description": "Python BigQuery GCP pipelines"},
                    ),
                )
                connection.commit()

            recommendations = select_queue_recommendations(database_path)

            self.assertEqual(len(recommendations), 1)
            self.assertEqual(recommendations[0].resume_variant_key, "data-engineering")
            self.assertEqual(recommendations[0].snippet_key, "pipeline-delivery")
            self.assertIn("matched data engineering signals", recommendations[0].explanation)

    def test_analytics_engineer_recommendation_prefers_analytics_resume_and_snippet(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            database_path = Path(tmp_dir) / "runtime" / "jobs_ai.db"
            initialize_schema(database_path)

            with closing(connect_database(database_path)) as connection:
                insert_job(
                    connection,
                    _job_record(
                        source="manual",
                        company="Acme Data",
                        title="Analytics Engineer",
                        location="Sacramento, CA",
                        apply_url="https://boards.greenhouse.io/acme/jobs/1",
                        portal_type="greenhouse",
                        raw_payload={"description": "Looker semantic models and SQL"},
                    ),
                )
                connection.commit()

            recommendations = select_queue_recommendations(database_path)

            self.assertEqual(len(recommendations), 1)
            self.assertEqual(recommendations[0].resume_variant_key, "analytics-engineering")
            self.assertEqual(recommendations[0].snippet_key, "analytics-modeling")
            self.assertIn("matched analytics engineering signals", recommendations[0].explanation)

    def test_telemetry_recommendation_prefers_observability_resume_and_snippet(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            database_path = Path(tmp_dir) / "runtime" / "jobs_ai.db"
            initialize_schema(database_path)

            with closing(connect_database(database_path)) as connection:
                insert_job(
                    connection,
                    _job_record(
                        source="manual",
                        company="Signal Atlas",
                        title="Telemetry Engineer",
                        location="Remote",
                        raw_payload={"description": "Observability pipelines and instrumentation"},
                    ),
                )
                connection.commit()

            recommendations = select_queue_recommendations(database_path)

            self.assertEqual(len(recommendations), 1)
            self.assertEqual(recommendations[0].resume_variant_key, "telemetry-observability")
            self.assertEqual(recommendations[0].snippet_key, "observability-signals")
            self.assertIn("matched telemetry / observability signals", recommendations[0].explanation)

    def test_cli_recommend_reports_resume_variant_snippet_and_explanation(self) -> None:
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
                        raw_payload={"description": "Python BigQuery GCP pipelines"},
                    ),
                )
                connection.commit()

            result = RUNNER.invoke(app, ["recommend"], env=env)

            self.assertEqual(result.exit_code, 0)
            self.assertIn("jobs_ai recommend", result.stdout)
            self.assertIn("resume variant: data-engineering (Data Engineering Resume)", result.stdout)
            self.assertIn("profile snippet: pipeline-delivery (Pipeline Delivery)", result.stdout)
            self.assertIn("explanation: matched data engineering signals from title or stack", result.stdout)


if __name__ == "__main__":
    unittest.main()
