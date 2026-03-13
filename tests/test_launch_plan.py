from __future__ import annotations

from contextlib import closing
from datetime import datetime, timezone
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
from jobs_ai.launch_plan import build_launch_plan
from jobs_ai.session_export import export_launch_preview_session
from jobs_ai.session_manifest import load_session_manifest

RUNNER = CliRunner()


def _job_record(
    *,
    source: str,
    company: str,
    title: str,
    location: str,
    raw_payload: dict[str, object] | None = None,
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
        "raw_json": json.dumps(raw_payload or {}, ensure_ascii=True),
    }


class LaunchPlanTest(unittest.TestCase):
    def test_valid_manifest_produces_launch_plan(self) -> None:
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

            manifest = load_session_manifest(
                export_launch_preview_session(
                    database_path,
                    exports_dir,
                    created_at=datetime(2026, 3, 13, 19, 0, 0, tzinfo=timezone.utc),
                ).export_path
            )

            plan = build_launch_plan(manifest)

            self.assertEqual(plan.total_items, 1)
            self.assertEqual(plan.launchable_items, 1)
            self.assertEqual(plan.skipped_items, 0)
            self.assertEqual(plan.items[0].launch_order, 1)
            self.assertTrue(plan.items[0].launchable)
            self.assertEqual(plan.items[0].company, "Northwind Talent")
            self.assertEqual(plan.items[0].title, "Senior Data Engineer")

    def test_cli_launch_plan_flags_and_skips_incomplete_items_clearly(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            manifest_path = Path(tmp_dir) / "session.json"
            manifest_path.write_text(
                json.dumps(
                    {
                        "created_at": "2026-03-13T19:00:00Z",
                        "item_count": 3,
                        "items": [
                            {
                                "rank": 1,
                                "company": "Northwind Talent",
                                "title": "Senior Data Engineer",
                                "apply_url": "https://agency.example/jobs/2",
                                "recommended_resume_variant": {
                                    "key": "data-engineering",
                                    "label": "Data Engineering Resume",
                                },
                                "recommended_profile_snippet": {
                                    "key": "pipeline-delivery",
                                    "label": "Pipeline Delivery",
                                    "text": "Python-first pipeline delivery across SQL warehouses, BigQuery/GCP, and production data systems.",
                                },
                            },
                            {
                                "rank": 2,
                                "company": "Contoso",
                                "title": "Analytics Engineer",
                                "apply_url": None,
                                "recommended_resume_variant": {
                                    "key": "analytics-engineering",
                                    "label": "Analytics Engineering Resume",
                                },
                                "recommended_profile_snippet": {
                                    "key": "warehouse-modeling",
                                    "label": "Warehouse Modeling",
                                    "text": "Modeled analytics datasets and business-facing metrics in SQL-first warehouse environments.",
                                },
                            },
                            {
                                "rank": 3,
                                "company": "Fabrikam",
                                "title": "Telemetry Engineer",
                                "apply_url": "https://example.com/jobs/3",
                                "recommended_resume_variant": None,
                                "recommended_profile_snippet": {
                                    "key": "observability-delivery",
                                    "label": "Observability Delivery",
                                    "text": "Delivered telemetry pipelines and observability tooling for production systems.",
                                },
                            },
                        ],
                    },
                    indent=2,
                )
                + "\n",
                encoding="utf-8",
            )

            result = RUNNER.invoke(app, ["launch-plan", str(manifest_path)])

            self.assertEqual(result.exit_code, 0)
            self.assertIn("jobs_ai launch-plan", result.stdout)
            self.assertIn("total items: 3", result.stdout)
            self.assertIn("launchable items: 1", result.stdout)
            self.assertIn("skipped items: 2", result.stdout)
            self.assertIn("1. launch order 1 | Northwind Talent | Senior Data Engineer", result.stdout)
            self.assertIn("2. launch order skipped | Contoso | Analytics Engineer", result.stdout)
            self.assertIn("status: skipped (apply_url missing)", result.stdout)
            self.assertIn("3. launch order skipped | Fabrikam | Telemetry Engineer", result.stdout)
            self.assertIn("status: skipped (recommended_resume_variant incomplete)", result.stdout)

    def test_launch_order_is_deterministic(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            manifest_path = Path(tmp_dir) / "session.json"
            manifest_path.write_text(
                json.dumps(
                    {
                        "created_at": "2026-03-13T19:00:00Z",
                        "item_count": 3,
                        "items": [
                            {
                                "rank": 10,
                                "company": "Alpha Data",
                                "title": "Data Engineer",
                                "apply_url": "https://example.com/jobs/1",
                                "recommended_resume_variant": {
                                    "key": "data-engineering",
                                    "label": "Data Engineering Resume",
                                },
                                "recommended_profile_snippet": {
                                    "key": "pipeline-delivery",
                                    "label": "Pipeline Delivery",
                                    "text": "Python-first pipeline delivery across SQL warehouses, BigQuery/GCP, and production data systems.",
                                },
                            },
                            {
                                "rank": 20,
                                "company": "Beta Metrics",
                                "title": "Analytics Engineer",
                                "apply_url": None,
                                "recommended_resume_variant": {
                                    "key": "analytics-engineering",
                                    "label": "Analytics Engineering Resume",
                                },
                                "recommended_profile_snippet": {
                                    "key": "warehouse-modeling",
                                    "label": "Warehouse Modeling",
                                    "text": "Modeled analytics datasets and business-facing metrics in SQL-first warehouse environments.",
                                },
                            },
                            {
                                "rank": 30,
                                "company": "Gamma Telemetry",
                                "title": "Telemetry Engineer",
                                "apply_url": "https://example.com/jobs/3",
                                "recommended_resume_variant": {
                                    "key": "telemetry-observability",
                                    "label": "Telemetry / Observability Resume",
                                },
                                "recommended_profile_snippet": {
                                    "key": "observability-delivery",
                                    "label": "Observability Delivery",
                                    "text": "Delivered telemetry pipelines and observability tooling for production systems.",
                                },
                            },
                        ],
                    },
                    indent=2,
                )
                + "\n",
                encoding="utf-8",
            )

            manifest = load_session_manifest(manifest_path)
            first_plan = build_launch_plan(manifest)
            second_plan = build_launch_plan(manifest)

            self.assertEqual(
                [item.company for item in first_plan.items],
                ["Alpha Data", "Beta Metrics", "Gamma Telemetry"],
            )
            self.assertEqual([item.launch_order for item in first_plan.items], [1, None, 2])
            self.assertEqual(
                [item.launch_order for item in second_plan.items],
                [1, None, 2],
            )


if __name__ == "__main__":
    unittest.main()
