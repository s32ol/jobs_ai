from __future__ import annotations

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
from jobs_ai.launch_dry_run import OPEN_URL_ACTION, build_launch_dry_run
from jobs_ai.launch_executor import LaunchExecutionReport, NO_OP_EXECUTOR_MODE
from jobs_ai.launch_plan import build_launch_plan
from jobs_ai.main import render_launch_dry_run_report
from jobs_ai.session_manifest import load_session_manifest

RUNNER = CliRunner()


class LaunchDryRunTest(unittest.TestCase):
    def test_build_launch_dry_run_follows_deterministic_launch_order(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            manifest_path = Path(tmp_dir) / "session.json"
            manifest_path.write_text(
                json.dumps(
                    {
                        "created_at": "2026-03-13T20:00:00Z",
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

            dry_run = build_launch_dry_run(build_launch_plan(load_session_manifest(manifest_path)))

            self.assertEqual(dry_run.total_items, 3)
            self.assertEqual(dry_run.launchable_items, 2)
            self.assertEqual(dry_run.skipped_items, 1)
            self.assertEqual([step.company for step in dry_run.steps], ["Alpha Data", "Gamma Telemetry"])
            self.assertEqual([step.launch_order for step in dry_run.steps], [1, 2])
            self.assertTrue(all(step.action_label == OPEN_URL_ACTION for step in dry_run.steps))

    def test_cli_launch_dry_run_skips_incomplete_items_from_actions(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            manifest_path = Path(tmp_dir) / "session.json"
            manifest_path.write_text(
                json.dumps(
                    {
                        "created_at": "2026-03-13T20:00:00Z",
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

            result = RUNNER.invoke(app, ["launch-dry-run", str(manifest_path)])

            self.assertEqual(result.exit_code, 0)
            self.assertEqual(
                result.stdout,
                (
                    "[1] Northwind Talent | Senior Data Engineer\n"
                    "URL: https://agency.example/jobs/2\n"
                    "Executor: noop\n"
                    "Action: OPEN_URL\n"
                    "Result: dry run only\n"
                ),
            )

    def test_render_launch_dry_run_report_uses_missing_url_placeholder(self) -> None:
        rendered = render_launch_dry_run_report(
            (
                LaunchExecutionReport(
                    executor_mode=NO_OP_EXECUTOR_MODE,
                    launch_order=1,
                    action_label=OPEN_URL_ACTION,
                    company="Northwind Talent",
                    title="Senior Data Engineer",
                    apply_url="",
                    status="noop",
                ),
            )
        )

        self.assertEqual(
            rendered,
            "\n".join(
                [
                    "[1] Northwind Talent | Senior Data Engineer",
                    "URL: <missing>",
                    "Executor: noop",
                    "Action: OPEN_URL",
                    "Result: dry run only",
                ]
            ),
        )

    def test_render_launch_dry_run_report_orders_entries_deterministically(self) -> None:
        rendered = render_launch_dry_run_report(
            (
                LaunchExecutionReport(
                    executor_mode=NO_OP_EXECUTOR_MODE,
                    launch_order=2,
                    action_label=OPEN_URL_ACTION,
                    company="Gamma Telemetry",
                    title="Telemetry Engineer",
                    apply_url="https://example.com/jobs/2",
                    status="noop",
                ),
                LaunchExecutionReport(
                    executor_mode=NO_OP_EXECUTOR_MODE,
                    launch_order=1,
                    action_label=OPEN_URL_ACTION,
                    company="Alpha Data",
                    title="Data Engineer",
                    apply_url="https://example.com/jobs/1",
                    status="noop",
                ),
            )
        )

        self.assertEqual(
            rendered,
            (
                "[1] Alpha Data | Data Engineer\n"
                "URL: https://example.com/jobs/1\n"
                "Executor: noop\n"
                "Action: OPEN_URL\n"
                "Result: dry run only\n\n"
                "[2] Gamma Telemetry | Telemetry Engineer\n"
                "URL: https://example.com/jobs/2\n"
                "Executor: noop\n"
                "Action: OPEN_URL\n"
                "Result: dry run only"
            ),
        )


if __name__ == "__main__":
    unittest.main()
