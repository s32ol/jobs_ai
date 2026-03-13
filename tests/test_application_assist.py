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

from jobs_ai.application_assist import build_application_assist
from jobs_ai.cli import app
from jobs_ai.launch_plan import build_launch_plan
from jobs_ai.session_manifest import load_session_manifest

RUNNER = CliRunner()


class ApplicationAssistTest(unittest.TestCase):
    def test_build_application_assist_follows_deterministic_launch_order(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            manifest_path = Path(tmp_dir) / "session.json"
            _write_manifest(
                manifest_path,
                {
                    "created_at": "2026-03-13T20:00:00Z",
                    "item_count": 3,
                    "items": [
                        _complete_item(
                            rank=10,
                            company="Alpha Data",
                            title="Data Engineer",
                            apply_url="https://example.com/jobs/1",
                            resume_key="data-engineering",
                            resume_label="Data Engineering Resume",
                            snippet_key="pipeline-delivery",
                            snippet_label="Pipeline Delivery",
                            snippet_text="Python-first pipeline delivery across SQL warehouses.",
                        ),
                        _complete_item(
                            rank=20,
                            company="Beta Metrics",
                            title="Analytics Engineer",
                            apply_url=None,
                            resume_key="analytics-engineering",
                            resume_label="Analytics Engineering Resume",
                            snippet_key="analytics-modeling",
                            snippet_label="Analytics Modeling",
                            snippet_text="Modeled analytics datasets and BI-facing metrics.",
                        ),
                        _complete_item(
                            rank=30,
                            company="Gamma Telemetry",
                            title="Telemetry Engineer",
                            apply_url="https://example.com/jobs/3",
                            resume_key="telemetry-observability",
                            resume_label="Telemetry / Observability Resume",
                            snippet_key="observability-signals",
                            snippet_label="Observability Signals",
                            snippet_text="Delivered observability pipelines and telemetry workflows.",
                        ),
                    ],
                },
            )

            assist = build_application_assist(build_launch_plan(load_session_manifest(manifest_path)))

            self.assertEqual(assist.total_items, 3)
            self.assertEqual([entry.company for entry in assist.assist_items], ["Alpha Data", "Gamma Telemetry"])
            self.assertEqual([entry.launch_order for entry in assist.assist_items], [1, 2])

    def test_cli_application_assist_shows_expected_fields_for_launchable_items(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            manifest_path = Path(tmp_dir) / "session.json"
            _write_manifest(
                manifest_path,
                {
                    "created_at": "2026-03-13T20:00:00Z",
                    "item_count": 1,
                    "items": [
                        _complete_item(
                            rank=1,
                            company="Northwind Talent",
                            title="Senior Data Engineer",
                            apply_url="https://agency.example/jobs/2",
                            resume_key="data-engineering",
                            resume_label="Data Engineering Resume",
                            snippet_key="pipeline-delivery",
                            snippet_label="Pipeline Delivery",
                            snippet_text="Python-first pipeline delivery across SQL warehouses.",
                        )
                    ],
                },
            )

            result = RUNNER.invoke(app, ["application-assist", str(manifest_path)])

            self.assertEqual(result.exit_code, 0)
            self.assertIn("jobs_ai application-assist", result.stdout)
            self.assertIn(f"manifest path: {manifest_path}", result.stdout)
            self.assertIn("launchable items: 1", result.stdout)
            self.assertIn("status: success", result.stdout)
            self.assertIn("tip: rerun with --portal-hints for portal-specific guidance", result.stdout)
            self.assertIn("[1] Northwind Talent | Senior Data Engineer", result.stdout)
            self.assertIn("URL: https://agency.example/jobs/2", result.stdout)
            self.assertIn("Resume: data-engineering (Data Engineering Resume)", result.stdout)
            self.assertIn("Snippet: pipeline-delivery (Pipeline Delivery)", result.stdout)
            self.assertIn("Text: Python-first pipeline delivery across SQL warehouses.", result.stdout)
            self.assertIn(
                f"python -m jobs_ai launch-dry-run --confirm --executor browser_stub {manifest_path}",
                result.stdout,
            )

    def test_cli_application_assist_skips_incomplete_items(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            manifest_path = Path(tmp_dir) / "session.json"
            _write_manifest(
                manifest_path,
                {
                    "created_at": "2026-03-13T20:00:00Z",
                    "item_count": 3,
                    "items": [
                        _complete_item(
                            rank=1,
                            company="Northwind Talent",
                            title="Senior Data Engineer",
                            apply_url="https://agency.example/jobs/2",
                            resume_key="data-engineering",
                            resume_label="Data Engineering Resume",
                            snippet_key="pipeline-delivery",
                            snippet_label="Pipeline Delivery",
                            snippet_text="Python-first pipeline delivery across SQL warehouses.",
                        ),
                        _complete_item(
                            rank=2,
                            company="Contoso",
                            title="Analytics Engineer",
                            apply_url=None,
                            resume_key="analytics-engineering",
                            resume_label="Analytics Engineering Resume",
                            snippet_key="analytics-modeling",
                            snippet_label="Analytics Modeling",
                            snippet_text="Modeled analytics datasets and BI-facing metrics.",
                        ),
                        {
                            "rank": 3,
                            "company": "Fabrikam",
                            "title": "Telemetry Engineer",
                            "apply_url": "https://example.com/jobs/3",
                            "recommended_resume_variant": {
                                "key": "telemetry-observability",
                                "label": "Telemetry / Observability Resume",
                            },
                            "recommended_profile_snippet": None,
                        },
                    ],
                },
            )

            result = RUNNER.invoke(app, ["application-assist", str(manifest_path)])

            self.assertEqual(result.exit_code, 0)
            self.assertIn("[1] Northwind Talent | Senior Data Engineer", result.stdout)
            self.assertNotIn("Contoso | Analytics Engineer", result.stdout)
            self.assertNotIn("Fabrikam | Telemetry Engineer", result.stdout)

    def test_cli_application_assist_handles_empty_launchable_set(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            manifest_path = Path(tmp_dir) / "session.json"
            _write_manifest(
                manifest_path,
                {
                    "created_at": "2026-03-13T20:00:00Z",
                    "item_count": 1,
                    "items": [
                        _complete_item(
                            rank=1,
                            company="Contoso",
                            title="Analytics Engineer",
                            apply_url=None,
                            resume_key="analytics-engineering",
                            resume_label="Analytics Engineering Resume",
                            snippet_key="analytics-modeling",
                            snippet_label="Analytics Modeling",
                            snippet_text="Modeled analytics datasets and BI-facing metrics.",
                        )
                    ],
                },
            )

            result = RUNNER.invoke(app, ["application-assist", str(manifest_path)])

            self.assertEqual(result.exit_code, 0)
            self.assertIn("jobs_ai application-assist", result.stdout)
            self.assertIn(f"manifest path: {manifest_path}", result.stdout)
            self.assertIn("launchable items: 0", result.stdout)
            self.assertIn("status: no launchable application assists", result.stdout)
            self.assertIn(
                f"python -m jobs_ai launch-plan {manifest_path}",
                result.stdout,
            )

    def test_cli_application_assist_portal_hints_show_detected_portal_details(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            manifest_path = Path(tmp_dir) / "session.json"
            _write_manifest(
                manifest_path,
                {
                    "created_at": "2026-03-13T20:00:00Z",
                    "item_count": 1,
                    "items": [
                        _complete_item(
                            rank=1,
                            company="Acme Data",
                            title="Data Engineer",
                            apply_url=(
                                "https://jobs.ashbyhq.com/acme"
                                "?jobId=123e4567-e89b-12d3-a456-426614174000&utm_source=linkedin"
                            ),
                            resume_key="data-engineering",
                            resume_label="Data Engineering Resume",
                            snippet_key="pipeline-delivery",
                            snippet_label="Pipeline Delivery",
                            snippet_text="Python-first pipeline delivery across SQL warehouses.",
                        )
                    ],
                },
            )

            result = RUNNER.invoke(app, ["application-assist", "--portal-hints", str(manifest_path)])

            self.assertEqual(result.exit_code, 0)
            self.assertIn("portal: Ashby", result.stdout)
            self.assertIn(
                "company apply_url: https://jobs.ashbyhq.com/acme/123e4567-e89b-12d3-a456-426614174000",
                result.stdout,
            )
            self.assertIn("portal hints: Ashby links may hide the job id", result.stdout)


def _complete_item(
    *,
    rank: int,
    company: str,
    title: str,
    apply_url: str | None,
    resume_key: str,
    resume_label: str,
    snippet_key: str,
    snippet_label: str,
    snippet_text: str,
) -> dict[str, object | None]:
    return {
        "rank": rank,
        "company": company,
        "title": title,
        "apply_url": apply_url,
        "recommended_resume_variant": {
            "key": resume_key,
            "label": resume_label,
        },
        "recommended_profile_snippet": {
            "key": snippet_key,
            "label": snippet_label,
            "text": snippet_text,
        },
    }


def _write_manifest(manifest_path: Path, payload: dict[str, object]) -> None:
    manifest_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    unittest.main()
