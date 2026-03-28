from __future__ import annotations

from datetime import datetime
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

from jobs_ai.application_assist import build_application_assist
from jobs_ai.application_prefill import ApplicationPrefillResult
from jobs_ai.cli import app
from jobs_ai.launch_plan import build_launch_plan
from jobs_ai.session_manifest import ManifestSelection, load_session_manifest

RUNNER = CliRunner()
FIXED_LOCAL_TIMESTAMP = "2026-03-17T18:15:00-07:00"


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
            resolved_manifest_path = manifest_path.resolve()

            self.assertEqual(result.exit_code, 0)
            self.assertIn("jobs_ai application-assist", result.stdout)
            self.assertIn(f"manifest path: {resolved_manifest_path}", result.stdout)
            self.assertIn("launchable items: 1", result.stdout)
            self.assertIn("status: success", result.stdout)
            self.assertIn("tip: rerun with --portal-hints for portal-specific guidance", result.stdout)
            self.assertIn("[1] Northwind Talent | Senior Data Engineer", result.stdout)
            self.assertIn("URL: https://agency.example/jobs/2", result.stdout)
            self.assertIn("Resume: data-engineering (Data Engineering Resume)", result.stdout)
            self.assertIn("Snippet: pipeline-delivery (Pipeline Delivery)", result.stdout)
            self.assertIn("Text: Python-first pipeline delivery across SQL warehouses.", result.stdout)
            self.assertIn(
                f"python -m jobs_ai launch-dry-run --confirm --executor browser_stub {resolved_manifest_path}",
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
            resolved_manifest_path = manifest_path.resolve()

            self.assertEqual(result.exit_code, 0)
            self.assertIn("jobs_ai application-assist", result.stdout)
            self.assertIn(f"manifest path: {resolved_manifest_path}", result.stdout)
            self.assertIn("launchable items: 0", result.stdout)
            self.assertIn("status: no launchable application assists", result.stdout)
            self.assertIn(
                f"python -m jobs_ai launch-plan {resolved_manifest_path}",
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

    def test_cli_application_assist_can_prompt_and_log_outcome_after_prefill(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            project_root = Path(tmp_dir) / "workspace"
            manifest_path = project_root / "session.json"
            env = {"JOBS_AI_DB_PATH": str(project_root / "runtime" / "jobs_ai.db")}
            _write_manifest(
                manifest_path,
                {
                    "created_at": "2026-03-17T23:16:42Z",
                    "item_count": 1,
                    "items": [
                        _complete_item(
                            rank=1,
                            company="Care Access",
                            title="Enterprise Performance Analytics Engineer",
                            apply_url="https://job-boards.greenhouse.io/careaccess/jobs/4052147009",
                            resume_key="analytics-engineering",
                            resume_label="Analytics Engineering Resume",
                            snippet_key="analytics-modeling",
                            snippet_label="Analytics Modeling",
                            snippet_text="Analytics engineering work centered on SQL modeling.",
                        )
                    ],
                },
            )
            browser = _StubBrowser()
            expected_log_path = (
                project_root
                / "data"
                / "applications"
                / "2026-03-17-care-access-greenhouse.json"
            )

            with (
                patch("jobs_ai.workspace.discover_project_root", return_value=project_root),
                patch("jobs_ai.cli.create_prefill_browser_backend", return_value=browser),
                patch(
                    "jobs_ai.cli.run_application_prefill",
                    return_value=_prefill_result(manifest_path),
                ),
                patch(
                    "jobs_ai.application_log._current_local_datetime",
                    return_value=_fixed_local_datetime(),
                ),
                patch(
                    "jobs_ai.cli.click.prompt",
                    side_effect=["", "applied", "prefill + manual fix"],
                ),
            ):
                result = RUNNER.invoke(
                    app,
                    [
                        "application-assist",
                        str(manifest_path),
                        "--prefill",
                        "--launch-order",
                        "1",
                        "--log-outcome",
                    ],
                    env=env,
                )

            self.assertEqual(result.exit_code, 0)
            self.assertTrue(browser.closed)
            self.assertIn("mode: review-first prefill", result.stdout)
            self.assertIn("Browser remains open for manual review.", result.stdout)
            self.assertIn("jobs_ai application-log", result.stdout)
            self.assertIn(
                "Logged application [1] Care Access - Enterprise Performance Analytics Engineer",
                result.stdout,
            )
            payload = json.loads(expected_log_path.read_text(encoding="utf-8"))
            self.assertEqual(payload["status"], "applied")
            self.assertEqual(payload["notes"], "prefill + manual fix")
            self.assertEqual(payload["timestamp"], FIXED_LOCAL_TIMESTAMP)

    def test_cli_application_assist_can_auto_log_outcome_non_interactively(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            project_root = Path(tmp_dir) / "workspace"
            manifest_path = project_root / "session.json"
            env = {"JOBS_AI_DB_PATH": str(project_root / "runtime" / "jobs_ai.db")}
            _write_manifest(
                manifest_path,
                {
                    "created_at": "2026-03-17T23:16:42Z",
                    "item_count": 1,
                    "items": [
                        _complete_item(
                            rank=1,
                            company="Care Access",
                            title="Enterprise Performance Analytics Engineer",
                            apply_url="https://job-boards.greenhouse.io/careaccess/jobs/4052147009",
                            resume_key="analytics-engineering",
                            resume_label="Analytics Engineering Resume",
                            snippet_key="analytics-modeling",
                            snippet_label="Analytics Modeling",
                            snippet_text="Analytics engineering work centered on SQL modeling.",
                        )
                    ],
                },
            )
            browser = _StubBrowser()
            expected_log_path = (
                project_root
                / "data"
                / "applications"
                / "2026-03-17-care-access-greenhouse.json"
            )

            with (
                patch("jobs_ai.workspace.discover_project_root", return_value=project_root),
                patch("jobs_ai.cli.create_prefill_browser_backend", return_value=browser),
                patch(
                    "jobs_ai.cli.run_application_prefill",
                    return_value=_prefill_result(manifest_path),
                ),
                patch(
                    "jobs_ai.application_log._current_local_datetime",
                    return_value=_fixed_local_datetime(),
                ),
                patch(
                    "jobs_ai.cli.click.prompt",
                    side_effect=AssertionError("prompt should stay unused"),
                ),
            ):
                result = RUNNER.invoke(
                    app,
                    [
                        "application-assist",
                        str(manifest_path),
                        "--prefill",
                        "--launch-order",
                        "1",
                        "--no-hold-open",
                        "--log-status",
                        "applied",
                        "--log-notes",
                        "minor dropdown fixes",
                    ],
                    env=env,
                )

            self.assertEqual(result.exit_code, 0)
            self.assertTrue(browser.closed)
            self.assertIn("jobs_ai application-log", result.stdout)
            payload = json.loads(expected_log_path.read_text(encoding="utf-8"))
            self.assertEqual(payload["status"], "applied")
            self.assertEqual(payload["notes"], "minor dropdown fixes")

    def test_cli_application_assist_preserves_success_when_post_run_logging_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            project_root = Path(tmp_dir) / "workspace"
            manifest_path = project_root / "session.json"
            env = {"JOBS_AI_DB_PATH": str(project_root / "runtime" / "jobs_ai.db")}
            _write_manifest(
                manifest_path,
                {
                    "created_at": "2026-03-17T23:16:42Z",
                    "item_count": 1,
                    "items": [
                        _complete_item(
                            rank=1,
                            company="Care Access",
                            title="Enterprise Performance Analytics Engineer",
                            apply_url="https://job-boards.greenhouse.io/careaccess/jobs/4052147009",
                            resume_key="analytics-engineering",
                            resume_label="Analytics Engineering Resume",
                            snippet_key="analytics-modeling",
                            snippet_label="Analytics Modeling",
                            snippet_text="Analytics engineering work centered on SQL modeling.",
                        )
                    ],
                },
            )
            browser = _StubBrowser()

            with (
                patch("jobs_ai.workspace.discover_project_root", return_value=project_root),
                patch("jobs_ai.cli.create_prefill_browser_backend", return_value=browser),
                patch(
                    "jobs_ai.cli.run_application_prefill",
                    return_value=_prefill_result(manifest_path),
                ),
                patch(
                    "jobs_ai.cli.write_application_log",
                    side_effect=ValueError("existing log file is not valid JSON"),
                ),
            ):
                result = RUNNER.invoke(
                    app,
                    [
                        "application-assist",
                        str(manifest_path),
                        "--prefill",
                        "--launch-order",
                        "1",
                        "--no-hold-open",
                        "--log-status",
                        "applied",
                    ],
                    env=env,
                )

            self.assertEqual(result.exit_code, 0)
            self.assertTrue(browser.closed)
            self.assertIn("mode: review-first prefill", result.stdout)
            self.assertIn(
                "Outcome logging failed after application-assist completed successfully.",
                result.stdout,
            )
            self.assertIn("jobs_ai application-log", result.stdout)
            self.assertIn("status: failed", result.stdout)
            self.assertIn("existing log file is not valid JSON", result.stdout)


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
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


class _StubBrowser:
    backend_name = "stub"

    def __init__(self) -> None:
        self.closed = False

    def close(self) -> None:
        self.closed = True


def _fixed_local_datetime() -> datetime:
    return datetime.fromisoformat(FIXED_LOCAL_TIMESTAMP)


def _prefill_result(manifest_path: Path) -> ApplicationPrefillResult:
    return ApplicationPrefillResult(
        manifest_path=manifest_path.resolve(),
        applicant_profile_path=manifest_path.parent / ".jobs_ai_applicant_profile.json",
        launch_order=1,
        company="Care Access",
        title="Enterprise Performance Analytics Engineer",
        original_apply_url="https://job-boards.greenhouse.io/careaccess/jobs/4052147009",
        opened_url="https://job-boards.greenhouse.io/careaccess/jobs/4052147009",
        page_title="Apply",
        portal_type="greenhouse",
        portal_label="Greenhouse",
        support_level="supported",
        browser_backend="stub",
        recommended_resume_variant=ManifestSelection(
            key="analytics-engineering",
            label="Analytics Engineering Resume",
            text=None,
        ),
        recommended_profile_snippet=ManifestSelection(
            key="analytics-modeling",
            label="Analytics Modeling",
            text="Analytics engineering work centered on SQL modeling.",
        ),
        resolved_resume_path=manifest_path.parent / "resume.pdf",
        filled_fields=(),
        skipped_fields=(),
        unresolved_required_fields=(),
        submit_controls=("Submit application",),
        stopped_before_submit=True,
        status="success",
        notes=(),
    )


if __name__ == "__main__":
    unittest.main()
