from __future__ import annotations

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
from jobs_ai.launch_dry_run import (
    OPEN_URL_ACTION,
    LaunchDryRun,
    LaunchDryRunStep,
    build_launch_dry_run,
)
from jobs_ai.launch_executor import (
    BROWSER_STUB_EXECUTOR_MODE,
    LaunchExecutionReport,
    NO_OP_EXECUTION_STATUS,
    NO_OP_EXECUTOR_MODE,
    OPENED_EXECUTION_STATUS,
    PRINTED_EXECUTION_STATUS,
    REMOTE_PRINT_EXECUTOR_MODE,
    SKIPPED_MISSING_URL_EXECUTION_STATUS,
    BrowserLaunchExecutor,
    LaunchStepExecutor,
    NoOpLaunchExecutor,
    RemotePrintLaunchExecutor,
    collect_launch_execution_reports,
    execute_launch_dry_run,
    select_launch_executor,
)
from jobs_ai.launch_plan import build_launch_plan
from jobs_ai.session_manifest import ManifestSelection, load_session_manifest

RUNNER = CliRunner()


class RecordingLaunchExecutor:
    def __init__(self) -> None:
        self.launch_orders: list[int] = []

    def execute_step(self, step: LaunchDryRunStep) -> LaunchExecutionReport:
        self.launch_orders.append(step.launch_order)
        return LaunchExecutionReport(
            executor_mode="recording",
            launch_order=step.launch_order,
            action_label=step.action_label,
            company=step.company,
            title=step.title,
            apply_url=step.apply_url,
            status="recorded",
        )


class LaunchExecutorTest(unittest.TestCase):
    def test_no_op_executor_accepts_launchable_steps(self) -> None:
        dry_run = _make_dry_run(
            steps=(
                _make_step(launch_order=1, company="Northwind Talent"),
                _make_step(launch_order=2, company="Fabrikam"),
            )
        )

        executor = NoOpLaunchExecutor()
        result = execute_launch_dry_run(dry_run, executor)
        report = executor.execute_step(dry_run.steps[0])

        self.assertIsInstance(executor, LaunchStepExecutor)
        self.assertIs(result, dry_run)
        self.assertEqual(result.steps, dry_run.steps)
        self.assertEqual(report.executor_mode, NO_OP_EXECUTOR_MODE)
        self.assertEqual(report.status, NO_OP_EXECUTION_STATUS)

    def test_execute_launch_dry_run_preserves_deterministic_order(self) -> None:
        dry_run = _make_dry_run(
            steps=(
                _make_step(launch_order=1, company="Alpha Data"),
                _make_step(launch_order=2, company="Gamma Telemetry"),
            )
        )
        executor = RecordingLaunchExecutor()

        result = execute_launch_dry_run(dry_run, executor)

        self.assertIs(result, dry_run)
        self.assertEqual(executor.launch_orders, [1, 2])

    def test_browser_launch_executor_opens_url_and_conforms_to_contract(self) -> None:
        executor = BrowserLaunchExecutor()
        step = _make_step(launch_order=1, company="Northwind Talent")

        with patch("jobs_ai.launch_executor.webbrowser.open", return_value=True) as open_browser:
            report = executor.execute_step(step)

        self.assertIsInstance(executor, LaunchStepExecutor)
        open_browser.assert_called_once_with("https://example.com/jobs/1", new=2)
        self.assertEqual(report.executor_mode, BROWSER_STUB_EXECUTOR_MODE)
        self.assertEqual(report.status, OPENED_EXECUTION_STATUS)

    def test_browser_launch_executor_skips_missing_urls_without_opening_browser(self) -> None:
        dry_run = _make_dry_run(
            steps=(
                _make_step(launch_order=1, company="Northwind Talent", apply_url=""),
                _make_step(launch_order=2, company="Fabrikam", apply_url="   "),
            )
        )
        executor = BrowserLaunchExecutor()

        with patch("jobs_ai.launch_executor.webbrowser.open") as open_browser:
            reports = collect_launch_execution_reports(dry_run, executor)

        open_browser.assert_not_called()
        self.assertEqual(
            [report.status for report in reports],
            [SKIPPED_MISSING_URL_EXECUTION_STATUS] * 2,
        )
        self.assertEqual(executor.reported_actions, list(reports))

    def test_browser_launch_executor_preserves_deterministic_order(self) -> None:
        dry_run = _make_dry_run(
            steps=(
                _make_step(launch_order=1, company="Alpha Data"),
                _make_step(launch_order=2, company="Gamma Telemetry"),
            )
        )

        with patch("jobs_ai.launch_executor.webbrowser.open", return_value=True) as open_browser:
            reports = collect_launch_execution_reports(dry_run, BrowserLaunchExecutor())

        self.assertEqual([report.launch_order for report in reports], [1, 2])
        self.assertEqual(
            [call.args[0] for call in open_browser.call_args_list],
            ["https://example.com/jobs/1", "https://example.com/jobs/2"],
        )

    def test_remote_print_launch_executor_reports_normalized_url_without_opening_browser(self) -> None:
        executor = RemotePrintLaunchExecutor()
        step = _make_step(
            launch_order=1,
            company="Northwind Talent",
            apply_url=" https://example.com/jobs/1 ",
        )

        with patch("jobs_ai.launch_executor.webbrowser.open") as open_browser:
            report = executor.execute_step(step)

        self.assertIsInstance(executor, LaunchStepExecutor)
        open_browser.assert_not_called()
        self.assertEqual(report.executor_mode, REMOTE_PRINT_EXECUTOR_MODE)
        self.assertEqual(report.status, PRINTED_EXECUTION_STATUS)
        self.assertEqual(report.apply_url, "https://example.com/jobs/1")
        self.assertEqual(executor.reported_actions, [report])

    def test_remote_print_launch_executor_skips_missing_urls_without_opening_browser(self) -> None:
        dry_run = _make_dry_run(
            steps=(
                _make_step(launch_order=1, company="Northwind Talent", apply_url=""),
                _make_step(launch_order=2, company="Fabrikam", apply_url="   "),
            )
        )
        executor = RemotePrintLaunchExecutor()

        with patch("jobs_ai.launch_executor.webbrowser.open") as open_browser:
            reports = collect_launch_execution_reports(dry_run, executor)

        open_browser.assert_not_called()
        self.assertEqual(
            [report.status for report in reports],
            [SKIPPED_MISSING_URL_EXECUTION_STATUS] * 2,
        )
        self.assertEqual(executor.reported_actions, list(reports))

    def test_select_launch_executor_defaults_to_no_op_and_supports_browser_stub(self) -> None:
        self.assertIsInstance(select_launch_executor(), NoOpLaunchExecutor)
        self.assertIsInstance(
            select_launch_executor(BROWSER_STUB_EXECUTOR_MODE),
            BrowserLaunchExecutor,
        )
        self.assertIsInstance(
            select_launch_executor(REMOTE_PRINT_EXECUTOR_MODE),
            RemotePrintLaunchExecutor,
        )

    def test_select_launch_executor_rejects_unknown_mode(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            "unsupported launch executor mode: mystery",
        ):
            select_launch_executor("mystery")

    def test_cli_launch_dry_run_defaults_to_no_op_executor(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            manifest_path = _write_manifest(Path(tmp_dir) / "session.json")
            captured_executor: LaunchStepExecutor | None = None

            def capture_executor(
                dry_run: LaunchDryRun,
                executor: LaunchStepExecutor,
            ) -> tuple[LaunchExecutionReport, ...]:
                nonlocal captured_executor
                captured_executor = executor
                return ()

            with patch(
                "jobs_ai.cli.collect_launch_execution_reports_for_steps",
                side_effect=capture_executor,
            ):
                result = RUNNER.invoke(app, ["launch-dry-run", str(manifest_path)])

            self.assertEqual(result.exit_code, 0)
            self.assertIsInstance(captured_executor, NoOpLaunchExecutor)

    def test_cli_launch_dry_run_accepts_browser_stub_executor(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            manifest_path = _write_manifest(Path(tmp_dir) / "session.json")
            captured_executor: LaunchStepExecutor | None = None

            def capture_executor(
                dry_run: LaunchDryRun,
                executor: LaunchStepExecutor,
            ) -> tuple[LaunchExecutionReport, ...]:
                nonlocal captured_executor
                captured_executor = executor
                return ()

            with patch(
                "jobs_ai.cli.collect_launch_execution_reports_for_steps",
                side_effect=capture_executor,
            ):
                result = RUNNER.invoke(
                    app,
                    ["launch-dry-run", "--executor", BROWSER_STUB_EXECUTOR_MODE, str(manifest_path)],
                )

            self.assertEqual(result.exit_code, 0)
            self.assertIsInstance(captured_executor, BrowserLaunchExecutor)

    def test_cli_launch_dry_run_formats_noop_output(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            manifest_path = _write_manifest(Path(tmp_dir) / "session.json")

            result = RUNNER.invoke(app, ["launch-dry-run", str(manifest_path)])

            self.assertEqual(result.exit_code, 0)
            self.assertEqual(
                result.stdout,
                (
                    "[1] Company 1 | Role 1\n"
                    "URL: https://example.com/jobs/1\n"
                    "Executor: noop\n"
                    "Action: OPEN_URL\n"
                    "Result: dry run only\n"
                ),
            )

    def test_cli_launch_dry_run_formats_browser_stub_output(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            manifest_path = _write_manifest(Path(tmp_dir) / "session.json")

            with patch("jobs_ai.launch_executor.webbrowser.open", return_value=True) as open_browser:
                result = RUNNER.invoke(
                    app,
                    ["launch-dry-run", "--executor", BROWSER_STUB_EXECUTOR_MODE, str(manifest_path)],
                )

            self.assertEqual(result.exit_code, 0)
            open_browser.assert_called_once_with("https://example.com/jobs/1", new=2)
            self.assertEqual(
                result.stdout,
                (
                    "Launching 1 application:\n"
                    "[1] Company 1 | Role 1\n"
                    "[1] Company 1 | Role 1\n"
                    "URL: https://example.com/jobs/1\n"
                    "Executor: browser_stub\n"
                    "Action: OPEN_URL\n"
                    "Result: opened in browser\n"
                ),
            )

    def test_cli_launch_dry_run_preserves_deterministic_order_across_executor_modes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            manifest_path = _write_manifest(Path(tmp_dir) / "session.json", item_count=2)

            default_result = RUNNER.invoke(app, ["launch-dry-run", str(manifest_path)])
            with patch("jobs_ai.launch_executor.webbrowser.open", return_value=True) as open_browser:
                browser_stub_result = RUNNER.invoke(
                    app,
                    ["launch-dry-run", "--executor", BROWSER_STUB_EXECUTOR_MODE, str(manifest_path)],
                )

            self.assertEqual(default_result.exit_code, 0)
            self.assertEqual(browser_stub_result.exit_code, 0)
            self.assertEqual(
                [call.args[0] for call in open_browser.call_args_list],
                ["https://example.com/jobs/1", "https://example.com/jobs/2"],
            )
            self.assertEqual(
                default_result.stdout,
                (
                    "[1] Company 1 | Role 1\n"
                    "URL: https://example.com/jobs/1\n"
                    "Executor: noop\n"
                    "Action: OPEN_URL\n"
                    "Result: dry run only\n\n"
                    "[2] Company 2 | Role 2\n"
                    "URL: https://example.com/jobs/2\n"
                    "Executor: noop\n"
                    "Action: OPEN_URL\n"
                    "Result: dry run only\n"
                ),
            )
            self.assertEqual(
                browser_stub_result.stdout,
                (
                    "Launching 2 applications:\n"
                    "[1] Company 1 | Role 1\n"
                    "[2] Company 2 | Role 2\n"
                    "[1] Company 1 | Role 1\n"
                    "URL: https://example.com/jobs/1\n"
                    "Executor: browser_stub\n"
                    "Action: OPEN_URL\n"
                    "Result: opened in browser\n\n"
                    "[2] Company 2 | Role 2\n"
                    "URL: https://example.com/jobs/2\n"
                    "Executor: browser_stub\n"
                    "Action: OPEN_URL\n"
                    "Result: opened in browser\n"
                ),
            )

    def test_cli_launch_dry_run_limit_restricts_browser_launches_deterministically(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            manifest_path = _write_manifest(Path(tmp_dir) / "session.json", item_count=3)

            with patch("jobs_ai.launch_executor.webbrowser.open", return_value=True) as open_browser:
                result = RUNNER.invoke(
                    app,
                    [
                        "launch-dry-run",
                        "--executor",
                        BROWSER_STUB_EXECUTOR_MODE,
                        "--limit",
                        "2",
                        str(manifest_path),
                    ],
                )

            self.assertEqual(result.exit_code, 0)
            self.assertEqual(
                [call.args[0] for call in open_browser.call_args_list],
                ["https://example.com/jobs/1", "https://example.com/jobs/2"],
            )
            self.assertEqual(
                result.stdout,
                (
                    "Launching 2 applications:\n"
                    "[1] Company 1 | Role 1\n"
                    "[2] Company 2 | Role 2\n"
                    "[1] Company 1 | Role 1\n"
                    "URL: https://example.com/jobs/1\n"
                    "Executor: browser_stub\n"
                    "Action: OPEN_URL\n"
                    "Result: opened in browser\n\n"
                    "[2] Company 2 | Role 2\n"
                    "URL: https://example.com/jobs/2\n"
                    "Executor: browser_stub\n"
                    "Action: OPEN_URL\n"
                    "Result: opened in browser\n"
                ),
            )

    def test_cli_launch_dry_run_confirm_prompts_before_browser_launch(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            manifest_path = _write_manifest(Path(tmp_dir) / "session.json")

            with patch("jobs_ai.launch_executor.webbrowser.open", return_value=True) as open_browser:
                result = RUNNER.invoke(
                    app,
                    [
                        "launch-dry-run",
                        "--executor",
                        BROWSER_STUB_EXECUTOR_MODE,
                        "--confirm",
                        str(manifest_path),
                    ],
                    input="y\n",
                )

            self.assertEqual(result.exit_code, 0)
            self.assertIn("Launching 1 application:", result.stdout)
            self.assertIn("Open 1 application tab in browser_stub mode?", result.stdout)
            open_browser.assert_called_once_with("https://example.com/jobs/1", new=2)

    def test_cli_launch_dry_run_declining_confirm_prevents_browser_open(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            manifest_path = _write_manifest(Path(tmp_dir) / "session.json", item_count=2)

            with patch("jobs_ai.launch_executor.webbrowser.open") as open_browser:
                result = RUNNER.invoke(
                    app,
                    [
                        "launch-dry-run",
                        "--executor",
                        BROWSER_STUB_EXECUTOR_MODE,
                        "--confirm",
                        str(manifest_path),
                    ],
                    input="n\n",
                )

            self.assertEqual(result.exit_code, 0)
            open_browser.assert_not_called()
            self.assertIn("Launching 2 applications:", result.stdout)
            self.assertIn("Open 2 application tabs in browser_stub mode?", result.stdout)
            self.assertIn("Launch cancelled. No browser tabs were opened.", result.stdout)
            self.assertNotIn("Executor: browser_stub", result.stdout)

    def test_cli_launch_dry_run_accepting_confirm_preserves_launch_count_and_order(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            manifest_path = _write_manifest(Path(tmp_dir) / "session.json", item_count=2)

            with patch("jobs_ai.launch_executor.webbrowser.open", return_value=True) as open_browser:
                result = RUNNER.invoke(
                    app,
                    [
                        "launch-dry-run",
                        "--executor",
                        BROWSER_STUB_EXECUTOR_MODE,
                        "--confirm",
                        str(manifest_path),
                    ],
                    input="y\n",
                )

            self.assertEqual(result.exit_code, 0)
            self.assertEqual(
                [call.args[0] for call in open_browser.call_args_list],
                ["https://example.com/jobs/1", "https://example.com/jobs/2"],
            )
            self.assertIn("Launching 2 applications:", result.stdout)
            self.assertIn("Open 2 application tabs in browser_stub mode?", result.stdout)
            self.assertIn("Executor: browser_stub", result.stdout)

    def test_cli_launch_dry_run_noop_mode_never_opens_browser_with_safety_flags(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            manifest_path = _write_manifest(Path(tmp_dir) / "session.json", item_count=2)

            with patch("jobs_ai.launch_executor.webbrowser.open") as open_browser:
                with patch("jobs_ai.cli.click.confirm", side_effect=AssertionError("confirm should stay unused")):
                    result = RUNNER.invoke(
                        app,
                        [
                            "launch-dry-run",
                            "--limit",
                            "1",
                            "--confirm",
                            str(manifest_path),
                        ],
                    )

            self.assertEqual(result.exit_code, 0)
            open_browser.assert_not_called()
            self.assertEqual(
                result.stdout,
                (
                    "[1] Company 1 | Role 1\n"
                    "URL: https://example.com/jobs/1\n"
                    "Executor: noop\n"
                    "Action: OPEN_URL\n"
                    "Result: dry run only\n\n"
                    "[2] Company 2 | Role 2\n"
                    "URL: https://example.com/jobs/2\n"
                    "Executor: noop\n"
                    "Action: OPEN_URL\n"
                    "Result: dry run only\n"
                ),
            )

    def test_build_launch_dry_run_still_works_through_adapter_layer(self) -> None:
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

            dry_run = execute_launch_dry_run(
                build_launch_dry_run(build_launch_plan(load_session_manifest(manifest_path))),
                NoOpLaunchExecutor(),
            )

            self.assertEqual(dry_run.total_items, 3)
            self.assertEqual(dry_run.launchable_items, 2)
            self.assertEqual(dry_run.skipped_items, 1)
            self.assertEqual([step.launch_order for step in dry_run.steps], [1, 2])
            self.assertEqual(
                [step.action_label for step in dry_run.steps],
                [OPEN_URL_ACTION, OPEN_URL_ACTION],
            )


def _make_step(launch_order: int, company: str, *, apply_url: str | None = None) -> LaunchDryRunStep:
    return LaunchDryRunStep(
        launch_order=launch_order,
        action_label=OPEN_URL_ACTION,
        company=company,
        title="Data Engineer",
        apply_url=apply_url if apply_url is not None else f"https://example.com/jobs/{launch_order}",
        recommended_resume_variant=ManifestSelection(
            key="data-engineering",
            label="Data Engineering Resume",
        ),
        recommended_profile_snippet=ManifestSelection(
            key="pipeline-delivery",
            label="Pipeline Delivery",
            text="Python-first pipeline delivery across SQL warehouses.",
        ),
    )


def _make_dry_run(steps: tuple[LaunchDryRunStep, ...]) -> LaunchDryRun:
    return LaunchDryRun(
        manifest_path=Path("/tmp/session.json"),
        created_at="2026-03-13T20:00:00Z",
        total_items=len(steps),
        launchable_items=len(steps),
        skipped_items=0,
        steps=steps,
    )


def _write_manifest(manifest_path: Path, *, item_count: int = 1) -> Path:
    items = []
    for rank in range(1, item_count + 1):
        items.append(
            {
                "rank": rank,
                "company": f"Company {rank}",
                "title": f"Role {rank}",
                "apply_url": f"https://example.com/jobs/{rank}",
                "recommended_resume_variant": {
                    "key": "data-engineering",
                    "label": "Data Engineering Resume",
                },
                "recommended_profile_snippet": {
                    "key": "pipeline-delivery",
                    "label": "Pipeline Delivery",
                    "text": "Python-first pipeline delivery across SQL warehouses.",
                },
            }
        )

    manifest_path.write_text(
        json.dumps(
            {
                "created_at": "2026-03-13T20:00:00Z",
                "item_count": item_count,
                "items": items,
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    return manifest_path


if __name__ == "__main__":
    unittest.main()
