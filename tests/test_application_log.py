from __future__ import annotations

from datetime import datetime, timedelta, timezone
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

RUNNER = CliRunner()
FIXED_LOCAL_TIMESTAMP = datetime(2026, 3, 17, 18, 15, 0, tzinfo=timezone(timedelta(hours=-7)))


def _complete_item(
    *,
    rank: int,
    company: str,
    title: str,
    apply_url: str | None,
    portal_type: str | None = None,
) -> dict[str, object]:
    return {
        "rank": rank,
        "job_id": rank,
        "company": company,
        "title": title,
        "apply_url": apply_url,
        "portal_type": portal_type,
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


class ApplicationLogTest(unittest.TestCase):
    def test_cli_application_log_writes_manual_log_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            project_root = Path(tmp_dir) / "workspace"
            database_path = project_root / "runtime" / "jobs_ai.db"
            env = {"JOBS_AI_DB_PATH": str(database_path)}
            expected_log_path = (
                project_root
                / "data"
                / "applications"
                / "2026-03-17-care-access-greenhouse.json"
            )

            with (
                patch("jobs_ai.workspace.discover_project_root", return_value=project_root),
                patch(
                    "jobs_ai.application_log._current_local_datetime",
                    return_value=FIXED_LOCAL_TIMESTAMP,
                ),
            ):
                result = RUNNER.invoke(
                    app,
                    [
                        "application-log",
                        "--company",
                        "Care Access",
                        "--role",
                        "Enterprise Performance Analytics Engineer",
                        "--portal",
                        "greenhouse",
                        "--apply-url",
                        "https://job-boards.greenhouse.io/careaccess/jobs/4052147009",
                        "--status",
                        "applied",
                        "--notes",
                        "prefill + manual fix (conditional referral field)",
                    ],
                    env=env,
                )

            self.assertEqual(result.exit_code, 0)
            self.assertIn("jobs_ai application-log", result.stdout)
            self.assertIn(
                "Logged application Care Access - Enterprise Performance Analytics Engineer",
                result.stdout,
            )
            self.assertIn(f"log path: {expected_log_path}", result.stdout)
            payload = json.loads(expected_log_path.read_text(encoding="utf-8"))
            self.assertEqual(
                payload,
                {
                    "company": "Care Access",
                    "role": "Enterprise Performance Analytics Engineer",
                    "portal": "greenhouse",
                    "apply_url": "https://job-boards.greenhouse.io/careaccess/jobs/4052147009",
                    "status": "applied",
                    "method": "jobs_ai application-assist",
                    "notes": "prefill + manual fix (conditional referral field)",
                    "timestamp": "2026-03-17T18:15:00-07:00",
                },
            )

    def test_cli_application_log_manifest_mode_uses_launch_order_and_infers_portal(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            project_root = Path(tmp_dir) / "workspace"
            manifest_path = project_root / "data" / "exports" / "launch-preview-session.json"
            database_path = project_root / "runtime" / "jobs_ai.db"
            env = {"JOBS_AI_DB_PATH": str(database_path)}
            manifest_path.parent.mkdir(parents=True, exist_ok=True)
            manifest_path.write_text(
                json.dumps(
                    {
                        "created_at": "2026-03-17T23:16:42Z",
                        "item_count": 3,
                        "items": [
                            _complete_item(
                                rank=1,
                                company="Alpha Data",
                                title="Data Engineer",
                                apply_url="https://example.com/jobs/1",
                                portal_type="greenhouse",
                            ),
                            _complete_item(
                                rank=2,
                                company="Skip Me",
                                title="Incomplete Job",
                                apply_url=None,
                                portal_type="greenhouse",
                            ),
                            _complete_item(
                                rank=3,
                                company="Care Access",
                                title="Enterprise Performance Analytics Engineer",
                                apply_url="https://job-boards.greenhouse.io/careaccess/jobs/4052147009",
                                portal_type=None,
                            ),
                        ],
                    },
                    indent=2,
                )
                + "\n",
                encoding="utf-8",
            )
            expected_log_path = (
                project_root
                / "data"
                / "applications"
                / "2026-03-17-care-access-greenhouse.json"
            )

            with (
                patch("jobs_ai.workspace.discover_project_root", return_value=project_root),
                patch(
                    "jobs_ai.application_log._current_local_datetime",
                    return_value=FIXED_LOCAL_TIMESTAMP,
                ),
            ):
                result = RUNNER.invoke(
                    app,
                    [
                        "application-log",
                        "--manifest",
                        str(manifest_path),
                        "--launch-order",
                        "2",
                        "--status",
                        "applied",
                        "--notes",
                        "prefill + manual fix (conditional referral field)",
                    ],
                    env=env,
                )

            self.assertEqual(result.exit_code, 0)
            self.assertIn(f"manifest path: {manifest_path.resolve()}", result.stdout)
            self.assertIn(
                "Logged application [2] Care Access - Enterprise Performance Analytics Engineer",
                result.stdout,
            )
            payload = json.loads(expected_log_path.read_text(encoding="utf-8"))
            self.assertEqual(payload["company"], "Care Access")
            self.assertEqual(payload["role"], "Enterprise Performance Analytics Engineer")
            self.assertEqual(payload["portal"], "greenhouse")
            self.assertEqual(
                payload["apply_url"],
                "https://job-boards.greenhouse.io/careaccess/jobs/4052147009",
            )

    def test_cli_application_log_manifest_values_can_be_overridden(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            project_root = Path(tmp_dir) / "workspace"
            manifest_path = project_root / "data" / "exports" / "launch-preview-session.json"
            database_path = project_root / "runtime" / "jobs_ai.db"
            env = {"JOBS_AI_DB_PATH": str(database_path)}
            manifest_path.parent.mkdir(parents=True, exist_ok=True)
            manifest_path.write_text(
                json.dumps(
                    {
                        "created_at": "2026-03-17T23:16:42Z",
                        "item_count": 1,
                        "items": [
                            _complete_item(
                                rank=1,
                                company="Care Access",
                                title="Enterprise Performance Analytics Engineer",
                                apply_url="https://job-boards.greenhouse.io/careaccess/jobs/4052147009",
                                portal_type="greenhouse",
                            )
                        ],
                    },
                    indent=2,
                )
                + "\n",
                encoding="utf-8",
            )
            expected_log_path = (
                project_root
                / "data"
                / "applications"
                / "2026-03-17-care-access-inc-greenhouse.json"
            )

            with (
                patch("jobs_ai.workspace.discover_project_root", return_value=project_root),
                patch(
                    "jobs_ai.application_log._current_local_datetime",
                    return_value=FIXED_LOCAL_TIMESTAMP,
                ),
            ):
                result = RUNNER.invoke(
                    app,
                    [
                        "application-log",
                        "--manifest",
                        str(manifest_path),
                        "--status",
                        "opened",
                        "--company",
                        "Care Access, Inc.",
                    ],
                    env=env,
                )

            self.assertEqual(result.exit_code, 0)
            payload = json.loads(expected_log_path.read_text(encoding="utf-8"))
            self.assertEqual(payload["company"], "Care Access, Inc.")
            self.assertEqual(payload["status"], "opened")

    def test_cli_application_log_reports_invalid_launch_order_clearly(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            project_root = Path(tmp_dir) / "workspace"
            manifest_path = project_root / "data" / "exports" / "launch-preview-session.json"
            database_path = project_root / "runtime" / "jobs_ai.db"
            env = {"JOBS_AI_DB_PATH": str(database_path)}
            manifest_path.parent.mkdir(parents=True, exist_ok=True)
            manifest_path.write_text(
                json.dumps(
                    {
                        "created_at": "2026-03-17T23:16:42Z",
                        "item_count": 1,
                        "items": [
                            _complete_item(
                                rank=1,
                                company="Care Access",
                                title="Enterprise Performance Analytics Engineer",
                                apply_url="https://job-boards.greenhouse.io/careaccess/jobs/4052147009",
                                portal_type="greenhouse",
                            )
                        ],
                    },
                    indent=2,
                )
                + "\n",
                encoding="utf-8",
            )

            with patch("jobs_ai.workspace.discover_project_root", return_value=project_root):
                result = RUNNER.invoke(
                    app,
                    [
                        "application-log",
                        "--manifest",
                        str(manifest_path),
                        "--launch-order",
                        "2",
                        "--status",
                        "applied",
                    ],
                    env=env,
                )

            self.assertEqual(result.exit_code, 1)
            self.assertIn("jobs_ai application-log", result.stdout)
            self.assertIn("status: failed", result.stdout)
            self.assertIn("launch order 2 was not found in the manifest", result.stdout)


if __name__ == "__main__":
    unittest.main()
