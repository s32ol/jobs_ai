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

from jobs_ai.application_tracking import get_application_status
from jobs_ai.cli import app
from jobs_ai.db import connect_database, initialize_schema, insert_job

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
    job_id: int | None,
    company: str,
    title: str,
    apply_url: str | None,
) -> Path:
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(
        json.dumps(
            {
                "created_at": "2026-03-15T12:00:00Z",
                "item_count": 1,
                "items": [
                    {
                        "rank": 1,
                        "job_id": job_id,
                        "company": company,
                        "title": title,
                        "apply_url": apply_url,
                    }
                ],
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    return manifest_path


class OpenCommandTest(unittest.TestCase):
    def test_cli_open_opens_manifest_item_and_marks_applied(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            project_root = Path(tmp_dir) / "workspace"
            database_path = project_root / "runtime" / "jobs_ai.db"
            env = {"JOBS_AI_DB_PATH": str(database_path)}
            initialize_schema(database_path)

            with closing(connect_database(database_path)) as connection:
                job_id = insert_job(
                    connection,
                    _job_record(
                        source="manual",
                        company="Northwind Talent",
                        title="Platform Data Engineer",
                        location="Remote",
                        apply_url="https://example.com/jobs/1",
                    ),
                )
                connection.commit()

            manifest_path = _write_manifest(
                project_root / "data" / "exports" / "manual-open.json",
                job_id=job_id,
                company="Northwind Talent",
                title="Platform Data Engineer",
                apply_url="https://example.com/jobs/1",
            )

            with patch("jobs_ai.launch_executor.webbrowser.open", return_value=True) as open_browser:
                result = RUNNER.invoke(
                    app,
                    ["open", "--manifest", str(manifest_path), "--index", "1"],
                    env=env,
                    input="y\n",
                )

            self.assertEqual(result.exit_code, 0)
            self.assertIn("Opened: [1] Northwind Talent - Platform Data Engineer", result.stdout)
            self.assertIn("What happened?", result.stdout)
            self.assertIn("requested status: applied", result.stdout)
            open_browser.assert_called_once_with("https://example.com/jobs/1", new=2)
            self.assertEqual(
                get_application_status(database_path, job_id=job_id).snapshot.current_status,
                "applied",
            )

    def test_cli_open_maps_skip_choice_to_skipped_status(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            project_root = Path(tmp_dir) / "workspace"
            database_path = project_root / "runtime" / "jobs_ai.db"
            env = {"JOBS_AI_DB_PATH": str(database_path)}
            initialize_schema(database_path)

            with closing(connect_database(database_path)) as connection:
                job_id = insert_job(
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
                project_root / "data" / "exports" / "manual-skip.json",
                job_id=job_id,
                company="Contoso",
                title="Analytics Engineer",
                apply_url="https://example.com/jobs/2",
            )

            with patch("jobs_ai.launch_executor.webbrowser.open", return_value=True) as open_browser:
                result = RUNNER.invoke(
                    app,
                    ["open", "--manifest", str(manifest_path), "--index", "1"],
                    env=env,
                    input="s\n",
                )

            self.assertEqual(result.exit_code, 0)
            self.assertIn("requested status: skipped", result.stdout)
            open_browser.assert_called_once_with("https://example.com/jobs/2", new=2)
            self.assertEqual(
                get_application_status(database_path, job_id=job_id).snapshot.current_status,
                "skipped",
            )

    def test_cli_open_leaves_status_unchanged_for_empty_response(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            project_root = Path(tmp_dir) / "workspace"
            database_path = project_root / "runtime" / "jobs_ai.db"
            env = {"JOBS_AI_DB_PATH": str(database_path)}
            initialize_schema(database_path)

            with closing(connect_database(database_path)) as connection:
                job_id = insert_job(
                    connection,
                    _job_record(
                        source="manual",
                        company="Fabrikam",
                        title="Backend Engineer",
                        location="Remote",
                        apply_url="https://example.com/jobs/3",
                    ),
                )
                connection.commit()

            manifest_path = _write_manifest(
                project_root / "data" / "exports" / "manual-later.json",
                job_id=job_id,
                company="Fabrikam",
                title="Backend Engineer",
                apply_url="https://example.com/jobs/3",
            )

            with patch("jobs_ai.launch_executor.webbrowser.open", return_value=True) as open_browser:
                result = RUNNER.invoke(
                    app,
                    ["open", "--manifest", str(manifest_path), "--index", "1"],
                    env=env,
                    input="\n",
                )

            self.assertEqual(result.exit_code, 0)
            self.assertIn("Status left unchanged for [1] Fabrikam - Backend Engineer.", result.stdout)
            open_browser.assert_called_once_with("https://example.com/jobs/3", new=2)
            detail = get_application_status(database_path, job_id=job_id)
            self.assertEqual(detail.snapshot.current_status, "new")
            self.assertEqual(detail.history, ())

    def test_cli_open_remote_print_reports_url_without_opening_browser(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            project_root = Path(tmp_dir) / "workspace"
            database_path = project_root / "runtime" / "jobs_ai.db"
            env = {"JOBS_AI_DB_PATH": str(database_path)}
            initialize_schema(database_path)

            with closing(connect_database(database_path)) as connection:
                job_id = insert_job(
                    connection,
                    _job_record(
                        source="manual",
                        company="Fabrikam",
                        title="Backend Engineer",
                        location="Remote",
                        apply_url=" https://example.com/jobs/3 ",
                    ),
                )
                connection.commit()

            manifest_path = _write_manifest(
                project_root / "data" / "exports" / "manual-remote-print.json",
                job_id=job_id,
                company="Fabrikam",
                title="Backend Engineer",
                apply_url=" https://example.com/jobs/3 ",
            )

            with patch("jobs_ai.launch_executor.webbrowser.open") as open_browser:
                result = RUNNER.invoke(
                    app,
                    [
                        "open",
                        "--manifest",
                        str(manifest_path),
                        "--index",
                        "1",
                        "--executor",
                        "remote_print",
                    ],
                    env=env,
                    input="\n",
                )

            self.assertEqual(result.exit_code, 0)
            open_browser.assert_not_called()
            self.assertIn("Printed: [1] Fabrikam - Backend Engineer", result.stdout)
            self.assertIn("apply_url: https://example.com/jobs/3", result.stdout)
            self.assertIn("Status left unchanged for [1] Fabrikam - Backend Engineer.", result.stdout)
            detail = get_application_status(database_path, job_id=job_id)
            self.assertEqual(detail.snapshot.current_status, "new")
            self.assertEqual(detail.history, ())

    def test_cli_open_reports_invalid_manifest_index(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            project_root = Path(tmp_dir) / "workspace"
            database_path = project_root / "runtime" / "jobs_ai.db"
            env = {"JOBS_AI_DB_PATH": str(database_path)}
            initialize_schema(database_path)

            manifest_path = _write_manifest(
                project_root / "data" / "exports" / "invalid-index.json",
                job_id=None,
                company="Northwind Talent",
                title="Platform Data Engineer",
                apply_url="https://example.com/jobs/1",
            )

            with patch("jobs_ai.launch_executor.webbrowser.open") as open_browser:
                result = RUNNER.invoke(
                    app,
                    ["open", "--manifest", str(manifest_path), "--index", "2"],
                    env=env,
                )

            self.assertEqual(result.exit_code, 1)
            self.assertIn("jobs_ai open", result.stdout)
            self.assertIn("error: manifest index 2 exceeds manifest size 1", result.stdout)
            open_browser.assert_not_called()

    def test_cli_open_fails_clearly_when_manifest_item_has_no_apply_url(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            project_root = Path(tmp_dir) / "workspace"
            database_path = project_root / "runtime" / "jobs_ai.db"
            env = {"JOBS_AI_DB_PATH": str(database_path)}
            initialize_schema(database_path)

            manifest_path = _write_manifest(
                project_root / "data" / "exports" / "missing-url.json",
                job_id=None,
                company="Contoso",
                title="Analytics Engineer",
                apply_url=None,
            )

            with patch("jobs_ai.launch_executor.webbrowser.open") as open_browser:
                result = RUNNER.invoke(
                    app,
                    ["open", "--manifest", str(manifest_path), "--index", "1"],
                    env=env,
                )

            self.assertEqual(result.exit_code, 1)
            self.assertIn("jobs_ai open", result.stdout)
            self.assertIn("error: manifest index 1 is missing apply_url", result.stdout)
            open_browser.assert_not_called()


if __name__ == "__main__":
    unittest.main()
