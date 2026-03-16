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

from jobs_ai.cli import app
from jobs_ai.db import connect_database, initialize_schema, insert_job
from jobs_ai.launch_preview import select_launch_preview
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
    portal_type: str | None = None,
    ingest_batch_id: str | None = None,
    source_query: str | None = None,
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
        "ingest_batch_id": ingest_batch_id,
        "source_query": source_query,
        "import_source": None,
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
    ingest_batch_id: str | None = None,
    source_query: str | None = None,
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
            ingest_batch_id=ingest_batch_id,
            source_query=source_query,
        ),
    )
    if status != "new":
        connection.execute(
            "UPDATE jobs SET status = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
            (status, job_id),
        )
    return job_id


class SessionStartTest(unittest.TestCase):
    def test_cli_session_start_defaults_to_limit_25_and_writes_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            project_root = Path(tmp_dir) / "workspace"
            database_path = project_root / "runtime" / "jobs_ai.db"
            env = {"JOBS_AI_DB_PATH": str(database_path)}
            initialize_schema(database_path)

            with closing(connect_database(database_path)) as connection:
                for index in range(30):
                    _insert_job_with_status(
                        connection,
                        source="manual",
                        company=f"Company {index + 1}",
                        title="Data Engineer",
                        location="Remote",
                        apply_url=f"https://example.com/jobs/{index + 1}",
                        raw_payload={"description": "Python pipelines"},
                    )
                connection.commit()

            with patch("jobs_ai.workspace.discover_project_root", return_value=project_root):
                result = RUNNER.invoke(app, ["session", "start"], env=env)

            self.assertEqual(result.exit_code, 0)
            self.assertIn("jobs_ai session start", result.stdout)
            self.assertIn("selected jobs: 25", result.stdout)
            self.assertIn("limit: 25", result.stdout)

            export_files = sorted((project_root / "data" / "exports").glob("launch-preview-session-*.json"))
            self.assertEqual(len(export_files), 1)

            manifest = load_session_manifest(export_files[0])
            self.assertEqual(manifest.item_count, 25)

    def test_cli_session_start_can_scope_to_one_ingest_batch(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            project_root = Path(tmp_dir) / "workspace"
            database_path = project_root / "runtime" / "jobs_ai.db"
            env = {"JOBS_AI_DB_PATH": str(database_path)}
            initialize_schema(database_path)

            with closing(connect_database(database_path)) as connection:
                scoped_job_id = _insert_job_with_status(
                    connection,
                    source="manual",
                    company="Scoped Co",
                    title="Platform Data Engineer",
                    location="Remote",
                    apply_url="https://example.com/jobs/scoped",
                    raw_payload={"description": "Python BigQuery GCP"},
                    ingest_batch_id="discover-scope-run",
                    source_query="python backend engineer remote",
                )
                _insert_job_with_status(
                    connection,
                    source="manual",
                    company="Global Co",
                    title="Analytics Engineer",
                    location="Remote",
                    apply_url="https://example.com/jobs/global",
                    raw_payload={"description": "Looker dashboards"},
                    ingest_batch_id="discover-other-run",
                    source_query="analytics engineer remote",
                )
                connection.commit()

            with patch("jobs_ai.workspace.discover_project_root", return_value=project_root):
                result = RUNNER.invoke(
                    app,
                    ["session", "start", "--batch-id", "discover-scope-run"],
                    env=env,
                )

            self.assertEqual(result.exit_code, 0)
            self.assertIn("selection scope: batch discover-scope-run", result.stdout)
            self.assertIn("source query: python backend engineer remote", result.stdout)

            export_files = sorted((project_root / "data" / "exports").glob("launch-preview-session-*.json"))
            manifest = load_session_manifest(export_files[0])
            self.assertEqual(manifest.item_count, 1)
            self.assertIsNotNone(manifest.selection_scope)
            assert manifest.selection_scope is not None
            self.assertEqual(manifest.selection_scope.batch_id, "discover-scope-run")
            self.assertEqual(manifest.selection_scope.source_query, "python backend engineer remote")
            self.assertEqual(manifest.items[0].job_id, scoped_job_id)

    def test_cli_session_start_without_scope_keeps_global_new_job_behavior(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            project_root = Path(tmp_dir) / "workspace"
            database_path = project_root / "runtime" / "jobs_ai.db"
            env = {"JOBS_AI_DB_PATH": str(database_path)}
            initialize_schema(database_path)

            with closing(connect_database(database_path)) as connection:
                first_job_id = _insert_job_with_status(
                    connection,
                    source="manual",
                    company="Scoped Co",
                    title="Platform Data Engineer",
                    location="Remote",
                    apply_url="https://example.com/jobs/scoped",
                    raw_payload={"description": "Python BigQuery GCP"},
                    ingest_batch_id="discover-scope-run",
                )
                second_job_id = _insert_job_with_status(
                    connection,
                    source="manual",
                    company="Global Co",
                    title="Analytics Engineer",
                    location="Remote",
                    apply_url="https://example.com/jobs/global",
                    raw_payload={"description": "Looker dashboards"},
                    ingest_batch_id="discover-other-run",
                )
                connection.commit()

            with patch("jobs_ai.workspace.discover_project_root", return_value=project_root):
                result = RUNNER.invoke(app, ["session", "start", "--limit", "2"], env=env)

            self.assertEqual(result.exit_code, 0)
            self.assertNotIn("selection scope:", result.stdout)

            export_files = sorted((project_root / "data" / "exports").glob("launch-preview-session-*.json"))
            manifest = load_session_manifest(export_files[0])
            self.assertEqual(manifest.item_count, 2)
            self.assertEqual(
                {item.job_id for item in manifest.items},
                {first_job_id, second_job_id},
            )
            self.assertIsNone(manifest.selection_scope)

    def test_cli_session_start_freezes_one_preview_batch_and_summary_matches_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            project_root = Path(tmp_dir) / "workspace"
            database_path = project_root / "runtime" / "jobs_ai.db"
            env = {"JOBS_AI_DB_PATH": str(database_path)}
            initialize_schema(database_path)

            with closing(connect_database(database_path)) as connection:
                _insert_job_with_status(
                    connection,
                    source="manual",
                    company="Acme Data",
                    title="Analytics Engineer",
                    location="Sacramento, CA",
                    raw_payload={"description": "Looker dashboards"},
                    apply_url="https://boards.greenhouse.io/acme/jobs/1",
                    portal_type="greenhouse",
                )
                top_job_id = _insert_job_with_status(
                    connection,
                    source="staffing recruiter",
                    company="Northwind Talent",
                    title="Platform Data Engineer",
                    location="Remote",
                    raw_payload={"description": "Python BigQuery GCP contract"},
                    apply_url="https://agency.example/jobs/2",
                )
                second_job_id = _insert_job_with_status(
                    connection,
                    source="manual",
                    company="Bright Metrics",
                    title="Data Engineer",
                    location="Remote",
                    raw_payload={"description": "Python pipelines"},
                    apply_url="https://jobs.example.com/3",
                )
                connection.commit()

            with patch("jobs_ai.workspace.discover_project_root", return_value=project_root):
                with patch(
                    "jobs_ai.session_start.select_launch_preview",
                    wraps=select_launch_preview,
                ) as select_preview:
                    result = RUNNER.invoke(app, ["session", "start", "--limit", "2"], env=env)

            self.assertEqual(result.exit_code, 0)
            self.assertEqual(select_preview.call_count, 1)

            export_files = sorted((project_root / "data" / "exports").glob("launch-preview-session-*.json"))
            manifest = load_session_manifest(export_files[0])
            self.assertEqual([item.rank for item in manifest.items], [1, 2])
            self.assertEqual(
                [payload["job_id"] for payload in json.loads(export_files[0].read_text(encoding="utf-8"))["items"]],
                [top_job_id, second_job_id],
            )
            self.assertLess(
                result.stdout.find(f"[job {top_job_id}]"),
                result.stdout.find(f"[job {second_job_id}]"),
            )

    def test_cli_session_start_portal_hints_show_counts_and_details(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            project_root = Path(tmp_dir) / "workspace"
            database_path = project_root / "runtime" / "jobs_ai.db"
            env = {"JOBS_AI_DB_PATH": str(database_path)}
            initialize_schema(database_path)

            with closing(connect_database(database_path)) as connection:
                _insert_job_with_status(
                    connection,
                    source="manual",
                    company="Acme Data",
                    title="Data Engineer",
                    location="Remote",
                    raw_payload={"description": "Python BigQuery GCP pipelines"},
                    apply_url="https://boards.greenhouse.io/acme?gh_jid=12345&gh_src=linkedin",
                )
                _insert_job_with_status(
                    connection,
                    source="manual",
                    company="Plain Jobs",
                    title="Data Engineer",
                    location="Remote",
                    raw_payload={"description": "Python pipelines"},
                    apply_url="https://jobs.example.com/2",
                )
                connection.commit()

            with patch("jobs_ai.workspace.discover_project_root", return_value=project_root):
                result = RUNNER.invoke(
                    app,
                    ["session", "start", "--limit", "2", "--portal-hints"],
                    env=env,
                )

            self.assertEqual(result.exit_code, 0)
            self.assertIn("portal hints: 1", result.stdout)
            self.assertIn("portal: Greenhouse", result.stdout)
            self.assertIn(
                "company apply_url: https://boards.greenhouse.io/acme/jobs/12345",
                result.stdout,
            )

    def test_cli_session_start_shows_resolved_resume_file_when_configured(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            project_root = Path(tmp_dir) / "workspace"
            database_path = project_root / "runtime" / "jobs_ai.db"
            resume_path = project_root / "resumes" / "data-engineering.pdf"
            resume_path.parent.mkdir(parents=True, exist_ok=True)
            resume_path.write_text("resume", encoding="utf-8")
            env = {
                "JOBS_AI_DB_PATH": str(database_path),
                "JOBS_AI_RESUME_DATA_ENGINEERING_PATH": str(resume_path),
            }
            initialize_schema(database_path)

            with closing(connect_database(database_path)) as connection:
                _insert_job_with_status(
                    connection,
                    source="manual",
                    company="Bright Metrics",
                    title="Data Engineer",
                    location="Remote",
                    raw_payload={"description": "Python pipelines"},
                    apply_url="https://jobs.example.com/2",
                )
                connection.commit()

            with patch("jobs_ai.workspace.discover_project_root", return_value=project_root):
                result = RUNNER.invoke(app, ["session", "start"], env=env)

            self.assertEqual(result.exit_code, 0)
            self.assertIn("resume files resolved: 1", result.stdout)
            self.assertIn(f"resume file: {resume_path}", result.stdout)

    def test_cli_session_start_reports_unresolved_resume_file_with_fallback_reason(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            project_root = Path(tmp_dir) / "workspace"
            database_path = project_root / "runtime" / "jobs_ai.db"
            env = {"JOBS_AI_DB_PATH": str(database_path)}
            initialize_schema(database_path)

            with closing(connect_database(database_path)) as connection:
                _insert_job_with_status(
                    connection,
                    source="manual",
                    company="Bright Metrics",
                    title="Data Engineer",
                    location="Remote",
                    raw_payload={"description": "Python pipelines"},
                    apply_url="https://jobs.example.com/2",
                )
                connection.commit()

            with patch("jobs_ai.workspace.discover_project_root", return_value=project_root):
                result = RUNNER.invoke(app, ["session", "start"], env=env)

            self.assertEqual(result.exit_code, 0)
            self.assertIn("resume files resolved: 0", result.stdout)
            self.assertIn("resume file: unresolved", result.stdout)
            self.assertIn("JOBS_AI_RESUME_DATA_ENGINEERING_PATH", result.stdout)
            self.assertIn(".jobs_ai_resume_paths.json", result.stdout)

    def test_cli_session_start_open_defaults_to_browser_stub(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            project_root = Path(tmp_dir) / "workspace"
            database_path = project_root / "runtime" / "jobs_ai.db"
            env = {"JOBS_AI_DB_PATH": str(database_path)}
            initialize_schema(database_path)

            with closing(connect_database(database_path)) as connection:
                _insert_job_with_status(
                    connection,
                    source="staffing recruiter",
                    company="Northwind Talent",
                    title="Platform Data Engineer",
                    location="Remote",
                    raw_payload={"description": "Python BigQuery GCP contract"},
                    apply_url="https://agency.example/jobs/2",
                )
                _insert_job_with_status(
                    connection,
                    source="manual",
                    company="Bright Metrics",
                    title="Data Engineer",
                    location="Remote",
                    raw_payload={"description": "Python pipelines"},
                    apply_url="https://jobs.example.com/3",
                )
                connection.commit()

            with patch("jobs_ai.workspace.discover_project_root", return_value=project_root):
                with patch("jobs_ai.launch_executor.webbrowser.open", return_value=True) as open_browser:
                    result = RUNNER.invoke(
                        app,
                        ["session", "start", "--limit", "2", "--open"],
                        env=env,
                    )

            self.assertEqual(result.exit_code, 0)
            self.assertEqual(
                [call.args[0] for call in open_browser.call_args_list],
                ["https://agency.example/jobs/2", "https://jobs.example.com/3"],
            )
            self.assertIn("open executor: browser_stub", result.stdout)
            self.assertIn("opened in browser: 2", result.stdout)
            self.assertIn("python -m jobs_ai session mark opened --manifest", result.stdout)

    def test_cli_session_start_open_with_noop_does_not_open_browser(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            project_root = Path(tmp_dir) / "workspace"
            database_path = project_root / "runtime" / "jobs_ai.db"
            env = {"JOBS_AI_DB_PATH": str(database_path)}
            initialize_schema(database_path)

            with closing(connect_database(database_path)) as connection:
                _insert_job_with_status(
                    connection,
                    source="staffing recruiter",
                    company="Northwind Talent",
                    title="Platform Data Engineer",
                    location="Remote",
                    raw_payload={"description": "Python BigQuery GCP contract"},
                    apply_url="https://agency.example/jobs/2",
                )
                connection.commit()

            with patch("jobs_ai.workspace.discover_project_root", return_value=project_root):
                with patch("jobs_ai.launch_executor.webbrowser.open") as open_browser:
                    result = RUNNER.invoke(
                        app,
                        ["session", "start", "--open", "--executor", "noop"],
                        env=env,
                    )

            self.assertEqual(result.exit_code, 0)
            open_browser.assert_not_called()
            self.assertIn("open executor: noop", result.stdout)
            self.assertIn("dry run only: 1", result.stdout)

    def test_cli_session_start_out_dir_and_label_customize_artifact_path(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            project_root = Path(tmp_dir) / "workspace"
            database_path = project_root / "runtime" / "jobs_ai.db"
            env = {"JOBS_AI_DB_PATH": str(database_path)}
            initialize_schema(database_path)

            with closing(connect_database(database_path)) as connection:
                _insert_job_with_status(
                    connection,
                    source="manual",
                    company="Acme Data",
                    title="Data Engineer",
                    location="Remote",
                    raw_payload={"description": "Python pipelines"},
                    apply_url="https://jobs.example.com/2",
                )
                connection.commit()

            with patch("jobs_ai.workspace.discover_project_root", return_value=project_root):
                result = RUNNER.invoke(
                    app,
                    [
                        "session",
                        "start",
                        "--label",
                        "morning batch",
                        "--out-dir",
                        "data/custom-exports",
                    ],
                    env=env,
                )

            self.assertEqual(result.exit_code, 0)
            self.assertIn("label: morning-batch", result.stdout)

            export_files = sorted(
                (project_root / "data" / "custom-exports").glob(
                    "launch-preview-session-morning-batch-*.json"
                )
            )
            self.assertEqual(len(export_files), 1)
            payload = json.loads(export_files[0].read_text(encoding="utf-8"))
            self.assertEqual(payload["label"], "morning-batch")

    def test_cli_session_start_rejects_executor_without_open(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            project_root = Path(tmp_dir) / "workspace"
            database_path = project_root / "runtime" / "jobs_ai.db"
            env = {"JOBS_AI_DB_PATH": str(database_path)}
            initialize_schema(database_path)

            with patch("jobs_ai.workspace.discover_project_root", return_value=project_root):
                result = RUNNER.invoke(
                    app,
                    ["session", "start", "--executor", "browser_stub"],
                    env=env,
                )

            self.assertEqual(result.exit_code, 1)
            self.assertIn("jobs_ai session start", result.stdout)
            self.assertIn("status: failed", result.stdout)
            self.assertIn("--executor can only be used together with --open", result.stdout)


if __name__ == "__main__":
    unittest.main()
