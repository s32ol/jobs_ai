from __future__ import annotations

from contextlib import closing
import json
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch
from urllib.parse import quote

from typer.testing import CliRunner

SRC_PATH = Path(__file__).resolve().parents[1] / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

from jobs_ai.application_tracking import get_application_status
from jobs_ai.cli import app
from jobs_ai.collect.fetch import FetchError, FetchRequest, FetchResponse
from jobs_ai.db import connect_database, initialize_schema, insert_job
from jobs_ai.discover.search import build_search_plans
from jobs_ai.session_manifest import load_session_manifest

FIXTURE_ROOT = Path(__file__).resolve().parent / "fixtures" / "collect"
RUNNER = CliRunner()


def _fixture_text(name: str) -> str:
    return (FIXTURE_ROOT / name).read_text(encoding="utf-8")


def _response(
    url: str,
    text: str,
    *,
    final_url: str | None = None,
    status_code: int = 200,
    content_type: str = "text/html; charset=utf-8",
) -> FetchResponse:
    return FetchResponse(
        url=url,
        final_url=final_url or url,
        status_code=status_code,
        content_type=content_type,
        text=text,
    )


def _mapping_fetcher(payloads: dict[str, FetchResponse | Exception | str]):
    def fetcher(request: FetchRequest) -> FetchResponse:
        payload = payloads.get(request.url)
        if payload is None:
            raise FetchError(f"unable to fetch {request.url}: no fixture available")
        if isinstance(payload, Exception):
            raise payload
        if isinstance(payload, FetchResponse):
            return payload
        return _response(request.url, payload)

    return fetcher


def _search_results_html(*target_urls: str) -> str:
    links = "\n".join(
        f'<a href="https://duckduckgo.com/l/?uddg={quote(url, safe="")}">{url}</a>'
        for url in target_urls
    )
    return f"<!doctype html><html><body>{links}</body></html>"


def _empty_search_payloads(search_plans) -> dict[str, str]:
    return {
        plan.search_url: "<html><body></body></html>"
        for plan in search_plans
    }


class RunCommandTest(unittest.TestCase):
    def test_cli_run_wires_limits_flags_and_paths_to_workflow(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            project_root = Path(tmp_dir) / "workspace"
            database_path = project_root / "runtime" / "jobs_ai.db"
            env = {"JOBS_AI_DB_PATH": str(database_path)}
            fake_result = SimpleNamespace(import_result=None)

            with patch("jobs_ai.workspace.discover_project_root", return_value=project_root):
                with patch("jobs_ai.cli.run_operator_workflow", return_value=fake_result) as run_workflow:
                    with patch("jobs_ai.cli.render_run_report", return_value="workflow report") as render_report:
                        result = RUNNER.invoke(
                            app,
                            [
                                "run",
                                "python backend engineer remote",
                                "--limit",
                                "25",
                                "--discover-limit",
                                "40",
                                "--collect-limit",
                                "10",
                                "--open",
                                "--portal-hints",
                                "--executor",
                                "noop",
                                "--label",
                                "morning batch",
                                "--out-dir",
                                "custom-bundle",
                            ],
                            env=env,
                        )

            self.assertEqual(result.exit_code, 0)
            self.assertEqual(result.stdout.strip(), "workflow report")
            self.assertEqual(run_workflow.call_count, 1)
            self.assertEqual(
                run_workflow.call_args.kwargs,
                {
                    "query": "python backend engineer remote",
                    "discover_limit": 40,
                    "collect_limit": 10,
                    "session_limit": 25,
                    "out_dir": Path("custom-bundle"),
                    "label": "morning batch",
                    "open_urls": True,
                    "executor_mode": "noop",
                },
            )
            self.assertEqual(render_report.call_count, 1)
            self.assertTrue(render_report.call_args.kwargs["show_portal_hints"])

    def test_cli_run_end_to_end_offline_and_session_mark_updates_statuses(self) -> None:
        query = "backend engineer remote"
        search_plans = build_search_plans(query)
        fetcher = _mapping_fetcher(
            {
                **_empty_search_payloads(search_plans),
                search_plans[0].search_url: _search_results_html(
                    "https://boards.greenhouse.io/acme/jobs/12345",
                ),
                "https://boards.greenhouse.io/acme": _fixture_text("greenhouse_board.html"),
            }
        )

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
                legacy_job_id = insert_job(
                    connection,
                    {
                        "source": "manual",
                        "source_job_id": None,
                        "company": "Legacy Queue",
                        "title": "Data Engineer",
                        "location": "Remote",
                        "apply_url": "https://example.com/jobs/legacy",
                        "portal_type": None,
                        "salary_text": None,
                        "posted_at": None,
                        "found_at": "2026-03-10T08:00:00Z",
                        "raw_json": json.dumps({"description": "legacy global queue job"}, ensure_ascii=True),
                    },
                )
                connection.commit()

            with patch("jobs_ai.workspace.discover_project_root", return_value=project_root):
                with patch("jobs_ai.run_workflow.fetch_text", side_effect=fetcher):
                    with patch("jobs_ai.launch_executor.webbrowser.open", return_value=True) as open_browser:
                        run_result = RUNNER.invoke(
                            app,
                            [
                                "run",
                                query,
                                "--limit",
                                "2",
                                "--open",
                                "--portal-hints",
                                "--label",
                                "morning batch",
                                "--out-dir",
                                "operator-run",
                            ],
                            env=env,
                        )

            self.assertEqual(run_result.exit_code, 0)
            self.assertIn("jobs_ai run", run_result.stdout)
            self.assertIn("confirmed sources: 1", run_result.stdout)
            self.assertIn("imported jobs: 2", run_result.stdout)
            self.assertIn("selected jobs: 2", run_result.stdout)
            self.assertIn("launchable jobs: 2", run_result.stdout)
            self.assertIn("resume variants:", run_result.stdout)
            self.assertIn(str(resume_path), run_result.stdout)
            self.assertIn("portal hint details:", run_result.stdout)
            self.assertIn("python -m jobs_ai session mark opened --manifest", run_result.stdout)
            self.assertEqual(
                [call.args[0] for call in open_browser.call_args_list],
                [
                    "https://boards.greenhouse.io/acme/jobs/12345",
                    "https://boards.greenhouse.io/acme/jobs/98765",
                ],
            )

            workflow_dir = project_root / "operator-run"
            self.assertTrue((workflow_dir / "discover_report.json").exists())
            self.assertTrue((workflow_dir / "collect" / "leads.import.json").exists())

            manifest_files = sorted(
                workflow_dir.glob("launch-preview-session-morning-batch-*.json")
            )
            self.assertEqual(len(manifest_files), 1)
            manifest_path = manifest_files[0]
            manifest = load_session_manifest(manifest_path)
            self.assertEqual(manifest.item_count, 2)
            self.assertIsNotNone(manifest.selection_scope)
            assert manifest.selection_scope is not None
            self.assertEqual(manifest.selection_scope.source_query, query)
            manifest_job_ids = {item.job_id for item in manifest.items}
            self.assertNotIn(legacy_job_id, manifest_job_ids)
            self.assertEqual(len(manifest_job_ids), 2)

            with closing(connect_database(database_path)) as connection:
                row = connection.execute("SELECT COUNT(*) AS count FROM jobs").fetchone()
            self.assertEqual(row["count"], 3)

            with patch("jobs_ai.workspace.discover_project_root", return_value=project_root):
                mark_opened = RUNNER.invoke(
                    app,
                    ["session", "mark", "opened", "--manifest", str(manifest_path), "--all"],
                    env=env,
                )
            self.assertEqual(mark_opened.exit_code, 0)
            self.assertIn("updated jobs: 2", mark_opened.stdout)

            job_ids = [item.job_id for item in manifest.items]
            self.assertEqual(
                [get_application_status(database_path, job_id=job_id).snapshot.current_status for job_id in job_ids],
                ["opened", "opened"],
            )

            with patch("jobs_ai.workspace.discover_project_root", return_value=project_root):
                mark_applied = RUNNER.invoke(
                    app,
                    ["session", "mark", "applied", "--manifest", str(manifest_path), "--index", "1"],
                    env=env,
                )
            self.assertEqual(mark_applied.exit_code, 0)
            self.assertIn("updated jobs: 1", mark_applied.stdout)
            self.assertEqual(
                get_application_status(database_path, job_id=job_ids[0]).snapshot.current_status,
                "applied",
            )
            self.assertEqual(
                get_application_status(database_path, job_id=job_ids[1]).snapshot.current_status,
                "opened",
            )


if __name__ == "__main__":
    unittest.main()
