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
from jobs_ai.jobs.fast_apply import select_fast_apply_selections
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


class FastApplySelectionTest(unittest.TestCase):
    def test_select_fast_apply_selections_filters_to_launchable_likely_matches(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            database_path = Path(tmp_dir) / "runtime" / "jobs_ai.db"
            initialize_schema(database_path)

            with closing(connect_database(database_path)) as connection:
                matching_job_id = _insert_job_with_status(
                    connection,
                    source="manual",
                    company="Stripe",
                    title="Python Backend Engineer",
                    location="Remote",
                    apply_url="https://boards.greenhouse.io/stripe/jobs/12345",
                    raw_payload={"description": "Python backend platform systems"},
                )
                _insert_job_with_status(
                    connection,
                    source="manual",
                    company="Datadog",
                    title="Backend Engineer",
                    location="Remote",
                    apply_url=None,
                    raw_payload={"description": "Python backend services"},
                )
                _insert_job_with_status(
                    connection,
                    source="manual",
                    company="Figma",
                    title="Frontend Engineer",
                    location="Remote",
                    apply_url="https://jobs.example.com/frontend",
                    raw_payload={"description": "TypeScript design systems"},
                )
                connection.commit()

            selections = select_fast_apply_selections(database_path, limit=10)

            self.assertEqual(len(selections), 1)
            self.assertEqual(selections[0].preview.job_id, matching_job_id)
            self.assertEqual(selections[0].matched_families, ("backend",))
            self.assertIn("matched families: backend", selections[0].fit_reason)

    def test_select_fast_apply_selections_prefers_requested_families_before_score(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            database_path = Path(tmp_dir) / "runtime" / "jobs_ai.db"
            initialize_schema(database_path)

            with closing(connect_database(database_path)) as connection:
                data_job_id = _insert_job_with_status(
                    connection,
                    source="staffing recruiter",
                    company="Northwind Talent",
                    title="Platform Data Engineer",
                    location="Remote",
                    apply_url="https://agency.example/jobs/1",
                    raw_payload={"description": "Python BigQuery GCP contract"},
                )
                backend_job_id = _insert_job_with_status(
                    connection,
                    source="manual",
                    company="Stripe",
                    title="Python Backend Engineer",
                    location="Remote",
                    apply_url="https://jobs.example.com/backend",
                    raw_payload={"description": "Python backend distributed systems"},
                )
                connection.commit()

            selections = select_fast_apply_selections(
                database_path,
                families=("backend",),
            )

            self.assertEqual(
                [selection.preview.job_id for selection in selections[:2]],
                [backend_job_id, data_job_id],
            )
            self.assertTrue(selections[0].requested_family_hit)
            self.assertFalse(selections[1].requested_family_hit)

    def test_select_fast_apply_selections_remote_only_filters_non_remote_jobs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            database_path = Path(tmp_dir) / "runtime" / "jobs_ai.db"
            initialize_schema(database_path)

            with closing(connect_database(database_path)) as connection:
                remote_job_id = _insert_job_with_status(
                    connection,
                    source="manual",
                    company="Remote Co",
                    title="Data Engineer",
                    location="Remote",
                    apply_url="https://jobs.example.com/remote",
                    raw_payload={"description": "Python pipelines"},
                )
                _insert_job_with_status(
                    connection,
                    source="manual",
                    company="Hybrid Co",
                    title="Data Engineer",
                    location="Hybrid - San Jose, CA",
                    apply_url="https://jobs.example.com/hybrid",
                    raw_payload={"description": "Python pipelines"},
                )
                connection.commit()

            selections = select_fast_apply_selections(
                database_path,
                remote_only=True,
            )

            self.assertEqual([selection.preview.job_id for selection in selections], [remote_job_id])

    def test_select_fast_apply_selections_easy_apply_first_prefers_supported_portals(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            database_path = Path(tmp_dir) / "runtime" / "jobs_ai.db"
            initialize_schema(database_path)

            with closing(connect_database(database_path)) as connection:
                unsupported_job_id = _insert_job_with_status(
                    connection,
                    source="staffing recruiter",
                    company="Agency High Score",
                    title="Platform Data Engineer",
                    location="Remote",
                    apply_url="https://agency.example/jobs/high-score",
                    raw_payload={"description": "Python BigQuery GCP contract"},
                )
                supported_job_id = _insert_job_with_status(
                    connection,
                    source="manual",
                    company="Hosted Portal Co",
                    title="Python Backend Engineer",
                    location="Remote",
                    apply_url="https://boards.greenhouse.io/hosted/jobs/12345",
                    portal_type="greenhouse",
                    raw_payload={"description": "Python backend services"},
                )
                connection.commit()

            selections = select_fast_apply_selections(
                database_path,
                easy_apply_first=True,
            )

            self.assertEqual(
                [selection.preview.job_id for selection in selections[:2]],
                [supported_job_id, unsupported_job_id],
            )
            self.assertTrue(selections[0].easy_apply_supported)
            self.assertFalse(selections[1].easy_apply_supported)


class FastApplyCommandTest(unittest.TestCase):
    def test_cli_fast_apply_exports_fast_apply_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            project_root = Path(tmp_dir) / "workspace"
            database_path = project_root / "runtime" / "jobs_ai.db"
            env = {"JOBS_AI_DB_PATH": str(database_path)}
            initialize_schema(database_path)

            with closing(connect_database(database_path)) as connection:
                shortlisted_job_id = _insert_job_with_status(
                    connection,
                    source="manual",
                    company="Stripe",
                    title="Python Backend Engineer",
                    location="Remote",
                    apply_url="https://boards.greenhouse.io/stripe/jobs/12345",
                    raw_payload={"description": "Python backend platform systems"},
                    ingest_batch_id="discover-fast-apply",
                    source_query="python backend engineer remote",
                )
                _insert_job_with_status(
                    connection,
                    source="manual",
                    company="Design Co",
                    title="Product Designer",
                    location="Remote",
                    apply_url="https://jobs.example.com/designer",
                    raw_payload={"description": "Design systems"},
                    ingest_batch_id="discover-fast-apply",
                    source_query="python backend engineer remote",
                )
                connection.commit()

            with patch("jobs_ai.workspace.discover_project_root", return_value=project_root):
                result = RUNNER.invoke(
                    app,
                    [
                        "fast-apply",
                        "--batch-id",
                        "discover-fast-apply",
                        "--families",
                        "backend",
                        "--limit",
                        "5",
                    ],
                    env=env,
                )

            self.assertEqual(result.exit_code, 0)
            self.assertIn("jobs_ai fast-apply", result.stdout)
            self.assertIn("families: backend", result.stdout)
            self.assertIn("selection source: fast-apply shortlist from likely resume-matching launchable jobs", result.stdout)
            self.assertIn(f"[job {shortlisted_job_id}]", result.stdout)

            export_files = sorted((project_root / "data" / "exports").glob("launch-preview-session-*.json"))
            self.assertEqual(len(export_files), 1)
            manifest = load_session_manifest(export_files[0])
            self.assertEqual(manifest.item_count, 1)
            self.assertIsNotNone(manifest.selection_scope)
            assert manifest.selection_scope is not None
            self.assertEqual(manifest.selection_scope.batch_id, "discover-fast-apply")
            self.assertEqual(manifest.selection_scope.selection_mode, "fast_apply")
            self.assertEqual(manifest.items[0].job_id, shortlisted_job_id)

    def test_cli_fast_apply_manifest_items_can_be_marked_without_touching_unmarked_jobs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            project_root = Path(tmp_dir) / "workspace"
            database_path = project_root / "runtime" / "jobs_ai.db"
            env = {"JOBS_AI_DB_PATH": str(database_path)}
            initialize_schema(database_path)

            with closing(connect_database(database_path)) as connection:
                first_job_id = _insert_job_with_status(
                    connection,
                    source="manual",
                    company="Stripe",
                    title="Python Backend Engineer",
                    location="Remote",
                    apply_url="https://boards.greenhouse.io/stripe/jobs/12345",
                    raw_payload={"description": "Python backend services"},
                )
                second_job_id = _insert_job_with_status(
                    connection,
                    source="manual",
                    company="Datadog",
                    title="Backend Engineer",
                    location="Remote",
                    apply_url="https://jobs.lever.co/datadog/67890",
                    raw_payload={"description": "Python backend distributed systems"},
                )
                connection.commit()

            with patch("jobs_ai.workspace.discover_project_root", return_value=project_root):
                fast_apply_result = RUNNER.invoke(
                    app,
                    ["fast-apply", "--limit", "2", "--easy-apply-first"],
                    env=env,
                )
            self.assertEqual(fast_apply_result.exit_code, 0)

            manifest_files = sorted((project_root / "data" / "exports").glob("launch-preview-session-*.json"))
            self.assertEqual(len(manifest_files), 1)
            manifest_path = manifest_files[0]

            with patch("jobs_ai.workspace.discover_project_root", return_value=project_root):
                mark_applied = RUNNER.invoke(
                    app,
                    ["session", "mark", "applied", "--manifest", str(manifest_path), "--index", "1"],
                    env=env,
                )
            self.assertEqual(mark_applied.exit_code, 0)
            self.assertIn("updated jobs: 1", mark_applied.stdout)

            manifest = load_session_manifest(manifest_path)
            first_manifest_job_id = manifest.items[0].job_id
            second_manifest_job_id = manifest.items[1].job_id
            assert first_manifest_job_id is not None
            assert second_manifest_job_id is not None

            self.assertEqual(
                get_application_status(database_path, job_id=first_manifest_job_id).snapshot.current_status,
                "applied",
            )
            self.assertEqual(
                get_application_status(database_path, job_id=second_manifest_job_id).snapshot.current_status,
                "new",
            )
            self.assertEqual(
                {
                    get_application_status(database_path, job_id=first_job_id).snapshot.current_status,
                    get_application_status(database_path, job_id=second_job_id).snapshot.current_status,
                },
                {"applied", "new"},
            )


if __name__ == "__main__":
    unittest.main()
