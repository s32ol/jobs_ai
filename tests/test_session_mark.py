from __future__ import annotations

from contextlib import closing
import json
from pathlib import Path
import sys
import tempfile
import unittest

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
        ),
    )
    if status != "new":
        connection.execute(
            "UPDATE jobs SET status = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
            (status, job_id),
        )
    return job_id


def _write_manifest(manifest_path: Path, items: list[dict[str, object | None]]) -> Path:
    manifest_path.write_text(
        json.dumps(
            {
                "created_at": "2026-03-15T12:00:00Z",
                "item_count": len(items),
                "items": items,
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    return manifest_path


class SessionMarkTest(unittest.TestCase):
    def test_cli_session_mark_updates_direct_job_ids(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            database_path = Path(tmp_dir) / "runtime" / "jobs_ai.db"
            env = {"JOBS_AI_DB_PATH": str(database_path)}
            initialize_schema(database_path)

            with closing(connect_database(database_path)) as connection:
                first_job_id = _insert_job_with_status(
                    connection,
                    source="manual",
                    company="Acme Data",
                    title="Data Engineer",
                    location="Remote",
                    raw_payload={"description": "Python pipelines"},
                    apply_url="https://example.com/jobs/1",
                )
                second_job_id = _insert_job_with_status(
                    connection,
                    source="manual",
                    company="Bright Metrics",
                    title="Platform Engineer",
                    location="Remote",
                    raw_payload={"description": "Backend services"},
                    apply_url="https://example.com/jobs/2",
                )
                connection.commit()

            result = RUNNER.invoke(
                app,
                ["session", "mark", "opened", str(first_job_id), str(second_job_id)],
                env=env,
            )

            self.assertEqual(result.exit_code, 0)
            self.assertIn("jobs_ai session mark", result.stdout)
            self.assertIn("requested status: opened", result.stdout)
            self.assertIn("updated jobs: 2", result.stdout)
            self.assertIn("target source: job ids", result.stdout)

            self.assertEqual(
                get_application_status(database_path, job_id=first_job_id).snapshot.current_status,
                "opened",
            )
            self.assertEqual(
                get_application_status(database_path, job_id=second_job_id).snapshot.current_status,
                "opened",
            )

    def test_cli_session_mark_reports_partial_failures_and_dedupes_job_ids(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            database_path = Path(tmp_dir) / "runtime" / "jobs_ai.db"
            env = {"JOBS_AI_DB_PATH": str(database_path)}
            initialize_schema(database_path)

            with closing(connect_database(database_path)) as connection:
                new_job_id = _insert_job_with_status(
                    connection,
                    source="manual",
                    company="Acme Data",
                    title="Data Engineer",
                    location="Remote",
                    raw_payload={"description": "Python pipelines"},
                    apply_url="https://example.com/jobs/1",
                )
                already_applied_job_id = _insert_job_with_status(
                    connection,
                    status="applied",
                    source="manual",
                    company="Bright Metrics",
                    title="Analytics Engineer",
                    location="Remote",
                    raw_payload={"description": "SQL modeling"},
                    apply_url="https://example.com/jobs/2",
                )
                connection.commit()

            result = RUNNER.invoke(
                app,
                [
                    "session",
                    "mark",
                    "applied",
                    str(new_job_id),
                    str(already_applied_job_id),
                    "999",
                    str(new_job_id),
                ],
                env=env,
            )

            self.assertEqual(result.exit_code, 0)
            self.assertIn("updated jobs: 1", result.stdout)
            self.assertIn("skipped targets: 3", result.stdout)
            self.assertIn(f"job {already_applied_job_id}: already applied", result.stdout)
            self.assertIn("job 999: job id was not found", result.stdout)
            self.assertIn(f"job {new_job_id}: duplicate target ignored", result.stdout)

            updated_detail = get_application_status(database_path, job_id=new_job_id)
            self.assertEqual(updated_detail.snapshot.current_status, "applied")
            self.assertEqual([entry.status for entry in updated_detail.history], ["applied"])

            untouched_detail = get_application_status(database_path, job_id=already_applied_job_id)
            self.assertEqual(untouched_detail.snapshot.current_status, "applied")
            self.assertEqual(untouched_detail.history, ())

    def test_cli_session_mark_supports_pipeline_statuses(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            database_path = Path(tmp_dir) / "runtime" / "jobs_ai.db"
            env = {"JOBS_AI_DB_PATH": str(database_path)}
            initialize_schema(database_path)

            with closing(connect_database(database_path)) as connection:
                job_id = _insert_job_with_status(
                    connection,
                    source="manual",
                    company="Acme Data",
                    title="Data Engineer",
                    location="Remote",
                    raw_payload={"description": "Python pipelines"},
                    apply_url="https://example.com/jobs/1",
                )
                connection.commit()

            first_result = RUNNER.invoke(app, ["session", "mark", "applied", str(job_id)], env=env)
            second_result = RUNNER.invoke(app, ["session", "mark", "interview", str(job_id)], env=env)

            self.assertEqual(first_result.exit_code, 0)
            self.assertEqual(second_result.exit_code, 0)
            self.assertIn("requested status: interview", second_result.stdout)
            self.assertEqual(
                get_application_status(database_path, job_id=job_id).snapshot.current_status,
                "interview",
            )

    def test_cli_session_mark_manifest_all_marks_only_launchable_items(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            database_path = Path(tmp_dir) / "runtime" / "jobs_ai.db"
            env = {"JOBS_AI_DB_PATH": str(database_path)}
            initialize_schema(database_path)

            with closing(connect_database(database_path)) as connection:
                launchable_job_id = _insert_job_with_status(
                    connection,
                    source="manual",
                    company="Northwind Talent",
                    title="Platform Data Engineer",
                    location="Remote",
                    raw_payload={"description": "Python BigQuery GCP contract"},
                    apply_url="https://example.com/jobs/1",
                )
                skipped_job_id = _insert_job_with_status(
                    connection,
                    source="manual",
                    company="Contoso",
                    title="Analytics Engineer",
                    location="Remote",
                    raw_payload={"description": "SQL dashboards"},
                    apply_url="https://example.com/jobs/2",
                )
                connection.commit()

            manifest_path = _write_manifest(
                Path(tmp_dir) / "session.json",
                [
                    {
                        "rank": 1,
                        "job_id": launchable_job_id,
                        "company": "Northwind Talent",
                        "title": "Platform Data Engineer",
                        "location": "Remote",
                        "source": "manual",
                        "apply_url": "https://example.com/jobs/1",
                        "score": 20,
                        "recommended_resume_variant": {
                            "key": "data-engineering",
                            "label": "Data Engineering Resume",
                        },
                        "recommended_profile_snippet": {
                            "key": "pipeline-delivery",
                            "label": "Pipeline Delivery",
                            "text": "Python-first pipeline delivery across SQL warehouses, BigQuery/GCP, and production data systems.",
                        },
                        "explanation": "matched data engineering signals from title or stack",
                    },
                    {
                        "rank": 2,
                        "job_id": skipped_job_id,
                        "company": "Contoso",
                        "title": "Analytics Engineer",
                        "location": "Remote",
                        "source": "manual",
                        "apply_url": None,
                        "score": 12,
                        "recommended_resume_variant": {
                            "key": "analytics-engineering",
                            "label": "Analytics Engineering Resume",
                        },
                        "recommended_profile_snippet": {
                            "key": "warehouse-modeling",
                            "label": "Warehouse Modeling",
                            "text": "Modeled analytics datasets and business-facing metrics in SQL-first warehouse environments.",
                        },
                        "explanation": "matched analytics signals from title or stack",
                    },
                ],
            )

            result = RUNNER.invoke(
                app,
                [
                    "session",
                    "mark",
                    "opened",
                    "--manifest",
                    str(manifest_path),
                    "--all",
                ],
                env=env,
            )

            self.assertEqual(result.exit_code, 0)
            self.assertIn(f"manifest path: {manifest_path}", result.stdout)
            self.assertIn("target scope: all launchable items (1 of 2)", result.stdout)
            self.assertIn("manifest launchable items: 1", result.stdout)
            self.assertIn("updated jobs: 1", result.stdout)
            self.assertEqual(
                get_application_status(database_path, job_id=launchable_job_id).snapshot.current_status,
                "opened",
            )
            self.assertEqual(
                get_application_status(database_path, job_id=skipped_job_id).snapshot.current_status,
                "new",
            )

    def test_cli_session_mark_reports_missing_manifest_clearly(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            database_path = Path(tmp_dir) / "runtime" / "jobs_ai.db"
            env = {"JOBS_AI_DB_PATH": str(database_path)}
            initialize_schema(database_path)

            missing_manifest_path = Path(tmp_dir) / "missing-session.json"
            result = RUNNER.invoke(
                app,
                [
                    "session",
                    "mark",
                    "opened",
                    "--manifest",
                    str(missing_manifest_path),
                    "--all",
                ],
                env=env,
            )

            self.assertEqual(result.exit_code, 1)
            self.assertIn("jobs_ai session mark", result.stdout)
            self.assertIn("status: failed", result.stdout)
            self.assertIn(f"manifest path: {missing_manifest_path}", result.stdout)
            self.assertIn("manifest was not found", result.stdout)


if __name__ == "__main__":
    unittest.main()
