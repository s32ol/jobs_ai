from __future__ import annotations

from contextlib import closing
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

from typer.testing import CliRunner

SRC_PATH = Path(__file__).resolve().parents[1] / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

from jobs_ai.application_tracking import (
    get_application_status,
    list_application_statuses,
    record_application_status,
)
from jobs_ai.cli import app
from jobs_ai.db import connect_database, initialize_schema, insert_job

RUNNER = CliRunner()


def _sqlite_cli_env(database_path: Path) -> dict[str, str]:
    return {
        "JOBS_AI_DB_BACKEND": "sqlite",
        "JOBS_AI_DB_PATH": str(database_path),
    }


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


@patch.dict("os.environ", {"JOBS_AI_DB_BACKEND": "sqlite"}, clear=False)
class ApplicationTrackingTest(unittest.TestCase):
    def test_record_application_status_allows_manual_updates_and_records_timestamps(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            database_path = Path(tmp_dir) / "runtime" / "jobs_ai.db"
            initialize_schema(database_path)

            with closing(connect_database(database_path)) as connection:
                job_id = insert_job(
                    connection,
                    _job_record(
                        source="manual",
                        company="Acme Data",
                        title="Data Engineer",
                        location="Remote",
                    ),
                )
                connection.commit()

            opened_snapshot = record_application_status(database_path, job_id=job_id, status="opened")
            applied_snapshot = record_application_status(database_path, job_id=job_id, status="applied")

            self.assertEqual(opened_snapshot.current_status, "opened")
            self.assertEqual(applied_snapshot.current_status, "applied")
            self.assertRegex(opened_snapshot.latest_timestamp or "", r"^\d{4}-\d{2}-\d{2} ")
            self.assertRegex(applied_snapshot.latest_timestamp or "", r"^\d{4}-\d{2}-\d{2} ")
            self.assertEqual(applied_snapshot.applied_timestamp, applied_snapshot.latest_timestamp)

            with closing(connect_database(database_path)) as connection:
                job_row = connection.execute(
                    "SELECT status, applied_at FROM jobs WHERE id = ?",
                    (job_id,),
                ).fetchone()
            self.assertIsNotNone(job_row)
            assert job_row is not None
            self.assertEqual(job_row["status"], "applied")
            self.assertEqual(job_row["applied_at"], applied_snapshot.applied_timestamp)

            detail = get_application_status(database_path, job_id=job_id)
            self.assertEqual(detail.snapshot.current_status, "applied")
            self.assertEqual([entry.status for entry in detail.history], ["opened", "applied"])
            self.assertTrue(all(entry.timestamp for entry in detail.history))
            self.assertEqual(detail.snapshot.applied_timestamp, applied_snapshot.applied_timestamp)

    def test_record_application_status_rejects_invalid_status(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            database_path = Path(tmp_dir) / "runtime" / "jobs_ai.db"
            initialize_schema(database_path)

            with closing(connect_database(database_path)) as connection:
                job_id = insert_job(
                    connection,
                    _job_record(
                        source="manual",
                        company="Acme Data",
                        title="Data Engineer",
                        location="Remote",
                    ),
                )
                connection.commit()

            with self.assertRaisesRegex(ValueError, "invalid status 'submitted'"):
                record_application_status(database_path, job_id=job_id, status="submitted")

    def test_record_application_status_supports_pipeline_outcomes_and_blocks_backwards_transitions(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            database_path = Path(tmp_dir) / "runtime" / "jobs_ai.db"
            initialize_schema(database_path)

            with closing(connect_database(database_path)) as connection:
                job_id = insert_job(
                    connection,
                    _job_record(
                        source="manual",
                        company="Acme Data",
                        title="Data Engineer",
                        location="Remote",
                    ),
                )
                connection.commit()

            record_application_status(database_path, job_id=job_id, status="applied")
            record_application_status(database_path, job_id=job_id, status="recruiter_screen")
            interview_snapshot = record_application_status(database_path, job_id=job_id, status="interview")

            self.assertEqual(interview_snapshot.current_status, "interview")
            detail = get_application_status(database_path, job_id=job_id)
            self.assertEqual(
                [entry.status for entry in detail.history],
                ["applied", "recruiter_screen", "interview"],
            )
            self.assertIsNotNone(detail.snapshot.applied_timestamp)

            with self.assertRaisesRegex(
                ValueError,
                "cannot move from interview to opened",
            ):
                record_application_status(database_path, job_id=job_id, status="opened")

    def test_cli_track_mark_records_status_update(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            database_path = Path(tmp_dir) / "runtime" / "jobs_ai.db"
            env = _sqlite_cli_env(database_path)
            initialize_schema(database_path)

            with closing(connect_database(database_path)) as connection:
                job_id = insert_job(
                    connection,
                    _job_record(
                        source="manual",
                        company="Northwind Talent",
                        title="Platform Data Engineer",
                        location="Remote",
                    ),
                )
                connection.commit()

            result = RUNNER.invoke(app, ["track", "mark", str(job_id), "opened"], env=env)

            self.assertEqual(result.exit_code, 0)
            self.assertIn("jobs_ai track mark", result.stdout)
            self.assertIn(f"job id: {job_id}", result.stdout)
            self.assertIn("recorded status: opened", result.stdout)
            self.assertRegex(result.stdout, r"timestamp: \d{4}-\d{2}-\d{2} ")

    def test_cli_invalid_location_marks_job_id(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            database_path = Path(tmp_dir) / "runtime" / "jobs_ai.db"
            env = _sqlite_cli_env(database_path)
            initialize_schema(database_path)

            with closing(connect_database(database_path)) as connection:
                job_id = insert_job(
                    connection,
                    _job_record(
                        source="manual",
                        company="Northwind Talent",
                        title="Platform Data Engineer",
                        location="Toronto, Canada",
                    ),
                )
                connection.commit()

            result = RUNNER.invoke(app, ["invalid-location", str(job_id)], env=env)

            self.assertEqual(result.exit_code, 0)
            self.assertIn("jobs_ai invalid-location", result.stdout)
            self.assertIn(f"reference: {job_id}", result.stdout)
            self.assertIn("reference kind: job_id", result.stdout)
            self.assertIn("requested status: invalid_location", result.stdout)
            self.assertIn("matched rows: 1", result.stdout)
            self.assertIn("updated jobs: 1", result.stdout)
            self.assertIn(f"updated job ids: {job_id}", result.stdout)
            self.assertEqual(
                get_application_status(database_path, job_id=job_id).snapshot.current_status,
                "invalid_location",
            )
            self.assertEqual(
                [entry.status for entry in get_application_status(database_path, job_id=job_id).history],
                ["invalid_location"],
            )

    def test_cli_invalid_location_marks_url_and_all_matches(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            database_path = Path(tmp_dir) / "runtime" / "jobs_ai.db"
            env = _sqlite_cli_env(database_path)
            initialize_schema(database_path)
            tracking_url = "https://boards.greenhouse.io/qualifiedhealth?gh_jid=5153317008&utm_source=test"
            canonical_url = "https://boards.greenhouse.io/qualifiedhealth/jobs/5153317008"

            with closing(connect_database(database_path)) as connection:
                first_job_id = insert_job(
                    connection,
                    _job_record(
                        source="manual",
                        company="Qualified Health",
                        title="Data Engineer",
                        location="Remote",
                        apply_url=tracking_url,
                    ),
                )
                second_job_id = insert_job(
                    connection,
                    _job_record(
                        source="manual",
                        company="Qualified Health",
                        title="Senior Data Engineer",
                        location="Remote",
                        apply_url=canonical_url,
                    ),
                )
                connection.commit()

            result = RUNNER.invoke(app, ["invalid-location", tracking_url], env=env)

            self.assertEqual(result.exit_code, 0)
            self.assertIn("jobs_ai invalid-location", result.stdout)
            self.assertIn(f"reference: {tracking_url}", result.stdout)
            self.assertIn("reference kind: apply_url", result.stdout)
            self.assertIn("matched rows: 2", result.stdout)
            self.assertIn(
                "canonical/preferred row: [job "
                f"{second_job_id}] Qualified Health | Senior Data Engineer | Remote | status new",
                result.stdout,
            )
            self.assertIn(f"updated job ids: {first_job_id}, {second_job_id}", result.stdout)
            self.assertEqual(
                get_application_status(database_path, job_id=first_job_id).snapshot.current_status,
                "invalid_location",
            )
            self.assertEqual(
                get_application_status(database_path, job_id=second_job_id).snapshot.current_status,
                "invalid_location",
            )

    def test_cli_invalid_location_is_idempotent_when_already_invalid(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            database_path = Path(tmp_dir) / "runtime" / "jobs_ai.db"
            env = _sqlite_cli_env(database_path)
            initialize_schema(database_path)

            with closing(connect_database(database_path)) as connection:
                job_id = insert_job(
                    connection,
                    _job_record(
                        source="manual",
                        company="Northwind Talent",
                        title="Platform Data Engineer",
                        location="Toronto, Canada",
                    ),
                )
                connection.execute(
                    "UPDATE jobs SET status = 'invalid_location', updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                    (job_id,),
                )
                connection.execute(
                    """
                    INSERT INTO application_tracking (job_id, status, created_at)
                    VALUES (?, ?, ?)
                    """,
                    (job_id, "invalid_location", "2026-03-17T10:00:00Z"),
                )
                connection.commit()

            result = RUNNER.invoke(app, ["invalid-location", str(job_id)], env=env)

            self.assertEqual(result.exit_code, 0)
            self.assertIn("jobs_ai invalid-location", result.stdout)
            self.assertIn("requested status: invalid_location", result.stdout)
            self.assertIn("updated jobs: 0", result.stdout)
            self.assertIn("skipped targets: 1", result.stdout)
            self.assertIn(f"job {job_id}: already invalid_location", result.stdout)
            self.assertIn("status: success", result.stdout)
            detail = get_application_status(database_path, job_id=job_id)
            self.assertEqual(detail.snapshot.current_status, "invalid_location")
            self.assertEqual([entry.status for entry in detail.history], ["invalid_location"])

    def test_cli_track_mark_accepts_apply_url_reference(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            database_path = Path(tmp_dir) / "runtime" / "jobs_ai.db"
            env = _sqlite_cli_env(database_path)
            initialize_schema(database_path)
            tracking_url = "https://boards.greenhouse.io/qualifiedhealth?gh_jid=5153317008&utm_source=test"
            canonical_url = "https://boards.greenhouse.io/qualifiedhealth/jobs/5153317008"

            with closing(connect_database(database_path)) as connection:
                job_id = insert_job(
                    connection,
                    _job_record(
                        source="manual",
                        company="Qualified Health",
                        title="Platform Engineer",
                        location="Remote",
                        apply_url=canonical_url,
                    ),
                )
                connection.commit()

            result = RUNNER.invoke(
                app,
                ["track", "mark", tracking_url, "invalid_location"],
                env=env,
            )

            self.assertEqual(result.exit_code, 0)
            self.assertIn("jobs_ai track mark", result.stdout)
            self.assertIn(f"reference: {tracking_url}", result.stdout)
            self.assertIn("reference kind: apply_url", result.stdout)
            self.assertIn("requested status: invalid_location", result.stdout)
            self.assertIn("matched rows: 1", result.stdout)
            self.assertIn(f"canonical/preferred row: [job {job_id}] Qualified Health", result.stdout)
            self.assertIn("updated jobs: 1", result.stdout)
            self.assertIn(f"updated job ids: {job_id}", result.stdout)
            self.assertEqual(
                get_application_status(database_path, job_id=job_id).snapshot.current_status,
                "invalid_location",
            )

    def test_cli_track_mark_apply_url_updates_all_matched_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            database_path = Path(tmp_dir) / "runtime" / "jobs_ai.db"
            env = _sqlite_cli_env(database_path)
            initialize_schema(database_path)
            apply_url = "https://example.com/jobs/data-platform"

            with closing(connect_database(database_path)) as connection:
                sibling_job_id = insert_job(
                    connection,
                    _job_record(
                        source="manual",
                        company="Northwind Talent",
                        title="Data Engineer",
                        location="Remote",
                        apply_url=apply_url,
                    ),
                )
                preferred_job_id = insert_job(
                    connection,
                    _job_record(
                        source="manual",
                        company="Northwind Talent",
                        title="Senior Platform Data Engineer",
                        location="Remote",
                        apply_url=apply_url,
                    ),
                )
                connection.execute(
                    "UPDATE jobs SET status = 'opened', updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                    (preferred_job_id,),
                )
                connection.commit()

            result = RUNNER.invoke(
                app,
                ["track", "mark", apply_url, "invalid_location"],
                env=env,
            )

            self.assertEqual(result.exit_code, 0)
            self.assertIn("matched rows: 2", result.stdout)
            self.assertIn("updated jobs: 2", result.stdout)
            self.assertIn(
                f"canonical/preferred row: [job {preferred_job_id}] Northwind Talent",
                result.stdout,
            )
            self.assertIn(
                f"updated job ids: {sibling_job_id}, {preferred_job_id}",
                result.stdout,
            )
            self.assertEqual(
                get_application_status(database_path, job_id=sibling_job_id).snapshot.current_status,
                "invalid_location",
            )
            self.assertEqual(
                get_application_status(database_path, job_id=preferred_job_id).snapshot.current_status,
                "invalid_location",
            )

    def test_cli_track_mark_apply_url_reports_missing_url(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            database_path = Path(tmp_dir) / "runtime" / "jobs_ai.db"
            env = _sqlite_cli_env(database_path)
            initialize_schema(database_path)

            result = RUNNER.invoke(
                app,
                ["track", "mark", "https://example.com/jobs/missing", "invalid_location"],
                env=env,
            )

            self.assertEqual(result.exit_code, 1)
            self.assertIn("jobs_ai track mark", result.stdout)
            self.assertIn("status: failed", result.stdout)
            self.assertIn("no jobs found for URL", result.stdout)

    def test_cli_track_mark_rejects_invalid_status(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            database_path = Path(tmp_dir) / "runtime" / "jobs_ai.db"
            env = _sqlite_cli_env(database_path)
            initialize_schema(database_path)

            with closing(connect_database(database_path)) as connection:
                job_id = insert_job(
                    connection,
                    _job_record(
                        source="manual",
                        company="Northwind Talent",
                        title="Platform Data Engineer",
                        location="Remote",
                    ),
                )
                connection.commit()

            result = RUNNER.invoke(app, ["track", "mark", str(job_id), "submitted"], env=env)

            self.assertEqual(result.exit_code, 1)
            self.assertIn("jobs_ai track mark", result.stdout)
            self.assertIn("status: failed", result.stdout)
            self.assertIn("invalid status 'submitted'", result.stdout)

    def test_cli_track_mark_supports_new_outcome_statuses(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            database_path = Path(tmp_dir) / "runtime" / "jobs_ai.db"
            env = _sqlite_cli_env(database_path)
            initialize_schema(database_path)

            with closing(connect_database(database_path)) as connection:
                job_id = insert_job(
                    connection,
                    _job_record(
                        source="manual",
                        company="Northwind Talent",
                        title="Platform Data Engineer",
                        location="Remote",
                    ),
                )
                connection.commit()

            applied_result = RUNNER.invoke(app, ["track", "mark", str(job_id), "applied"], env=env)
            interview_result = RUNNER.invoke(app, ["track", "mark", str(job_id), "interview"], env=env)

            self.assertEqual(applied_result.exit_code, 0)
            self.assertEqual(interview_result.exit_code, 0)
            self.assertIn("recorded status: interview", interview_result.stdout)

    def test_cli_track_mark_supports_invalid_location_status(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            database_path = Path(tmp_dir) / "runtime" / "jobs_ai.db"
            env = _sqlite_cli_env(database_path)
            initialize_schema(database_path)

            with closing(connect_database(database_path)) as connection:
                job_id = insert_job(
                    connection,
                    _job_record(
                        source="manual",
                        company="Northwind Talent",
                        title="Platform Data Engineer",
                        location="Toronto, Canada",
                    ),
                )
                connection.commit()

            result = RUNNER.invoke(app, ["track", "mark", str(job_id), "invalid_location"], env=env)

            self.assertEqual(result.exit_code, 0)
            self.assertIn("recorded status: invalid_location", result.stdout)
            detail = get_application_status(database_path, job_id=job_id)
            self.assertEqual(detail.snapshot.current_status, "invalid_location")
            self.assertEqual([entry.status for entry in detail.history], ["invalid_location"])

    def test_cli_track_mark_apply_url_updates_all_canonical_matches(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            database_path = Path(tmp_dir) / "runtime" / "jobs_ai.db"
            env = _sqlite_cli_env(database_path)
            tracking_url = "https://boards.greenhouse.io/qualifiedhealth?gh_jid=5153317008&utm_source=test"
            canonical_url = "https://boards.greenhouse.io/qualifiedhealth/jobs/5153317008"
            initialize_schema(database_path)

            with closing(connect_database(database_path)) as connection:
                first_job_id = insert_job(
                    connection,
                    _job_record(
                        source="manual",
                        company="Qualified Health",
                        title="Data Engineer",
                        location="Toronto, Canada",
                        apply_url=tracking_url,
                    ),
                )
                preferred_job_id = insert_job(
                    connection,
                    _job_record(
                        source="manual",
                        company="Qualified Health",
                        title="Senior Platform Data Engineer",
                        location="Toronto, Canada",
                        apply_url=canonical_url,
                    ),
                )
                connection.commit()

            result = RUNNER.invoke(
                app,
                ["track", "mark", tracking_url, "invalid_location"],
                env=env,
            )

            self.assertEqual(result.exit_code, 0)
            self.assertIn(f"reference: {tracking_url}", result.stdout)
            self.assertIn("reference kind: apply_url", result.stdout)
            self.assertIn("matched rows: 2", result.stdout)
            self.assertIn(
                (
                    "canonical/preferred row: "
                    f"[job {preferred_job_id}] Qualified Health | "
                    "Senior Platform Data Engineer | Toronto, Canada | status new"
                ),
                result.stdout,
            )
            self.assertIn(
                f"updated job ids: {first_job_id}, {preferred_job_id}",
                result.stdout,
            )
            self.assertIn(
                f"- [job {first_job_id}] Qualified Health | Data Engineer | Toronto, Canada | current=invalid_location",
                result.stdout,
            )
            self.assertIn(
                (
                    f"- [job {preferred_job_id}] Qualified Health | "
                    "Senior Platform Data Engineer | Toronto, Canada | current=invalid_location"
                ),
                result.stdout,
            )
            self.assertEqual(
                get_application_status(database_path, job_id=first_job_id).snapshot.current_status,
                "invalid_location",
            )
            self.assertEqual(
                get_application_status(database_path, job_id=preferred_job_id).snapshot.current_status,
                "invalid_location",
            )

    def test_cli_track_mark_supports_status_first_bulk_superseded(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            database_path = Path(tmp_dir) / "runtime" / "jobs_ai.db"
            env = _sqlite_cli_env(database_path)
            initialize_schema(database_path)

            with closing(connect_database(database_path)) as connection:
                first_job_id = insert_job(
                    connection,
                    _job_record(
                        source="manual",
                        company="Northwind Talent",
                        title="Platform Data Engineer",
                        location="Remote",
                    ),
                )
                second_job_id = insert_job(
                    connection,
                    _job_record(
                        source="manual",
                        company="Acme Data",
                        title="Analytics Engineer",
                        location="Remote",
                    ),
                )
                connection.commit()

            result = RUNNER.invoke(
                app,
                ["track", "mark", "superseded", str(first_job_id), str(second_job_id)],
                env=env,
            )

            self.assertEqual(result.exit_code, 0)
            self.assertIn("jobs_ai track mark", result.stdout)
            self.assertIn("requested status: superseded", result.stdout)
            self.assertIn("updated jobs: 2", result.stdout)
            self.assertEqual(
                get_application_status(database_path, job_id=first_job_id).snapshot.current_status,
                "superseded",
            )
            self.assertEqual(
                get_application_status(database_path, job_id=second_job_id).snapshot.current_status,
                "superseded",
            )

    def test_cli_track_mark_applied_keeps_one_applied_winner_per_duplicate_cluster(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            database_path = Path(tmp_dir) / "runtime" / "jobs_ai.db"
            env = _sqlite_cli_env(database_path)
            initialize_schema(database_path)

            with closing(connect_database(database_path)) as connection:
                first_job_id = insert_job(
                    connection,
                    _job_record(
                        source="manual",
                        company="Qualified Health",
                        title="Data Engineer",
                        location="Remote",
                        apply_url="https://example.com/jobs/data-engineer",
                    ),
                )
                connection.execute(
                    "UPDATE jobs SET status = 'applied', updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                    (first_job_id,),
                )
                second_job_id = insert_job(
                    connection,
                    _job_record(
                        source="manual",
                        company="Qualified Health",
                        title="Sr. Data Engineer - Healthcare Data Infrastructure",
                        location="Remote",
                        apply_url="https://example.com/jobs/data-engineer",
                    ),
                )
                connection.execute(
                    "UPDATE jobs SET status = 'opened', updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                    (second_job_id,),
                )
                third_job_id = insert_job(
                    connection,
                    _job_record(
                        source="manual",
                        company="Qualified Health",
                        title="Platform Data Engineer",
                        location="Remote",
                        apply_url="https://example.com/jobs/data-engineer",
                    ),
                )
                connection.commit()

            result = RUNNER.invoke(app, ["track", "mark", str(second_job_id), "applied"], env=env)

            self.assertEqual(result.exit_code, 0)
            self.assertIn("recorded status: applied", result.stdout)
            self.assertEqual(
                get_application_status(database_path, job_id=first_job_id).snapshot.current_status,
                "superseded",
            )
            self.assertEqual(
                get_application_status(database_path, job_id=second_job_id).snapshot.current_status,
                "applied",
            )
            self.assertEqual(
                get_application_status(database_path, job_id=third_job_id).snapshot.current_status,
                "superseded",
            )

    def test_cli_track_list_and_status_display_current_status_and_history(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            database_path = Path(tmp_dir) / "runtime" / "jobs_ai.db"
            env = _sqlite_cli_env(database_path)
            initialize_schema(database_path)

            with closing(connect_database(database_path)) as connection:
                first_job_id = insert_job(
                    connection,
                    _job_record(
                        source="manual",
                        company="Acme Data",
                        title="Data Engineer",
                        location="Remote",
                        apply_url="https://example.com/jobs/1",
                    ),
                )
                second_job_id = insert_job(
                    connection,
                    _job_record(
                        source="manual",
                        company="Bright Metrics",
                        title="Analytics Engineer",
                        location="Sacramento, CA",
                        apply_url="https://example.com/jobs/2",
                    ),
                )
                connection.commit()

            record_application_status(database_path, job_id=second_job_id, status="opened")
            record_application_status(database_path, job_id=second_job_id, status="applied")

            listed_jobs = list_application_statuses(database_path)
            self.assertEqual([snapshot.job_id for snapshot in listed_jobs], [first_job_id, second_job_id])
            self.assertIsNone(listed_jobs[0].applied_timestamp)
            self.assertIsNotNone(listed_jobs[1].applied_timestamp)

            list_result = RUNNER.invoke(app, ["track", "list"], env=env)
            status_result = RUNNER.invoke(app, ["track", "status", str(second_job_id)], env=env)

            self.assertEqual(list_result.exit_code, 0)
            self.assertIn("jobs_ai track list", list_result.stdout)
            self.assertIn(f"[job {first_job_id}] Acme Data | Data Engineer | Remote", list_result.stdout)
            self.assertIn("current status: new", list_result.stdout)
            self.assertIn(f"[job {second_job_id}] Bright Metrics | Analytics Engineer | Sacramento, CA", list_result.stdout)
            self.assertIn("current status: applied", list_result.stdout)
            self.assertIn("   latest timestamp: none", list_result.stdout)
            self.assertRegex(list_result.stdout, r"applied at: \d{4}-\d{2}-\d{2} ")

            self.assertEqual(status_result.exit_code, 0)
            self.assertIn("jobs_ai track status", status_result.stdout)
            self.assertIn(f"job id: {second_job_id}", status_result.stdout)
            self.assertIn("current status: applied", status_result.stdout)
            self.assertIn("tracking entries: 2", status_result.stdout)
            self.assertRegex(status_result.stdout, r"- \d{4}-\d{2}-\d{2} .* \| opened")
            self.assertRegex(status_result.stdout, r"- \d{4}-\d{2}-\d{2} .* \| applied")

    def test_cli_track_list_applied_falls_back_to_latest_tracking_timestamp_when_applied_at_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            database_path = Path(tmp_dir) / "runtime" / "jobs_ai.db"
            env = _sqlite_cli_env(database_path)
            initialize_schema(database_path)

            with closing(connect_database(database_path)) as connection:
                job_id = insert_job(
                    connection,
                    _job_record(
                        source="manual",
                        company="Fallback Labs",
                        title="Applied Timestamp Tester",
                        location="Remote",
                        apply_url="https://example.com/jobs/fallback",
                    ),
                )
                connection.execute(
                    """
                    INSERT INTO application_tracking (job_id, status, created_at)
                    VALUES (?, ?, ?)
                    """,
                    (job_id, "applied", "2026-03-16T09:30:00Z"),
                )
                connection.execute(
                    "UPDATE jobs SET status = 'applied', applied_at = NULL, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                    (job_id,),
                )
                connection.commit()

            snapshots = list_application_statuses(database_path, status="applied")
            self.assertEqual(len(snapshots), 1)
            self.assertIsNone(snapshots[0].applied_timestamp)
            self.assertEqual(snapshots[0].latest_timestamp, "2026-03-16T09:30:00Z")

            list_result = RUNNER.invoke(app, ["track", "list", "--status", "applied"], env=env)

            self.assertEqual(list_result.exit_code, 0)
            self.assertIn("applied at: 2026-03-16T09:30:00Z", list_result.stdout)
            self.assertNotIn("latest timestamp:", list_result.stdout)

    def test_cli_applied_marks_job_id_reference(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            database_path = Path(tmp_dir) / "runtime" / "jobs_ai.db"
            env = _sqlite_cli_env(database_path)
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

            result = RUNNER.invoke(app, ["applied", str(job_id)], env=env)

            self.assertEqual(result.exit_code, 0)
            self.assertIn("jobs_ai applied", result.stdout)
            self.assertIn(f"reference: {job_id}", result.stdout)
            self.assertIn("reference kind: job_id", result.stdout)
            self.assertIn("requested status: applied", result.stdout)
            self.assertIn("matched rows: 1", result.stdout)
            self.assertIn("updated jobs: 1", result.stdout)
            self.assertIn(f"updated job ids: {job_id}", result.stdout)
            self.assertIn("status: success", result.stdout)
            detail = get_application_status(database_path, job_id=job_id)
            self.assertEqual(detail.snapshot.current_status, "applied")
            self.assertEqual([entry.status for entry in detail.history], ["applied"])

    def test_cli_applied_marks_exact_apply_url_match(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            database_path = Path(tmp_dir) / "runtime" / "jobs_ai.db"
            env = _sqlite_cli_env(database_path)
            initialize_schema(database_path)
            apply_url = "https://job-boards.greenhouse.io/qualifiedhealth/jobs/5154151008"

            with closing(connect_database(database_path)) as connection:
                job_id = insert_job(
                    connection,
                    _job_record(
                        source="manual",
                        company="Qualified Health",
                        title="Data Engineer",
                        location="Remote",
                        apply_url=apply_url,
                    ),
                )
                connection.commit()

            result = RUNNER.invoke(app, ["applied", apply_url], env=env)

            self.assertEqual(result.exit_code, 0)
            self.assertIn("jobs_ai applied", result.stdout)
            self.assertIn(f"reference: {apply_url}", result.stdout)
            self.assertIn("reference kind: apply_url", result.stdout)
            self.assertIn(f"normalized apply_url: {apply_url}", result.stdout)
            self.assertIn("matched rows: 1", result.stdout)
            self.assertIn("updated jobs: 1", result.stdout)
            self.assertIn(f"updated job ids: {job_id}", result.stdout)
            self.assertIn("status: success", result.stdout)
            detail = get_application_status(database_path, job_id=job_id)
            self.assertEqual(detail.snapshot.current_status, "applied")
            self.assertEqual([entry.status for entry in detail.history], ["applied"])

    def test_cli_applied_marks_all_canonical_url_matches(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            database_path = Path(tmp_dir) / "runtime" / "jobs_ai.db"
            env = _sqlite_cli_env(database_path)
            initialize_schema(database_path)
            tracking_url = "https://boards.greenhouse.io/qualifiedhealth?gh_jid=5153317008&utm_source=test"
            canonical_url = "https://boards.greenhouse.io/qualifiedhealth/jobs/5153317008"

            with closing(connect_database(database_path)) as connection:
                first_job_id = insert_job(
                    connection,
                    _job_record(
                        source="manual",
                        company="Qualified Health",
                        title="Data Engineer",
                        location="Remote",
                        apply_url=tracking_url,
                    ),
                )
                second_job_id = insert_job(
                    connection,
                    _job_record(
                        source="manual",
                        company="Qualified Health",
                        title="Senior Data Engineer",
                        location="Remote",
                        apply_url=canonical_url,
                    ),
                )
                connection.execute(
                    "UPDATE jobs SET status = 'opened', updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                    (second_job_id,),
                )
                connection.commit()

            result = RUNNER.invoke(app, ["applied", tracking_url], env=env)

            self.assertEqual(result.exit_code, 0)
            self.assertIn("jobs_ai applied", result.stdout)
            self.assertIn(f"reference: {tracking_url}", result.stdout)
            self.assertIn("reference kind: apply_url", result.stdout)
            self.assertIn(f"normalized apply_url: {tracking_url}", result.stdout)
            self.assertIn(f"canonical lookup url: {canonical_url}", result.stdout)
            self.assertIn("matched rows: 2", result.stdout)
            self.assertIn("updated jobs: 2", result.stdout)
            self.assertIn(
                f"updated job ids: {first_job_id}, {second_job_id}",
                result.stdout,
            )
            self.assertEqual(
                get_application_status(database_path, job_id=first_job_id).snapshot.current_status,
                "applied",
            )
            self.assertEqual(
                get_application_status(database_path, job_id=second_job_id).snapshot.current_status,
                "applied",
            )

    def test_cli_applied_is_idempotent_for_already_applied_url_cluster(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            database_path = Path(tmp_dir) / "runtime" / "jobs_ai.db"
            env = _sqlite_cli_env(database_path)
            initialize_schema(database_path)
            tracking_url = "https://boards.greenhouse.io/qualifiedhealth?gh_jid=5153317008&utm_source=test"
            canonical_url = "https://boards.greenhouse.io/qualifiedhealth/jobs/5153317008"

            with closing(connect_database(database_path)) as connection:
                first_job_id = insert_job(
                    connection,
                    _job_record(
                        source="manual",
                        company="Qualified Health",
                        title="Data Engineer",
                        location="Remote",
                        apply_url=tracking_url,
                    ),
                )
                second_job_id = insert_job(
                    connection,
                    _job_record(
                        source="manual",
                        company="Qualified Health",
                        title="Senior Data Engineer",
                        location="Remote",
                        apply_url=canonical_url,
                    ),
                )
                first_timestamp = "2026-03-18T10:00:00Z"
                second_timestamp = "2026-03-18T10:05:00Z"
                connection.execute(
                    """
                    INSERT INTO application_tracking (job_id, status, created_at)
                    VALUES (?, ?, ?)
                    """,
                    (first_job_id, "applied", first_timestamp),
                )
                connection.execute(
                    """
                    INSERT INTO application_tracking (job_id, status, created_at)
                    VALUES (?, ?, ?)
                    """,
                    (second_job_id, "applied", second_timestamp),
                )
                connection.execute(
                    """
                    UPDATE jobs
                    SET status = 'applied', applied_at = ?, updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                    """,
                    (first_timestamp, first_job_id),
                )
                connection.execute(
                    """
                    UPDATE jobs
                    SET status = 'applied', applied_at = ?, updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                    """,
                    (second_timestamp, second_job_id),
                )
                connection.commit()

            result = RUNNER.invoke(app, ["applied", tracking_url], env=env)

            self.assertEqual(result.exit_code, 0)
            self.assertIn("jobs_ai applied", result.stdout)
            self.assertIn("updated jobs: 0", result.stdout)
            self.assertIn("skipped targets: 2", result.stdout)
            self.assertIn(f"job {first_job_id}: already applied", result.stdout)
            self.assertIn(f"job {second_job_id}: already applied", result.stdout)
            self.assertIn("status: success", result.stdout)
            self.assertEqual(
                [entry.status for entry in get_application_status(database_path, job_id=first_job_id).history],
                ["applied"],
            )
            self.assertEqual(
                [entry.status for entry in get_application_status(database_path, job_id=second_job_id).history],
                ["applied"],
            )

    def test_cli_applied_reports_invalid_references(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            database_path = Path(tmp_dir) / "runtime" / "jobs_ai.db"
            env = _sqlite_cli_env(database_path)
            initialize_schema(database_path)
            missing_url = "https://example.com/jobs/missing"

            missing_job_result = RUNNER.invoke(app, ["applied", "999"], env=env)
            missing_url_result = RUNNER.invoke(app, ["applied", missing_url], env=env)

            self.assertEqual(missing_job_result.exit_code, 1)
            self.assertIn("jobs_ai applied", missing_job_result.stdout)
            self.assertIn("reference: 999", missing_job_result.stdout)
            self.assertIn("status: failed", missing_job_result.stdout)
            self.assertIn("error: job id 999 was not found", missing_job_result.stdout)

            self.assertEqual(missing_url_result.exit_code, 1)
            self.assertIn("jobs_ai applied", missing_url_result.stdout)
            self.assertIn(f"reference: {missing_url}", missing_url_result.stdout)
            self.assertIn("status: failed", missing_url_result.stdout)
            self.assertIn(f"error: no jobs found for URL: {missing_url}", missing_url_result.stdout)

    def test_cli_applied_reports_transition_rejection(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            database_path = Path(tmp_dir) / "runtime" / "jobs_ai.db"
            env = _sqlite_cli_env(database_path)
            initialize_schema(database_path)

            with closing(connect_database(database_path)) as connection:
                job_id = insert_job(
                    connection,
                    _job_record(
                        source="manual",
                        company="Acme Data",
                        title="Data Engineer",
                        location="Remote",
                        apply_url="https://example.com/jobs/blocked",
                    ),
                )
                connection.execute(
                    "UPDATE jobs SET status = 'interview', updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                    (job_id,),
                )
                connection.commit()

            result = RUNNER.invoke(app, ["applied", str(job_id)], env=env)

            self.assertEqual(result.exit_code, 1)
            self.assertIn("jobs_ai applied", result.stdout)
            self.assertIn(f"reference: {job_id}", result.stdout)
            self.assertIn("requested status: applied", result.stdout)
            self.assertIn("updated jobs: 0", result.stdout)
            self.assertIn("skipped targets: 1", result.stdout)
            self.assertIn("status: failed", result.stdout)
            self.assertIn(
                f"job {job_id}: cannot move from interview to applied",
                result.stdout,
            )
            self.assertEqual(
                get_application_status(database_path, job_id=job_id).snapshot.current_status,
                "interview",
            )
            self.assertEqual(get_application_status(database_path, job_id=job_id).history, ())


if __name__ == "__main__":
    unittest.main()
