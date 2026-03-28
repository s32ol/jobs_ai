from __future__ import annotations

from contextlib import closing
import json
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest
from unittest.mock import patch

from typer.testing import CliRunner

SRC_PATH = Path(__file__).resolve().parents[1] / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

from jobs_ai.cli import app
from jobs_ai.db import (
    connect_database,
    find_jobs_by_apply_url,
    initialize_schema,
    insert_job,
    resolve_canonical_duplicate_group,
    schema_exists,
)

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
        "raw_json": json.dumps(raw_payload or {}, ensure_ascii=True),
    }


_OLD_JOBS_SCHEMA_SQL = """
CREATE TABLE jobs (
    id INTEGER PRIMARY KEY,
    source TEXT NOT NULL,
    source_job_id TEXT,
    company TEXT NOT NULL,
    title TEXT NOT NULL,
    location TEXT,
    apply_url TEXT,
    portal_type TEXT,
    salary_text TEXT,
    posted_at TEXT,
    found_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    ingest_batch_id TEXT,
    source_query TEXT,
    import_source TEXT,
    source_registry_id INTEGER,
    canonical_apply_url TEXT,
    identity_key TEXT,
    status TEXT NOT NULL DEFAULT 'new',
    raw_json TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE applications (
    id INTEGER PRIMARY KEY,
    job_id INTEGER NOT NULL,
    state TEXT NOT NULL DEFAULT 'draft',
    resume_variant TEXT,
    notes TEXT,
    last_attempted_at TEXT,
    applied_at TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE application_tracking (
    id INTEGER PRIMARY KEY,
    job_id INTEGER NOT NULL,
    status TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
"""


def _create_old_jobs_database(
    database_path: Path,
    *,
    jobs: list[dict[str, object]],
    tracking_rows: list[dict[str, object]] | None = None,
    application_rows: list[dict[str, object]] | None = None,
) -> None:
    database_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(database_path) as connection:
        connection.executescript(_OLD_JOBS_SCHEMA_SQL)
        for job in jobs:
            connection.execute(
                """
                INSERT INTO jobs (
                    id,
                    source,
                    source_job_id,
                    company,
                    title,
                    location,
                    apply_url,
                    portal_type,
                    salary_text,
                    posted_at,
                    found_at,
                    ingest_batch_id,
                    source_query,
                    import_source,
                    source_registry_id,
                    canonical_apply_url,
                    identity_key,
                    status,
                    raw_json,
                    created_at,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    job["id"],
                    job["source"],
                    job.get("source_job_id"),
                    job["company"],
                    job["title"],
                    job["location"],
                    job.get("apply_url"),
                    job.get("portal_type"),
                    job.get("salary_text"),
                    job.get("posted_at"),
                    job.get("found_at", "2026-03-13T08:00:00Z"),
                    job.get("ingest_batch_id"),
                    job.get("source_query"),
                    job.get("import_source"),
                    job.get("source_registry_id"),
                    job.get("canonical_apply_url"),
                    job.get("identity_key"),
                    job.get("status", "new"),
                    job.get("raw_json"),
                    job.get("created_at", "2026-03-13T08:00:00Z"),
                    job.get("updated_at", "2026-03-13T08:00:00Z"),
                ),
            )

        for application_row in application_rows or []:
            connection.execute(
                """
                INSERT INTO applications (
                    job_id,
                    state,
                    resume_variant,
                    notes,
                    last_attempted_at,
                    applied_at,
                    created_at,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    application_row["job_id"],
                    application_row.get("state", "draft"),
                    application_row.get("resume_variant"),
                    application_row.get("notes"),
                    application_row.get("last_attempted_at"),
                    application_row.get("applied_at"),
                    application_row.get("created_at", "2026-03-13T08:00:00Z"),
                    application_row.get("updated_at", "2026-03-13T08:00:00Z"),
                ),
            )

        for tracking_row in tracking_rows or []:
            connection.execute(
                """
                INSERT INTO application_tracking (
                    job_id,
                    status,
                    created_at
                ) VALUES (?, ?, ?)
                """,
                (
                    tracking_row["job_id"],
                    tracking_row["status"],
                    tracking_row.get("created_at", "2026-03-13T08:00:00Z"),
                ),
            )
        connection.commit()


class DatabaseTest(unittest.TestCase):
    def test_initialize_schema_creates_database_required_tables_and_indexes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            database_path = Path(tmp_dir) / "data" / "jobs_ai.db"

            with patch.dict("os.environ", {"JOBS_AI_DB_BACKEND": "sqlite"}, clear=False):
                initialize_schema(database_path)

            self.assertTrue(database_path.exists())
            with patch.dict("os.environ", {"JOBS_AI_DB_BACKEND": "sqlite"}, clear=False):
                self.assertTrue(schema_exists(database_path))
                with closing(connect_database(database_path)) as connection:
                    table_rows = connection.execute(
                        """
                        SELECT name
                        FROM sqlite_master
                        WHERE type = 'table'
                          AND name IN ('jobs', 'applications', 'application_tracking', 'session_history', 'source_registry')
                        """
                    ).fetchall()
                    index_rows = connection.execute(
                        """
                        SELECT name
                        FROM sqlite_master
                        WHERE type = 'index'
                          AND name IN (
                              'idx_jobs_apply_url',
                              'idx_jobs_source_company_title_location',
                              'idx_jobs_ingest_batch_id',
                              'idx_jobs_canonical_apply_url',
                              'idx_jobs_identity_key',
                              'idx_jobs_source_registry_id',
                              'idx_applications_job_id',
                              'idx_application_tracking_job_id',
                              'idx_session_history_created_at',
                              'idx_session_history_ingest_batch_id',
                              'idx_source_registry_normalized_url',
                              'idx_source_registry_status',
                              'idx_source_registry_last_verified_at'
                          )
                        """
                    ).fetchall()
            self.assertEqual(
                {row["name"] for row in table_rows},
                {"jobs", "applications", "application_tracking", "session_history", "source_registry"},
            )
            self.assertEqual(
                {row["name"] for row in index_rows},
                {
                    "idx_jobs_apply_url",
                    "idx_jobs_source_company_title_location",
                    "idx_jobs_ingest_batch_id",
                    "idx_jobs_canonical_apply_url",
                    "idx_jobs_identity_key",
                    "idx_jobs_source_registry_id",
                    "idx_applications_job_id",
                    "idx_application_tracking_job_id",
                    "idx_session_history_created_at",
                    "idx_session_history_ingest_batch_id",
                    "idx_source_registry_normalized_url",
                    "idx_source_registry_status",
                    "idx_source_registry_last_verified_at",
                },
            )

    def test_initialize_schema_backfills_jobs_applied_at_from_applied_tracking(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            database_path = Path(tmp_dir) / "runtime" / "jobs_ai.db"
            _create_old_jobs_database(
                database_path,
                jobs=[
                    {
                        "id": 1,
                        "source": "manual",
                        "company": "Qualified Health",
                        "title": "Platform Engineer",
                        "location": "Remote",
                        "apply_url": "https://example.com/jobs/1",
                        "status": "applied",
                        "raw_json": json.dumps({}, ensure_ascii=True),
                        "created_at": "2026-03-13T08:00:00Z",
                        "updated_at": "2026-03-13T08:00:00Z",
                    }
                ],
                tracking_rows=[
                    {
                        "job_id": 1,
                        "status": "applied",
                        "created_at": "2026-03-13T09:00:00Z",
                    },
                    {
                        "job_id": 1,
                        "status": "applied",
                        "created_at": "2026-03-13T10:00:00Z",
                    },
                ],
            )

            with patch.dict("os.environ", {"JOBS_AI_DB_BACKEND": "sqlite"}, clear=False):
                initialize_schema(database_path)

            with patch.dict("os.environ", {"JOBS_AI_DB_BACKEND": "sqlite"}, clear=False):
                with closing(connect_database(database_path)) as connection:
                    column_rows = connection.execute("PRAGMA table_info(jobs)").fetchall()
                    job_row = connection.execute(
                        "SELECT status, applied_at FROM jobs WHERE id = ?",
                        (1,),
                    ).fetchone()

            self.assertIn("applied_at", {row["name"] for row in column_rows})
            self.assertEqual(job_row["status"], "applied")
            self.assertEqual(job_row["applied_at"], "2026-03-13T10:00:00Z")

    def test_initialize_schema_leaves_jobs_applied_at_null_without_applied_tracking_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            database_path = Path(tmp_dir) / "runtime" / "jobs_ai.db"
            _create_old_jobs_database(
                database_path,
                jobs=[
                    {
                        "id": 1,
                        "source": "manual",
                        "company": "Acme Data",
                        "title": "Data Engineer",
                        "location": "Remote",
                        "apply_url": "https://example.com/jobs/2",
                        "status": "applied",
                        "raw_json": json.dumps({}, ensure_ascii=True),
                        "created_at": "2026-03-13T08:00:00Z",
                        "updated_at": "2026-03-13T08:00:00Z",
                    }
                ],
                tracking_rows=[
                    {
                        "job_id": 1,
                        "status": "opened",
                        "created_at": "2026-03-13T09:00:00Z",
                    }
                ],
            )

            with patch.dict("os.environ", {"JOBS_AI_DB_BACKEND": "sqlite"}, clear=False):
                initialize_schema(database_path)

            with patch.dict("os.environ", {"JOBS_AI_DB_BACKEND": "sqlite"}, clear=False):
                with closing(connect_database(database_path)) as connection:
                    job_row = connection.execute(
                        "SELECT status, applied_at FROM jobs WHERE id = ?",
                        (1,),
                    ).fetchone()

            self.assertEqual(job_row["status"], "applied")
            self.assertIsNone(job_row["applied_at"])

    def test_resolve_canonical_duplicate_group_sets_jobs_applied_at_for_applied_transition(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            database_path = Path(tmp_dir) / "runtime" / "jobs_ai.db"
            with patch.dict("os.environ", {"JOBS_AI_DB_BACKEND": "sqlite"}, clear=False):
                initialize_schema(database_path)

            apply_url = "https://example.com/jobs/3"
            with closing(connect_database(database_path)) as connection:
                winner_job_id = insert_job(
                    connection,
                    _job_record(
                        source="manual",
                        company="Qualified Health",
                        title="Senior Platform Engineer",
                        location="Remote",
                        apply_url=apply_url,
                    ),
                )
                loser_job_id = insert_job(
                    connection,
                    _job_record(
                        source="manual",
                        company="Qualified Health",
                        title="Platform Engineer",
                        location="Remote",
                        apply_url=apply_url,
                    ),
                )
                connection.execute(
                    "UPDATE jobs SET status = ?, updated_at = ? WHERE id = ?",
                    ("opened", "2026-03-13T09:00:00Z", winner_job_id),
                )
                connection.execute(
                    """
                    INSERT INTO applications (
                        job_id,
                        state,
                        applied_at,
                        created_at,
                        updated_at
                    ) VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        winner_job_id,
                        "submitted",
                        "2026-03-13T09:30:00Z",
                        "2026-03-13T09:30:00Z",
                        "2026-03-13T09:30:00Z",
                    ),
                )
                connection.commit()

            with closing(connect_database(database_path)) as connection:
                resolution = resolve_canonical_duplicate_group(
                    connection,
                    canonical_apply_url=apply_url,
                )
                connection.commit()

            self.assertIsNotNone(resolution)
            with closing(connect_database(database_path)) as connection:
                winner_row = connection.execute(
                    "SELECT status, applied_at FROM jobs WHERE id = ?",
                    (winner_job_id,),
                ).fetchone()
                loser_row = connection.execute(
                    "SELECT status FROM jobs WHERE id = ?",
                    (loser_job_id,),
                ).fetchone()
                tracking_row = connection.execute(
                    """
                    SELECT created_at
                    FROM application_tracking
                    WHERE job_id = ? AND status = 'applied'
                    ORDER BY created_at DESC, id DESC
                    LIMIT 1
                    """,
                    (winner_job_id,),
                ).fetchone()

            self.assertEqual(winner_row["status"], "applied")
            self.assertEqual(winner_row["applied_at"], tracking_row["created_at"])
            self.assertEqual(loser_row["status"], "superseded")

    def test_cli_db_init_and_status_commands_succeed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            database_path = Path(tmp_dir) / "runtime" / "jobs_ai.db"
            env = {
                "JOBS_AI_DB_BACKEND": "sqlite",
                "JOBS_AI_DB_PATH": str(database_path),
            }

            init_result = RUNNER.invoke(app, ["db", "init"], env=env)
            status_result = RUNNER.invoke(app, ["db", "status"], env=env)

            self.assertEqual(init_result.exit_code, 0)
            self.assertIn("jobs_ai database init", init_result.stdout)
            self.assertIn("python -m jobs_ai import data/raw/sample_job_leads.json", init_result.stdout)
            self.assertEqual(status_result.exit_code, 0)
            self.assertIn("schema: ready", status_result.stdout)
            self.assertIn("python -m jobs_ai import data/raw/sample_job_leads.json", status_result.stdout)

    def test_find_jobs_by_apply_url_returns_exact_matches_in_id_order(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            database_path = Path(tmp_dir) / "runtime" / "jobs_ai.db"
            env = {"JOBS_AI_DB_BACKEND": "sqlite", "JOBS_AI_DB_PATH": str(database_path)}
            apply_url = "https://job-boards.greenhouse.io/qualifiedhealth/jobs/5153317008"
            with patch.dict("os.environ", env, clear=False):
                initialize_schema(database_path)

                with closing(connect_database(database_path)) as connection:
                    first_job_id = insert_job(
                        connection,
                        _job_record(
                            source="manual",
                            company="Qualified Health",
                            title="Data Engineer",
                            location="Remote",
                            apply_url=apply_url,
                            portal_type="greenhouse",
                        ),
                    )
                    second_job_id = insert_job(
                        connection,
                        _job_record(
                            source="manual",
                            company="Qualified Health",
                            title="Senior Data Engineer",
                            location="Remote",
                            apply_url=apply_url,
                            portal_type="greenhouse",
                        ),
                    )
                    connection.execute(
                        "UPDATE jobs SET status = ? WHERE id = ?",
                        ("opened", second_job_id),
                    )
                    connection.commit()

                result = find_jobs_by_apply_url(database_path, apply_url)

            self.assertEqual(result.backend, "sqlite")
            self.assertFalse(result.fallback_triggered)
            self.assertEqual([match.job_id for match in result.matches], [first_job_id, second_job_id])
            self.assertEqual(result.matches[0].company, "Qualified Health")
            self.assertEqual(result.matches[1].title, "Senior Data Engineer")
            self.assertEqual(result.matches[1].status, "opened")
            self.assertEqual(result.matches[1].portal_type, "greenhouse")

    def test_cli_check_url_reports_exact_match_details(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            database_path = Path(tmp_dir) / "runtime" / "jobs_ai.db"
            env = {"JOBS_AI_DB_BACKEND": "sqlite", "JOBS_AI_DB_PATH": str(database_path)}
            apply_url = "https://job-boards.greenhouse.io/qualifiedhealth/jobs/5153317008"
            with patch.dict("os.environ", env, clear=False):
                initialize_schema(database_path)

                with closing(connect_database(database_path)) as connection:
                    job_id = insert_job(
                        connection,
                        _job_record(
                            source="manual",
                            company="Qualified Health",
                            title="Platform Engineer",
                            location="Remote",
                            apply_url=apply_url,
                            portal_type="greenhouse",
                        ),
                    )
                    connection.execute(
                        "UPDATE jobs SET status = ? WHERE id = ?",
                        ("applied", job_id),
                    )
                    connection.commit()

            result = RUNNER.invoke(app, ["check-url", apply_url], env=env)

            self.assertEqual(result.exit_code, 0)
            self.assertIn("jobs_ai check-url", result.stdout)
            self.assertIn("status: exact match found", result.stdout)
            self.assertIn("active backend: sqlite", result.stdout)
            self.assertIn(f"job id: {job_id}", result.stdout)
            self.assertIn("company: Qualified Health", result.stdout)
            self.assertIn("title: Platform Engineer", result.stdout)
            self.assertIn(f"apply_url: {apply_url}", result.stdout)
            self.assertIn("status: applied", result.stdout)
            self.assertIn("portal_type: greenhouse", result.stdout)

    def test_cli_check_url_reports_missing_exact_match(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            database_path = Path(tmp_dir) / "runtime" / "jobs_ai.db"
            env = {"JOBS_AI_DB_BACKEND": "sqlite", "JOBS_AI_DB_PATH": str(database_path)}
            with patch.dict("os.environ", env, clear=False):
                initialize_schema(database_path)

            result = RUNNER.invoke(
                app,
                [
                    "check-url",
                    "https://job-boards.greenhouse.io/example/jobs/0000000000",
                ],
                env=env,
            )

            self.assertEqual(result.exit_code, 0)
            self.assertIn("status: no job found for exact apply_url", result.stdout)
            self.assertIn(
                "apply_url: https://job-boards.greenhouse.io/example/jobs/0000000000",
                result.stdout,
            )

    def test_cli_check_url_without_inspect_remains_short_report(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            database_path = Path(tmp_dir) / "runtime" / "jobs_ai.db"
            env = {"JOBS_AI_DB_BACKEND": "sqlite", "JOBS_AI_DB_PATH": str(database_path)}
            apply_url = "https://job-boards.greenhouse.io/qualifiedhealth/jobs/5153317008"
            with patch.dict("os.environ", env, clear=False):
                initialize_schema(database_path)

                with closing(connect_database(database_path)) as connection:
                    insert_job(
                        connection,
                        _job_record(
                            source="manual",
                            company="Qualified Health",
                            title="Platform Engineer",
                            location="Remote",
                            apply_url=apply_url,
                            portal_type="greenhouse",
                        ),
                    )
                    connection.commit()

            result = RUNNER.invoke(app, ["check-url", apply_url], env=env)

            self.assertEqual(result.exit_code, 0)
            self.assertIn("status: exact match found", result.stdout)
            self.assertNotIn("inspect mode: yes", result.stdout)
            self.assertNotIn("total matches:", result.stdout)
            self.assertNotIn("canonical/preferred row:", result.stdout)

    def test_cli_check_url_inspect_reports_enriched_single_match(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            database_path = Path(tmp_dir) / "runtime" / "jobs_ai.db"
            env = {"JOBS_AI_DB_BACKEND": "sqlite", "JOBS_AI_DB_PATH": str(database_path)}
            apply_url = "https://job-boards.greenhouse.io/qualifiedhealth/jobs/5153317008"
            with patch.dict("os.environ", env, clear=False):
                initialize_schema(database_path)

                with closing(connect_database(database_path)) as connection:
                    job_id = insert_job(
                        connection,
                        _job_record(
                            source="manual",
                            company="Qualified Health",
                            title="Platform Engineer",
                            location="Remote",
                            apply_url=apply_url,
                            portal_type="greenhouse",
                        ),
                    )
                    connection.commit()

            result = RUNNER.invoke(app, ["check-url", apply_url, "--inspect"], env=env)

            self.assertEqual(result.exit_code, 0)
            self.assertIn("status: exact match found (inspect mode)", result.stdout)
            self.assertIn("inspect mode: yes", result.stdout)
            self.assertIn("total matches: 1", result.stdout)
            self.assertIn(f"canonical/preferred row: {job_id}", result.stdout)
            self.assertIn("effective state for this url: new", result.stdout)
            self.assertIn("url already handled: no", result.stdout)
            self.assertIn("preferred row: yes", result.stdout)
            self.assertIn("location: Remote", result.stdout)
            self.assertIn("source: manual", result.stdout)
            self.assertIn(f"canonical_apply_url: {apply_url}", result.stdout)
            self.assertIn(
                "identity_key: greenhouse|qualified health|platform engineer|remote",
                result.stdout,
            )
            self.assertIn("actionable in normal queue/session flow: yes", result.stdout)
            self.assertIn("launchable: yes", result.stdout)
            self.assertIn("warnings: none", result.stdout)
            self.assertIn("recommended resume variant:", result.stdout)
            self.assertIn("recommended profile snippet:", result.stdout)

    def test_cli_check_url_inspect_reports_group_summary_for_multiple_matches(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            database_path = Path(tmp_dir) / "runtime" / "jobs_ai.db"
            env = {"JOBS_AI_DB_BACKEND": "sqlite", "JOBS_AI_DB_PATH": str(database_path)}
            apply_url = "https://job-boards.greenhouse.io/qualifiedhealth/jobs/5153317008"
            with patch.dict("os.environ", env, clear=False):
                initialize_schema(database_path)

                with closing(connect_database(database_path)) as connection:
                    sibling_job_id = insert_job(
                        connection,
                        _job_record(
                            source="manual",
                            company="Qualified Health",
                            title="Data Engineer",
                            location="Remote",
                            apply_url=apply_url,
                            portal_type="greenhouse",
                        ),
                    )
                    preferred_job_id = insert_job(
                        connection,
                        _job_record(
                            source="manual",
                            company="Qualified Health",
                            title="Senior Data Engineer",
                            location="Remote",
                            apply_url=apply_url,
                            portal_type="greenhouse",
                        ),
                    )
                    connection.execute(
                        "UPDATE jobs SET status = ? WHERE id = ?",
                        ("applied", preferred_job_id),
                    )
                    connection.commit()

            result = RUNNER.invoke(app, ["check-url", apply_url, "--inspect"], env=env)

            self.assertEqual(result.exit_code, 0)
            self.assertIn("total matches: 2", result.stdout)
            self.assertIn(f"canonical/preferred row: {preferred_job_id}", result.stdout)
            self.assertIn(f"sibling duplicates: {sibling_job_id}", result.stdout)
            self.assertIn("effective state for this url: applied", result.stdout)
            self.assertIn("url already handled: yes", result.stdout)
            self.assertIn(f"rows already applied: {preferred_job_id}", result.stdout)
            self.assertIn("rows already opened: none", result.stdout)
            self.assertIn("rows already rejected: none", result.stdout)

    def test_cli_check_url_inspect_surfaces_actionable_vs_non_actionable_state(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            database_path = Path(tmp_dir) / "runtime" / "jobs_ai.db"
            env = {"JOBS_AI_DB_BACKEND": "sqlite", "JOBS_AI_DB_PATH": str(database_path)}
            apply_url = "https://job-boards.greenhouse.io/qualifiedhealth/jobs/5153317008"
            with patch.dict("os.environ", env, clear=False):
                initialize_schema(database_path)

                with closing(connect_database(database_path)) as connection:
                    insert_job(
                        connection,
                        _job_record(
                            source="manual",
                            company="Qualified Health",
                            title="Data Engineer",
                            location="Remote",
                            apply_url=apply_url,
                            portal_type="greenhouse",
                        ),
                    )
                    opened_job_id = insert_job(
                        connection,
                        _job_record(
                            source="manual",
                            company="Qualified Health",
                            title="Senior Data Engineer",
                            location="Remote",
                            apply_url=apply_url,
                            portal_type="greenhouse",
                        ),
                    )
                    connection.execute(
                        "UPDATE jobs SET status = ? WHERE id = ?",
                        ("opened", opened_job_id),
                    )
                    connection.commit()

            result = RUNNER.invoke(app, ["check-url", apply_url, "--inspect"], env=env)

            self.assertEqual(result.exit_code, 0)
            self.assertIn("actionable in normal queue/session flow: yes", result.stdout)
            self.assertIn("actionable in normal queue/session flow: no", result.stdout)
            self.assertIn(
                "warnings: not actionable in normal queue/session flow because status = opened",
                result.stdout,
            )
            self.assertIn(f"rows already opened: {opened_job_id}", result.stdout)

    def test_cli_check_url_inspect_shows_recommendation_data_when_available(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            database_path = Path(tmp_dir) / "runtime" / "jobs_ai.db"
            env = {"JOBS_AI_DB_BACKEND": "sqlite", "JOBS_AI_DB_PATH": str(database_path)}
            apply_url = "https://agency.example/jobs/2"
            with patch.dict("os.environ", env, clear=False):
                initialize_schema(database_path)

                with closing(connect_database(database_path)) as connection:
                    insert_job(
                        connection,
                        _job_record(
                            source="staffing recruiter",
                            company="Northwind Talent",
                            title="Senior Data Engineer",
                            location="Remote",
                            raw_payload={"description": "Python BigQuery GCP pipelines"},
                            apply_url=apply_url,
                        ),
                    )
                    connection.commit()

            result = RUNNER.invoke(app, ["check-url", apply_url, "--inspect"], env=env)

            self.assertEqual(result.exit_code, 0)
            self.assertIn(
                "recommended resume variant: data-engineering (Data Engineering Resume)",
                result.stdout,
            )
            self.assertIn(
                "recommended profile snippet: pipeline-delivery (Pipeline Delivery)",
                result.stdout,
            )
            self.assertIn(
                (
                    "recommended profile snippet text: Python-first pipeline delivery "
                    "across SQL warehouses, BigQuery/GCP, and production data systems."
                ),
                result.stdout,
            )


if __name__ == "__main__":
    unittest.main()
