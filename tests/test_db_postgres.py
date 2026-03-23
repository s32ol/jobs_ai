from __future__ import annotations

from contextlib import closing
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

SRC_PATH = Path(__file__).resolve().parents[1] / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

from jobs_ai.config import POSTGRES_SQLITE_FALLBACK_WARNING, load_settings
from jobs_ai.db import initialize_schema, initialize_schema_connection
from jobs_ai.db_postgres import (
    BackendStatusResult,
    DatabasePingResult,
    _sync_identity_sequences,
    is_empty_postgres_target,
    migrate_sqlite_to_postgres,
)
from jobs_ai.db_runtime import connect_sqlite_database
from jobs_ai.main import render_db_backend_status_report, render_db_ping_report


class DatabasePostgresConfigTest(unittest.TestCase):
    def test_load_settings_supports_postgres_backend_and_pg_parts(self) -> None:
        settings = load_settings(
            {
                "JOBS_AI_DB_BACKEND": "postgres",
                "JOBS_AI_SQLITE_PATH": "runtime/jobs_ai.db",
                "PGHOST": "ep-demo.us-east-2.aws.neon.tech",
                "PGPORT": "5432",
                "PGDATABASE": "neondb",
                "PGUSER": "neondb_owner",
                "PGPASSWORD": "secret-value",
                "PGSSLMODE": "require",
            }
        )

        self.assertEqual(settings.database_backend, "postgres")
        self.assertEqual(settings.database_backend_source, "env")
        self.assertFalse(settings.database_fallback_triggered)
        self.assertIsNone(settings.database_warning)
        self.assertEqual(settings.database_path, Path("runtime/jobs_ai.db"))
        self.assertEqual(
            settings.database_url,
            "postgresql://neondb_owner:secret-value@ep-demo.us-east-2.aws.neon.tech:5432/neondb?sslmode=require",
        )

    def test_load_settings_defaults_to_postgres_when_database_url_exists(self) -> None:
        settings = load_settings(
            {
                "JOBS_AI_SQLITE_PATH": "runtime/jobs_ai.db",
                "DATABASE_URL": "postgresql://demo:secret@example.neon.tech/neondb?sslmode=require",
            }
        )

        self.assertEqual(settings.database_backend, "postgres")
        self.assertEqual(settings.database_backend_source, "default")
        self.assertFalse(settings.database_fallback_triggered)
        self.assertIsNone(settings.database_warning)

    def test_load_settings_falls_back_to_sqlite_when_database_url_missing(self) -> None:
        settings = load_settings(
            {
                "JOBS_AI_SQLITE_PATH": "runtime/jobs_ai.db",
            }
        )

        self.assertEqual(settings.database_backend, "sqlite")
        self.assertEqual(settings.database_backend_source, "default")
        self.assertTrue(settings.database_fallback_triggered)
        self.assertEqual(settings.database_warning, POSTGRES_SQLITE_FALLBACK_WARNING)

    def test_load_settings_respects_explicit_sqlite_override(self) -> None:
        settings = load_settings(
            {
                "JOBS_AI_DB_BACKEND": "sqlite",
                "DATABASE_URL": "postgresql://demo:secret@example.neon.tech/neondb?sslmode=require",
            }
        )

        self.assertEqual(settings.database_backend, "sqlite")
        self.assertEqual(settings.database_backend_source, "env")
        self.assertFalse(settings.database_fallback_triggered)
        self.assertIsNone(settings.database_warning)

    def test_load_settings_falls_back_to_sqlite_for_explicit_postgres_without_database_url(self) -> None:
        settings = load_settings(
            {
                "JOBS_AI_DB_BACKEND": "postgres",
                "JOBS_AI_SQLITE_PATH": "runtime/jobs_ai.db",
            }
        )

        self.assertEqual(settings.database_backend, "sqlite")
        self.assertEqual(settings.database_backend_source, "env")
        self.assertTrue(settings.database_fallback_triggered)
        self.assertEqual(settings.database_warning, POSTGRES_SQLITE_FALLBACK_WARNING)

    def test_load_settings_falls_back_to_sqlite_for_invalid_postgres_url(self) -> None:
        settings = load_settings(
            {
                "DATABASE_URL": "not-a-postgres-url",
                "JOBS_AI_SQLITE_PATH": "runtime/jobs_ai.db",
            }
        )

        self.assertEqual(settings.database_backend, "sqlite")
        self.assertEqual(settings.database_backend_source, "default")
        self.assertTrue(settings.database_fallback_triggered)
        self.assertEqual(settings.database_warning, POSTGRES_SQLITE_FALLBACK_WARNING)
        self.assertIsNone(settings.database_url)


class DatabasePostgresSchemaTest(unittest.TestCase):
    def test_initialize_schema_connection_emits_postgres_schema_statements(self) -> None:
        connection = _RecordingPostgresConnection()

        initialize_schema_connection(connection, backfill_identity=False)

        executed_sql = "\n".join(connection.executed_sql)
        self.assertIn("CREATE TABLE IF NOT EXISTS source_registry", executed_sql)
        self.assertIn("CREATE TABLE IF NOT EXISTS jobs", executed_sql)
        self.assertIn(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_source_registry_normalized_url",
            executed_sql,
        )
        self.assertIn("ALTER TABLE jobs ADD COLUMN ingest_batch_id TEXT", executed_sql)

    def test_initialize_schema_connection_can_skip_postgres_secondary_indexes(self) -> None:
        connection = _RecordingPostgresConnection()

        initialize_schema_connection(
            connection,
            backfill_identity=False,
            include_secondary_indexes=False,
        )

        executed_sql = "\n".join(connection.executed_sql)
        self.assertIn("CREATE TABLE IF NOT EXISTS source_registry", executed_sql)
        self.assertNotIn(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_source_registry_normalized_url",
            executed_sql,
        )


class DatabasePostgresMigrationTest(unittest.TestCase):
    def test_migrate_sqlite_to_postgres_uses_fast_path_for_empty_target(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            source_path = Path(tmp_dir) / "source.db"
            target_path = Path(tmp_dir) / "target.db"
            _create_source_database(source_path)

            with patch(
                "jobs_ai.db_postgres.connect_database",
                side_effect=lambda *_args, **_kwargs: connect_sqlite_database(target_path),
            ):
                result = migrate_sqlite_to_postgres(
                    source_path,
                    database_url="postgresql://demo:secret@example.neon.tech/neondb?sslmode=require",
                )

            self.assertTrue(result.fast_path_used)
            self.assertEqual(result.source_registry.inserted_count, 1)
            self.assertEqual(result.jobs.inserted_count, 2)
            self.assertEqual(result.applications.inserted_count, 1)
            self.assertEqual(result.application_tracking.inserted_count, 1)
            self.assertEqual(result.session_history.inserted_count, 1)

            with closing(connect_sqlite_database(target_path)) as connection:
                job_rows = connection.execute(
                    """
                    SELECT id, source_registry_id, company, title
                    FROM jobs
                    ORDER BY id
                    """
                ).fetchall()
                application_row = connection.execute(
                    "SELECT id, job_id, state FROM applications ORDER BY id LIMIT 1"
                ).fetchone()
                tracking_row = connection.execute(
                    "SELECT id, job_id, status FROM application_tracking ORDER BY id LIMIT 1"
                ).fetchone()
                session_row = connection.execute(
                    "SELECT id, manifest_path FROM session_history ORDER BY id LIMIT 1"
                ).fetchone()

            self.assertEqual([int(row["id"]) for row in job_rows], [101, 102])
            self.assertEqual(int(job_rows[0]["source_registry_id"]), 1)
            self.assertIsNone(job_rows[1]["source_registry_id"])
            self.assertEqual(int(application_row["id"]), 301)
            self.assertEqual(int(application_row["job_id"]), 101)
            self.assertEqual(application_row["state"], "submitted")
            self.assertEqual(int(tracking_row["id"]), 401)
            self.assertEqual(int(tracking_row["job_id"]), 101)
            self.assertEqual(tracking_row["status"], "applied")
            self.assertEqual(int(session_row["id"]), 501)
            self.assertEqual(session_row["manifest_path"], "sessions/session-001.json")

    def test_migrate_sqlite_to_postgres_preserves_children_and_is_rerunnable(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            source_path = Path(tmp_dir) / "source.db"
            target_path = Path(tmp_dir) / "target.db"
            _create_source_database(source_path)
            _create_target_database_with_duplicates(target_path)

            with patch(
                "jobs_ai.db_postgres.connect_database",
                side_effect=lambda *_args, **_kwargs: connect_sqlite_database(target_path),
            ):
                first_result = migrate_sqlite_to_postgres(
                    source_path,
                    database_url="postgresql://demo:secret@example.neon.tech/neondb?sslmode=require",
                )
                second_result = migrate_sqlite_to_postgres(
                    source_path,
                    database_url="postgresql://demo:secret@example.neon.tech/neondb?sslmode=require",
                )

            self.assertFalse(first_result.fast_path_used)
            self.assertEqual(first_result.source_registry.matched_count, 1)
            self.assertEqual(first_result.jobs.matched_count, 1)
            self.assertEqual(first_result.jobs.inserted_count, 1)
            self.assertEqual(first_result.applications.inserted_count, 1)
            self.assertEqual(first_result.application_tracking.inserted_count, 1)
            self.assertEqual(first_result.session_history.inserted_count, 1)

            with closing(connect_sqlite_database(target_path)) as connection:
                job_rows = connection.execute(
                    """
                    SELECT id, company, title, status, source_registry_id
                    FROM jobs
                    ORDER BY id
                    """
                ).fetchall()
                application_row = connection.execute(
                    "SELECT id, job_id, state FROM applications ORDER BY id LIMIT 1"
                ).fetchone()
                tracking_row = connection.execute(
                    "SELECT id, job_id, status FROM application_tracking ORDER BY id LIMIT 1"
                ).fetchone()
                session_row = connection.execute(
                    "SELECT id, manifest_path FROM session_history ORDER BY id LIMIT 1"
                ).fetchone()

            self.assertEqual(len(job_rows), 2)
            jobs_by_id = {int(row["id"]): row for row in job_rows}
            self.assertEqual(jobs_by_id[9001]["status"], "applied")
            self.assertEqual(int(jobs_by_id[9001]["source_registry_id"]), 8001)
            self.assertEqual(jobs_by_id[102]["title"], "Analytics Engineer")
            self.assertEqual(int(application_row["job_id"]), 9001)
            self.assertEqual(application_row["state"], "submitted")
            self.assertEqual(int(tracking_row["job_id"]), 9001)
            self.assertEqual(tracking_row["status"], "applied")
            self.assertEqual(session_row["manifest_path"], "sessions/session-001.json")

            self.assertEqual(second_result.source_registry.inserted_count, 0)
            self.assertEqual(second_result.jobs.inserted_count, 0)
            self.assertEqual(second_result.applications.inserted_count, 0)
            self.assertEqual(second_result.application_tracking.inserted_count, 0)
            self.assertEqual(second_result.session_history.inserted_count, 0)
            self.assertGreaterEqual(second_result.jobs.unchanged_count, 2)
            self.assertFalse(second_result.fast_path_used)


class DatabasePostgresHelperTest(unittest.TestCase):
    def test_is_empty_postgres_target_checks_required_tables(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            database_path = Path(tmp_dir) / "target.db"
            _initialize_sqlite_schema(database_path)

            with closing(connect_sqlite_database(database_path)) as connection:
                self.assertTrue(is_empty_postgres_target(connection))
                connection.execute(
                    """
                    INSERT INTO session_history (
                        id,
                        manifest_path,
                        item_count,
                        launchable_count,
                        ingest_batch_id,
                        source_query,
                        created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        1,
                        "sessions/session-001.json",
                        1,
                        1,
                        "batch-001",
                        "analytics engineer",
                        "2026-03-14T08:00:00Z",
                    ),
                )
                self.assertFalse(is_empty_postgres_target(connection))

    def test_sync_identity_sequences_uses_max_ids_for_required_tables(self) -> None:
        connection = _SequenceRecordingPostgresConnection(
            {
                "jobs": 102,
                "applications": 301,
                "application_tracking": 401,
                "session_history": 0,
                "source_registry": 1,
            }
        )

        _sync_identity_sequences(connection)

        self.assertEqual(
            connection.setval_calls,
            [
                ("jobs", 102, True),
                ("applications", 301, True),
                ("application_tracking", 401, True),
                ("session_history", 1, False),
                ("source_registry", 1, True),
            ],
        )


class DatabaseBackendReportingTest(unittest.TestCase):
    def test_render_db_backend_status_report_shows_source_and_fallback(self) -> None:
        report = render_db_backend_status_report(
            BackendStatusResult(
                backend="sqlite",
                backend_source="default",
                fallback_triggered=True,
                warning=POSTGRES_SQLITE_FALLBACK_WARNING,
                target_label="runtime/jobs_ai.db",
                sqlite_path=Path("runtime/jobs_ai.db"),
                database_url_configured=False,
                reachable=True,
                missing_tables=(),
                message="schema ready",
            )
        )

        self.assertIn("active backend: sqlite", report)
        self.assertIn("backend source: default", report)
        self.assertIn("fallback triggered: yes", report)
        self.assertIn(f"warning: {POSTGRES_SQLITE_FALLBACK_WARNING}", report)

    def test_render_db_ping_report_shows_source_and_fallback(self) -> None:
        report = render_db_ping_report(
            DatabasePingResult(
                backend="postgres",
                backend_source="default",
                fallback_triggered=False,
                warning=None,
                target_label="postgresql://demo@example.neon.tech/neondb",
                ok=True,
                message="PostgreSQL 16.8",
            )
        )

        self.assertIn("active backend: postgres", report)
        self.assertIn("backend source: default", report)
        self.assertIn("fallback triggered: no", report)


class _RecordingCursor:
    def __init__(self, rows: list[dict[str, object]] | None = None) -> None:
        self._rows = list(rows or [])

    def fetchone(self):
        if not self._rows:
            return None
        return self._rows[0]

    def fetchall(self):
        return list(self._rows)


class _RecordingPostgresConnection:
    backend_name = "postgres"

    def __init__(self) -> None:
        self.executed_sql: list[str] = []

    def execute(self, query: str, params=()):
        del params
        self.executed_sql.append(query.strip())
        if "information_schema.columns" in query:
            return _RecordingCursor([])
        return _RecordingCursor([])


class _SequenceRecordingPostgresConnection:
    backend_name = "postgres"

    def __init__(self, max_ids: dict[str, int]) -> None:
        self.max_ids = dict(max_ids)
        self.setval_calls: list[tuple[str, int, bool]] = []

    def execute(self, query: str, params=()):
        normalized_query = " ".join(query.split())
        if normalized_query.startswith("SELECT COALESCE(MAX(id), 0) AS max_id FROM "):
            table_name = normalized_query.rsplit(" ", 1)[-1]
            return _RecordingCursor([{"max_id": self.max_ids[table_name]}])
        if "SELECT setval(" in normalized_query:
            table_name = normalized_query.split("pg_get_serial_sequence('", 1)[1].split("'", 1)[0]
            value = int(params[0])
            is_called = "true" in normalized_query.lower().split("pg_get_serial_sequence", 1)[1]
            self.setval_calls.append((table_name, value, is_called))
            return _RecordingCursor([{"setval": value}])
        return _RecordingCursor([])


def _create_source_database(database_path: Path) -> None:
    _initialize_sqlite_schema(database_path)
    with closing(connect_sqlite_database(database_path)) as connection:
        connection.execute(
            """
            INSERT INTO source_registry (
                id,
                source_url,
                normalized_url,
                portal_type,
                company,
                label,
                status,
                first_seen_at,
                created_at,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                1,
                "https://boards.greenhouse.io/acme",
                "https://boards.greenhouse.io/acme",
                "greenhouse",
                "Acme Data",
                "Acme",
                "active",
                "2026-03-10T08:00:00Z",
                "2026-03-10T08:00:00Z",
                "2026-03-12T08:00:00Z",
            ),
        )
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
                101,
                "greenhouse",
                "job-101",
                "Acme Data",
                "Senior Data Engineer",
                "Remote",
                "https://boards.greenhouse.io/acme/jobs/101",
                "greenhouse",
                "$175k",
                "2026-03-11",
                "2026-03-12T08:00:00Z",
                "import-001",
                "data engineer remote",
                "seed.json",
                1,
                "https://boards.greenhouse.io/acme/jobs/101",
                "greenhouse|job_id|job-101",
                "applied",
                '{"id": 101}',
                "2026-03-12T08:00:00Z",
                "2026-03-14T08:00:00Z",
            ),
        )
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
                102,
                "manual",
                None,
                "Northwind",
                "Analytics Engineer",
                "Remote",
                "https://jobs.example.com/northwind/analytics-engineer",
                None,
                None,
                "2026-03-13",
                "2026-03-13T08:00:00Z",
                "import-001",
                "analytics engineer remote",
                "seed.json",
                None,
                "https://jobs.example.com/northwind/analytics-engineer",
                "jobs.example.com|northwind|analytics engineer|remote",
                "new",
                '{"id": 102}',
                "2026-03-13T08:00:00Z",
                "2026-03-13T08:00:00Z",
            ),
        )
        connection.execute(
            """
            INSERT INTO applications (
                id,
                job_id,
                state,
                resume_variant,
                notes,
                last_attempted_at,
                applied_at,
                created_at,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                301,
                101,
                "submitted",
                "data-engineering",
                "manual submit",
                "2026-03-14T08:00:00Z",
                "2026-03-14T08:00:00Z",
                "2026-03-14T08:00:00Z",
                "2026-03-14T08:00:00Z",
            ),
        )
        connection.execute(
            """
            INSERT INTO application_tracking (
                id,
                job_id,
                status,
                created_at
            ) VALUES (?, ?, ?, ?)
            """,
            (
                401,
                101,
                "applied",
                "2026-03-14T08:00:00Z",
            ),
        )
        connection.execute(
            """
            INSERT INTO session_history (
                id,
                manifest_path,
                item_count,
                launchable_count,
                ingest_batch_id,
                source_query,
                created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                501,
                "sessions/session-001.json",
                2,
                2,
                "import-001",
                "data engineer remote",
                "2026-03-14T08:00:00Z",
            ),
        )
        connection.commit()


def _create_target_database_with_duplicates(database_path: Path) -> None:
    _initialize_sqlite_schema(database_path)
    with closing(connect_sqlite_database(database_path)) as connection:
        connection.execute(
            """
            INSERT INTO source_registry (
                id,
                source_url,
                normalized_url,
                portal_type,
                company,
                label,
                status,
                first_seen_at,
                created_at,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                8001,
                "https://boards.greenhouse.io/acme",
                "https://boards.greenhouse.io/acme",
                "greenhouse",
                "Acme Data",
                "Acme",
                "manual_review",
                "2026-03-01T08:00:00Z",
                "2026-03-01T08:00:00Z",
                "2026-03-01T08:00:00Z",
            ),
        )
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
                9001,
                "greenhouse",
                "job-101",
                "Acme Data",
                "Senior Data Engineer",
                "Remote",
                "https://boards.greenhouse.io/acme/jobs/101",
                "greenhouse",
                None,
                None,
                "2026-03-10T08:00:00Z",
                None,
                None,
                None,
                8001,
                "https://boards.greenhouse.io/acme/jobs/101",
                "greenhouse|job_id|job-101",
                "new",
                '{"id": 9001}',
                "2026-03-10T08:00:00Z",
                "2026-03-10T08:00:00Z",
            ),
        )
        connection.commit()


def _initialize_sqlite_schema(database_path: Path) -> None:
    with patch.dict("os.environ", {"JOBS_AI_DB_BACKEND": "sqlite"}, clear=False):
        initialize_schema(database_path)


if __name__ == "__main__":
    unittest.main()
