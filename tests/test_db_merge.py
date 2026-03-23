from __future__ import annotations

from contextlib import closing
import json
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest

from typer.testing import CliRunner

SRC_PATH = Path(__file__).resolve().parents[1] / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

from jobs_ai.cli import app
from jobs_ai.db import connect_database, initialize_schema
from jobs_ai.db_merge import merge_sqlite_databases
from jobs_ai.jobs.identity import build_job_identity

RUNNER = CliRunner()

_OLD_SCHEMA_SQL = """
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


def _insert_registry_row(
    connection: sqlite3.Connection,
    *,
    source_url: str,
    normalized_url: str | None = None,
    portal_type: str | None = None,
    company: str | None = None,
    label: str | None = None,
    status: str = "manual_review",
    first_seen_at: str = "2026-03-20T00:00:00Z",
    last_verified_at: str | None = None,
    notes: str | None = None,
    provenance: str | None = None,
    verification_reason_code: str | None = None,
    verification_reason: str | None = None,
    created_at: str | None = None,
    updated_at: str | None = None,
) -> int:
    cursor = connection.execute(
        """
        INSERT INTO source_registry (
            source_url,
            normalized_url,
            portal_type,
            company,
            label,
            status,
            first_seen_at,
            last_verified_at,
            notes,
            provenance,
            verification_reason_code,
            verification_reason,
            created_at,
            updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            source_url,
            normalized_url or source_url,
            portal_type,
            company,
            label,
            status,
            first_seen_at,
            last_verified_at,
            notes,
            provenance,
            verification_reason_code,
            verification_reason,
            created_at or first_seen_at,
            updated_at or created_at or first_seen_at,
        ),
    )
    return int(cursor.lastrowid)


def _insert_job_row(
    connection: sqlite3.Connection,
    *,
    source: str,
    company: str,
    title: str,
    location: str,
    apply_url: str | None = None,
    portal_type: str | None = None,
    source_job_id: str | None = None,
    source_registry_id: int | None = None,
    status: str = "new",
    salary_text: str | None = None,
    posted_at: str | None = None,
    found_at: str = "2026-03-20T00:00:00Z",
    ingest_batch_id: str | None = None,
    source_query: str | None = None,
    import_source: str | None = None,
    raw_json: str | None = None,
    created_at: str | None = None,
    updated_at: str | None = None,
) -> int:
    identity = build_job_identity(
        {
            "source": source,
            "source_job_id": source_job_id,
            "company": company,
            "title": title,
            "location": location,
            "apply_url": apply_url,
            "portal_type": portal_type,
        }
    )
    cursor = connection.execute(
        """
        INSERT INTO jobs (
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
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
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
            identity.canonical_apply_url,
            identity.identity_key,
            status,
            raw_json or json.dumps({"company": company, "title": title}, ensure_ascii=True),
            created_at or found_at,
            updated_at or created_at or found_at,
        ),
    )
    return int(cursor.lastrowid)


def _insert_application_row(
    connection: sqlite3.Connection,
    *,
    job_id: int,
    state: str = "draft",
    resume_variant: str | None = None,
    notes: str | None = None,
    last_attempted_at: str | None = None,
    applied_at: str | None = None,
    created_at: str = "2026-03-20T00:00:00Z",
    updated_at: str | None = None,
) -> int:
    cursor = connection.execute(
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
            job_id,
            state,
            resume_variant,
            notes,
            last_attempted_at,
            applied_at,
            created_at,
            updated_at or created_at,
        ),
    )
    return int(cursor.lastrowid)


def _insert_tracking_row(
    connection: sqlite3.Connection,
    *,
    job_id: int,
    status: str,
    created_at: str = "2026-03-20T00:00:00Z",
) -> int:
    cursor = connection.execute(
        """
        INSERT INTO application_tracking (
            job_id,
            status,
            created_at
        ) VALUES (?, ?, ?)
        """,
        (job_id, status, created_at),
    )
    return int(cursor.lastrowid)


def _insert_session_history_row(
    connection: sqlite3.Connection,
    *,
    manifest_path: str,
    item_count: int,
    launchable_count: int,
    ingest_batch_id: str | None = None,
    source_query: str | None = None,
    created_at: str = "2026-03-20T00:00:00Z",
) -> int:
    cursor = connection.execute(
        """
        INSERT INTO session_history (
            manifest_path,
            item_count,
            launchable_count,
            ingest_batch_id,
            source_query,
            created_at
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            manifest_path,
            item_count,
            launchable_count,
            ingest_batch_id,
            source_query,
            created_at,
        ),
    )
    return int(cursor.lastrowid)


def _create_old_schema_source_database(database_path: Path) -> None:
    database_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(database_path) as connection:
        connection.executescript(_OLD_SCHEMA_SQL)
        cursor = connection.execute(
            """
            INSERT INTO jobs (
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
                status,
                raw_json,
                created_at,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "manual",
                "gh-12345",
                "Acme Data",
                "Analytics Engineer",
                "Remote",
                "https://boards.greenhouse.io/acme?gh_jid=12345&utm_source=laptop",
                None,
                None,
                "2026-03-14",
                "2026-03-16T10:00:00Z",
                "opened",
                json.dumps({"source": "old-schema"}, ensure_ascii=True),
                "2026-03-16T10:00:00Z",
                "2026-03-16T10:00:00Z",
            ),
        )
        job_id = int(cursor.lastrowid)
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
                job_id,
                "submitted",
                "analytics_v1",
                "old schema laptop note",
                "2026-03-16T10:15:00Z",
                None,
                "2026-03-16T10:15:00Z",
                "2026-03-16T10:15:00Z",
            ),
        )
        connection.execute(
            """
            INSERT INTO application_tracking (
                job_id,
                status,
                created_at
            ) VALUES (?, ?, ?)
            """,
            (job_id, "opened", "2026-03-16T10:20:00Z"),
        )
        connection.commit()


class DatabaseMergeTest(unittest.TestCase):
    def test_merge_resolves_duplicate_jobs_and_remaps_child_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            target_path = tmp_path / "server.db"
            source_path = tmp_path / "laptop.db"
            initialize_schema(target_path)
            initialize_schema(source_path)

            with closing(connect_database(target_path)) as target_connection:
                target_registry_id = _insert_registry_row(
                    target_connection,
                    source_url="https://boards.greenhouse.io/acme",
                    portal_type="greenhouse",
                    company="Acme Data",
                    status="active",
                    first_seen_at="2026-03-10T08:00:00Z",
                )
                target_job_id = _insert_job_row(
                    target_connection,
                    source="manual",
                    source_job_id="gh-12345",
                    company="Acme Data",
                    title="Analytics Engineer",
                    location="Remote",
                    apply_url="https://boards.greenhouse.io/acme/jobs/12345",
                    portal_type="greenhouse",
                    source_registry_id=target_registry_id,
                    status="new",
                    found_at="2026-03-10T08:00:00Z",
                )
                target_connection.commit()

            with closing(connect_database(source_path)) as source_connection:
                source_registry_id = _insert_registry_row(
                    source_connection,
                    source_url="https://boards.greenhouse.io/acme",
                    portal_type="greenhouse",
                    company="Acme Data",
                    status="active",
                    first_seen_at="2026-03-12T08:00:00Z",
                )
                source_job_id = _insert_job_row(
                    source_connection,
                    source="manual",
                    source_job_id="gh-12345",
                    company="Acme Data",
                    title="Analytics Engineer",
                    location="Remote",
                    apply_url="https://boards.greenhouse.io/acme?gh_jid=12345&utm_source=laptop",
                    portal_type="greenhouse",
                    source_registry_id=source_registry_id,
                    status="opened",
                    found_at="2026-03-12T08:00:00Z",
                )
                _insert_application_row(
                    source_connection,
                    job_id=source_job_id,
                    state="draft",
                    resume_variant="analytics_v2",
                    notes="macbook follow-up",
                    created_at="2026-03-12T08:15:00Z",
                )
                _insert_tracking_row(
                    source_connection,
                    job_id=source_job_id,
                    status="applied",
                    created_at="2026-03-12T08:30:00Z",
                )
                source_connection.commit()

            result = merge_sqlite_databases(target_path, source_path)

            self.assertEqual(result.jobs.inserted_count, 0)
            self.assertEqual(result.jobs.matched_count, 1)
            self.assertEqual(
                {entry.rule: entry.count for entry in result.jobs.rule_counts},
                {"canonical apply_url match": 1},
            )

            with closing(connect_database(target_path)) as connection:
                job_count = connection.execute(
                    "SELECT COUNT(*) AS count FROM jobs"
                ).fetchone()
                application_rows = connection.execute(
                    "SELECT job_id, resume_variant, notes FROM applications"
                ).fetchall()
                tracking_rows = connection.execute(
                    "SELECT job_id, status FROM application_tracking"
                ).fetchall()
                merged_job_row = connection.execute(
                    "SELECT id, status, source_registry_id FROM jobs WHERE id = ?",
                    (target_job_id,),
                ).fetchone()

            self.assertEqual(job_count["count"], 1)
            self.assertEqual(len(application_rows), 1)
            self.assertEqual(int(application_rows[0]["job_id"]), target_job_id)
            self.assertEqual(application_rows[0]["resume_variant"], "analytics_v2")
            self.assertEqual(len(tracking_rows), 1)
            self.assertEqual(int(tracking_rows[0]["job_id"]), target_job_id)
            self.assertEqual(tracking_rows[0]["status"], "applied")
            self.assertEqual(merged_job_row["status"], "applied")
            self.assertEqual(int(merged_job_row["source_registry_id"]), target_registry_id)

    def test_merge_updates_source_registry_by_normalized_url_and_remaps_new_jobs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            target_path = tmp_path / "server.db"
            source_path = tmp_path / "laptop.db"
            initialize_schema(target_path)
            initialize_schema(source_path)

            with closing(connect_database(target_path)) as target_connection:
                target_registry_id = _insert_registry_row(
                    target_connection,
                    source_url="https://jobs.lever.co/acme",
                    portal_type="lever",
                    company=None,
                    status="manual_review",
                    notes="server note",
                    provenance="server",
                    first_seen_at="2026-03-10T08:00:00Z",
                    last_verified_at="2026-03-10T08:00:00Z",
                    verification_reason_code="manual_review",
                    verification_reason="needs verification",
                )
                target_connection.commit()

            with closing(connect_database(source_path)) as source_connection:
                source_registry_id = _insert_registry_row(
                    source_connection,
                    source_url="https://jobs.lever.co/acme",
                    portal_type="lever",
                    company="Acme Data",
                    label="Acme",
                    status="active",
                    notes="laptop note",
                    provenance="laptop",
                    first_seen_at="2026-03-09T08:00:00Z",
                    last_verified_at="2026-03-12T08:00:00Z",
                    verification_reason_code="collected",
                    verification_reason="verified from laptop",
                )
                _insert_job_row(
                    source_connection,
                    source="manual",
                    source_job_id="lever-999",
                    company="Acme Data",
                    title="Platform Analytics Engineer",
                    location="Remote",
                    apply_url="https://jobs.lever.co/acme/999",
                    portal_type="lever",
                    source_registry_id=source_registry_id,
                    status="new",
                    found_at="2026-03-12T09:00:00Z",
                )
                source_connection.commit()

            result = merge_sqlite_databases(target_path, source_path)

            self.assertEqual(result.source_registry.inserted_count, 0)
            self.assertEqual(result.source_registry.updated_count, 1)
            self.assertEqual(result.jobs.inserted_count, 1)

            with closing(connect_database(target_path)) as connection:
                registry_rows = connection.execute(
                    """
                    SELECT
                        id,
                        company,
                        label,
                        status,
                        notes,
                        provenance,
                        first_seen_at,
                        last_verified_at,
                        verification_reason_code
                    FROM source_registry
                    """
                ).fetchall()
                job_row = connection.execute(
                    "SELECT source_registry_id FROM jobs WHERE source_job_id = 'lever-999'"
                ).fetchone()

            self.assertEqual(len(registry_rows), 1)
            self.assertEqual(int(registry_rows[0]["id"]), target_registry_id)
            self.assertEqual(registry_rows[0]["company"], "Acme Data")
            self.assertEqual(registry_rows[0]["label"], "Acme")
            self.assertEqual(registry_rows[0]["status"], "active")
            self.assertEqual(
                registry_rows[0]["notes"],
                "server note\nlaptop note",
            )
            self.assertEqual(
                registry_rows[0]["provenance"],
                "server\nlaptop",
            )
            self.assertEqual(registry_rows[0]["first_seen_at"], "2026-03-09T08:00:00Z")
            self.assertEqual(registry_rows[0]["last_verified_at"], "2026-03-12T08:00:00Z")
            self.assertEqual(registry_rows[0]["verification_reason_code"], "collected")
            self.assertEqual(int(job_row["source_registry_id"]), target_registry_id)

    def test_merge_dry_run_does_not_modify_target(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            target_path = tmp_path / "server.db"
            source_path = tmp_path / "laptop.db"
            initialize_schema(target_path)
            initialize_schema(source_path)

            with closing(connect_database(target_path)) as target_connection:
                _insert_job_row(
                    target_connection,
                    source="manual",
                    source_job_id="server-1",
                    company="Server Only",
                    title="Data Engineer",
                    location="Remote",
                    apply_url="https://example.com/server-only",
                    status="new",
                )
                target_connection.commit()

            with closing(connect_database(source_path)) as source_connection:
                _insert_job_row(
                    source_connection,
                    source="manual",
                    source_job_id="laptop-1",
                    company="Laptop Only",
                    title="Analytics Engineer",
                    location="Remote",
                    apply_url="https://example.com/laptop-only",
                    status="new",
                )
                source_connection.commit()

            result = merge_sqlite_databases(target_path, source_path, dry_run=True)

            self.assertTrue(result.dry_run)
            self.assertEqual(result.jobs.inserted_count, 1)

            with closing(connect_database(target_path)) as connection:
                rows = connection.execute(
                    "SELECT company FROM jobs ORDER BY id"
                ).fetchall()

            self.assertEqual([row["company"] for row in rows], ["Server Only"])

    def test_merge_backfills_old_source_schema_before_merging(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            target_path = tmp_path / "server.db"
            source_path = tmp_path / "old-laptop.db"
            initialize_schema(target_path)
            _create_old_schema_source_database(source_path)

            result = merge_sqlite_databases(target_path, source_path)

            self.assertEqual(result.jobs.inserted_count, 1)
            self.assertIn("session_history", result.source_schema_before.missing_tables)
            self.assertIn("source_registry", result.source_schema_before.missing_tables)
            self.assertIn("canonical_apply_url", result.source_schema_before.missing_job_columns)
            self.assertIn("identity_key", result.source_schema_before.missing_job_columns)

            with closing(connect_database(target_path)) as target_connection:
                target_row = target_connection.execute(
                    """
                    SELECT
                        canonical_apply_url,
                        identity_key,
                        status
                    FROM jobs
                    LIMIT 1
                    """
                ).fetchone()

            with closing(connect_database(source_path)) as source_connection:
                source_columns = {
                    row["name"]
                    for row in source_connection.execute("PRAGMA table_info(jobs)").fetchall()
                }
                source_tables = {
                    row["name"]
                    for row in source_connection.execute(
                        "SELECT name FROM sqlite_master WHERE type = 'table'"
                    ).fetchall()
                }

            self.assertEqual(
                target_row["canonical_apply_url"],
                "https://boards.greenhouse.io/acme/jobs/12345",
            )
            self.assertTrue(target_row["identity_key"])
            self.assertEqual(target_row["status"], "opened")
            self.assertTrue(
                {
                    "ingest_batch_id",
                    "source_query",
                    "import_source",
                    "source_registry_id",
                    "canonical_apply_url",
                    "identity_key",
                }.issubset(source_columns)
            )
            self.assertIn("session_history", source_tables)
            self.assertIn("source_registry", source_tables)

    def test_cli_db_merge_skips_identical_child_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            target_path = tmp_path / "server.db"
            source_path = tmp_path / "laptop.db"
            initialize_schema(target_path)
            initialize_schema(source_path)

            with closing(connect_database(target_path)) as target_connection:
                target_job_id = _insert_job_row(
                    target_connection,
                    source="manual",
                    source_job_id="shared-1",
                    company="Shared Co",
                    title="Data Engineer",
                    location="Remote",
                    apply_url="https://example.com/shared-job",
                    status="opened",
                    found_at="2026-03-10T08:00:00Z",
                )
                _insert_application_row(
                    target_connection,
                    job_id=target_job_id,
                    state="draft",
                    resume_variant="core_v1",
                    notes="same note",
                    created_at="2026-03-11T08:00:00Z",
                )
                _insert_tracking_row(
                    target_connection,
                    job_id=target_job_id,
                    status="opened",
                    created_at="2026-03-11T09:00:00Z",
                )
                _insert_session_history_row(
                    target_connection,
                    manifest_path="/tmp/shared-manifest.json",
                    item_count=3,
                    launchable_count=2,
                    ingest_batch_id="batch-1",
                    source_query="data engineer remote",
                    created_at="2026-03-11T10:00:00Z",
                )
                target_connection.commit()

            with closing(connect_database(source_path)) as source_connection:
                source_job_id = _insert_job_row(
                    source_connection,
                    source="manual",
                    source_job_id="shared-1",
                    company="Shared Co",
                    title="Data Engineer",
                    location="Remote",
                    apply_url="https://example.com/shared-job",
                    status="opened",
                    found_at="2026-03-12T08:00:00Z",
                )
                _insert_application_row(
                    source_connection,
                    job_id=source_job_id,
                    state="draft",
                    resume_variant="core_v1",
                    notes="same note",
                    created_at="2026-03-11T08:00:00Z",
                )
                _insert_tracking_row(
                    source_connection,
                    job_id=source_job_id,
                    status="opened",
                    created_at="2026-03-11T09:00:00Z",
                )
                _insert_session_history_row(
                    source_connection,
                    manifest_path="/tmp/shared-manifest.json",
                    item_count=3,
                    launchable_count=2,
                    ingest_batch_id="batch-1",
                    source_query="data engineer remote",
                    created_at="2026-03-11T10:00:00Z",
                )
                source_connection.commit()

            env = {"JOBS_AI_DB_PATH": str(target_path)}
            result = RUNNER.invoke(app, ["db", "merge", str(source_path)], env=env)

            self.assertEqual(result.exit_code, 0)
            self.assertIn("jobs_ai database merge", result.stdout)
            self.assertIn("jobs matched to existing: 1", result.stdout)
            self.assertIn("applications skipped as duplicates: 1", result.stdout)
            self.assertIn("application tracking skipped as duplicates: 1", result.stdout)
            self.assertIn("session history skipped as duplicates: 1", result.stdout)

            with closing(connect_database(target_path)) as connection:
                application_count = connection.execute(
                    "SELECT COUNT(*) AS count FROM applications"
                ).fetchone()
                tracking_count = connection.execute(
                    "SELECT COUNT(*) AS count FROM application_tracking"
                ).fetchone()
                session_count = connection.execute(
                    "SELECT COUNT(*) AS count FROM session_history"
                ).fetchone()

            self.assertEqual(application_count["count"], 1)
            self.assertEqual(tracking_count["count"], 1)
            self.assertEqual(session_count["count"], 1)


if __name__ == "__main__":
    unittest.main()
