from __future__ import annotations

from contextlib import closing
from pathlib import Path
import sys
import tempfile
import unittest

from typer.testing import CliRunner

SRC_PATH = Path(__file__).resolve().parents[1] / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

from jobs_ai.cli import app
from jobs_ai.db import connect_database, initialize_schema, schema_exists

RUNNER = CliRunner()


class DatabaseTest(unittest.TestCase):
    def test_initialize_schema_creates_database_required_tables_and_indexes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            database_path = Path(tmp_dir) / "data" / "jobs_ai.db"

            initialize_schema(database_path)

            self.assertTrue(database_path.exists())
            self.assertTrue(schema_exists(database_path))
            with closing(connect_database(database_path)) as connection:
                table_rows = connection.execute(
                    """
                    SELECT name
                    FROM sqlite_master
                    WHERE type = 'table'
                      AND name IN ('jobs', 'applications', 'application_tracking')
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
                          'idx_applications_job_id',
                          'idx_application_tracking_job_id'
                      )
                    """
                ).fetchall()
            self.assertEqual(
                {row["name"] for row in table_rows},
                {"jobs", "applications", "application_tracking"},
            )
            self.assertEqual(
                {row["name"] for row in index_rows},
                {
                    "idx_jobs_apply_url",
                    "idx_jobs_source_company_title_location",
                    "idx_applications_job_id",
                    "idx_application_tracking_job_id",
                },
            )

    def test_cli_db_init_and_status_commands_succeed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            database_path = Path(tmp_dir) / "runtime" / "jobs_ai.db"
            env = {"JOBS_AI_DB_PATH": str(database_path)}

            init_result = RUNNER.invoke(app, ["db", "init"], env=env)
            status_result = RUNNER.invoke(app, ["db", "status"], env=env)

            self.assertEqual(init_result.exit_code, 0)
            self.assertIn("jobs_ai database init", init_result.stdout)
            self.assertIn("python -m jobs_ai import data/raw/sample_job_leads.json", init_result.stdout)
            self.assertEqual(status_result.exit_code, 0)
            self.assertIn("schema: ready", status_result.stdout)
            self.assertIn("python -m jobs_ai import data/raw/sample_job_leads.json", status_result.stdout)


if __name__ == "__main__":
    unittest.main()
