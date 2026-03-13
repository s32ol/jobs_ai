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

from jobs_ai.cli import app
from jobs_ai.db import connect_database
from jobs_ai.jobs.importer import OPTIONAL_IMPORT_FIELDS, REQUIRED_IMPORT_FIELDS
from jobs_ai.jobs.normalization import normalize_job_import_fields

RUNNER = CliRunner()
REPO_ROOT = Path(__file__).resolve().parents[1]
SAMPLE_IMPORT_PATH = REPO_ROOT / "data" / "raw" / "sample_job_leads.json"
IMPORT_FIELDS = REQUIRED_IMPORT_FIELDS + OPTIONAL_IMPORT_FIELDS


class ImportTest(unittest.TestCase):
    def test_normalize_job_import_fields_cleans_whitespace(self) -> None:
        normalized = normalize_job_import_fields(
            {
                "source": "  manual  ",
                "source_job_id": "  acme-123  ",
                "company": "  Acme   Data  ",
                "title": "  Senior\tData   Engineer  ",
                "location": "  Remote   US  ",
                "apply_url": "  https://example.com/jobs/data-engineer  ",
                "portal_type": "  GreenHouse  ",
                "salary_text": "  $140,000   -   $170,000  ",
                "posted_at": " 2026-03-10 ",
                "found_at": " 2026-03-12T08:15:00Z ",
            },
            IMPORT_FIELDS,
        )

        self.assertEqual(normalized["source"], "manual")
        self.assertEqual(normalized["source_job_id"], "acme-123")
        self.assertEqual(normalized["company"], "Acme Data")
        self.assertEqual(normalized["title"], "Senior Data Engineer")
        self.assertEqual(normalized["location"], "Remote US")
        self.assertEqual(normalized["apply_url"], "https://example.com/jobs/data-engineer")
        self.assertEqual(normalized["portal_type"], "greenhouse")
        self.assertEqual(normalized["salary_text"], "$140,000 - $170,000")
        self.assertEqual(normalized["posted_at"], "2026-03-10")
        self.assertEqual(normalized["found_at"], "2026-03-12T08:15:00Z")

    def test_normalize_job_import_fields_converts_empty_strings_to_none(self) -> None:
        normalized = normalize_job_import_fields(
            {
                "source": "   ",
                "source_job_id": "   ",
                "company": "\t",
                "title": "\n",
                "location": "   ",
                "apply_url": "   ",
                "portal_type": "   ",
                "salary_text": "   ",
            },
            IMPORT_FIELDS,
        )

        self.assertIsNone(normalized["source"])
        self.assertIsNone(normalized["source_job_id"])
        self.assertIsNone(normalized["company"])
        self.assertIsNone(normalized["title"])
        self.assertIsNone(normalized["location"])
        self.assertIsNone(normalized["apply_url"])
        self.assertIsNone(normalized["portal_type"])
        self.assertIsNone(normalized["salary_text"])

    def test_normalize_job_import_fields_lowercases_portal_type(self) -> None:
        normalized = normalize_job_import_fields(
            {"portal_type": "  WorkDay  "},
            ("portal_type",),
        )

        self.assertEqual(normalized["portal_type"], "workday")

    def test_cli_import_loads_sample_json_into_jobs_table(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            database_path = Path(tmp_dir) / "runtime" / "jobs_ai.db"
            env = {"JOBS_AI_DB_PATH": str(database_path)}

            result = RUNNER.invoke(app, ["import", str(SAMPLE_IMPORT_PATH)], env=env)

            self.assertEqual(result.exit_code, 0)
            self.assertIn("jobs_ai import", result.stdout)
            self.assertIn("inserted: 2", result.stdout)
            self.assertIn("status: success", result.stdout)
            self.assertIn("python -m jobs_ai score", result.stdout)

            with closing(connect_database(database_path)) as connection:
                rows = connection.execute(
                    """
                    SELECT
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
                        raw_json
                    FROM jobs
                    ORDER BY id
                    """
                ).fetchall()

            self.assertEqual(len(rows), 2)
            self.assertEqual(rows[0]["source"], "manual")
            self.assertEqual(rows[0]["source_job_id"], "acme-123")
            self.assertEqual(rows[0]["company"], "Acme Data")
            self.assertEqual(rows[0]["portal_type"], "greenhouse")
            self.assertEqual(rows[0]["salary_text"], "$140,000 - $170,000")
            self.assertEqual(rows[0]["posted_at"], "2026-03-10")
            self.assertEqual(rows[0]["found_at"], "2026-03-12T08:15:00Z")
            self.assertIn('"company": "Acme Data"', rows[0]["raw_json"])
            self.assertIsNone(rows[1]["source_job_id"])
            self.assertTrue(rows[1]["found_at"])

    def test_cli_import_stores_normalized_values_in_jobs_table(self) -> None:
        payload = [
            {
                "source": "  manual  ",
                "source_job_id": "   ",
                "company": "  Acme   Data  ",
                "title": "  Senior\tData   Engineer  ",
                "location": "  Remote   US  ",
                "apply_url": "  https://example.com/jobs/data-engineer  ",
                "portal_type": "  GreenHouse  ",
                "salary_text": "   ",
            }
        ]

        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            database_path = tmp_path / "runtime" / "jobs_ai.db"
            input_path = tmp_path / "normalized_job_leads.json"
            input_path.write_text(json.dumps(payload), encoding="utf-8")
            env = {"JOBS_AI_DB_PATH": str(database_path)}

            result = RUNNER.invoke(app, ["import", str(input_path)], env=env)

            self.assertEqual(result.exit_code, 0)
            self.assertIn("inserted: 1", result.stdout)
            self.assertIn("status: success", result.stdout)

            with closing(connect_database(database_path)) as connection:
                row = connection.execute(
                    """
                    SELECT
                        source,
                        source_job_id,
                        company,
                        title,
                        location,
                        apply_url,
                        portal_type,
                        salary_text,
                        raw_json
                    FROM jobs
                    """
                ).fetchone()

            self.assertEqual(row["source"], "manual")
            self.assertIsNone(row["source_job_id"])
            self.assertEqual(row["company"], "Acme Data")
            self.assertEqual(row["title"], "Senior Data Engineer")
            self.assertEqual(row["location"], "Remote US")
            self.assertEqual(row["apply_url"], "https://example.com/jobs/data-engineer")
            self.assertEqual(row["portal_type"], "greenhouse")
            self.assertIsNone(row["salary_text"])
            self.assertEqual(json.loads(row["raw_json"]), payload[0])

    def test_cli_import_skips_duplicate_by_exact_apply_url(self) -> None:
        payload = [
            {
                "source": "manual",
                "company": "Bright Metrics",
                "title": "Platform Data Engineer",
                "location": "Remote",
                "apply_url": "https://example.com/jobs/platform-data-engineer",
            },
            {
                "source": "manual",
                "company": "Another Company",
                "title": "Different Title",
                "location": "Hybrid",
                "apply_url": "https://example.com/jobs/platform-data-engineer",
            },
        ]

        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            database_path = tmp_path / "runtime" / "jobs_ai.db"
            input_path = tmp_path / "duplicate_apply_url.json"
            input_path.write_text(json.dumps(payload), encoding="utf-8")
            env = {"JOBS_AI_DB_PATH": str(database_path)}

            result = RUNNER.invoke(app, ["import", str(input_path)], env=env)

            self.assertEqual(result.exit_code, 0)
            self.assertIn("inserted: 1", result.stdout)
            self.assertIn("skipped: 1", result.stdout)
            self.assertIn("status: success", result.stdout)
            self.assertIn("skipped records:", result.stdout)
            self.assertIn(
                "record 2: duplicate skipped via exact apply_url match: "
                "https://example.com/jobs/platform-data-engineer",
                result.stdout,
            )

            with closing(connect_database(database_path)) as connection:
                row = connection.execute("SELECT COUNT(*) AS count FROM jobs").fetchone()

            self.assertEqual(row["count"], 1)

    def test_cli_import_skips_duplicate_by_exact_fallback_key_when_apply_url_missing(self) -> None:
        payload = [
            {
                "source": "manual",
                "company": "Bright Metrics",
                "title": "Platform Data Engineer",
                "location": "Remote",
            },
            {
                "source": "manual",
                "company": "Bright Metrics",
                "title": "Platform Data Engineer",
                "location": "Remote",
            },
        ]

        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            database_path = tmp_path / "runtime" / "jobs_ai.db"
            input_path = tmp_path / "duplicate_fallback_key.json"
            input_path.write_text(json.dumps(payload), encoding="utf-8")
            env = {"JOBS_AI_DB_PATH": str(database_path)}

            result = RUNNER.invoke(app, ["import", str(input_path)], env=env)

            self.assertEqual(result.exit_code, 0)
            self.assertIn("inserted: 1", result.stdout)
            self.assertIn("skipped: 1", result.stdout)
            self.assertIn("status: success", result.stdout)
            self.assertIn(
                "record 2: duplicate skipped via exact fallback key: "
                "manual | Bright Metrics | Platform Data Engineer | Remote",
                result.stdout,
            )

            with closing(connect_database(database_path)) as connection:
                row = connection.execute(
                    "SELECT apply_url, COUNT(*) AS count FROM jobs GROUP BY apply_url"
                ).fetchone()

            self.assertEqual(row["count"], 1)
            self.assertIsNone(row["apply_url"])

    def test_cli_import_inserts_non_duplicates_when_apply_url_differs(self) -> None:
        payload = [
            {
                "source": "manual",
                "company": "Bright Metrics",
                "title": "Platform Data Engineer",
                "location": "Remote",
                "apply_url": "https://example.com/jobs/platform-data-engineer",
            },
            {
                "source": "manual",
                "company": "Bright Metrics",
                "title": "Platform Data Engineer",
                "location": "Remote",
                "apply_url": "https://example.com/jobs/platform-data-engineer-v2",
            },
        ]

        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            database_path = tmp_path / "runtime" / "jobs_ai.db"
            input_path = tmp_path / "non_duplicate_apply_urls.json"
            input_path.write_text(json.dumps(payload), encoding="utf-8")
            env = {"JOBS_AI_DB_PATH": str(database_path)}

            result = RUNNER.invoke(app, ["import", str(input_path)], env=env)

            self.assertEqual(result.exit_code, 0)
            self.assertIn("inserted: 2", result.stdout)
            self.assertIn("skipped: 0", result.stdout)
            self.assertIn("status: success", result.stdout)

            with closing(connect_database(database_path)) as connection:
                rows = connection.execute(
                    "SELECT apply_url FROM jobs ORDER BY id"
                ).fetchall()

            self.assertEqual(
                [row["apply_url"] for row in rows],
                [
                    "https://example.com/jobs/platform-data-engineer",
                    "https://example.com/jobs/platform-data-engineer-v2",
                ],
            )

    def test_cli_import_reports_invalid_records_clearly(self) -> None:
        payload = [
            {
                "source": "manual",
                "company": "Bright Metrics",
                "title": "Platform Data Engineer",
                "location": "Remote",
                "apply_url": "https://example.com/jobs/platform-data-engineer",
            },
            {
                "source": "manual",
                "company": "Broken Example",
                "title": "Analytics Engineer",
                "apply_url": "https://example.com/jobs/analytics-engineer",
            },
        ]

        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            database_path = tmp_path / "runtime" / "jobs_ai.db"
            input_path = tmp_path / "mixed_job_leads.json"
            input_path.write_text(json.dumps(payload), encoding="utf-8")
            env = {"JOBS_AI_DB_PATH": str(database_path)}

            result = RUNNER.invoke(app, ["import", str(input_path)], env=env)

            self.assertEqual(result.exit_code, 1)
            self.assertIn("inserted: 1", result.stdout)
            self.assertIn("skipped: 1", result.stdout)
            self.assertIn("status: completed with errors", result.stdout)
            self.assertIn("record 2: missing required fields: location", result.stdout)

            with closing(connect_database(database_path)) as connection:
                row = connection.execute("SELECT COUNT(*) AS count FROM jobs").fetchone()

            self.assertEqual(row["count"], 1)


if __name__ == "__main__":
    unittest.main()
