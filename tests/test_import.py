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
from jobs_ai.db import connect_database, initialize_schema, insert_job
from jobs_ai.jobs.importer import OPTIONAL_IMPORT_FIELDS, REQUIRED_IMPORT_FIELDS
from jobs_ai.jobs.normalization import normalize_job_import_fields, should_auto_skip_job
from jobs_ai.jobs.queue import select_apply_queue

RUNNER = CliRunner()
REPO_ROOT = Path(__file__).resolve().parents[1]
SAMPLE_IMPORT_PATH = REPO_ROOT / "data" / "raw" / "sample_job_leads.json"
IMPORT_FIELDS = REQUIRED_IMPORT_FIELDS + OPTIONAL_IMPORT_FIELDS


def _job_record(
    *,
    source: str,
    company: str,
    title: str,
    location: str,
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
        "raw_json": json.dumps({}, ensure_ascii=True),
    }


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

    def test_should_auto_skip_job_matches_filtered_titles(self) -> None:
        self.assertTrue(should_auto_skip_job("Senior Legal Counsel"))
        self.assertTrue(should_auto_skip_job("PARALEGAL, Employment"))
        self.assertFalse(should_auto_skip_job("Senior Data Engineer"))

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

    def test_cli_import_auto_skips_filtered_titles_and_keeps_them_out_of_queue(self) -> None:
        payload = [
            {
                "source": "manual",
                "company": "Acme Corp",
                "title": "Senior Legal Counsel",
                "location": "Remote",
                "apply_url": "https://example.com/jobs/legal-counsel",
            }
        ]

        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            database_path = tmp_path / "runtime" / "jobs_ai.db"
            input_path = tmp_path / "auto_skip_job_leads.json"
            input_path.write_text(json.dumps(payload), encoding="utf-8")
            env = {"JOBS_AI_DB_PATH": str(database_path)}

            result = RUNNER.invoke(app, ["import", str(input_path)], env=env)

            self.assertEqual(result.exit_code, 0)
            self.assertIn("auto-skip: title matched filter", result.stdout)
            self.assertIn("inserted: 1", result.stdout)

            with closing(connect_database(database_path)) as connection:
                row = connection.execute(
                    """
                    SELECT title, status, raw_json
                    FROM jobs
                    LIMIT 1
                    """
                ).fetchone()

            self.assertEqual(row["title"], "Senior Legal Counsel")
            self.assertEqual(row["status"], "skipped")
            self.assertEqual(json.loads(row["raw_json"])["reason"], "auto_filtered_role")
            self.assertEqual(select_apply_queue(database_path), ())

    def test_cli_import_prefers_more_specific_title_for_exact_apply_url_duplicates(self) -> None:
        payload = [
            {
                "source": "manual",
                "company": "Qualified Health",
                "title": "Data Engineer",
                "location": "Remote",
                "apply_url": "https://example.com/jobs/data-engineer",
            },
            {
                "source": "manual",
                "company": "Qualified Health",
                "title": "Sr. Data Engineer - Healthcare Data Infrastructure",
                "location": "Remote",
                "apply_url": "https://example.com/jobs/data-engineer",
            },
        ]

        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            database_path = tmp_path / "runtime" / "jobs_ai.db"
            input_path = tmp_path / "duplicate_apply_url_specificity.json"
            input_path.write_text(json.dumps(payload), encoding="utf-8")
            env = {"JOBS_AI_DB_PATH": str(database_path)}

            result = RUNNER.invoke(app, ["import", str(input_path)], env=env)

            self.assertEqual(result.exit_code, 0)
            self.assertIn("inserted: 2", result.stdout)
            self.assertIn("duplicates skipped: 0", result.stdout)
            self.assertIn("canonical duplicate groups resolved: 1", result.stdout)
            self.assertIn("rows marked superseded: 1", result.stdout)
            self.assertIn("status: success", result.stdout)

            with closing(connect_database(database_path)) as connection:
                rows = connection.execute(
                    "SELECT title, status FROM jobs ORDER BY id"
                ).fetchall()

            self.assertEqual(
                [(row["title"], row["status"]) for row in rows],
                [
                    ("Data Engineer", "superseded"),
                    ("Sr. Data Engineer - Healthcare Data Infrastructure", "new"),
                ],
            )

    def test_cli_import_collapses_existing_applied_cluster_to_one_applied_row(self) -> None:
        payload = [
            {
                "source": "manual",
                "company": "Qualified Health",
                "title": "Platform Data Engineer",
                "location": "Remote",
                "apply_url": "https://example.com/jobs/data-engineer",
            }
        ]

        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            database_path = tmp_path / "runtime" / "jobs_ai.db"
            input_path = tmp_path / "duplicate_apply_url_applied_cluster.json"
            input_path.write_text(json.dumps(payload), encoding="utf-8")
            env = {"JOBS_AI_DB_PATH": str(database_path)}
            initialize_schema(database_path)

            with closing(connect_database(database_path)) as connection:
                applied_job_id = insert_job(
                    connection,
                    _job_record(
                        source="manual",
                        company="Qualified Health",
                        title="Data Engineer",
                        location="Remote",
                        apply_url="https://example.com/jobs/data-engineer",
                    ),
                )
                specific_job_id = insert_job(
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
                    "UPDATE jobs SET status = 'applied', updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                    (applied_job_id,),
                )
                connection.execute(
                    "UPDATE jobs SET status = 'opened', updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                    (specific_job_id,),
                )
                connection.commit()

            result = RUNNER.invoke(app, ["import", str(input_path)], env=env)

            self.assertEqual(result.exit_code, 0)
            self.assertIn("inserted: 1", result.stdout)
            self.assertIn("canonical duplicate groups resolved: 1", result.stdout)
            self.assertIn("rows marked superseded: 2", result.stdout)

            with closing(connect_database(database_path)) as connection:
                rows = connection.execute(
                    "SELECT id, title, status FROM jobs ORDER BY id"
                ).fetchall()

            self.assertEqual(
                [(row["title"], row["status"]) for row in rows],
                [
                    ("Data Engineer", "applied"),
                    (
                        "Sr. Data Engineer - Healthcare Data Infrastructure",
                        "superseded",
                    ),
                    ("Platform Data Engineer", "superseded"),
                ],
            )

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
                "record 2: duplicate skipped via identity key match: "
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

    def test_cli_import_auto_supersedes_duplicate_by_raw_apply_url(self) -> None:
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
            self.assertIn("inserted: 2", result.stdout)
            self.assertIn("duplicates skipped: 0", result.stdout)
            self.assertIn("canonical duplicate groups resolved: 1", result.stdout)
            self.assertIn("rows marked superseded: 1", result.stdout)
            self.assertIn("status: success", result.stdout)

            with closing(connect_database(database_path)) as connection:
                rows = connection.execute(
                    "SELECT id, apply_url, canonical_apply_url, status FROM jobs ORDER BY id"
                ).fetchall()

            self.assertEqual(len(rows), 2)
            self.assertEqual(
                [row["status"] for row in rows],
                ["new", "superseded"],
            )
            self.assertEqual(rows[0]["apply_url"], "https://example.com/jobs/platform-data-engineer")
            self.assertEqual(rows[0]["canonical_apply_url"], "https://example.com/jobs/platform-data-engineer")
            self.assertEqual(rows[1]["canonical_apply_url"], "https://example.com/jobs/platform-data-engineer")

    def test_cli_import_auto_supersedes_duplicate_by_canonical_portal_apply_url_across_runs(self) -> None:
        first_payload = [
            {
                "source": "manual",
                "company": "Acme Data",
                "title": "Data Engineer",
                "location": "Remote",
                "apply_url": "https://boards.greenhouse.io/acme?gh_jid=12345&gh_src=linkedin",
                "portal_type": "greenhouse",
            }
        ]
        second_payload = [
            {
                "source": "manual",
                "company": "Acme Data",
                "title": "Data Engineer",
                "location": "Remote",
                "apply_url": "https://boards.greenhouse.io/acme/jobs/12345?utm_source=jobs",
                "portal_type": "greenhouse",
            }
        ]

        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            database_path = tmp_path / "runtime" / "jobs_ai.db"
            first_input_path = tmp_path / "first_job_leads.json"
            second_input_path = tmp_path / "second_job_leads.json"
            first_input_path.write_text(json.dumps(first_payload), encoding="utf-8")
            second_input_path.write_text(json.dumps(second_payload), encoding="utf-8")
            env = {"JOBS_AI_DB_PATH": str(database_path)}

            first_result = RUNNER.invoke(
                app,
                ["import", str(first_input_path), "--batch-id", "first-run"],
                env=env,
            )
            second_result = RUNNER.invoke(
                app,
                ["import", str(second_input_path), "--batch-id", "second-run"],
                env=env,
            )

            self.assertEqual(first_result.exit_code, 0)
            self.assertEqual(second_result.exit_code, 0)
            self.assertIn("inserted: 1", first_result.stdout)
            self.assertIn("inserted: 1", second_result.stdout)
            self.assertIn("duplicates skipped: 0", second_result.stdout)
            self.assertIn("canonical duplicate groups resolved: 1", second_result.stdout)
            self.assertIn("rows marked superseded: 1", second_result.stdout)

            with closing(connect_database(database_path)) as connection:
                rows = connection.execute(
                    "SELECT apply_url, canonical_apply_url, ingest_batch_id, status FROM jobs ORDER BY id"
                ).fetchall()

            self.assertEqual(len(rows), 2)
            self.assertEqual(
                [row["status"] for row in rows],
                ["new", "superseded"],
            )
            self.assertEqual(
                [row["canonical_apply_url"] for row in rows],
                [
                    "https://boards.greenhouse.io/acme/jobs/12345",
                    "https://boards.greenhouse.io/acme/jobs/12345",
                ],
            )
            self.assertEqual(
                [row["ingest_batch_id"] for row in rows],
                ["first-run", "second-run"],
            )

    def test_cli_import_tags_rows_with_batch_metadata_for_later_session_scoping(self) -> None:
        payload = [
            {
                "source": "manual",
                "company": "Acme Data",
                "title": "Data Engineer",
                "location": "Remote",
                "apply_url": "https://example.com/jobs/data-engineer",
            }
        ]

        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            database_path = tmp_path / "runtime" / "jobs_ai.db"
            input_path = tmp_path / "batch_tagged_job_leads.json"
            input_path.write_text(json.dumps(payload), encoding="utf-8")
            env = {"JOBS_AI_DB_PATH": str(database_path)}

            result = RUNNER.invoke(
                app,
                [
                    "import",
                    str(input_path),
                    "--batch-id",
                    "evening batch",
                    "--source-query",
                    "python backend engineer remote",
                ],
                env=env,
            )

            self.assertEqual(result.exit_code, 0)
            self.assertIn("batch id: evening-batch", result.stdout)
            self.assertIn("source query: python backend engineer remote", result.stdout)
            self.assertIn(
                "python -m jobs_ai session start --batch-id evening-batch --limit 25",
                result.stdout,
            )

            with closing(connect_database(database_path)) as connection:
                row = connection.execute(
                    """
                    SELECT ingest_batch_id, source_query, import_source
                    FROM jobs
                    """
                ).fetchone()

            self.assertEqual(row["ingest_batch_id"], "evening-batch")
            self.assertEqual(row["source_query"], "python backend engineer remote")
            self.assertEqual(row["import_source"], str(input_path.resolve()))

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
