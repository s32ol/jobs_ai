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
from jobs_ai.jobs.queue import select_apply_queue

RUNNER = CliRunner()


def _job_record(
    *,
    source: str,
    company: str,
    title: str,
    location: str,
    apply_url: str | None = None,
    portal_type: str | None = None,
    raw_payload: dict[str, object] | None = None,
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


def _insert_job_with_status(
    connection,
    *,
    status: str = "new",
    source: str,
    company: str,
    title: str,
    location: str,
    apply_url: str | None = None,
    portal_type: str | None = None,
    raw_payload: dict[str, object] | None = None,
) -> int:
    job_id = insert_job(
        connection,
        _job_record(
            source=source,
            company=company,
            title=title,
            location=location,
            apply_url=apply_url,
            portal_type=portal_type,
            raw_payload=raw_payload,
        ),
    )
    if status != "new":
        connection.execute(
            "UPDATE jobs SET status = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
            (status, job_id),
        )
    return job_id


class QueueTest(unittest.TestCase):
    def test_select_apply_queue_only_includes_new_jobs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            database_path = Path(tmp_dir) / "runtime" / "jobs_ai.db"
            initialize_schema(database_path)

            with closing(connect_database(database_path)) as connection:
                new_job_id = _insert_job_with_status(
                    connection,
                    source="manual",
                    company="Acme Data",
                    title="Analytics Engineer",
                    location="Remote",
                    status="new",
                )
                _insert_job_with_status(
                    connection,
                    source="staffing recruiter",
                    company="Northwind Talent",
                    title="Platform Data Engineer",
                    location="Remote",
                    status="applied",
                )
                connection.commit()

            queued_jobs = select_apply_queue(database_path)

            self.assertEqual([job.job_id for job in queued_jobs], [new_job_id])
            self.assertTrue(all(job.company != "Northwind Talent" for job in queued_jobs))

    def test_select_apply_queue_orders_jobs_by_score_descending(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            database_path = Path(tmp_dir) / "runtime" / "jobs_ai.db"
            initialize_schema(database_path)

            with closing(connect_database(database_path)) as connection:
                analytics_job_id = _insert_job_with_status(
                    connection,
                    source="manual",
                    company="Acme Data",
                    title="Analytics Engineer",
                    location="Sacramento, CA",
                    apply_url="https://boards.greenhouse.io/acme/jobs/1",
                    portal_type="greenhouse",
                    raw_payload={"description": "Looker dashboards"},
                )
                platform_job_id = _insert_job_with_status(
                    connection,
                    source="staffing recruiter",
                    company="Northwind Talent",
                    title="Platform Data Engineer",
                    location="Remote",
                    apply_url="https://agency.example/jobs/2",
                    raw_payload={"description": "Python BigQuery GCP contract"},
                )
                analyst_job_id = _insert_job_with_status(
                    connection,
                    source="manual",
                    company="Back Office Pro",
                    title="Business Systems Analyst",
                    location="San Jose, CA",
                    apply_url="https://jobs.example.com/3",
                    portal_type="workday",
                    raw_payload={"description": "ERP reporting"},
                )
                connection.commit()

            queued_jobs = select_apply_queue(database_path)

            self.assertEqual(
                [job.job_id for job in queued_jobs],
                [platform_job_id, analytics_job_id, analyst_job_id],
            )
            self.assertEqual([job.score for job in queued_jobs], sorted((job.score for job in queued_jobs), reverse=True))

    def test_select_apply_queue_applies_limit_after_ranking(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            database_path = Path(tmp_dir) / "runtime" / "jobs_ai.db"
            initialize_schema(database_path)

            with closing(connect_database(database_path)) as connection:
                _insert_job_with_status(
                    connection,
                    source="manual",
                    company="Acme Data",
                    title="Analytics Engineer",
                    location="Sacramento, CA",
                )
                top_job_id = _insert_job_with_status(
                    connection,
                    source="staffing recruiter",
                    company="Northwind Talent",
                    title="Platform Data Engineer",
                    location="Remote",
                    raw_payload={"description": "Python BigQuery GCP contract"},
                )
                second_job_id = _insert_job_with_status(
                    connection,
                    source="manual",
                    company="Bright Metrics",
                    title="Data Engineer",
                    location="Remote",
                    raw_payload={"description": "Python pipelines"},
                )
                connection.commit()

            queued_jobs = select_apply_queue(database_path, limit=2)

            self.assertEqual(len(queued_jobs), 2)
            self.assertEqual([job.job_id for job in queued_jobs], [top_job_id, second_job_id])

    def test_cli_queue_reports_ranked_working_set(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            database_path = Path(tmp_dir) / "runtime" / "jobs_ai.db"
            env = {"JOBS_AI_DB_PATH": str(database_path)}

            initialize_schema(database_path)
            with closing(connect_database(database_path)) as connection:
                _insert_job_with_status(
                    connection,
                    source="manual",
                    company="Acme Data",
                    title="Analytics Engineer",
                    location="Sacramento, CA",
                    apply_url="https://boards.greenhouse.io/acme/jobs/1",
                    portal_type="greenhouse",
                    raw_payload={"description": "Looker dashboards"},
                )
                _insert_job_with_status(
                    connection,
                    source="staffing recruiter",
                    company="Northwind Talent",
                    title="Platform Data Engineer",
                    location="Remote",
                    apply_url="https://agency.example/jobs/2",
                    raw_payload={"description": "Python BigQuery GCP contract"},
                )
                connection.commit()

            result = RUNNER.invoke(app, ["queue", "--limit", "1"], env=env)

            self.assertEqual(result.exit_code, 0)
            self.assertIn("jobs_ai queue", result.stdout)
            self.assertIn("working set size: 1", result.stdout)
            self.assertIn("limit: 1", result.stdout)
            self.assertIn(
                "1. score 93 | Northwind Talent | Platform Data Engineer | Remote | staffing recruiter",
                result.stdout,
            )
            self.assertIn(
                "reason: role=Platform Data Engineer; stack=Python, BigQuery, GCP; geo=Remote; "
                "source=staffing agencies / recruiter-driven contract roles",
                result.stdout,
            )


if __name__ == "__main__":
    unittest.main()
