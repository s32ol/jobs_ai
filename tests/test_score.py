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
from jobs_ai.config import SEARCH_PRIORITY, TARGET_ROLES
from jobs_ai.db import connect_database, initialize_schema, insert_job
from jobs_ai.jobs.scoring import rank_jobs, score_job

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
    job_id: int | None = None,
) -> dict[str, object]:
    raw_json = json.dumps(raw_payload or {}, ensure_ascii=True)
    record: dict[str, object] = {
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
        "raw_json": raw_json,
    }
    if job_id is not None:
        record["id"] = job_id
    return record


class ScoreTest(unittest.TestCase):
    def test_score_job_breakdown_is_rule_based_and_transparent(self) -> None:
        scored_job = score_job(
            _job_record(
                job_id=7,
                source="staffing recruiter",
                company="Northwind Talent",
                title="Platform Data Engineer",
                location="Remote",
                apply_url="https://agency.example/jobs/platform-data-engineer",
                raw_payload={
                    "description": "Python BigQuery Looker GCP pipeline role",
                },
            )
        )

        self.assertEqual(scored_job.total_score, 98)
        self.assertEqual(scored_job.role_score, 40)
        self.assertEqual(scored_job.matched_target_role, TARGET_ROLES[3])
        self.assertEqual(scored_job.stack_score, 20)
        self.assertEqual(scored_job.matched_stack_keywords, ("Python", "BigQuery", "Looker", "GCP"))
        self.assertEqual(scored_job.geography_score, 18)
        self.assertEqual(scored_job.source_score, 20)
        self.assertEqual(scored_job.source_category, SEARCH_PRIORITY[0])
        self.assertIn('title matched target role "Platform Data Engineer"', scored_job.role_reason)
        self.assertIn("matched stack keywords: Python, BigQuery, Looker, GCP", scored_job.stack_reason)
        self.assertIn('location matched geography priority "Remote"', scored_job.geography_reason)
        self.assertIn('staffing agencies / recruiter-driven contract roles via keyword "staffing"', scored_job.source_reason)

    def test_rank_jobs_orders_representative_examples(self) -> None:
        ranked_jobs = rank_jobs(
            [
                _job_record(
                    job_id=1,
                    source="manual",
                    company="Acme Data",
                    title="Analytics Engineer",
                    location="Sacramento, CA",
                    apply_url="https://boards.greenhouse.io/acme/jobs/1",
                    portal_type="greenhouse",
                    raw_payload={"description": "Looker semantic model ownership"},
                ),
                _job_record(
                    job_id=2,
                    source="staffing recruiter",
                    company="Northwind Talent",
                    title="Platform Data Engineer",
                    location="Remote",
                    apply_url="https://agency.example/jobs/2",
                    raw_payload={"description": "Python BigQuery GCP contract"},
                ),
                _job_record(
                    job_id=3,
                    source="manual",
                    company="Back Office Pro",
                    title="Business Systems Analyst",
                    location="San Jose, CA",
                    apply_url="https://jobs.example.com/3",
                    portal_type="workday",
                    raw_payload={"description": "ERP reporting"},
                ),
            ]
        )

        self.assertEqual([job.job_id for job in ranked_jobs], [2, 1, 3])
        self.assertGreater(ranked_jobs[0].total_score, ranked_jobs[1].total_score)
        self.assertGreater(ranked_jobs[1].total_score, ranked_jobs[2].total_score)

    def test_cli_score_reports_ranked_jobs_with_reasons(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            database_path = Path(tmp_dir) / "runtime" / "jobs_ai.db"
            env = {"JOBS_AI_DB_PATH": str(database_path)}

            initialize_schema(database_path)
            with closing(connect_database(database_path)) as connection:
                insert_job(
                    connection,
                    _job_record(
                        source="manual",
                        company="Acme Data",
                        title="Analytics Engineer",
                        location="Sacramento, CA",
                        apply_url="https://boards.greenhouse.io/acme/jobs/1",
                        portal_type="greenhouse",
                        raw_payload={"description": "Looker dashboards"},
                    ),
                )
                insert_job(
                    connection,
                    _job_record(
                        source="staffing recruiter",
                        company="Northwind Talent",
                        title="Platform Data Engineer",
                        location="Remote",
                        apply_url="https://agency.example/jobs/2",
                        raw_payload={"description": "Python BigQuery GCP contract"},
                    ),
                )
                connection.commit()

            result = RUNNER.invoke(app, ["score"], env=env)

            self.assertEqual(result.exit_code, 0)
            self.assertIn("jobs_ai score", result.stdout)
            self.assertIn("jobs scored: 2", result.stdout)
            self.assertIn("status: success", result.stdout)
            self.assertLess(
                result.stdout.index("Northwind Talent | Platform Data Engineer | Remote"),
                result.stdout.index("Acme Data | Analytics Engineer | Sacramento, CA"),
            )
            self.assertIn('role: title matched target role "Platform Data Engineer" (+40)', result.stdout)
            self.assertIn("stack: matched stack keywords: Python, BigQuery, GCP (+15)", result.stdout)
            self.assertIn('geography: location matched geography priority "Remote" (+18)', result.stdout)
            self.assertIn(
                'source: staffing agencies / recruiter-driven contract roles via keyword "staffing" (+20)',
                result.stdout,
            )


if __name__ == "__main__":
    unittest.main()
