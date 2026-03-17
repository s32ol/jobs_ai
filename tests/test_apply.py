from __future__ import annotations

from contextlib import closing
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

from typer.testing import CliRunner

SRC_PATH = Path(__file__).resolve().parents[1] / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

from jobs_ai.cli import app
from jobs_ai.db import connect_database, initialize_schema, insert_job
from jobs_ai.main import run_apply_workflow

RUNNER = CliRunner()


def _job_record(
    *,
    source: str,
    company: str,
    title: str,
    location: str,
    apply_url: str | None = None,
    raw_payload: dict[str, object] | None = None,
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


class ApplyCommandTest(unittest.TestCase):
    def test_run_apply_workflow_returns_launchable_ranked_matches(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            database_path = Path(tmp_dir) / "runtime" / "jobs_ai.db"
            initialize_schema(database_path)

            with closing(connect_database(database_path)) as connection:
                insert_job(
                    connection,
                    _job_record(
                        source="manual",
                        company="Stripe",
                        title="Python Backend Engineer",
                        location="Remote",
                        apply_url="https://boards.greenhouse.io/stripe/jobs/12345",
                        raw_payload={"description": "Python backend platform systems"},
                    ),
                )
                insert_job(
                    connection,
                    _job_record(
                        source="manual",
                        company="Datadog",
                        title="Backend Engineer",
                        location="Remote",
                        apply_url=None,
                        raw_payload={"description": "Python backend services"},
                    ),
                )
                insert_job(
                    connection,
                    _job_record(
                        source="manual",
                        company="Figma",
                        title="Frontend Engineer",
                        location="Remote",
                        apply_url="https://boards.greenhouse.io/figma/jobs/999",
                        raw_payload={"description": "TypeScript design systems"},
                    ),
                )
                connection.commit()

            result = run_apply_workflow(
                database_path,
                query="python backend",
                limit=10,
                print_only=True,
            )

            self.assertEqual(result.matching_count, 2)
            self.assertEqual(result.skipped_missing_apply_url_count, 1)
            self.assertEqual(result.selected_count, 1)
            self.assertEqual(result.items[0].company, "Stripe")
            self.assertEqual(
                result.items[0].apply_url,
                "https://boards.greenhouse.io/stripe/jobs/12345",
            )

    def test_cli_apply_print_only_lists_results_without_opening_browser(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            database_path = Path(tmp_dir) / "runtime" / "jobs_ai.db"
            env = {"JOBS_AI_DB_PATH": str(database_path)}
            initialize_schema(database_path)

            with closing(connect_database(database_path)) as connection:
                insert_job(
                    connection,
                    _job_record(
                        source="manual",
                        company="Stripe",
                        title="Python Backend Engineer",
                        location="Remote",
                        apply_url="https://boards.greenhouse.io/stripe/jobs/12345",
                        raw_payload={"description": "Python backend platform systems"},
                    ),
                )
                insert_job(
                    connection,
                    _job_record(
                        source="manual",
                        company="Datadog",
                        title="Backend Engineer",
                        location="Remote",
                        apply_url="https://jobs.lever.co/datadog/67890",
                        raw_payload={"description": "Python backend distributed systems"},
                    ),
                )
                connection.commit()

            with patch("jobs_ai.main.webbrowser.open") as open_browser:
                result = RUNNER.invoke(
                    app,
                    ["apply", "python", "--limit", "10", "--print-only"],
                    env=env,
                )

            self.assertEqual(result.exit_code, 0)
            self.assertFalse(open_browser.called)
            self.assertIn("jobs_ai apply", result.stdout)
            self.assertIn("Printing 2 applications...", result.stdout)
            self.assertIn("1. Python Backend Engineer - Stripe", result.stdout)
            self.assertIn("Apply URL: https://boards.greenhouse.io/stripe/jobs/12345", result.stdout)
            self.assertIn("2. Backend Engineer - Datadog", result.stdout)
            self.assertIn("Apply URL: https://jobs.lever.co/datadog/67890", result.stdout)

    def test_cli_apply_clamps_limit_and_opens_browser(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            database_path = Path(tmp_dir) / "runtime" / "jobs_ai.db"
            env = {"JOBS_AI_DB_PATH": str(database_path)}
            initialize_schema(database_path)

            with closing(connect_database(database_path)) as connection:
                for index in range(25):
                    insert_job(
                        connection,
                        _job_record(
                            source="manual",
                            company=f"Company {index}",
                            title=f"Python Backend Engineer {index}",
                            location="Remote",
                            apply_url=f"https://example.com/jobs/{index}",
                            raw_payload={"description": "Python backend platform systems"},
                        ),
                    )
                connection.commit()

            with patch("jobs_ai.main.webbrowser.open", return_value=True) as open_browser:
                result = RUNNER.invoke(
                    app,
                    ["apply", "python backend", "--limit", "25"],
                    env=env,
                )

            self.assertEqual(result.exit_code, 0)
            self.assertEqual(open_browser.call_count, 20)
            self.assertIn(
                "warning: requested limit 25 exceeds hard max 20; clamped to 20",
                result.stdout,
            )
            self.assertIn("Opening 20 applications...", result.stdout)
            open_browser.assert_any_call("https://example.com/jobs/0")


if __name__ == "__main__":
    unittest.main()
