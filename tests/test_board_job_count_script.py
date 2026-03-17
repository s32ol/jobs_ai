from __future__ import annotations

from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path
import importlib.util
import sys
import unittest
from unittest.mock import patch


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "board_job_count.py"
SCRIPT_SPEC = importlib.util.spec_from_file_location("board_job_count_script", SCRIPT_PATH)
if SCRIPT_SPEC is None or SCRIPT_SPEC.loader is None:
    raise RuntimeError(f"unable to load script module from {SCRIPT_PATH}")
board_job_count = importlib.util.module_from_spec(SCRIPT_SPEC)
sys.modules[SCRIPT_SPEC.name] = board_job_count
SCRIPT_SPEC.loader.exec_module(board_job_count)


class BoardJobCountScriptTest(unittest.TestCase):
    def test_main_prints_resume_match_estimate_and_top_companies(self) -> None:
        board_urls = [
            "https://jobs.lever.co/northwind",
            "https://boards.greenhouse.io/acme",
        ]
        results = {
            "https://jobs.lever.co/northwind": {
                "success": True,
                "portal": "Lever",
                "url": "https://jobs.lever.co/northwind",
                "count": 6,
                "company": "Northwind Talent",
                "resume_match_count": 2,
                "error": "",
            },
            "https://boards.greenhouse.io/acme": {
                "success": True,
                "portal": "Greenhouse",
                "url": "https://boards.greenhouse.io/acme",
                "count": 4,
                "company": "Acme Data",
                "resume_match_count": 1,
                "error": "",
            },
        }

        with (
            patch.object(board_job_count, "_load_board_urls", return_value=board_urls),
            patch.object(
                board_job_count,
                "_count_board",
                side_effect=lambda board_url, *, estimate_resume_matches: results[board_url],
            ),
        ):
            output = StringIO()
            with redirect_stdout(output):
                board_job_count.main(
                    [
                        "--boards-path",
                        "/tmp/boards.txt",
                        "--estimate-resume-matches",
                    ]
                )

        rendered = output.getvalue()
        self.assertIn("Resume match estimate", rendered)
        self.assertIn("Estimated matching jobs: 3", rendered)
        self.assertIn("Match rate: 30.0%", rendered)
        self.assertIn("Top matching companies", rendered)
        self.assertIn("Northwind Talent: 2", rendered)
        self.assertIn("Acme Data: 1", rendered)

    def test_contains_any_term_handles_phrase_boundaries_and_case(self) -> None:
        self.assertTrue(
            board_job_count._contains_any_term(
                "Senior Data-Platform Engineer",
                board_job_count.TARGET_TITLES,
            )
        )
        self.assertTrue(
            board_job_count._contains_any_term(
                "Build telemetry pipelines with Python on Google Cloud.",
                board_job_count.STACK_KEYWORDS,
            )
        )
        self.assertFalse(
            board_job_count._contains_any_term(
                "Experience with PostgreSQL preferred.",
                ("sql",),
            )
        )


if __name__ == "__main__":
    unittest.main()
