from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import csv
import json
import sys
import tempfile
import unittest
from unittest.mock import patch

from typer.testing import CliRunner

SRC_PATH = Path(__file__).resolve().parents[1] / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

from jobs_ai.cli import app
from jobs_ai.collect.census import run_board_census_command
from jobs_ai.collect.fetch import FetchError, FetchRequest, FetchResponse
from jobs_ai.main import render_board_census_report
from jobs_ai.workspace import build_workspace_paths

FIXTURE_ROOT = Path(__file__).resolve().parent / "fixtures" / "collect"
RUNNER = CliRunner()
FIXED_CREATED_AT = datetime(2026, 3, 13, 19, 45, 0, tzinfo=timezone.utc)
FIXED_TIMESTAMP = FIXED_CREATED_AT.isoformat(timespec="seconds").replace("+00:00", "Z")


def _fixture_text(name: str) -> str:
    return (FIXTURE_ROOT / name).read_text(encoding="utf-8")


class BoardCensusTest(unittest.TestCase):
    def test_run_board_census_counts_boards_writes_artifacts_and_fetches_duplicates_once(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            project_root = Path(tmp_dir) / "workspace"
            project_root.mkdir(parents=True, exist_ok=True)
            source_file = project_root / "boards.txt"
            source_file.write_text(
                "\n".join(
                    [
                        "https://boards.greenhouse.io/acme",
                        "https://boards.greenhouse.io/acme#jobs",
                        "https://jobs.lever.co/northwind",
                        "https://jobs.ashbyhq.com/signalops",
                        "https://jobs.lever.co/broken",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            paths = build_workspace_paths(Path("data/jobs_ai.db"), project_root=project_root)
            fetch_counts: dict[str, int] = {}

            def fetcher(request: FetchRequest) -> FetchResponse:
                fetch_counts[request.url] = fetch_counts.get(request.url, 0) + 1
                if request.url == "https://boards.greenhouse.io/acme":
                    return FetchResponse(
                        url=request.url,
                        final_url=request.url,
                        status_code=200,
                        content_type="text/html; charset=utf-8",
                        text=_fixture_text("greenhouse_board.html"),
                    )
                if request.url == "https://jobs.lever.co/northwind":
                    return FetchResponse(
                        url=request.url,
                        final_url=request.url,
                        status_code=200,
                        content_type="text/html; charset=utf-8",
                        text=_fixture_text("lever_board.html"),
                    )
                if request.url == "https://jobs.ashbyhq.com/signalops":
                    return FetchResponse(
                        url=request.url,
                        final_url=request.url,
                        status_code=200,
                        content_type="text/html; charset=utf-8",
                        text=_fixture_text("ashby_board.html"),
                    )
                if request.url == "https://jobs.lever.co/broken":
                    raise FetchError(
                        f"timed out after {request.timeout_seconds:.1f}s while fetching {request.url}"
                    )
                raise AssertionError(f"unexpected fetch URL: {request.url}")

            run = run_board_census_command(
                paths,
                from_file=source_file,
                out_dir=project_root / "out",
                label="fixture-run",
                timeout_seconds=5.0,
                created_at=FIXED_CREATED_AT,
                fetcher=fetcher,
            )

            payload = json.loads(run.artifact_paths.json_path.read_text(encoding="utf-8"))
            with run.artifact_paths.csv_path.open("r", encoding="utf-8", newline="") as input_file:
                csv_rows = list(csv.DictReader(input_file))
            json_exists = run.artifact_paths.json_path.exists()
            csv_exists = run.artifact_paths.csv_path.exists()

        self.assertEqual(run.run_id, "board-census-fixture-run-20260313T194500000000Z")
        self.assertEqual(run.created_at, FIXED_TIMESTAMP)
        self.assertEqual(run.finished_at, FIXED_TIMESTAMP)
        self.assertEqual(run.input_count, 5)
        self.assertEqual(run.unique_board_count, 4)
        self.assertEqual(run.duplicate_input_count, 1)
        self.assertEqual(run.counted_board_count, 3)
        self.assertEqual(run.failed_count, 1)
        self.assertEqual(fetch_counts["https://boards.greenhouse.io/acme"], 1)
        self.assertEqual(fetch_counts["https://jobs.lever.co/northwind"], 1)
        self.assertEqual(fetch_counts["https://jobs.ashbyhq.com/signalops"], 1)
        self.assertEqual(fetch_counts["https://jobs.lever.co/broken"], 1)
        self.assertEqual(
            {total.portal_type: total.job_count for total in run.portal_totals},
            {"greenhouse": 2, "lever": 2, "ashby": 2},
        )
        self.assertEqual(run.grand_total, 6)
        self.assertTrue(json_exists)
        self.assertTrue(csv_exists)
        self.assertEqual(payload["totals"]["grand_total"], 6)
        self.assertEqual(payload["totals"]["duplicates_collapsed"], 1)
        self.assertEqual(payload["totals"]["failed_boards"], 1)
        self.assertEqual(len(payload["board_counts"]), 3)
        self.assertEqual(len(payload["failed_boards"]), 1)
        self.assertEqual(payload["failed_boards"][0]["reason_code"], "fetch_failed")
        self.assertEqual(len(csv_rows), 4)
        self.assertEqual(
            [(row["status"], row["portal_type"], row["available_job_count"]) for row in csv_rows],
            [
                ("counted", "greenhouse", "2"),
                ("counted", "lever", "2"),
                ("counted", "ashby", "2"),
                ("failed", "lever", ""),
            ],
        )

    def test_run_board_census_counts_empty_supported_board_as_zero(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            project_root = Path(tmp_dir) / "workspace"
            project_root.mkdir(parents=True, exist_ok=True)
            source_file = project_root / "boards.txt"
            source_file.write_text("https://jobs.lever.co/emptyco\n", encoding="utf-8")
            paths = build_workspace_paths(Path("data/jobs_ai.db"), project_root=project_root)
            empty_board_html = """
<!doctype html>
<html>
  <head>
    <title>EmptyCo | Lever</title>
  </head>
  <body>
    <script>
      window.__LEVER_POSTINGS__ = {
        "company": "EmptyCo",
        "postings": []
      };
    </script>
  </body>
</html>
""".strip()

            def fetcher(request: FetchRequest) -> FetchResponse:
                self.assertEqual(request.url, "https://jobs.lever.co/emptyco")
                return FetchResponse(
                    url=request.url,
                    final_url=request.url,
                    status_code=200,
                    content_type="text/html; charset=utf-8",
                    text=empty_board_html,
                )

            run = run_board_census_command(
                paths,
                from_file=source_file,
                out_dir=project_root / "out",
                label=None,
                timeout_seconds=5.0,
                created_at=FIXED_CREATED_AT,
                fetcher=fetcher,
            )

        self.assertEqual(run.counted_board_count, 1)
        self.assertEqual(run.failed_count, 0)
        self.assertEqual(run.grand_total, 0)
        self.assertEqual(run.counted_boards[0].available_job_count, 0)
        self.assertEqual(run.counted_boards[0].portal_type, "lever")
        self.assertEqual(run.portal_totals[1].job_count, 0)

    def test_render_board_census_report_lists_sorted_per_board_counts_and_failures(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            project_root = Path(tmp_dir) / "workspace"
            project_root.mkdir(parents=True, exist_ok=True)
            source_file = project_root / "boards.txt"
            source_file.write_text(
                "\n".join(
                    [
                        "https://jobs.lever.co/emptyco",
                        "https://jobs.lever.co/northwind",
                        "https://boards.greenhouse.io/acme",
                        "https://jobs.ashbyhq.com/signalops",
                        "https://jobs.lever.co/broken",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            paths = build_workspace_paths(Path("data/jobs_ai.db"), project_root=project_root)
            empty_board_html = """
<!doctype html>
<html>
  <head>
    <title>EmptyCo | Lever</title>
  </head>
  <body>
    <script>
      window.__LEVER_POSTINGS__ = {
        "company": "EmptyCo",
        "postings": []
      };
    </script>
  </body>
</html>
""".strip()

            def fetcher(request: FetchRequest) -> FetchResponse:
                if request.url == "https://jobs.lever.co/emptyco":
                    return FetchResponse(
                        url=request.url,
                        final_url=request.url,
                        status_code=200,
                        content_type="text/html; charset=utf-8",
                        text=empty_board_html,
                    )
                if request.url == "https://jobs.lever.co/northwind":
                    return FetchResponse(
                        url=request.url,
                        final_url=request.url,
                        status_code=200,
                        content_type="text/html; charset=utf-8",
                        text=_fixture_text("lever_board.html"),
                    )
                if request.url == "https://boards.greenhouse.io/acme":
                    return FetchResponse(
                        url=request.url,
                        final_url=request.url,
                        status_code=200,
                        content_type="text/html; charset=utf-8",
                        text=_fixture_text("greenhouse_board.html"),
                    )
                if request.url == "https://jobs.ashbyhq.com/signalops":
                    return FetchResponse(
                        url=request.url,
                        final_url=request.url,
                        status_code=200,
                        content_type="text/html; charset=utf-8",
                        text=_fixture_text("ashby_board.html"),
                    )
                if request.url == "https://jobs.lever.co/broken":
                    raise FetchError(
                        f"timed out after {request.timeout_seconds:.1f}s while fetching {request.url}"
                    )
                raise AssertionError(f"unexpected fetch URL: {request.url}")

            run = run_board_census_command(
                paths,
                from_file=source_file,
                out_dir=project_root / "out",
                label="sorted",
                timeout_seconds=5.0,
                created_at=FIXED_CREATED_AT,
                fetcher=fetcher,
            )

        report = render_board_census_report(run)
        self.assertIn("Boards configured: 5", report)
        self.assertIn("Boards counted: 4", report)
        self.assertIn("Boards failed: 1", report)
        self.assertIn("Per-board counts", report)
        self.assertIn("Failed boards", report)
        self.assertIn("Portal totals", report)
        self.assertIn("Workday: not supported in board-census yet", report)
        self.assertIn("Grand total: 6", report)
        self.assertLess(
            report.index("Greenhouse | https://boards.greenhouse.io/acme | 2"),
            report.index("Lever | https://jobs.lever.co/northwind | 2"),
        )
        self.assertLess(
            report.index("Lever | https://jobs.lever.co/northwind | 2"),
            report.index("Lever | https://jobs.lever.co/emptyco | 0"),
        )
        self.assertLess(
            report.index("Lever | https://jobs.lever.co/emptyco | 0"),
            report.index("Ashby | https://jobs.ashbyhq.com/signalops | 2"),
        )
        self.assertIn(
            "Lever | https://jobs.lever.co/broken | fetch_failed | timed out after 5.0s while fetching https://jobs.lever.co/broken",
            report,
        )

    def test_cli_board_census_prints_short_summary(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            project_root = Path(tmp_dir) / "workspace"
            project_root.mkdir(parents=True, exist_ok=True)
            source_file = project_root / "boards.txt"
            source_file.write_text(
                "\n".join(
                    [
                        "https://boards.greenhouse.io/acme",
                        "https://jobs.lever.co/northwind",
                        "https://jobs.ashbyhq.com/signalops",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            env = {"JOBS_AI_DB_PATH": str(project_root / "runtime" / "jobs_ai.db")}

            with (
                patch("jobs_ai.workspace.discover_project_root", return_value=project_root),
                patch("jobs_ai.collect.census._current_utc_datetime", return_value=FIXED_CREATED_AT),
                patch(
                    "jobs_ai.collect.census.fetch_text",
                    new=self._fixture_fetcher(
                        {
                            "https://boards.greenhouse.io/acme": _fixture_text("greenhouse_board.html"),
                            "https://jobs.lever.co/northwind": _fixture_text("lever_board.html"),
                            "https://jobs.ashbyhq.com/signalops": _fixture_text("ashby_board.html"),
                        }
                    ),
                ),
            ):
                result = RUNNER.invoke(
                    app,
                    ["board-census", "--from-file", str(source_file), "--label", "fixture-run"],
                    env=env,
                )

        self.assertEqual(result.exit_code, 0)
        self.assertIn("Boards configured: 3", result.stdout)
        self.assertIn("Boards counted: 3", result.stdout)
        self.assertIn("Boards failed: 0", result.stdout)
        self.assertIn("Per-board counts", result.stdout)
        self.assertIn("Greenhouse | https://boards.greenhouse.io/acme | 2", result.stdout)
        self.assertIn("Lever | https://jobs.lever.co/northwind | 2", result.stdout)
        self.assertIn("Ashby | https://jobs.ashbyhq.com/signalops | 2", result.stdout)
        self.assertIn("Portal totals", result.stdout)
        self.assertIn("Greenhouse: 2", result.stdout)
        self.assertIn("Lever: 2", result.stdout)
        self.assertIn("Ashby: 2", result.stdout)
        self.assertIn("Workday: not supported in board-census yet", result.stdout)
        self.assertIn("Grand total: 6", result.stdout)

    def test_cli_board_census_rejects_empty_source_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            project_root = Path(tmp_dir) / "workspace"
            project_root.mkdir(parents=True, exist_ok=True)
            source_file = project_root / "boards.txt"
            source_file.write_text("# no boards yet\n\n", encoding="utf-8")
            env = {"JOBS_AI_DB_PATH": str(project_root / "runtime" / "jobs_ai.db")}

            with patch("jobs_ai.workspace.discover_project_root", return_value=project_root):
                result = RUNNER.invoke(
                    app,
                    ["board-census", "--from-file", str(source_file)],
                    env=env,
                )

        self.assertEqual(result.exit_code, 1)
        self.assertIn("jobs_ai board-census", result.stdout)
        self.assertIn("at least one board URL is required", result.stdout)

    @staticmethod
    def _fixture_fetcher(payloads: dict[str, str]):
        def fetcher(request: FetchRequest) -> FetchResponse:
            if request.url not in payloads:
                raise AssertionError(f"unexpected fetch URL: {request.url}")
            return FetchResponse(
                url=request.url,
                final_url=request.url,
                status_code=200,
                content_type="text/html; charset=utf-8",
                text=payloads[request.url],
            )

        return fetcher


if __name__ == "__main__":
    unittest.main()
