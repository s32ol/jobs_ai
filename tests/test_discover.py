from __future__ import annotations

from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path
import json
import sys
import tempfile
import unittest
from urllib.parse import quote

from typer.testing import CliRunner

SRC_PATH = Path(__file__).resolve().parents[1] / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

from jobs_ai.cli import _resolve_discover_query, app
from jobs_ai.collect.fetch import FetchError, FetchRequest, FetchResponse
from jobs_ai.db import connect_database
from jobs_ai.discover.cli import run_discover_command
from jobs_ai.discover.harness import run_discovery
from jobs_ai.discover.search import build_search_plans
from jobs_ai.main import render_discover_report
from jobs_ai.workspace import build_workspace_paths

FIXTURE_ROOT = Path(__file__).resolve().parent / "fixtures" / "collect"
RUNNER = CliRunner()
FIXED_CREATED_AT = datetime(2026, 3, 15, 4, 20, 0, tzinfo=timezone.utc)
FIXED_TIMESTAMP = FIXED_CREATED_AT.isoformat(timespec="seconds").replace("+00:00", "Z")


def _fixture_text(name: str) -> str:
    return (FIXTURE_ROOT / name).read_text(encoding="utf-8")


def _response(
    url: str,
    text: str,
    *,
    final_url: str | None = None,
    status_code: int = 200,
    content_type: str = "text/html; charset=utf-8",
) -> FetchResponse:
    return FetchResponse(
        url=url,
        final_url=final_url or url,
        status_code=status_code,
        content_type=content_type,
        text=text,
    )


def _mapping_fetcher(payloads: dict[str, FetchResponse | Exception | str]):
    def fetcher(request: FetchRequest) -> FetchResponse:
        payload = payloads.get(request.url)
        if payload is None:
            raise FetchError(f"unable to fetch {request.url}: no fixture available")
        if isinstance(payload, Exception):
            raise payload
        if isinstance(payload, FetchResponse):
            return payload
        return _response(request.url, payload)

    return fetcher


def _search_results_html(*target_urls: str) -> str:
    links = "\n".join(
        f'<a href="https://duckduckgo.com/l/?uddg={quote(url, safe="")}">{url}</a>'
        for url in target_urls
    )
    return f"<!doctype html><html><body>{links}</body></html>"


def _empty_search_payloads(search_plans) -> dict[str, str]:
    return {
        plan.search_url: "<html><body></body></html>"
        for plan in search_plans
    }


class DiscoverTest(unittest.TestCase):
    def test_resolve_discover_query_accepts_argument_or_option_and_rejects_bad_input(self) -> None:
        self.assertEqual(
            _resolve_discover_query("python backend engineer remote", None),
            "python backend engineer remote",
        )
        self.assertEqual(
            _resolve_discover_query(None, "data analyst remote"),
            "data analyst remote",
        )
        self.assertEqual(
            _resolve_discover_query("ml engineer remote", "ml engineer remote"),
            "ml engineer remote",
        )
        with self.assertRaisesRegex(ValueError, "provide a discover query"):
            _resolve_discover_query(None, None)
        with self.assertRaisesRegex(ValueError, "must match"):
            _resolve_discover_query("python", "data")

    def test_run_discovery_dedupes_search_hits_to_one_supported_source(self) -> None:
        query = "platform data engineer remote"
        search_plans = build_search_plans(query)
        greenhouse_search_url = search_plans[0].search_url
        fetcher = _mapping_fetcher(
            {
                **_empty_search_payloads(search_plans),
                greenhouse_search_url: _search_results_html(
                    "https://boards.greenhouse.io/acme/jobs/12345?gh_src=linkedin",
                    "https://boards.greenhouse.io/acme",
                ),
                "https://boards.greenhouse.io/acme": _fixture_text("greenhouse_board.html"),
            }
        )

        run = run_discovery(
            query,
            limit=10,
            timeout_seconds=5.0,
            created_at=FIXED_CREATED_AT,
            fetcher=fetcher,
        )

        self.assertEqual(run.report.raw_hit_count, 2)
        self.assertEqual(run.report.candidate_source_count, 1)
        self.assertEqual(run.report.confirmed_count, 1)
        self.assertEqual(run.report.manual_review_count, 0)
        self.assertEqual(run.confirmed_sources, ("https://boards.greenhouse.io/acme",))
        self.assertEqual(run.report.candidate_results[0].candidate.supporting_results[0].target_url, "https://boards.greenhouse.io/acme/jobs/12345?gh_src=linkedin")

    def test_run_discovery_ignores_search_engine_internal_links(self) -> None:
        query = "site reliability engineer remote"
        search_plans = build_search_plans(query)
        greenhouse_search_url = search_plans[0].search_url
        fetcher = _mapping_fetcher(
            {
                **_empty_search_payloads(search_plans),
                greenhouse_search_url: (
                    "<!doctype html><html><body>"
                    '<a href="https://duckduckgo.com/html/?q=ignored">ignored nav</a>'
                    f'<a href="https://duckduckgo.com/l/?uddg={quote("https://boards.greenhouse.io/acme", safe="")}">acme</a>'
                    "</body></html>"
                ),
                "https://boards.greenhouse.io/acme": _fixture_text("greenhouse_board.html"),
            }
        )

        run = run_discovery(
            query,
            limit=10,
            timeout_seconds=5.0,
            created_at=FIXED_CREATED_AT,
            fetcher=fetcher,
        )

        self.assertEqual(run.report.raw_hit_count, 1)
        self.assertEqual(run.report.manual_review_count, 0)
        self.assertEqual(run.report.confirmed_count, 1)

    def test_run_discovery_marks_ambiguous_supported_source_for_manual_review(self) -> None:
        query = "data analyst remote"
        search_plans = build_search_plans(query)
        fetcher = _mapping_fetcher(
            {
                **_empty_search_payloads(search_plans),
                search_plans[0].search_url: _search_results_html("https://boards.greenhouse.io/acme"),
                "https://boards.greenhouse.io/acme": _fixture_text("greenhouse_board_partial.html"),
            }
        )

        run = run_discovery(
            query,
            limit=10,
            timeout_seconds=5.0,
            created_at=FIXED_CREATED_AT,
            fetcher=fetcher,
        )

        self.assertEqual(run.report.confirmed_count, 0)
        self.assertEqual(run.report.manual_review_count, 1)
        self.assertEqual(run.report.candidate_results[0].reason_code, "verification_greenhouse_parse_ambiguous")
        self.assertEqual(run.confirmed_sources, ())

    def test_run_discovery_writes_workday_hits_to_manual_review_artifact(self) -> None:
        query = "staff data engineer remote"
        search_plans = build_search_plans(query)
        workday_plan = next(
            plan
            for plan in search_plans
            if plan.portal_type == "workday" and plan.site_filter == "myworkdayjobs.com"
        )
        original_workday_url = (
            "https://wd5.myworkdayjobs.com/en-US/Company/job/Title-ID/apply"
            "?source=linkedin&utm_campaign=spring"
        )
        normalized_workday_url = "https://wd5.myworkdayjobs.com/en-US/Company/job/Title-ID"
        fetcher = _mapping_fetcher(
            {
                **_empty_search_payloads(search_plans),
                workday_plan.search_url: _search_results_html(
                    original_workday_url,
                    normalized_workday_url,
                ),
            }
        )

        with tempfile.TemporaryDirectory() as tmp_dir:
            project_root = Path(tmp_dir) / "workspace"
            project_root.mkdir(parents=True, exist_ok=True)
            paths = build_workspace_paths(Path("data/jobs_ai.db"), project_root=project_root)
            output_dir = project_root / "artifacts"

            run = run_discover_command(
                paths,
                query=query,
                limit=10,
                out_dir=output_dir,
                label="workday-review",
                timeout_seconds=5.0,
                report_only=False,
                collect=False,
                import_results=False,
                created_at=FIXED_CREATED_AT,
                fetcher=fetcher,
            )

            artifact_paths = run.report.artifact_paths
            assert artifact_paths is not None
            manual_review_payload = json.loads(
                artifact_paths.manual_review_sources_path.read_text(encoding="utf-8")
            )
            item = manual_review_payload["items"][0]

            self.assertEqual(run.report.confirmed_count, 0)
            self.assertEqual(run.report.manual_review_count, 1)
            self.assertEqual(item["portal_type"], "workday")
            self.assertEqual(item["reason_code"], "workday_manual_review")
            self.assertEqual(item["source_query"], workday_plan.search_text)
            self.assertEqual(item["original_url"], original_workday_url)
            self.assertEqual(item["normalized_url"], normalized_workday_url)
            self.assertEqual(len(item["supporting_results"]), 2)

            summary = render_discover_report(run.report)
            self.assertIn("workday sources (manual review): 1", summary)
            self.assertIn("manual review (Workday portal)", summary)

    def test_run_discover_command_writes_artifacts_and_chains_collect_import(self) -> None:
        query = "backend engineer remote"
        search_plans = build_search_plans(query)
        fetcher = _mapping_fetcher(
            {
                **_empty_search_payloads(search_plans),
                search_plans[0].search_url: _search_results_html("https://boards.greenhouse.io/acme/jobs/12345"),
                "https://boards.greenhouse.io/acme": _fixture_text("greenhouse_board.html"),
            }
        )

        with tempfile.TemporaryDirectory() as tmp_dir:
            project_root = Path(tmp_dir) / "workspace"
            project_root.mkdir(parents=True, exist_ok=True)
            paths = build_workspace_paths(Path("data/jobs_ai.db"), project_root=project_root)
            output_dir = project_root / "artifacts"

            run = run_discover_command(
                paths,
                query=query,
                limit=10,
                out_dir=output_dir,
                label="discovery-batch",
                timeout_seconds=5.0,
                report_only=False,
                collect=True,
                import_results=True,
                created_at=FIXED_CREATED_AT,
                fetcher=fetcher,
            )

            artifact_paths = run.report.artifact_paths
            assert artifact_paths is not None
            confirmed_text = artifact_paths.confirmed_sources_path.read_text(encoding="utf-8")
            manual_review_payload = json.loads(
                artifact_paths.manual_review_sources_path.read_text(encoding="utf-8")
            )
            discover_report_payload = json.loads(
                artifact_paths.discover_report_path.read_text(encoding="utf-8")
            )

            collect_summary = run.report.collect_summary
            import_summary = run.report.import_summary
            assert collect_summary is not None
            assert import_summary is not None

            self.assertEqual(run.report.run_id, "discover-discovery-batch-20260315T042000000000Z")
            self.assertEqual(confirmed_text, "https://boards.greenhouse.io/acme\n")
            self.assertEqual(manual_review_payload["item_count"], 0)
            self.assertEqual(discover_report_payload["run_id"], "discover-discovery-batch-20260315T042000000000Z")
            self.assertEqual(discover_report_payload["created_at"], FIXED_TIMESTAMP)
            self.assertEqual(discover_report_payload["confirmed_count"], 1)
            self.assertEqual(discover_report_payload["collect"]["status"], "success")
            self.assertEqual(discover_report_payload["import"]["status"], "success")
            self.assertTrue(collect_summary.executed)
            self.assertEqual(collect_summary.collected_count, 2)
            self.assertTrue(import_summary.executed)
            self.assertEqual(import_summary.inserted_count, 2)
            self.assertEqual(import_summary.errors, ())
            self.assertTrue((output_dir / "collect" / "leads.import.json").exists())

            with closing(connect_database(paths.database_path)) as connection:
                row = connection.execute("SELECT COUNT(*) AS count FROM jobs").fetchone()

            self.assertEqual(row["count"], 2)

    def test_cli_discover_requires_query_and_prints_summary(self) -> None:
        result = RUNNER.invoke(app, ["discover"])
        self.assertEqual(result.exit_code, 1)
        self.assertIn("provide a discover query", result.stdout)


if __name__ == "__main__":
    unittest.main()
