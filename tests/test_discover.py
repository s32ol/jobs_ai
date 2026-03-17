from __future__ import annotations

from contextlib import closing, redirect_stderr, redirect_stdout
from datetime import datetime, timezone
import io
from pathlib import Path
import json
import sys
import tempfile
import unittest
from unittest.mock import patch
from urllib.parse import quote

from typer.testing import CliRunner

SRC_PATH = Path(__file__).resolve().parents[1] / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

from jobs_ai.cli import _resolve_discover_query, app, run as cli_run
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


def _duckduckgo_challenge_html(query: str = "python backend engineer remote") -> str:
    return (
        "<!doctype html><html><head><title>DuckDuckGo</title></head><body>"
        '<form id="challenge-form" action="/anomaly.js">'
        '<div class="anomaly-modal">Unfortunately, bots use DuckDuckGo too.</div>'
        "<div>Select all squares containing a duck:</div>"
        f"<input name=\"q\" value=\"{query}\">"
        "</form></body></html>"
    )


def _duckduckgo_zero_results_html(query: str = "python backend engineer remote") -> str:
    return (
        "<!doctype html><html><head><title>DuckDuckGo</title></head><body>"
        f"<div>No results found for {query}</div>"
        "</body></html>"
    )


def _duckduckgo_parse_failure_html() -> str:
    return (
        "<!doctype html><html><head><title>DuckDuckGo</title></head><body>"
        '<div class="result results_links">'
        '<a class="result__a" href="/l/?uddg=">Broken outbound result</a>'
        "</div></body></html>"
    )


def _empty_search_payloads(search_plans) -> dict[str, str]:
    return {
        plan.search_url: _duckduckgo_zero_results_html(plan.search_text)
        for plan in search_plans
    }


def _sequence_fetcher(payloads: dict[str, list[FetchResponse | Exception | str]]):
    counters = {url: 0 for url in payloads}

    def fetcher(request: FetchRequest) -> FetchResponse:
        queue = payloads.get(request.url)
        if queue is None:
            raise FetchError(f"unable to fetch {request.url}: no fixture available")
        index = counters[request.url]
        if index >= len(queue):
            payload = queue[-1]
        else:
            payload = queue[index]
            counters[request.url] = index + 1
        if isinstance(payload, Exception):
            raise payload
        if isinstance(payload, FetchResponse):
            return payload
        return _response(request.url, payload)

    return fetcher


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

    def test_run_discovery_confirms_modern_greenhouse_board_root_from_regional_direct_job_hit(self) -> None:
        query = "partner solution engineer remote"
        search_plans = build_search_plans(query)
        greenhouse_search_url = search_plans[0].search_url
        direct_job_url = "https://job-boards.eu.greenhouse.io/parloa/jobs/4780830101"
        board_root_url = "https://job-boards.eu.greenhouse.io/parloa"
        fetcher = _mapping_fetcher(
            {
                **_empty_search_payloads(search_plans),
                greenhouse_search_url: _search_results_html(direct_job_url),
                board_root_url: _response(
                    board_root_url,
                    _fixture_text("greenhouse_board_modern.html"),
                    final_url=board_root_url,
                ),
            }
        )

        run = run_discovery(
            query,
            limit=10,
            timeout_seconds=5.0,
            created_at=FIXED_CREATED_AT,
            fetcher=fetcher,
        )

        self.assertEqual(run.report.confirmed_count, 1)
        self.assertEqual(run.report.manual_review_count, 0)
        self.assertEqual(run.confirmed_sources, (board_root_url,))
        self.assertEqual(
            run.report.candidate_results[0].candidate.supporting_results[0].target_url,
            direct_job_url,
        )

    def test_run_discovery_normalizes_regional_greenhouse_job_board_hits(self) -> None:
        query = "forward deployed engineer remote"
        search_plans = build_search_plans(query)
        greenhouse_search_url = search_plans[0].search_url
        fetcher = _mapping_fetcher(
            {
                **_empty_search_payloads(search_plans),
                greenhouse_search_url: _search_results_html(
                    "https://job-boards.eu.greenhouse.io/parloa/jobs/4694390101",
                ),
                "https://job-boards.eu.greenhouse.io/parloa": _fixture_text("greenhouse_board_modern.html"),
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
        self.assertEqual(run.report.candidate_source_count, 1)
        self.assertEqual(run.report.confirmed_count, 1)
        self.assertEqual(run.report.manual_review_count, 0)
        self.assertEqual(run.confirmed_sources, ("https://job-boards.eu.greenhouse.io/parloa",))
        self.assertEqual(
            run.report.candidate_results[0].candidate.supporting_results[0].target_url,
            "https://job-boards.eu.greenhouse.io/parloa/jobs/4694390101",
        )

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

    def test_run_discovery_classifies_duckduckgo_challenge_as_provider_anomaly(self) -> None:
        query = "python backend engineer remote"
        search_plans = build_search_plans(query)
        fetcher = _mapping_fetcher(
            {
                plan.search_url: _duckduckgo_challenge_html(plan.search_text)
                for plan in search_plans
            }
        )

        with patch("jobs_ai.discover.search.SEARCH_RETRY_DELAYS_SECONDS", (0.0, 0.0)):
            run = run_discovery(
                query,
                limit=10,
                timeout_seconds=5.0,
                created_at=FIXED_CREATED_AT,
                fetcher=fetcher,
            )

        self.assertEqual(run.report.raw_hit_count, 0)
        self.assertEqual(run.report.confirmed_count, 0)
        self.assertTrue(run.report.has_fatal_search_failure)
        self.assertEqual(run.report.search_results[0].status, "provider_anomaly")
        self.assertEqual(run.report.search_results[0].attempt_count, 3)
        self.assertIn("DuckDuckGo challenge", run.report.search_results[0].error)
        self.assertIn(
            "duckduckgo_anomaly_modal",
            run.report.search_results[0].evidence.detected_patterns,
        )

    def test_run_discovery_classifies_explicit_zero_results(self) -> None:
        query = "python backend engineer remote"
        search_plans = build_search_plans(query)
        fetcher = _mapping_fetcher(
            {
                plan.search_url: _duckduckgo_zero_results_html(plan.search_text)
                for plan in search_plans
            }
        )

        run = run_discovery(
            query,
            limit=10,
            timeout_seconds=5.0,
            created_at=FIXED_CREATED_AT,
            fetcher=fetcher,
        )

        self.assertEqual(run.report.raw_hit_count, 0)
        self.assertEqual(run.report.confirmed_count, 0)
        self.assertFalse(run.report.has_fatal_search_failure)
        self.assertEqual(run.report.search_results[0].status, "zero_results")
        self.assertEqual(run.report.search_results[0].attempt_count, 1)

    def test_run_discovery_classifies_result_page_parse_failure(self) -> None:
        query = "python backend engineer remote"
        search_plans = build_search_plans(query)
        fetcher = _mapping_fetcher(
            {
                plan.search_url: _duckduckgo_parse_failure_html()
                for plan in search_plans
            }
        )

        with patch("jobs_ai.discover.search.SEARCH_RETRY_DELAYS_SECONDS", (0.0, 0.0)):
            run = run_discovery(
                query,
                limit=10,
                timeout_seconds=5.0,
                created_at=FIXED_CREATED_AT,
                fetcher=fetcher,
            )

        self.assertEqual(run.report.search_results[0].status, "parse_failure")
        self.assertEqual(run.report.search_results[0].attempt_count, 3)
        self.assertIn(
            "search_result_parse_miss",
            run.report.search_results[0].evidence.detected_patterns,
        )

    def test_run_discovery_retries_provider_anomaly_then_recovers(self) -> None:
        query = "backend engineer remote"
        search_plans = build_search_plans(query)
        greenhouse_search_url = search_plans[0].search_url
        fetcher = _sequence_fetcher(
            {
                greenhouse_search_url: [
                    _duckduckgo_challenge_html(search_plans[0].search_text),
                    _search_results_html("https://boards.greenhouse.io/acme/jobs/12345"),
                ],
                **{
                    plan.search_url: [_duckduckgo_zero_results_html(plan.search_text)]
                    for plan in search_plans[1:]
                },
                "https://boards.greenhouse.io/acme": [_fixture_text("greenhouse_board.html")],
            }
        )

        with patch("jobs_ai.discover.search.SEARCH_RETRY_DELAYS_SECONDS", (0.0, 0.0)):
            run = run_discovery(
                query,
                limit=10,
                timeout_seconds=5.0,
                created_at=FIXED_CREATED_AT,
                fetcher=fetcher,
            )

        self.assertEqual(run.report.confirmed_count, 1)
        self.assertEqual(run.confirmed_sources, ("https://boards.greenhouse.io/acme",))
        self.assertEqual(run.report.search_results[0].status, "success")
        self.assertEqual(run.report.search_results[0].attempt_count, 2)
        self.assertEqual(run.report.search_results[0].attempts[0].status, "provider_anomaly")
        self.assertEqual(run.report.search_results[0].attempts[1].status, "success")

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

    def test_run_discover_command_marks_search_failure_and_can_capture_raw_html(self) -> None:
        query = "python backend engineer remote"
        search_plans = build_search_plans(query)
        payloads = {
            plan.search_url: _duckduckgo_challenge_html(plan.search_text)
            for plan in search_plans
        }

        with tempfile.TemporaryDirectory() as tmp_dir:
            project_root = Path(tmp_dir) / "workspace"
            project_root.mkdir(parents=True, exist_ok=True)
            paths = build_workspace_paths(Path("data/jobs_ai.db"), project_root=project_root)
            output_dir = project_root / "artifacts"

            with patch("jobs_ai.discover.search.SEARCH_RETRY_DELAYS_SECONDS", (0.0, 0.0)):
                run_without_capture = run_discover_command(
                    paths,
                    query=query,
                    limit=10,
                    out_dir=output_dir / "without-capture",
                    label="no-capture",
                    timeout_seconds=5.0,
                    report_only=False,
                    collect=True,
                    import_results=True,
                    capture_search_artifacts=False,
                    created_at=FIXED_CREATED_AT,
                    fetcher=_mapping_fetcher(payloads),
                )

                run_with_capture = run_discover_command(
                    paths,
                    query=query,
                    limit=10,
                    out_dir=output_dir / "with-capture",
                    label="with-capture",
                    timeout_seconds=5.0,
                    report_only=False,
                    collect=True,
                    import_results=True,
                    capture_search_artifacts=True,
                    created_at=FIXED_CREATED_AT,
                    fetcher=_mapping_fetcher(payloads),
                )

            self.assertTrue(run_without_capture.report.has_fatal_search_failure)
            self.assertEqual(run_without_capture.report.collect_summary.status, "skipped_search_failure")
            self.assertEqual(run_without_capture.report.import_summary.status, "skipped_search_failure")
            self.assertEqual(run_without_capture.report.search_results[0].raw_artifact_paths, ())

            without_artifacts = run_without_capture.report.artifact_paths
            assert without_artifacts is not None
            self.assertIsNone(without_artifacts.search_artifact_dir)

            with_artifacts = run_with_capture.report.artifact_paths
            assert with_artifacts is not None
            assert with_artifacts.search_artifact_dir is not None
            self.assertTrue(with_artifacts.search_artifact_dir.exists())
            self.assertTrue(run_with_capture.report.search_results[0].raw_artifact_paths)
            self.assertTrue(run_with_capture.report.search_results[0].raw_artifact_paths[0].exists())

            discover_report_payload = json.loads(
                with_artifacts.discover_report_path.read_text(encoding="utf-8")
            )
            self.assertEqual(discover_report_payload["status"], "failed")
            self.assertTrue(discover_report_payload["search_failure"])
            self.assertEqual(discover_report_payload["search_results"][0]["status"], "provider_anomaly")
            self.assertEqual(discover_report_payload["search_results"][0]["attempt_count"], 3)
            self.assertTrue(discover_report_payload["search_results"][0]["raw_artifact_paths"])
            self.assertEqual(
                discover_report_payload["collect"]["status"],
                "skipped_search_failure",
            )
            self.assertEqual(
                discover_report_payload["import"]["status"],
                "skipped_search_failure",
            )

            summary = render_discover_report(run_with_capture.report)
            self.assertIn("status: failed", summary)
            self.assertIn("search summary:", summary)
            self.assertIn("provider_anomaly: 6", summary)
            self.assertIn("boards.greenhouse.io | provider_anomaly", summary)
            self.assertIn("search artifacts dir:", summary)

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
            self.assertEqual(discover_report_payload["status"], "success")
            self.assertFalse(discover_report_payload["search_failure"])
            self.assertEqual(discover_report_payload["confirmed_count"], 1)
            self.assertEqual(discover_report_payload["search_results"][0]["status"], "success")
            self.assertEqual(discover_report_payload["search_results"][0]["attempt_count"], 1)
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

    def test_run_discover_command_chains_collect_import_for_modern_greenhouse_board(self) -> None:
        query = "forward deployed engineer remote"
        search_plans = build_search_plans(query)
        fetcher = _mapping_fetcher(
            {
                **_empty_search_payloads(search_plans),
                search_plans[0].search_url: _search_results_html(
                    "https://job-boards.eu.greenhouse.io/parloa/jobs/4694390101"
                ),
                "https://job-boards.eu.greenhouse.io/parloa": _fixture_text("greenhouse_board_modern.html"),
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
                label="modern-greenhouse",
                timeout_seconds=5.0,
                report_only=False,
                collect=True,
                import_results=True,
                created_at=FIXED_CREATED_AT,
                fetcher=fetcher,
            )

            artifact_paths = run.report.artifact_paths
            assert artifact_paths is not None
            collect_summary = run.report.collect_summary
            import_summary = run.report.import_summary
            assert collect_summary is not None
            assert import_summary is not None

            self.assertEqual(run.report.confirmed_count, 1)
            self.assertEqual(run.confirmed_sources, ("https://job-boards.eu.greenhouse.io/parloa",))
            self.assertEqual(collect_summary.status, "success")
            self.assertEqual(collect_summary.collected_count, 2)
            self.assertEqual(import_summary.status, "success")
            self.assertEqual(import_summary.inserted_count, 2)
            self.assertTrue((output_dir / "collect" / "leads.import.json").exists())

            with closing(connect_database(paths.database_path)) as connection:
                row = connection.execute("SELECT COUNT(*) AS count FROM jobs").fetchone()

            self.assertEqual(row["count"], 2)

    def test_cli_discover_requires_query_and_prints_summary(self) -> None:
        result = RUNNER.invoke(app, ["discover"])
        self.assertEqual(result.exit_code, 1)
        self.assertIn("provide a discover query", result.stdout)

    def test_cli_discover_exits_nonzero_for_fatal_search_anomaly(self) -> None:
        query = "python backend engineer remote"
        search_plans = build_search_plans(query)
        fetcher = _mapping_fetcher(
            {
                plan.search_url: _duckduckgo_challenge_html(plan.search_text)
                for plan in search_plans
            }
        )

        with tempfile.TemporaryDirectory() as tmp_dir:
            project_root = Path(tmp_dir) / "workspace"
            project_root.mkdir(parents=True, exist_ok=True)
            database_path = project_root / "runtime" / "jobs_ai.db"
            env = {"JOBS_AI_DB_PATH": str(database_path)}

            with patch("jobs_ai.workspace.discover_project_root", return_value=project_root):
                with patch("jobs_ai.discover.cli.fetch_text", side_effect=fetcher):
                    with patch("jobs_ai.discover.search.SEARCH_RETRY_DELAYS_SECONDS", (0.0, 0.0)):
                        result = RUNNER.invoke(
                            app,
                            ["discover", query, "--capture-search-artifacts"],
                            env=env,
                        )

        self.assertEqual(result.exit_code, 1)
        self.assertIn("status: failed", result.stdout)
        self.assertIn("provider_anomaly", result.stdout)
        self.assertIn("search artifacts dir:", result.stdout)

    def test_cli_run_function_returns_nonzero_for_fatal_search_anomaly(self) -> None:
        query = "python backend engineer remote"
        search_plans = build_search_plans(query)
        fetcher = _mapping_fetcher(
            {
                plan.search_url: _duckduckgo_challenge_html(plan.search_text)
                for plan in search_plans
            }
        )

        with tempfile.TemporaryDirectory() as tmp_dir:
            project_root = Path(tmp_dir) / "workspace"
            project_root.mkdir(parents=True, exist_ok=True)
            database_path = project_root / "runtime" / "jobs_ai.db"
            with patch.dict("os.environ", {"JOBS_AI_DB_PATH": str(database_path)}, clear=False):
                with patch("jobs_ai.workspace.discover_project_root", return_value=project_root):
                    with patch("jobs_ai.discover.cli.fetch_text", side_effect=fetcher):
                        with patch("jobs_ai.discover.search.SEARCH_RETRY_DELAYS_SECONDS", (0.0, 0.0)):
                            stdout_buffer = io.StringIO()
                            stderr_buffer = io.StringIO()
                            with redirect_stdout(stdout_buffer), redirect_stderr(stderr_buffer):
                                exit_code = cli_run(
                                    [
                                        "discover",
                                        query,
                                        "--capture-search-artifacts",
                                    ]
                                )

        self.assertEqual(exit_code, 1)


if __name__ == "__main__":
    unittest.main()
