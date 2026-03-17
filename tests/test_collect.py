from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
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
from jobs_ai.collect.cli import run_collect_command
from jobs_ai.collect.fetch import FetchError, FetchRequest, FetchResponse
from jobs_ai.collect.harness import run_collection
from jobs_ai.db import connect_database, initialize_schema
from jobs_ai.main import render_collect_report
from jobs_ai.jobs.importer import import_jobs_from_file
from jobs_ai.workspace import build_workspace_paths

FIXTURE_ROOT = Path(__file__).resolve().parent / "fixtures" / "collect"
RUNNER = CliRunner()
FIXED_CREATED_AT = datetime(2026, 3, 13, 19, 45, 0, tzinfo=timezone.utc)
FIXED_TIMESTAMP = FIXED_CREATED_AT.isoformat(timespec="seconds").replace("+00:00", "Z")


def _fixture_text(name: str) -> str:
    return (FIXTURE_ROOT / name).read_text(encoding="utf-8")


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


class CollectTest(unittest.TestCase):
    def test_run_collection_collects_greenhouse_board_postings(self) -> None:
        run = run_collection(
            ["https://boards.greenhouse.io/acme"],
            timeout_seconds=10.0,
            created_at=FIXED_CREATED_AT,
            fetcher=_fixture_fetcher(
                {
                    "https://boards.greenhouse.io/acme": _fixture_text("greenhouse_board.html"),
                }
            ),
        )

        self.assertEqual(run.report.collected_count, 2)
        self.assertEqual(run.report.manual_review_count, 0)
        self.assertEqual(run.report.skipped_count, 0)
        self.assertEqual(run.report.source_results[0].outcome, "collected")
        self.assertEqual(
            [lead.title for lead in run.collected_leads],
            ["Data Engineer", "Analytics Engineer"],
        )
        self.assertEqual(
            [lead.location for lead in run.collected_leads],
            ["Remote", "San Jose, CA"],
        )
        self.assertTrue(all(lead.portal_type == "greenhouse" for lead in run.collected_leads))

    def test_run_collection_collects_modern_greenhouse_board_postings_from_initial_state_payload(self) -> None:
        source_url = "https://boards.greenhouse.io/parloa"

        def fetcher(request: FetchRequest) -> FetchResponse:
            self.assertEqual(request.url, source_url)
            return FetchResponse(
                url=request.url,
                final_url="https://job-boards.eu.greenhouse.io/parloa",
                status_code=200,
                content_type="text/html; charset=utf-8",
                text=_fixture_text("greenhouse_board_modern.html"),
            )

        run = run_collection(
            [source_url],
            timeout_seconds=10.0,
            created_at=FIXED_CREATED_AT,
            fetcher=fetcher,
        )

        self.assertEqual(run.report.collected_count, 2)
        self.assertEqual(run.report.manual_review_count, 0)
        self.assertEqual(run.report.skipped_count, 0)
        self.assertEqual(run.report.source_results[0].outcome, "collected")
        self.assertEqual(run.report.source_results[0].source.portal_type, "greenhouse")
        self.assertEqual(
            [lead.title for lead in run.collected_leads],
            ["Forward Deployed Engineer, DevOps", "Partner Solution Engineer"],
        )
        self.assertEqual(
            [lead.location for lead in run.collected_leads],
            ["New York Office", "Remotely in the USA"],
        )
        self.assertEqual(
            [lead.source_job_id for lead in run.collected_leads],
            ["4694390101", "4780830101"],
        )

    def test_run_collection_collects_modern_greenhouse_direct_job_from_remix_payload(self) -> None:
        source_url = "https://job-boards.eu.greenhouse.io/parloa/jobs/4694390101"
        run = run_collection(
            [source_url],
            timeout_seconds=10.0,
            created_at=FIXED_CREATED_AT,
            fetcher=_fixture_fetcher(
                {
                    source_url: _fixture_text("greenhouse_job_modern.html"),
                }
            ),
        )

        self.assertEqual(run.report.collected_count, 1)
        self.assertEqual(run.report.manual_review_count, 0)
        self.assertEqual(run.report.skipped_count, 0)
        self.assertEqual(run.report.source_results[0].source.portal_type, "greenhouse")
        self.assertEqual(run.collected_leads[0].company, "Parloa")
        self.assertEqual(run.collected_leads[0].title, "Forward Deployed Engineer, DevOps")
        self.assertEqual(run.collected_leads[0].location, "New York Office")
        self.assertEqual(
            run.collected_leads[0].apply_url,
            "https://job-boards.eu.greenhouse.io/parloa/jobs/4694390101",
        )
        self.assertEqual(run.collected_leads[0].source_job_id, "4694390101")
        self.assertEqual(run.collected_leads[0].salary_text, "Salary Range: $225,000 - $335,000 USD")

    def test_run_collection_collects_lever_direct_job(self) -> None:
        run = run_collection(
            ["https://jobs.lever.co/northwind/lever-data-1"],
            timeout_seconds=10.0,
            created_at=FIXED_CREATED_AT,
            fetcher=_fixture_fetcher(
                {
                    "https://jobs.lever.co/northwind/lever-data-1": _fixture_text("lever_job.html"),
                }
            ),
        )

        self.assertEqual(run.report.collected_count, 1)
        self.assertEqual(run.collected_leads[0].company, "Northwind Talent")
        self.assertEqual(run.collected_leads[0].title, "Platform Data Engineer")
        self.assertEqual(run.collected_leads[0].location, "Remote")
        self.assertEqual(run.collected_leads[0].source_job_id, "lever-data-1")

    def test_run_collection_collects_ashby_board_postings(self) -> None:
        run = run_collection(
            ["https://jobs.ashbyhq.com/signalops"],
            timeout_seconds=10.0,
            created_at=FIXED_CREATED_AT,
            fetcher=_fixture_fetcher(
                {
                    "https://jobs.ashbyhq.com/signalops": _fixture_text("ashby_board.html"),
                }
            ),
        )

        self.assertEqual(run.report.collected_count, 2)
        self.assertEqual(
            [lead.source_job_id for lead in run.collected_leads],
            ["ashby-data-1", "ashby-obs-2"],
        )
        self.assertEqual(
            [lead.company for lead in run.collected_leads],
            ["Signal Ops", "Signal Ops"],
        )

    def test_run_collection_emits_manual_review_for_missing_required_fields(self) -> None:
        run = run_collection(
            ["https://jobs.ashbyhq.com/signalops/ashby-platform-3"],
            timeout_seconds=10.0,
            created_at=FIXED_CREATED_AT,
            fetcher=_fixture_fetcher(
                {
                    "https://jobs.ashbyhq.com/signalops/ashby-platform-3": _fixture_text(
                        "ashby_job_missing_location.html"
                    ),
                }
            ),
        )

        self.assertEqual(run.report.collected_count, 0)
        self.assertEqual(run.report.manual_review_count, 1)
        self.assertEqual(run.report.skipped_count, 0)
        self.assertEqual(run.report.source_results[0].outcome, "manual_review")
        self.assertEqual(run.report.source_results[0].reason_code, "ashby_parse_ambiguous")
        assert run.report.source_results[0].manual_review_item is not None
        self.assertEqual(run.report.source_results[0].manual_review_item.reason_code, "ashby_parse_ambiguous")
        self.assertIn("missing title, location, or URL", run.report.source_results[0].reason)

    def test_run_collection_emits_manual_review_for_partial_supported_board_markup(self) -> None:
        run = run_collection(
            ["https://boards.greenhouse.io/acme"],
            timeout_seconds=10.0,
            created_at=FIXED_CREATED_AT,
            fetcher=_fixture_fetcher(
                {
                    "https://boards.greenhouse.io/acme": _fixture_text("greenhouse_board_partial.html"),
                }
            ),
        )

        self.assertEqual(run.report.collected_count, 0)
        self.assertEqual(run.report.manual_review_count, 1)
        self.assertEqual(run.report.skipped_count, 0)
        self.assertEqual(run.report.source_results[0].outcome, "manual_review")
        self.assertEqual(run.report.source_results[0].reason_code, "greenhouse_parse_ambiguous")
        self.assertIn("missing title, location, or URL", run.report.source_results[0].reason)

    def test_run_collection_emits_manual_review_for_accessible_unsupported_html_page(self) -> None:
        source_url = "https://careers.example.com/jobs/platform-data-engineer"
        html_text = """
<!doctype html>
<html>
  <head>
    <title>ExampleCo Careers | Platform Data Engineer</title>
    <script type="application/ld+json">
      {"@context":"https://schema.org","@type":"JobPosting","title":"Platform Data Engineer"}
    </script>
  </head>
  <body>
    <main>
      <h1>Platform Data Engineer</h1>
      <p>Job description</p>
      <a href="/apply/platform-data-engineer">Apply now</a>
    </main>
  </body>
</html>
""".strip()

        def fetcher(request: FetchRequest) -> FetchResponse:
            self.assertEqual(request.url, source_url)
            return FetchResponse(
                url=request.url,
                final_url="https://careers.example.com/openings/platform-data-engineer",
                status_code=200,
                content_type="text/html; charset=utf-8",
                text=html_text,
            )

        run = run_collection(
            [source_url],
            timeout_seconds=10.0,
            created_at=FIXED_CREATED_AT,
            fetcher=fetcher,
        )

        self.assertEqual(run.report.collected_count, 0)
        self.assertEqual(run.report.manual_review_count, 1)
        self.assertEqual(run.report.skipped_count, 0)
        result = run.report.source_results[0]
        self.assertEqual(result.outcome, "manual_review")
        self.assertEqual(result.reason_code, "unsupported_accessible_html")
        self.assertIn("Accessible HTML was fetched", result.reason)
        self.assertEqual(
            result.suggested_next_action,
            "Review the page manually and copy company, title, location, and apply URL into leads.import.json.",
        )
        assert result.manual_review_item is not None
        self.assertEqual(result.manual_review_item.source_url, source_url)
        self.assertEqual(result.manual_review_item.reason_code, "unsupported_accessible_html")
        self.assertEqual(result.manual_review_item.suggested_next_action, result.suggested_next_action)
        assert result.manual_review_item.evidence is not None
        self.assertEqual(result.manual_review_item.evidence.final_url, "https://careers.example.com/openings/platform-data-engineer")
        self.assertEqual(result.manual_review_item.evidence.status_code, 200)
        self.assertEqual(
            result.manual_review_item.evidence.detected_patterns,
            ("job_posting_json_ld", "careers_keyword", "apply_keyword", "job_detail_keyword"),
        )

    def test_run_collection_surfaces_workday_manual_review_with_context_hints(self) -> None:
        source_url = (
            "https://acme.wd5.myworkdayjobs.com/en-US/External/job/Remote-USA/Data-Engineer_R12345?source=linkedin"
        )
        html_text = """
<!doctype html>
<html>
  <head>
    <title>Workday Careers | Data Engineer</title>
  </head>
  <body>
    <main>
      <h1>Data Engineer</h1>
      <p>Workday powered application page</p>
      <a href="/apply">Apply now</a>
    </main>
  </body>
</html>
""".strip()

        def fetcher(request: FetchRequest) -> FetchResponse:
            self.assertEqual(
                request.url,
                "https://acme.wd5.myworkdayjobs.com/en-US/External/job/Remote-USA/Data-Engineer_R12345",
            )
            return FetchResponse(
                url=request.url,
                final_url=request.url,
                status_code=200,
                content_type="text/html; charset=utf-8",
                text=html_text,
            )

        run = run_collection(
            [source_url],
            timeout_seconds=10.0,
            created_at=FIXED_CREATED_AT,
            fetcher=fetcher,
        )

        self.assertEqual(run.report.collected_count, 0)
        self.assertEqual(run.report.manual_review_count, 1)
        result = run.report.source_results[0]
        self.assertEqual(result.outcome, "manual_review")
        self.assertEqual(result.reason_code, "workday_manual_review")
        self.assertIn("R12345", result.reason)
        assert result.manual_review_item is not None
        self.assertEqual(result.manual_review_item.portal_type, "workday")
        self.assertIn("Workday tenant hint: acme.", result.manual_review_item.hints)
        self.assertIn("Workday site hint: External.", result.manual_review_item.hints)
        self.assertIn("Workday requisition hint: R12345.", result.manual_review_item.hints)

    def test_run_collection_routes_workday_source_to_manual_review_with_normalized_url(self) -> None:
        source_url = (
            "https://wd5.myworkdayjobs.com/en-US/Company/job/Title-ID/apply"
            "?source=linkedin&utm_campaign=spring#top"
        )
        normalized_url = "https://wd5.myworkdayjobs.com/en-US/Company/job/Title-ID"
        html_text = """
<!doctype html>
<html>
  <head>
    <title>Workday | Platform Data Engineer</title>
  </head>
  <body>
    <main>
      <h1>Platform Data Engineer</h1>
      <p>Job description</p>
      <a href="/apply">Apply now</a>
    </main>
  </body>
</html>
""".strip()

        def fetcher(request: FetchRequest) -> FetchResponse:
            self.assertEqual(request.url, normalized_url)
            return FetchResponse(
                url=request.url,
                final_url=request.url,
                status_code=200,
                content_type="text/html; charset=utf-8",
                text=html_text,
            )

        run = run_collection(
            [source_url],
            timeout_seconds=10.0,
            created_at=FIXED_CREATED_AT,
            fetcher=fetcher,
        )

        self.assertEqual(run.report.collected_count, 0)
        self.assertEqual(run.report.manual_review_count, 1)
        result = run.report.source_results[0]
        self.assertEqual(result.outcome, "manual_review")
        self.assertEqual(result.source.portal_type, "workday")
        self.assertEqual(result.source.normalized_url, normalized_url)
        self.assertEqual(result.reason_code, "workday_manual_review")
        assert result.manual_review_item is not None
        self.assertEqual(result.manual_review_item.normalized_url, normalized_url)

    def test_run_collection_skips_invalid_and_duplicate_sources(self) -> None:
        run = run_collection(
            [
                "mailto:test@example.com",
                "https://jobs.lever.co/northwind/lever-data-1",
                "https://jobs.lever.co/northwind/lever-data-1#fragment",
            ],
            timeout_seconds=10.0,
            created_at=FIXED_CREATED_AT,
            fetcher=_fixture_fetcher(
                {
                    "https://jobs.lever.co/northwind/lever-data-1": _fixture_text("lever_job.html"),
                }
            ),
        )

        self.assertEqual(run.report.collected_count, 1)
        self.assertEqual(run.report.skipped_count, 2)
        self.assertEqual(
            [result.outcome for result in run.report.source_results],
            ["skipped", "collected", "skipped"],
        )
        self.assertEqual(run.report.source_results[0].reason_code, "unsupported_url_scheme")
        self.assertEqual(run.report.source_results[2].reason_code, "duplicate_normalized_source")
        self.assertIn("unsupported URL scheme", run.report.source_results[0].reason)
        self.assertIn("duplicate source skipped", run.report.source_results[2].reason)

    def test_run_collection_skips_non_html_supported_source(self) -> None:
        def fetcher(request: FetchRequest) -> FetchResponse:
            return FetchResponse(
                url=request.url,
                final_url=request.url,
                status_code=200,
                content_type="application/pdf",
                text="%PDF-1.4",
            )

        run = run_collection(
            ["https://jobs.lever.co/northwind/lever-data-1"],
            timeout_seconds=10.0,
            created_at=FIXED_CREATED_AT,
            fetcher=fetcher,
        )

        self.assertEqual(run.report.collected_count, 0)
        self.assertEqual(run.report.manual_review_count, 0)
        self.assertEqual(run.report.skipped_count, 1)
        self.assertEqual(run.report.source_results[0].outcome, "skipped")
        self.assertEqual(run.report.source_results[0].reason_code, "non_html_content")
        self.assertIn("non-HTML content-type", run.report.source_results[0].reason)

    def test_run_collection_skips_fetch_failure(self) -> None:
        def fetcher(request: FetchRequest) -> FetchResponse:
            raise FetchError(f"timed out after {request.timeout_seconds:.1f}s while fetching {request.url}")

        run = run_collection(
            ["https://careers.example.com/jobs/platform-data-engineer"],
            timeout_seconds=3.0,
            created_at=FIXED_CREATED_AT,
            fetcher=fetcher,
        )

        self.assertEqual(run.report.collected_count, 0)
        self.assertEqual(run.report.manual_review_count, 0)
        self.assertEqual(run.report.skipped_count, 1)
        self.assertEqual(run.report.source_results[0].outcome, "skipped")
        self.assertEqual(run.report.source_results[0].reason_code, "fetch_failed")
        self.assertIn("timed out after 3.0s", run.report.source_results[0].reason)
        assert run.report.source_results[0].evidence is not None
        self.assertIn("timed out after 3.0s", run.report.source_results[0].evidence.error or "")

    def test_run_collect_command_reads_sources_from_file_after_positional_sources(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            project_root = Path(tmp_dir) / "workspace"
            project_root.mkdir(parents=True, exist_ok=True)
            source_file = project_root / "sources.txt"
            source_file.write_text(
                "\n".join(
                    [
                        "# sprint batch",
                        "https://jobs.lever.co/northwind/lever-data-1",
                        "",
                        "https://jobs.ashbyhq.com/signalops/ashby-platform-3",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            paths = build_workspace_paths(Path("data/jobs_ai.db"), project_root=project_root)

            run = run_collect_command(
                paths,
                sources=("https://boards.greenhouse.io/acme/jobs/12345",),
                from_file=source_file,
                out_dir=project_root / "out",
                label=None,
                timeout_seconds=10.0,
                report_only=False,
                created_at=FIXED_CREATED_AT,
                fetcher=_fixture_fetcher(
                    {
                        "https://boards.greenhouse.io/acme/jobs/12345": _fixture_text("greenhouse_job.html"),
                        "https://jobs.lever.co/northwind/lever-data-1": _fixture_text("lever_job.html"),
                        "https://jobs.ashbyhq.com/signalops/ashby-platform-3": _fixture_text("ashby_job.html"),
                    }
                ),
            )

        self.assertEqual(
            [result.source.source_url for result in run.report.source_results],
            [
                "https://boards.greenhouse.io/acme/jobs/12345",
                "https://jobs.lever.co/northwind/lever-data-1",
                "https://jobs.ashbyhq.com/signalops/ashby-platform-3",
            ],
        )
        self.assertEqual(run.report.collected_count, 3)

    def test_write_collect_artifacts_are_importer_compatible_and_deterministic(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_dir = Path(tmp_dir) / "artifacts"
            database_path = Path(tmp_dir) / "runtime" / "jobs_ai.db"
            initialize_schema(database_path)

            run = run_collection(
                ["https://boards.greenhouse.io/acme"],
                timeout_seconds=10.0,
                created_at=FIXED_CREATED_AT,
                fetcher=_fixture_fetcher(
                    {
                        "https://boards.greenhouse.io/acme": _fixture_text("greenhouse_board.html"),
                    }
                ),
            )
            paths = build_workspace_paths(Path("data/jobs_ai.db"), project_root=Path(tmp_dir))
            finalized_run = run_collect_command(
                paths,
                sources=("https://boards.greenhouse.io/acme",),
                from_file=None,
                out_dir=output_dir,
                label=None,
                timeout_seconds=10.0,
                report_only=False,
                created_at=FIXED_CREATED_AT,
                fetcher=_fixture_fetcher(
                    {
                        "https://boards.greenhouse.io/acme": _fixture_text("greenhouse_board.html"),
                    }
                ),
            )

            leads_path = finalized_run.report.artifact_paths
            assert leads_path is not None
            import_result = import_jobs_from_file(database_path, leads_path.leads_path)
            leads_text = leads_path.leads_path.read_text(encoding="utf-8")

        self.assertEqual(run.report.collected_count, 2)
        self.assertEqual(import_result.inserted_count, 2)
        self.assertEqual(import_result.errors, ())
        self.assertTrue(leads_text.endswith("\n"))
        self.assertEqual(
            json.loads(leads_text),
            [
                {
                    "source": "greenhouse",
                    "company": "Acme Data",
                    "title": "Data Engineer",
                    "location": "Remote",
                    "apply_url": "https://boards.greenhouse.io/acme/jobs/12345",
                    "source_job_id": "12345",
                    "portal_type": "greenhouse",
                    "salary_text": None,
                    "posted_at": None,
                    "found_at": None,
                },
                {
                    "source": "greenhouse",
                    "company": "Acme Data",
                    "title": "Analytics Engineer",
                    "location": "San Jose, CA",
                    "apply_url": "https://boards.greenhouse.io/acme/jobs/98765",
                    "source_job_id": "98765",
                    "portal_type": "greenhouse",
                    "salary_text": None,
                    "posted_at": None,
                    "found_at": None,
                },
            ],
        )

    def test_modern_greenhouse_collect_artifacts_are_importer_compatible(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_dir = Path(tmp_dir) / "artifacts"
            database_path = Path(tmp_dir) / "runtime" / "jobs_ai.db"
            initialize_schema(database_path)

            def fetcher(request: FetchRequest) -> FetchResponse:
                self.assertEqual(request.url, "https://boards.greenhouse.io/parloa")
                return FetchResponse(
                    url=request.url,
                    final_url="https://job-boards.eu.greenhouse.io/parloa",
                    status_code=200,
                    content_type="text/html; charset=utf-8",
                    text=_fixture_text("greenhouse_board_modern.html"),
                )

            finalized_run = run_collect_command(
                build_workspace_paths(Path("data/jobs_ai.db"), project_root=Path(tmp_dir)),
                sources=("https://boards.greenhouse.io/parloa",),
                from_file=None,
                out_dir=output_dir,
                label=None,
                timeout_seconds=10.0,
                report_only=False,
                created_at=FIXED_CREATED_AT,
                fetcher=fetcher,
            )

            artifact_paths = finalized_run.report.artifact_paths
            assert artifact_paths is not None
            import_result = import_jobs_from_file(database_path, artifact_paths.leads_path)
            leads_payload = json.loads(artifact_paths.leads_path.read_text(encoding="utf-8"))

        self.assertEqual(import_result.inserted_count, 2)
        self.assertEqual(import_result.errors, ())
        self.assertEqual(
            leads_payload,
            [
                {
                    "source": "greenhouse",
                    "company": "Parloa",
                    "title": "Forward Deployed Engineer, DevOps",
                    "location": "New York Office",
                    "apply_url": "https://job-boards.eu.greenhouse.io/parloa/jobs/4694390101",
                    "source_job_id": "4694390101",
                    "portal_type": "greenhouse",
                    "salary_text": None,
                    "posted_at": "2025-10-16T11:47:28-04:00",
                    "found_at": None,
                },
                {
                    "source": "greenhouse",
                    "company": "Parloa",
                    "title": "Partner Solution Engineer",
                    "location": "Remotely in the USA",
                    "apply_url": "https://job-boards.eu.greenhouse.io/parloa/jobs/4780830101",
                    "source_job_id": "4780830101",
                    "portal_type": "greenhouse",
                    "salary_text": None,
                    "posted_at": "2026-02-12T04:15:52-05:00",
                    "found_at": None,
                },
            ],
        )

    def test_run_collect_command_finalizes_mixed_run_artifacts_and_summary(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            project_root = Path(tmp_dir) / "workspace"
            project_root.mkdir(parents=True, exist_ok=True)
            paths = build_workspace_paths(Path("data/jobs_ai.db"), project_root=project_root)
            output_dir = project_root / "out" / "mixed"

            generic_source = "https://careers.example.com/jobs/platform-data-engineer"
            skipped_source = "https://broken.example.com/jobs/platform-data-engineer"
            generic_html = """
<!doctype html>
<html>
  <head>
    <title>ExampleCo Careers | Platform Data Engineer</title>
  </head>
  <body>
    <main>
      <h1>Platform Data Engineer</h1>
      <p>Job description</p>
      <a href="/apply/platform-data-engineer">Apply now</a>
    </main>
  </body>
</html>
""".strip()

            def fetcher(request: FetchRequest) -> FetchResponse:
                if request.url == "https://boards.greenhouse.io/acme":
                    return FetchResponse(
                        url=request.url,
                        final_url=request.url,
                        status_code=200,
                        content_type="text/html; charset=utf-8",
                        text=_fixture_text("greenhouse_board.html"),
                    )
                if request.url == generic_source:
                    return FetchResponse(
                        url=request.url,
                        final_url="https://careers.example.com/openings/platform-data-engineer",
                        status_code=200,
                        content_type="text/html; charset=utf-8",
                        text=generic_html,
                    )
                if request.url == skipped_source:
                    raise FetchError(f"timed out after {request.timeout_seconds:.1f}s while fetching {request.url}")
                raise AssertionError(f"unexpected fetch URL: {request.url}")

            run = run_collect_command(
                paths,
                sources=("https://boards.greenhouse.io/acme", generic_source, skipped_source),
                from_file=None,
                out_dir=output_dir,
                label="mixed-run",
                timeout_seconds=5.0,
                report_only=False,
                created_at=FIXED_CREATED_AT,
                fetcher=fetcher,
            )

            artifact_paths = run.report.artifact_paths
            assert artifact_paths is not None
            leads_payload = json.loads(artifact_paths.leads_path.read_text(encoding="utf-8"))
            manual_review_payload = json.loads(artifact_paths.manual_review_path.read_text(encoding="utf-8"))
            report_payload = json.loads(artifact_paths.run_report_path.read_text(encoding="utf-8"))
            summary = render_collect_report(run.report)
            self.assertEqual(run.report.collected_count, 2)
            self.assertEqual(run.report.manual_review_count, 1)
            self.assertEqual(run.report.skipped_count, 1)
            self.assertEqual(
                [result.outcome for result in run.report.source_results],
                ["collected", "manual_review", "skipped"],
            )
            self.assertTrue(artifact_paths.leads_path.exists())
            self.assertTrue(artifact_paths.manual_review_path.exists())
            self.assertTrue(artifact_paths.run_report_path.exists())
            self.assertEqual(len(leads_payload), 2)
            self.assertEqual(manual_review_payload["run_id"], "collect-mixed-run-20260313T194500000000Z")
            self.assertEqual(manual_review_payload["finished_at"], FIXED_TIMESTAMP)
            self.assertEqual(manual_review_payload["item_count"], 1)
            self.assertEqual(
                manual_review_payload["items"][0]["evidence"]["final_url"],
                "https://careers.example.com/openings/platform-data-engineer",
            )
            self.assertEqual(report_payload["run_id"], "collect-mixed-run-20260313T194500000000Z")
            self.assertEqual(report_payload["created_at"], FIXED_TIMESTAMP)
            self.assertEqual(report_payload["finished_at"], FIXED_TIMESTAMP)
            self.assertEqual(
                report_payload["inputs"]["sources"],
                [
                    "https://boards.greenhouse.io/acme",
                    generic_source,
                    skipped_source,
                ],
            )
            self.assertEqual(
                report_payload["totals"],
                {
                    "input_sources": 3,
                    "collected_automatically": 2,
                    "manual_review_needed": 1,
                    "skipped": 1,
                },
            )
            self.assertEqual(
                report_payload["sources"][1]["manual_review_item"]["reason_code"],
                "unsupported_accessible_html",
            )
            self.assertEqual(
                report_payload["sources"][2]["reason_code"],
                "fetch_failed",
            )
            self.assertIn("run id: collect-mixed-run-20260313T194500000000Z", summary)
            self.assertIn("collected automatically: 2", summary)
            self.assertIn("manual review needed: 1", summary)
            self.assertIn("skipped: 1", summary)
            self.assertIn("unsupported_accessible_html", summary)
            self.assertIn("fetch_failed", summary)
            self.assertIn(str(artifact_paths.leads_path), summary)
            self.assertIn(str(artifact_paths.manual_review_path), summary)
            self.assertIn(str(artifact_paths.run_report_path), summary)

    def test_report_only_writes_run_report_only(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            project_root = Path(tmp_dir) / "workspace"
            project_root.mkdir(parents=True, exist_ok=True)
            paths = build_workspace_paths(Path("data/jobs_ai.db"), project_root=project_root)
            out_dir = project_root / "out"
            out_dir.mkdir(parents=True, exist_ok=True)
            (out_dir / "leads.import.json").write_text("[]\n", encoding="utf-8")
            (out_dir / "manual_review.json").write_text("{}\n", encoding="utf-8")
            run = run_collect_command(
                paths,
                sources=("https://jobs.lever.co/northwind/lever-data-1",),
                from_file=None,
                out_dir=out_dir,
                label=None,
                timeout_seconds=10.0,
                report_only=True,
                created_at=FIXED_CREATED_AT,
                fetcher=_fixture_fetcher(
                    {
                        "https://jobs.lever.co/northwind/lever-data-1": _fixture_text("lever_job.html"),
                    }
                ),
            )

            artifact_paths = run.report.artifact_paths
            assert artifact_paths is not None
            self.assertIsNone(artifact_paths.leads_path)
            self.assertIsNone(artifact_paths.manual_review_path)
            self.assertTrue(artifact_paths.run_report_path.exists())
            self.assertFalse((out_dir / "leads.import.json").exists())
            self.assertFalse((out_dir / "manual_review.json").exists())
            report_payload = json.loads(artifact_paths.run_report_path.read_text(encoding="utf-8"))
            self.assertTrue(report_payload["report_only"])
            self.assertIsNone(report_payload["artifacts"]["leads_path"])
            self.assertIsNone(report_payload["artifacts"]["manual_review_path"])
            self.assertEqual(report_payload["finished_at"], FIXED_TIMESTAMP)

    def test_cli_collect_summary_and_exit_code(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            project_root = Path(tmp_dir) / "workspace"
            project_root.mkdir(parents=True, exist_ok=True)
            source_file = project_root / "sources.txt"
            source_file.write_text(
                "# sources\nhttps://boards.greenhouse.io/acme\n",
                encoding="utf-8",
            )
            env = {"JOBS_AI_DB_PATH": str(project_root / "runtime" / "jobs_ai.db")}

            with (
                patch("jobs_ai.workspace.discover_project_root", return_value=project_root),
                patch("jobs_ai.collect.cli._current_utc_datetime", return_value=FIXED_CREATED_AT),
                patch(
                    "jobs_ai.collect.cli.fetch_text",
                    new=_fixture_fetcher(
                        {
                            "https://boards.greenhouse.io/acme": _fixture_text("greenhouse_board.html"),
                        }
                    ),
                ),
            ):
                result = RUNNER.invoke(
                    app,
                    ["collect", "--from-file", str(source_file), "--label", "fixture-run"],
                    env=env,
                )

        self.assertEqual(result.exit_code, 0)
        self.assertIn("jobs_ai collect", result.stdout)
        self.assertIn("collected automatically: 2", result.stdout)
        self.assertIn("manual review needed: 0", result.stdout)
        self.assertIn("status: success", result.stdout)
        self.assertIn("python -m jobs_ai import", result.stdout)

    def test_cli_collect_requires_source_input(self) -> None:
        result = RUNNER.invoke(app, ["collect"])
        self.assertEqual(result.exit_code, 1)
        self.assertIn("status: failed", result.stdout)
        self.assertIn("at least one source URL is required", result.stdout)


if __name__ == "__main__":
    unittest.main()
