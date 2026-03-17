from __future__ import annotations

from contextlib import closing
from pathlib import Path
import json
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch
from urllib.parse import quote

from typer.testing import CliRunner

SRC_PATH = Path(__file__).resolve().parents[1] / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

from jobs_ai.cli import app
from jobs_ai.collect.fetch import FetchError, FetchRequest, FetchResponse
from jobs_ai.db import connect_database, initialize_schema
from jobs_ai.discover.search import build_search_plans
from jobs_ai.source_seed.starter_lists import (
    legacy_starter_lists,
    load_starter_list_entries,
    load_starter_list_items,
    recommended_starter_lists,
)
from jobs_ai.sources.company_harvester import resolve_company_harvest_sources
from jobs_ai.sources.discover_ats import discover_registry_ats_sources
from jobs_ai.sources.detect_sites import detect_registry_sources_from_sites
from jobs_ai.sources.seeding import available_seed_bulk_starter_lists
from jobs_ai.sources.registry import list_registry_sources, upsert_registry_source
from jobs_ai.workspace import build_workspace_paths, ensure_workspace

FIXTURE_ROOT = Path(__file__).resolve().parent / "fixtures" / "collect"
RUNNER = CliRunner()


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


def _duckduckgo_zero_results_html(query: str = "python backend engineer remote") -> str:
    return (
        "<!doctype html><html><head><title>DuckDuckGo</title></head><body>"
        f"<div>No results found for {query}</div>"
        "</body></html>"
    )


class SourceRegistryTest(unittest.TestCase):
    def test_company_harvest_source_aliases_resolve(self) -> None:
        self.assertEqual(
            resolve_company_harvest_sources(("ai-companies",)),
            ("ai-startups",),
        )
        self.assertEqual(
            resolve_company_harvest_sources(("startup-list", "all")),
            ("startup-list", "ai-startups", "remote-companies"),
        )

    def test_curated_seed_starter_lists_are_available(self) -> None:
        starter_lists = available_seed_bulk_starter_lists()
        recommended = recommended_starter_lists()
        legacy = legacy_starter_lists()

        self.assertEqual(
            recommended,
            ("ats-tech", "ats-data-ai", "ats-general-remote", "ats-startups"),
        )
        self.assertEqual(
            legacy,
            ("major-tech", "fortune-500", "staffing-large-employers"),
        )
        self.assertEqual(
            starter_lists,
            (*recommended, *legacy),
        )
        for starter_list in starter_lists:
            with self.subTest(starter_list=starter_list):
                entries = load_starter_list_entries(starter_list)
                self.assertTrue(entries)
                if starter_list.startswith("ats-"):
                    self.assertTrue(any("https://" in entry for entry in entries))
                    items = load_starter_list_items(starter_list)
                    self.assertTrue(items)
                    self.assertTrue(all(item.provider_type in {"greenhouse", "lever", "ashby"} for item in items))

    def test_cli_sources_add_normalizes_and_lists_verified_registry_source(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            project_root = Path(tmp_dir) / "workspace"
            database_path = project_root / "runtime" / "jobs_ai.db"
            env = {"JOBS_AI_DB_PATH": str(database_path)}

            with patch("jobs_ai.workspace.discover_project_root", return_value=project_root):
                with patch(
                    "jobs_ai.sources.registry.fetch_text",
                    side_effect=_mapping_fetcher(
                        {
                            "https://boards.greenhouse.io/acme": _fixture_text("greenhouse_board.html"),
                        }
                    ),
                ):
                    add_result = RUNNER.invoke(
                        app,
                        [
                            "sources",
                            "add",
                            "https://boards.greenhouse.io/acme/jobs/12345?gh_src=linkedin",
                            "--company",
                            "Acme Data",
                        ],
                        env=env,
                    )
                list_result = RUNNER.invoke(app, ["sources", "list"], env=env)

            self.assertEqual(add_result.exit_code, 0)
            self.assertIn("status: active", add_result.stdout)
            self.assertEqual(list_result.exit_code, 0)
            self.assertIn(
                "[1] active | greenhouse | Acme Data | https://boards.greenhouse.io/acme",
                list_result.stdout,
            )

    def test_cli_sources_add_accepts_direct_lever_board_url(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            project_root = Path(tmp_dir) / "workspace"
            database_path = project_root / "runtime" / "jobs_ai.db"
            env = {"JOBS_AI_DB_PATH": str(database_path)}

            with patch("jobs_ai.workspace.discover_project_root", return_value=project_root):
                with patch(
                    "jobs_ai.sources.registry.fetch_text",
                    side_effect=_mapping_fetcher(
                        {
                            "https://jobs.lever.co/offchainlabs": _fixture_text("lever_board.html"),
                        }
                    ),
                ):
                    add_result = RUNNER.invoke(
                        app,
                        [
                            "sources",
                            "add",
                            "https://jobs.lever.co/offchainlabs/platform-engineer-1?lever-source=LinkedIn",
                            "--company",
                            "Offchain Labs",
                        ],
                        env=env,
                    )

            self.assertEqual(add_result.exit_code, 0)
            self.assertIn("source URL: https://jobs.lever.co/offchainlabs", add_result.stdout)
            self.assertIn("status: active", add_result.stdout)

            entries = list_registry_sources(database_path)
            self.assertEqual(len(entries), 1)
            self.assertEqual(entries[0].portal_type, "lever")
            self.assertEqual(entries[0].source_url, "https://jobs.lever.co/offchainlabs")

    def test_cli_sources_collect_can_import_registry_jobs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            project_root = Path(tmp_dir) / "workspace"
            database_path = project_root / "runtime" / "jobs_ai.db"
            env = {"JOBS_AI_DB_PATH": str(database_path)}

            with patch("jobs_ai.workspace.discover_project_root", return_value=project_root):
                with patch(
                    "jobs_ai.sources.registry.fetch_text",
                    side_effect=_mapping_fetcher(
                        {
                            "https://boards.greenhouse.io/acme": _fixture_text("greenhouse_board.html"),
                        }
                    ),
                ):
                    RUNNER.invoke(
                        app,
                        ["sources", "add", "https://boards.greenhouse.io/acme"],
                        env=env,
                    )

                with patch(
                    "jobs_ai.sources.workflow.fetch_text",
                    side_effect=_mapping_fetcher(
                        {
                            "https://boards.greenhouse.io/acme": _fixture_text("greenhouse_board.html"),
                        }
                    ),
                ):
                    collect_result = RUNNER.invoke(
                        app,
                        [
                            "sources",
                            "collect",
                            "--import",
                            "--out-dir",
                            "registry-collect",
                        ],
                        env=env,
                    )

            self.assertEqual(collect_result.exit_code, 0)
            self.assertIn("selected registry sources: 1", collect_result.stdout)
            self.assertIn("imported jobs: 2", collect_result.stdout)

            with closing(connect_database(database_path)) as connection:
                row = connection.execute("SELECT COUNT(*) AS count FROM jobs").fetchone()
            self.assertEqual(row["count"], 2)
            self.assertTrue((project_root / "registry-collect" / "leads.import.json").exists())

    def test_cli_sources_extract_jobposting_imports_jobs_links_registry_and_skips_duplicates(self) -> None:
        html_text = """
<!doctype html>
<html>
  <head>
    <script type="application/ld+json">
      {
        "@context": "https://schema.org",
        "@graph": [
          {
            "@type": "Organization",
            "name": "OpenAI"
          },
          {
            "@type": "JobPosting",
            "title": "Research Engineer",
            "hiringOrganization": {"@type": "Organization", "name": "OpenAI"},
            "jobLocationType": "TELECOMMUTE",
            "applicantLocationRequirements": {"@type": "Country", "name": "United States"},
            "datePosted": "2026-03-15",
            "employmentType": "FULL_TIME",
            "applyUrl": "https://jobs.ashbyhq.com/openai/research-engineer"
          },
          {
            "@type": "JobPosting",
            "title": "Platform Engineer",
            "hiringOrganization": {"@type": "Organization", "name": "OpenAI"},
            "jobLocation": {
              "@type": "Place",
              "address": {
                "@type": "PostalAddress",
                "addressLocality": "San Francisco",
                "addressRegion": "CA",
                "addressCountry": "US"
              }
            },
            "datePosted": "2026-03-14",
            "employmentType": ["FULL_TIME"],
            "url": "/careers/platform-engineer"
          }
        ]
      }
    </script>
  </head>
  <body><main>Careers</main></body>
</html>
""".strip()

        with tempfile.TemporaryDirectory() as tmp_dir:
            project_root = Path(tmp_dir) / "workspace"
            database_path = project_root / "runtime" / "jobs_ai.db"
            env = {"JOBS_AI_DB_PATH": str(database_path)}

            with patch("jobs_ai.workspace.discover_project_root", return_value=project_root):
                initialize_schema(database_path)
                upsert_registry_source(
                    database_path,
                    source_url="https://jobs.ashbyhq.com/openai",
                    portal_type="ashby",
                    company="OpenAI",
                    status="active",
                )

                with patch(
                    "jobs_ai.sources.jobposting_parser.fetch_text",
                    side_effect=_mapping_fetcher(
                        {
                            "https://openai.com": _response(
                                "https://openai.com",
                                html_text,
                                final_url="https://openai.com/careers",
                            ),
                        }
                    ),
                ):
                    first_result = RUNNER.invoke(
                        app,
                        ["sources", "extract-jobposting", "openai.com"],
                        env=env,
                    )
                    second_result = RUNNER.invoke(
                        app,
                        ["sources", "extract-jobposting", "openai.com"],
                        env=env,
                    )

            self.assertEqual(first_result.exit_code, 0)
            self.assertIn("imported jobs: 2", first_result.stdout)
            self.assertIn("registry links: 1", first_result.stdout)
            self.assertEqual(second_result.exit_code, 0)
            self.assertIn("duplicates skipped: 2", second_result.stdout)

            with closing(connect_database(database_path)) as connection:
                rows = connection.execute(
                    """
                    SELECT title, location, apply_url, portal_type, source_registry_id
                    FROM jobs
                    ORDER BY title
                    """
                ).fetchall()

            self.assertEqual(len(rows), 2)
            self.assertEqual(rows[0]["title"], "Platform Engineer")
            self.assertEqual(rows[0]["location"], "San Francisco, CA, US")
            self.assertEqual(rows[0]["apply_url"], "https://openai.com/careers/platform-engineer")
            self.assertIsNone(rows[0]["portal_type"])
            self.assertIsNone(rows[0]["source_registry_id"])
            self.assertEqual(rows[1]["title"], "Research Engineer")
            self.assertEqual(rows[1]["location"], "Remote (United States)")
            self.assertEqual(rows[1]["portal_type"], "ashby")
            self.assertEqual(rows[1]["source_registry_id"], 1)

    def test_cli_sources_extract_jobposting_reads_targets_from_file(self) -> None:
        html_text = """
<!doctype html>
<html>
  <head>
    <script type="application/ld+json">
      [{
        "@context": "https://schema.org",
        "@type": "JobPosting",
        "title": "Data Engineer",
        "hiringOrganization": {"@type": "Organization", "name": "Stripe"},
        "jobLocation": {
          "@type": "Place",
          "address": {
            "@type": "PostalAddress",
            "addressLocality": "Seattle",
            "addressRegion": "WA"
          }
        },
        "datePosted": "2026-03-10",
        "employmentType": "FULL_TIME",
        "url": "/jobs/data-engineer"
      }]
    </script>
  </head>
  <body><main>Jobs</main></body>
</html>
""".strip()

        with tempfile.TemporaryDirectory() as tmp_dir:
            project_root = Path(tmp_dir) / "workspace"
            database_path = project_root / "runtime" / "jobs_ai.db"
            input_path = project_root / "domains.txt"
            input_path.parent.mkdir(parents=True, exist_ok=True)
            input_path.write_text("# seed\nstripe.com\n", encoding="utf-8")
            env = {"JOBS_AI_DB_PATH": str(database_path)}

            with patch("jobs_ai.workspace.discover_project_root", return_value=project_root):
                with patch(
                    "jobs_ai.sources.jobposting_parser.fetch_text",
                    side_effect=_mapping_fetcher(
                        {
                            "https://stripe.com": _response(
                                "https://stripe.com",
                                html_text,
                                final_url="https://stripe.com/careers",
                            ),
                        }
                    ),
                ):
                    result = RUNNER.invoke(
                        app,
                        ["sources", "extract-jobposting", "--from-file", str(input_path)],
                        env=env,
                    )

            self.assertEqual(result.exit_code, 0)
            self.assertIn("inputs processed: 1", result.stdout)
            self.assertIn("imported jobs: 1", result.stdout)

            with closing(connect_database(database_path)) as connection:
                row = connection.execute(
                    "SELECT company, title, location, apply_url FROM jobs"
                ).fetchone()

            self.assertEqual(row["company"], "Stripe")
            self.assertEqual(row["title"], "Data Engineer")
            self.assertEqual(row["location"], "Seattle, WA")
            self.assertEqual(row["apply_url"], "https://stripe.com/jobs/data-engineer")

    def test_cli_discover_can_add_confirmed_sources_to_registry(self) -> None:
        query = "platform data engineer remote"
        search_plans = build_search_plans(query)
        greenhouse_search_url = search_plans[0].search_url
        fetcher = _mapping_fetcher(
            {
                **{
                    plan.search_url: _duckduckgo_zero_results_html(plan.search_text)
                    for plan in search_plans
                },
                greenhouse_search_url: _search_results_html(
                    "https://boards.greenhouse.io/acme/jobs/12345"
                ),
                "https://boards.greenhouse.io/acme": _fixture_text("greenhouse_board.html"),
            }
        )

        with tempfile.TemporaryDirectory() as tmp_dir:
            project_root = Path(tmp_dir) / "workspace"
            database_path = project_root / "runtime" / "jobs_ai.db"
            env = {"JOBS_AI_DB_PATH": str(database_path)}

            with patch("jobs_ai.workspace.discover_project_root", return_value=project_root):
                with patch("jobs_ai.discover.cli.fetch_text", side_effect=fetcher):
                    result = RUNNER.invoke(
                        app,
                        ["discover", query, "--add-to-registry"],
                        env=env,
                    )

            self.assertEqual(result.exit_code, 0)
            self.assertIn("jobs_ai discover registry sync", result.stdout)
            entries = list_registry_sources(database_path)
            self.assertEqual(len(entries), 1)
            self.assertEqual(entries[0].status, "active")
            self.assertEqual(entries[0].source_url, "https://boards.greenhouse.io/acme")

    def test_cli_sources_seed_bulk_from_file_records_active_manual_review_and_workday(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            project_root = Path(tmp_dir) / "workspace"
            database_path = project_root / "runtime" / "jobs_ai.db"
            companies_path = project_root / "companies.txt"
            companies_path.parent.mkdir(parents=True, exist_ok=True)
            companies_path.write_text(
                "\n".join(
                    [
                        "Acme Data | https://boards.greenhouse.io/acme/jobs/12345?gh_src=linkedin",
                        "Northwind Analytics | https://boards.greenhouse.io/northwind",
                        "Contoso | https://contoso.example/careers",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            env = {"JOBS_AI_DB_PATH": str(database_path)}

            with patch("jobs_ai.workspace.discover_project_root", return_value=project_root):
                with patch(
                    "jobs_ai.sources.seeding.fetch_text",
                    side_effect=_mapping_fetcher(
                        {
                            "https://boards.greenhouse.io/acme": _fixture_text("greenhouse_board.html"),
                            "https://boards.greenhouse.io/northwind": _fixture_text("greenhouse_board_partial.html"),
                            "https://contoso.example/careers": (
                                "<!doctype html><html><body>"
                                '<a href="https://wd5.myworkdayjobs.com/en-US/External/job/Remote/R12345">Apply</a>'
                                "</body></html>"
                            ),
                        }
                    ),
                ):
                    result = RUNNER.invoke(
                        app,
                        ["sources", "seed-bulk", "--from-file", str(companies_path)],
                        env=env,
                    )

            self.assertEqual(result.exit_code, 0)
            self.assertIn("confirmed registry sources: 1", result.stdout)
            self.assertIn("manual review registry sources: 2", result.stdout)
            self.assertIn("failed inputs: 0", result.stdout)

            entries = list_registry_sources(database_path)
            self.assertEqual(len(entries), 3)
            self.assertEqual(entries[0].status, "active")
            self.assertEqual(entries[0].source_url, "https://boards.greenhouse.io/acme")
            self.assertEqual(entries[1].status, "manual_review")
            self.assertEqual(entries[1].source_url, "https://wd5.myworkdayjobs.com/en-US/External/job/Remote/R12345")
            self.assertEqual(entries[1].portal_type, "workday")
            self.assertEqual(entries[1].verification_reason_code, "workday_partial_support")
            self.assertEqual(entries[2].status, "manual_review")
            self.assertEqual(entries[2].source_url, "https://boards.greenhouse.io/northwind")
            self.assertEqual(entries[2].verification_reason_code, "greenhouse_parse_ambiguous")

    def test_cli_sources_seed_bulk_confirms_direct_ashby_url_into_active_registry(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            project_root = Path(tmp_dir) / "workspace"
            database_path = project_root / "runtime" / "jobs_ai.db"
            companies_path = project_root / "companies.txt"
            companies_path.parent.mkdir(parents=True, exist_ok=True)
            companies_path.write_text(
                "Signal Ops | https://jobs.ashbyhq.com/signalops\n",
                encoding="utf-8",
            )
            env = {"JOBS_AI_DB_PATH": str(database_path)}

            with patch("jobs_ai.workspace.discover_project_root", return_value=project_root):
                with patch(
                    "jobs_ai.sources.seeding.fetch_text",
                    side_effect=_mapping_fetcher(
                        {
                            "https://jobs.ashbyhq.com/signalops": _fixture_text("ashby_board.html"),
                        }
                    ),
                ):
                    result = RUNNER.invoke(
                        app,
                        ["sources", "seed-bulk", "--from-file", str(companies_path)],
                        env=env,
                    )

            self.assertEqual(result.exit_code, 0)
            self.assertIn("confirmed registry sources: 1", result.stdout)
            self.assertIn("manual review registry sources: 0", result.stdout)
            self.assertIn("failed inputs: 0", result.stdout)

            entries = list_registry_sources(database_path)
            self.assertEqual(len(entries), 1)
            self.assertEqual(entries[0].status, "active")
            self.assertEqual(entries[0].portal_type, "ashby")
            self.assertEqual(entries[0].source_url, "https://jobs.ashbyhq.com/signalops")

    def test_cli_sources_seed_bulk_keeps_blocked_direct_ashby_url_as_manual_review(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            project_root = Path(tmp_dir) / "workspace"
            database_path = project_root / "runtime" / "jobs_ai.db"
            companies_path = project_root / "companies.txt"
            companies_path.parent.mkdir(parents=True, exist_ok=True)
            companies_path.write_text(
                "Signal Ops | https://jobs.ashbyhq.com/signalops\n",
                encoding="utf-8",
            )
            env = {"JOBS_AI_DB_PATH": str(database_path)}

            with patch("jobs_ai.workspace.discover_project_root", return_value=project_root):
                with patch(
                    "jobs_ai.sources.seeding.fetch_text",
                    side_effect=_mapping_fetcher(
                        {
                            "https://jobs.ashbyhq.com/signalops": _response(
                                "https://jobs.ashbyhq.com/signalops",
                                "<!doctype html><html><body>Access denied</body></html>",
                                status_code=403,
                            ),
                        }
                    ),
                ):
                    result = RUNNER.invoke(
                        app,
                        ["sources", "seed-bulk", "--from-file", str(companies_path)],
                        env=env,
                    )

            self.assertEqual(result.exit_code, 0)
            self.assertIn("confirmed registry sources: 0", result.stdout)
            self.assertIn("manual review registry sources: 1", result.stdout)
            self.assertIn("failed inputs: 0", result.stdout)

            entries = list_registry_sources(database_path)
            self.assertEqual(len(entries), 1)
            self.assertEqual(entries[0].status, "manual_review")
            self.assertEqual(entries[0].portal_type, "ashby")
            self.assertEqual(entries[0].source_url, "https://jobs.ashbyhq.com/signalops")
            self.assertEqual(entries[0].verification_reason_code, "ashby_blocked_or_access_denied")

    def test_cli_sources_seed_bulk_reports_invalid_direct_ashby_url_as_failed_input(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            project_root = Path(tmp_dir) / "workspace"
            database_path = project_root / "runtime" / "jobs_ai.db"
            companies_path = project_root / "companies.txt"
            companies_path.parent.mkdir(parents=True, exist_ok=True)
            companies_path.write_text(
                "Signal Ops | https://jobs.ashbyhq.com/definitely-not-real\n",
                encoding="utf-8",
            )
            env = {"JOBS_AI_DB_PATH": str(database_path)}

            with patch("jobs_ai.workspace.discover_project_root", return_value=project_root):
                with patch(
                    "jobs_ai.sources.seeding.fetch_text",
                    side_effect=_mapping_fetcher(
                        {
                            "https://jobs.ashbyhq.com/definitely-not-real": _response(
                                "https://jobs.ashbyhq.com/definitely-not-real",
                                "<!doctype html><html><body>Not found</body></html>",
                                status_code=404,
                            ),
                        }
                    ),
                ):
                    result = RUNNER.invoke(
                        app,
                        ["sources", "seed-bulk", "--from-file", str(companies_path)],
                        env=env,
                    )

            self.assertEqual(result.exit_code, 0)
            self.assertIn("confirmed registry sources: 0", result.stdout)
            self.assertIn("manual review registry sources: 0", result.stdout)
            self.assertIn("failed inputs: 1", result.stdout)

            entries = list_registry_sources(database_path)
            self.assertEqual(entries, ())

    def test_cli_sources_detect_sites_confirms_direct_greenhouse_lever_and_ashby_links(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            project_root = Path(tmp_dir) / "workspace"
            database_path = project_root / "runtime" / "jobs_ai.db"
            companies_path = project_root / "domains.txt"
            companies_path.parent.mkdir(parents=True, exist_ok=True)
            companies_path.write_text(
                "\n".join(
                    [
                        "acme.example",
                        "northwind.example",
                        "signal.example",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            env = {"JOBS_AI_DB_PATH": str(database_path)}

            with patch("jobs_ai.workspace.discover_project_root", return_value=project_root):
                with patch(
                    "jobs_ai.sources.detect_sites.fetch_text",
                    side_effect=_mapping_fetcher(
                        {
                            "https://acme.example": (
                                "<!doctype html><html><body>"
                                '<a href="https://boards.greenhouse.io/acme/jobs/12345?gh_src=linkedin">Careers</a>'
                                "</body></html>"
                            ),
                            "https://northwind.example": (
                                "<!doctype html><html><body>"
                                '<a href="https://jobs.lever.co/northwind/lever-data-1?lever-source=LinkedIn">Jobs</a>'
                                "</body></html>"
                            ),
                            "https://signal.example": (
                                "<!doctype html><html><body>"
                                '<a href="https://jobs.ashbyhq.com/signalops/ashby-platform-3?utm_source=linkedin">Openings</a>'
                                "</body></html>"
                            ),
                            "https://boards.greenhouse.io/acme": _fixture_text("greenhouse_board.html"),
                            "https://jobs.lever.co/northwind": _fixture_text("lever_board.html"),
                            "https://jobs.ashbyhq.com/signalops": _fixture_text("ashby_board.html"),
                        }
                    ),
                ):
                    result = RUNNER.invoke(
                        app,
                        ["sources", "detect-sites", "--from-file", str(companies_path)],
                        env=env,
                    )

            self.assertEqual(result.exit_code, 0)
            self.assertIn("confirmed registry sources: 3", result.stdout)
            self.assertIn("manual review registry sources: 0", result.stdout)
            self.assertIn("failed inputs: 0", result.stdout)

            entries = list_registry_sources(database_path)
            self.assertEqual(len(entries), 3)
            self.assertTrue(all(entry.status == "active" for entry in entries))
            self.assertCountEqual(
                [entry.source_url for entry in entries],
                [
                    "https://boards.greenhouse.io/acme",
                    "https://jobs.lever.co/northwind",
                    "https://jobs.ashbyhq.com/signalops",
                ],
            )

    def test_cli_sources_detect_sites_follows_careers_redirect_to_supported_ats(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            project_root = Path(tmp_dir) / "workspace"
            database_path = project_root / "runtime" / "jobs_ai.db"
            companies_path = project_root / "domains.txt"
            companies_path.parent.mkdir(parents=True, exist_ok=True)
            companies_path.write_text("redirect.example\n", encoding="utf-8")
            env = {"JOBS_AI_DB_PATH": str(database_path)}

            with patch("jobs_ai.workspace.discover_project_root", return_value=project_root):
                with patch(
                    "jobs_ai.sources.detect_sites.fetch_text",
                    side_effect=_mapping_fetcher(
                        {
                            "https://redirect.example": (
                                "<!doctype html><html><body>"
                                '<a href="/careers">Work with us</a>'
                                "</body></html>"
                            ),
                            "https://redirect.example/careers": _response(
                                "https://redirect.example/careers",
                                "<!doctype html><html><body>redirected</body></html>",
                                final_url="https://boards.greenhouse.io/acme/jobs/12345?gh_src=linkedin",
                            ),
                            "https://boards.greenhouse.io/acme": _fixture_text("greenhouse_board.html"),
                        }
                    ),
                ):
                    result = RUNNER.invoke(
                        app,
                        ["sources", "detect-sites", "--from-file", str(companies_path)],
                        env=env,
                    )

            self.assertEqual(result.exit_code, 0)
            self.assertIn("confirmed registry sources: 1", result.stdout)
            entries = list_registry_sources(database_path)
            self.assertEqual(len(entries), 1)
            self.assertEqual(entries[0].status, "active")
            self.assertEqual(entries[0].source_url, "https://boards.greenhouse.io/acme")

    def test_cli_sources_detect_sites_records_workday_as_manual_review(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            project_root = Path(tmp_dir) / "workspace"
            database_path = project_root / "runtime" / "jobs_ai.db"
            companies_path = project_root / "domains.txt"
            companies_path.parent.mkdir(parents=True, exist_ok=True)
            companies_path.write_text("workday.example\n", encoding="utf-8")
            env = {"JOBS_AI_DB_PATH": str(database_path)}

            with patch("jobs_ai.workspace.discover_project_root", return_value=project_root):
                with patch(
                    "jobs_ai.sources.detect_sites.fetch_text",
                    side_effect=_mapping_fetcher(
                        {
                            "https://workday.example": (
                                "<!doctype html><html><body>"
                                '<a href="https://wd5.myworkdayjobs.com/en-US/External/job/Remote/R12345">Join our team</a>'
                                "</body></html>"
                            ),
                        }
                    ),
                ):
                    result = RUNNER.invoke(
                        app,
                        ["sources", "detect-sites", "--from-file", str(companies_path)],
                        env=env,
                    )

            self.assertEqual(result.exit_code, 0)
            self.assertIn("confirmed registry sources: 0", result.stdout)
            self.assertIn("manual review registry sources: 1", result.stdout)

            entries = list_registry_sources(database_path)
            self.assertEqual(len(entries), 1)
            self.assertEqual(entries[0].status, "manual_review")
            self.assertEqual(entries[0].portal_type, "workday")
            self.assertEqual(
                entries[0].source_url,
                "https://wd5.myworkdayjobs.com/en-US/External/job/Remote/R12345",
            )
            self.assertEqual(entries[0].verification_reason_code, "workday_partial_support")

    def test_cli_sources_detect_sites_uses_json_ld_jobposting_clues(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            project_root = Path(tmp_dir) / "workspace"
            database_path = project_root / "runtime" / "jobs_ai.db"
            companies_path = project_root / "domains.txt"
            companies_path.parent.mkdir(parents=True, exist_ok=True)
            companies_path.write_text("jsonld.example\n", encoding="utf-8")
            env = {"JOBS_AI_DB_PATH": str(database_path)}

            with patch("jobs_ai.workspace.discover_project_root", return_value=project_root):
                with patch(
                    "jobs_ai.sources.detect_sites.fetch_text",
                    side_effect=_mapping_fetcher(
                        {
                            "https://jsonld.example": (
                                "<!doctype html><html><head>"
                                '<script type="application/ld+json">'
                                '{"@context":"https://schema.org","@type":"JobPosting",'
                                '"title":"Platform Engineer",'
                                '"url":"https://jobs.lever.co/northwind/lever-data-1?lever-source=LinkedIn"}'
                                "</script>"
                                "</head><body><p>Careers</p></body></html>"
                            ),
                            "https://jobs.lever.co/northwind": _fixture_text("lever_board.html"),
                        }
                    ),
                ):
                    result = RUNNER.invoke(
                        app,
                        [
                            "sources",
                            "detect-sites",
                            "--from-file",
                            str(companies_path),
                            "--structured-clues",
                        ],
                        env=env,
                    )

            self.assertEqual(result.exit_code, 0)
            self.assertIn("confirmed registry sources: 1", result.stdout)
            entries = list_registry_sources(database_path)
            self.assertEqual(len(entries), 1)
            self.assertEqual(entries[0].portal_type, "lever")
            self.assertEqual(entries[0].source_url, "https://jobs.lever.co/northwind")

    def test_cli_sources_detect_sites_follows_machine_readable_job_feed_clues(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            project_root = Path(tmp_dir) / "workspace"
            database_path = project_root / "runtime" / "jobs_ai.db"
            companies_path = project_root / "domains.txt"
            companies_path.parent.mkdir(parents=True, exist_ok=True)
            companies_path.write_text("feeds.example\n", encoding="utf-8")
            env = {"JOBS_AI_DB_PATH": str(database_path)}

            with patch("jobs_ai.workspace.discover_project_root", return_value=project_root):
                with patch(
                    "jobs_ai.sources.detect_sites.fetch_text",
                    side_effect=_mapping_fetcher(
                        {
                            "https://feeds.example": (
                                "<!doctype html><html><head>"
                                '<link rel="alternate" type="application/json" href="/careers/jobs.json">'
                                "</head><body><p>Openings</p></body></html>"
                            ),
                            "https://feeds.example/careers/jobs.json": _response(
                                "https://feeds.example/careers/jobs.json",
                                json.dumps(
                                    {
                                        "jobs": [
                                            {
                                                "url": "https://jobs.ashbyhq.com/signalops/ashby-role-1"
                                            }
                                        ]
                                    }
                                ),
                                content_type="application/json; charset=utf-8",
                            ),
                            "https://jobs.ashbyhq.com/signalops": _fixture_text("ashby_board.html"),
                        }
                    ),
                ):
                    result = RUNNER.invoke(
                        app,
                        [
                            "sources",
                            "detect-sites",
                            "--from-file",
                            str(companies_path),
                            "--structured-clues",
                        ],
                        env=env,
                    )

            self.assertEqual(result.exit_code, 0)
            self.assertIn("confirmed registry sources: 1", result.stdout)
            entries = list_registry_sources(database_path)
            self.assertEqual(len(entries), 1)
            self.assertEqual(entries[0].portal_type, "ashby")
            self.assertEqual(entries[0].source_url, "https://jobs.ashbyhq.com/signalops")

    def test_cli_sources_detect_sites_reports_failed_input_without_registry_mutation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            project_root = Path(tmp_dir) / "workspace"
            database_path = project_root / "runtime" / "jobs_ai.db"
            companies_path = project_root / "domains.txt"
            companies_path.parent.mkdir(parents=True, exist_ok=True)
            companies_path.write_text("noleads.example\n", encoding="utf-8")
            env = {"JOBS_AI_DB_PATH": str(database_path)}

            with patch("jobs_ai.workspace.discover_project_root", return_value=project_root):
                with patch(
                    "jobs_ai.sources.detect_sites.fetch_text",
                    side_effect=_mapping_fetcher(
                        {
                            "https://noleads.example": (
                                "<!doctype html><html><body>"
                                '<a href="/about">About</a>'
                                "</body></html>"
                            ),
                        }
                    ),
                ):
                    result = RUNNER.invoke(
                        app,
                        ["sources", "detect-sites", "--from-file", str(companies_path)],
                        env=env,
                    )

            self.assertEqual(result.exit_code, 0)
            self.assertIn("failed inputs: 1", result.stdout)
            self.assertIn("noleads.example | no_careers_link_found", result.stdout)
            self.assertEqual(list_registry_sources(database_path), ())

    def test_cli_sources_detect_sites_upserts_duplicate_normalized_urls(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            project_root = Path(tmp_dir) / "workspace"
            database_path = project_root / "runtime" / "jobs_ai.db"
            companies_path = project_root / "domains.txt"
            companies_path.parent.mkdir(parents=True, exist_ok=True)
            companies_path.write_text(
                "\n".join(
                    [
                        "acme.example",
                        "https://acme.example/careers",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            env = {"JOBS_AI_DB_PATH": str(database_path)}

            with patch("jobs_ai.workspace.discover_project_root", return_value=project_root):
                with patch(
                    "jobs_ai.sources.detect_sites.fetch_text",
                    side_effect=_mapping_fetcher(
                        {
                            "https://acme.example": (
                                "<!doctype html><html><body>"
                                '<a href="/careers">Careers</a>'
                                "</body></html>"
                            ),
                            "https://acme.example/careers": (
                                "<!doctype html><html><body>"
                                '<a href="https://boards.greenhouse.io/acme/jobs/12345">Open role</a>'
                                "</body></html>"
                            ),
                            "https://boards.greenhouse.io/acme": _fixture_text("greenhouse_board.html"),
                        }
                    ),
                ):
                    result = RUNNER.invoke(
                        app,
                        ["sources", "detect-sites", "--from-file", str(companies_path)],
                        env=env,
                    )

            self.assertEqual(result.exit_code, 0)
            self.assertIn("confirmed registry sources: 1", result.stdout)
            self.assertIn("registry created: 1", result.stdout)
            self.assertIn("registry updated: 1", result.stdout)
            entries = list_registry_sources(database_path)
            self.assertEqual(len(entries), 1)
            self.assertEqual(entries[0].status, "active")
            self.assertEqual(entries[0].source_url, "https://boards.greenhouse.io/acme")
            self.assertIn("acme.example", entries[0].provenance or "")
            self.assertIn("https://acme.example/careers", entries[0].provenance or "")

    def test_cli_sources_harvest_companies_harvests_domains_and_feeds_detect_sites(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            project_root = Path(tmp_dir) / "workspace"
            database_path = project_root / "runtime" / "jobs_ai.db"
            output_dir = project_root / "harvest-artifacts"
            env = {"JOBS_AI_DB_PATH": str(database_path)}

            with patch("jobs_ai.workspace.discover_project_root", return_value=project_root):
                with patch(
                    "jobs_ai.sources.company_harvester.fetch_text",
                    side_effect=_mapping_fetcher(
                        {
                            "https://openstartuplist.com/": (
                                "<!doctype html><html><body>"
                                '<a href="https://www.acme.com">Acme</a>'
                                '<a href="https://blog.acme.com/launch">Acme blog</a>'
                                '<a href="https://linkedin.com/company/acme">Acme LinkedIn</a>'
                                '<a href="https://docs.github.com/en">GitHub docs</a>'
                                '<a href="https://boards.greenhouse.io/acme/jobs/12345">Acme ATS</a>'
                                '<a href="/companies/acme">Internal profile</a>'
                                '<a href="https://careers.beta.ai/jobs">Beta careers</a>'
                                "</body></html>"
                            ),
                            "https://acme.com": (
                                "<!doctype html><html><body>"
                                '<a href="https://boards.greenhouse.io/acme/jobs/12345?gh_src=linkedin">Careers</a>'
                                "</body></html>"
                            ),
                            "https://beta.ai": (
                                "<!doctype html><html><body>"
                                '<a href="https://jobs.lever.co/beta/lever-role-1?lever-source=LinkedIn">Open roles</a>'
                                "</body></html>"
                            ),
                            "https://boards.greenhouse.io/acme": _fixture_text("greenhouse_board.html"),
                            "https://jobs.lever.co/beta": _fixture_text("lever_board.html"),
                        }
                    ),
                ):
                    result = RUNNER.invoke(
                        app,
                        [
                            "sources",
                            "harvest-companies",
                            "--source",
                            "startup-list",
                            "--out-dir",
                            str(output_dir),
                            "--max-requests-per-second",
                            "1000",
                        ],
                        env=env,
                    )

            self.assertEqual(result.exit_code, 0)
            self.assertIn("Discovered 2 company domains", result.stdout)
            self.assertIn("confirmed registry sources: 2", result.stdout)
            self.assertTrue((output_dir / "harvested_domains.txt").exists())
            self.assertTrue((output_dir / "harvest_report.json").exists())
            self.assertEqual(
                (output_dir / "harvested_domains.txt").read_text(encoding="utf-8"),
                "acme.com\nbeta.ai\n",
            )

            entries = list_registry_sources(database_path)
            self.assertEqual(len(entries), 2)
            self.assertTrue(all(entry.status == "active" for entry in entries))
            self.assertCountEqual(
                [entry.source_url for entry in entries],
                [
                    "https://boards.greenhouse.io/acme",
                    "https://jobs.lever.co/beta",
                ],
            )

    def test_detect_registry_sources_from_sites_bounds_same_site_fetches(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            project_root = Path(tmp_dir) / "workspace"
            paths = build_workspace_paths(Path("runtime/jobs_ai.db"), project_root=project_root)
            ensure_workspace(paths)

            requested_urls: list[str] = []

            def fetcher(request: FetchRequest) -> FetchResponse:
                requested_urls.append(request.url)
                if request.url == "https://bounded.example":
                    links = "".join(
                        f'<a href="/jobs/{index}">Jobs {index}</a>'
                        for index in range(1, 11)
                    )
                    return _response(
                        request.url,
                        f"<!doctype html><html><body>{links}</body></html>",
                    )
                if request.url in {
                    "https://bounded.example/jobs/1",
                    "https://bounded.example/jobs/2",
                    "https://bounded.example/jobs/3",
                    "https://bounded.example/jobs/4",
                }:
                    return _response(
                        request.url,
                        "<!doctype html><html><body><p>No ATS here</p></body></html>",
                    )
                raise FetchError(f"unexpected fetch {request.url}")

            result = detect_registry_sources_from_sites(
                paths,
                companies=("https://bounded.example",),
                from_file=None,
                starter_lists=(),
                timeout_seconds=5.0,
                fetcher=fetcher,
            )

            self.assertEqual(result.failed_count, 1)
            self.assertEqual(result.confirmed_count, 0)
            self.assertEqual(result.manual_review_count, 0)
            self.assertEqual(
                requested_urls,
                [
                    "https://bounded.example",
                    "https://bounded.example/jobs/1",
                    "https://bounded.example/jobs/2",
                    "https://bounded.example/jobs/3",
                    "https://bounded.example/jobs/4",
                ],
            )
            self.assertEqual(result.input_results[0].fetched_page_count, 5)

    def test_cli_sources_seed_bulk_upserts_duplicate_normalized_urls(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            project_root = Path(tmp_dir) / "workspace"
            database_path = project_root / "runtime" / "jobs_ai.db"
            companies_path = project_root / "companies.txt"
            companies_path.parent.mkdir(parents=True, exist_ok=True)
            companies_path.write_text(
                "\n".join(
                    [
                        "Acme Data | https://boards.greenhouse.io/acme",
                        "Acme Data | https://boards.greenhouse.io/acme/jobs/12345?gh_src=linkedin",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            env = {"JOBS_AI_DB_PATH": str(database_path)}

            with patch("jobs_ai.workspace.discover_project_root", return_value=project_root):
                with patch(
                    "jobs_ai.sources.seeding.fetch_text",
                    side_effect=_mapping_fetcher(
                        {
                            "https://boards.greenhouse.io/acme": _fixture_text("greenhouse_board.html"),
                        }
                    ),
                ):
                    result = RUNNER.invoke(
                        app,
                        ["sources", "seed-bulk", "--from-file", str(companies_path)],
                        env=env,
                    )

            self.assertEqual(result.exit_code, 0)
            self.assertIn("confirmed registry sources: 1", result.stdout)
            self.assertIn("registry created: 1", result.stdout)
            self.assertIn("registry updated: 1", result.stdout)
            entries = list_registry_sources(database_path)
            self.assertEqual(len(entries), 1)
            self.assertEqual(entries[0].status, "active")
            self.assertEqual(entries[0].source_url, "https://boards.greenhouse.io/acme")
            self.assertIn("https://boards.greenhouse.io/acme", entries[0].provenance or "")
            self.assertIn("https://boards.greenhouse.io/acme/jobs/12345?gh_src=linkedin", entries[0].provenance or "")

    def test_cli_sources_seed_bulk_keeps_registry_collect_import_flow_working(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            project_root = Path(tmp_dir) / "workspace"
            database_path = project_root / "runtime" / "jobs_ai.db"
            companies_path = project_root / "companies.txt"
            companies_path.parent.mkdir(parents=True, exist_ok=True)
            companies_path.write_text(
                "Acme Data | https://boards.greenhouse.io/acme\n",
                encoding="utf-8",
            )
            env = {"JOBS_AI_DB_PATH": str(database_path)}

            with patch("jobs_ai.workspace.discover_project_root", return_value=project_root):
                with patch(
                    "jobs_ai.sources.seeding.fetch_text",
                    side_effect=_mapping_fetcher(
                        {
                            "https://boards.greenhouse.io/acme": _fixture_text("greenhouse_board.html"),
                        }
                    ),
                ):
                    seed_result = RUNNER.invoke(
                        app,
                        ["sources", "seed-bulk", "--from-file", str(companies_path)],
                        env=env,
                    )

                with patch(
                    "jobs_ai.sources.workflow.fetch_text",
                    side_effect=_mapping_fetcher(
                        {
                            "https://boards.greenhouse.io/acme": _fixture_text("greenhouse_board.html"),
                        }
                    ),
                ):
                    collect_result = RUNNER.invoke(
                        app,
                        [
                            "sources",
                            "collect",
                            "--import",
                            "--out-dir",
                            "registry-seed-bulk-collect",
                        ],
                        env=env,
                    )

            self.assertEqual(seed_result.exit_code, 0)
            self.assertEqual(collect_result.exit_code, 0)
            self.assertIn("selected registry sources: 1", collect_result.stdout)
            self.assertIn("imported jobs: 2", collect_result.stdout)
            with closing(connect_database(database_path)) as connection:
                row = connection.execute("SELECT COUNT(*) AS count FROM jobs").fetchone()
            self.assertEqual(row["count"], 2)


class DiscoverATSTest(unittest.TestCase):
    def test_discover_registry_ats_sources_detects_greenhouse_lever_and_ashby_boards(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            project_root = Path(tmp_dir) / "workspace"
            paths = build_workspace_paths(Path("runtime/jobs_ai.db"), project_root=project_root)
            ensure_workspace(paths)

            result = discover_registry_ats_sources(
                paths,
                limit=10,
                slug_candidates=("acme", "northwind", "signalops"),
                timeout_seconds=1.0,
                max_concurrency=1,
                max_requests_per_second=1000.0,
                fetcher=_mapping_fetcher(
                    {
                        "https://boards-api.greenhouse.io/v1/boards/acme/jobs": _response(
                            "https://boards-api.greenhouse.io/v1/boards/acme/jobs",
                            json.dumps(
                                {
                                    "jobs": [
                                        {
                                            "title": "Data Engineer",
                                            "absolute_url": "https://boards.greenhouse.io/acme/jobs/12345",
                                        }
                                    ]
                                }
                            ),
                            content_type="application/json; charset=utf-8",
                        ),
                        "https://api.lever.co/v0/postings/northwind": _response(
                            "https://api.lever.co/v0/postings/northwind",
                            json.dumps(
                                [
                                    {
                                        "text": "Platform Engineer",
                                        "hostedUrl": "https://jobs.lever.co/northwind/lever-data-1",
                                    }
                                ]
                            ),
                            content_type="application/json; charset=utf-8",
                        ),
                        "https://jobs.ashbyhq.com/signalops": _fixture_text("ashby_board.html"),
                    }
                ),
            )

            self.assertEqual(result.greenhouse_count, 1)
            self.assertEqual(result.lever_count, 1)
            self.assertEqual(result.ashby_count, 1)
            self.assertEqual(result.manual_review_count, 0)
            self.assertEqual(result.created_count, 3)
            self.assertCountEqual(
                result.discovered_source_urls,
                (
                    "https://boards.greenhouse.io/acme",
                    "https://jobs.lever.co/northwind",
                    "https://jobs.ashbyhq.com/signalops",
                ),
            )

            entries = list_registry_sources(paths.database_path)
            self.assertEqual(len(entries), 3)
            self.assertTrue(all(entry.status == "active" for entry in entries))

    def test_discover_registry_ats_sources_dedupes_duplicate_slug_inputs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            project_root = Path(tmp_dir) / "workspace"
            paths = build_workspace_paths(Path("runtime/jobs_ai.db"), project_root=project_root)
            ensure_workspace(paths)

            result = discover_registry_ats_sources(
                paths,
                limit=10,
                slug_candidates=("acme", "acme", "ACME"),
                timeout_seconds=1.0,
                max_concurrency=1,
                max_requests_per_second=1000.0,
                fetcher=_mapping_fetcher(
                    {
                        "https://boards-api.greenhouse.io/v1/boards/acme/jobs": _response(
                            "https://boards-api.greenhouse.io/v1/boards/acme/jobs",
                            json.dumps({"jobs": [{"title": "Data Engineer"}]}),
                            content_type="application/json; charset=utf-8",
                        ),
                    }
                ),
            )

            self.assertEqual(result.tested_slug_count, 1)
            self.assertEqual(result.greenhouse_count, 1)
            self.assertEqual(result.created_count, 1)
            self.assertEqual(len(list_registry_sources(paths.database_path)), 1)

    def test_discover_registry_ats_sources_upserts_existing_manual_review_entry(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            project_root = Path(tmp_dir) / "workspace"
            paths = build_workspace_paths(Path("runtime/jobs_ai.db"), project_root=project_root)
            ensure_workspace(paths)
            initialize_schema(paths.database_path)

            upsert_registry_source(
                paths.database_path,
                source_url="https://jobs.lever.co/northwind",
                portal_type="lever",
                company="Northwind",
                status="manual_review",
                verification_reason_code="seed_manual_review",
                verification_reason="seeded for review",
                mark_verified_at=True,
            )

            result = discover_registry_ats_sources(
                paths,
                limit=10,
                slug_candidates=("northwind",),
                timeout_seconds=1.0,
                max_concurrency=1,
                max_requests_per_second=1000.0,
                fetcher=_mapping_fetcher(
                    {
                        "https://api.lever.co/v0/postings/northwind": _response(
                            "https://api.lever.co/v0/postings/northwind",
                            json.dumps(
                                [
                                    {
                                        "text": "Platform Engineer",
                                        "hostedUrl": "https://jobs.lever.co/northwind/lever-data-1",
                                    }
                                ]
                            ),
                            content_type="application/json; charset=utf-8",
                        ),
                    }
                ),
            )

            self.assertEqual(result.updated_count, 1)
            self.assertEqual(result.lever_count, 1)
            entries = list_registry_sources(paths.database_path)
            self.assertEqual(len(entries), 1)
            self.assertEqual(entries[0].status, "active")
            self.assertEqual(entries[0].source_url, "https://jobs.lever.co/northwind")

    def test_discover_registry_ats_sources_respects_max_concurrency(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            project_root = Path(tmp_dir) / "workspace"
            paths = build_workspace_paths(Path("runtime/jobs_ai.db"), project_root=project_root)
            ensure_workspace(paths)

            state_lock = threading.Lock()
            current_requests = 0
            max_requests = 0

            def fetcher(request: FetchRequest) -> FetchResponse:
                nonlocal current_requests, max_requests
                with state_lock:
                    current_requests += 1
                    max_requests = max(max_requests, current_requests)
                try:
                    time.sleep(0.02)
                    return _response(
                        request.url,
                        "<!doctype html><html><body>not found</body></html>",
                        status_code=404,
                    )
                finally:
                    with state_lock:
                        current_requests -= 1

            result = discover_registry_ats_sources(
                paths,
                limit=10,
                slug_candidates=("acme", "northwind", "signalops", "orbital"),
                timeout_seconds=1.0,
                max_concurrency=2,
                max_requests_per_second=1000.0,
                fetcher=fetcher,
            )

            self.assertEqual(result.tested_slug_count, 4)
            self.assertLessEqual(max_requests, 2)
            self.assertEqual(result.created_count, 0)

    def test_discover_registry_ats_sources_stops_at_limit(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            project_root = Path(tmp_dir) / "workspace"
            paths = build_workspace_paths(Path("runtime/jobs_ai.db"), project_root=project_root)
            ensure_workspace(paths)

            result = discover_registry_ats_sources(
                paths,
                limit=2,
                slug_candidates=("acme", "northwind", "signalops"),
                timeout_seconds=1.0,
                max_concurrency=1,
                max_requests_per_second=1000.0,
                fetcher=_mapping_fetcher(
                    {
                        "https://boards-api.greenhouse.io/v1/boards/acme/jobs": _response(
                            "https://boards-api.greenhouse.io/v1/boards/acme/jobs",
                            json.dumps({"jobs": [{"title": "One"}]}),
                            content_type="application/json; charset=utf-8",
                        ),
                        "https://boards-api.greenhouse.io/v1/boards/northwind/jobs": _response(
                            "https://boards-api.greenhouse.io/v1/boards/northwind/jobs",
                            json.dumps({"jobs": [{"title": "Two"}]}),
                            content_type="application/json; charset=utf-8",
                        ),
                        "https://boards-api.greenhouse.io/v1/boards/signalops/jobs": _response(
                            "https://boards-api.greenhouse.io/v1/boards/signalops/jobs",
                            json.dumps({"jobs": [{"title": "Three"}]}),
                            content_type="application/json; charset=utf-8",
                        ),
                    }
                ),
            )

            self.assertEqual(result.greenhouse_count, 2)
            self.assertEqual(result.tested_slug_count, 2)
            self.assertEqual(len(result.discovered_source_urls), 2)
            self.assertEqual(len(list_registry_sources(paths.database_path)), 2)

    def test_discover_registry_ats_sources_supports_provider_filter_and_counts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            project_root = Path(tmp_dir) / "workspace"
            paths = build_workspace_paths(Path("runtime/jobs_ai.db"), project_root=project_root)
            ensure_workspace(paths)

            result = discover_registry_ats_sources(
                paths,
                limit=10,
                providers=("greenhouse",),
                slug_candidates=("acme", "northwind"),
                timeout_seconds=1.0,
                max_concurrency=1,
                max_requests_per_second=1000.0,
                fetcher=_mapping_fetcher(
                    {
                        "https://boards-api.greenhouse.io/v1/boards/acme/jobs": _response(
                            "https://boards-api.greenhouse.io/v1/boards/acme/jobs",
                            json.dumps({"jobs": [{"title": "One"}]}),
                            content_type="application/json; charset=utf-8",
                        ),
                        "https://boards-api.greenhouse.io/v1/boards/northwind/jobs": _response(
                            "https://boards-api.greenhouse.io/v1/boards/northwind/jobs",
                            json.dumps({"jobs": []}),
                            content_type="application/json; charset=utf-8",
                        ),
                    }
                ),
            )

            self.assertEqual(result.providers, ("greenhouse",))
            self.assertEqual(result.greenhouse_count, 1)
            self.assertEqual(result.lever_count, 0)
            self.assertEqual(result.ignored_count, 1)
            self.assertEqual(result.provider_counts[0].provider, "greenhouse")
            self.assertEqual(result.provider_counts[0].active, 1)
            self.assertEqual(result.provider_counts[0].manual_review, 0)
            self.assertEqual(result.provider_counts[0].ignored, 1)
            entries = list_registry_sources(paths.database_path)
            self.assertEqual(len(entries), 1)
            self.assertEqual(entries[0].source_url, "https://boards.greenhouse.io/acme")

    def test_cli_sources_discover_ats_writes_output_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            project_root = Path(tmp_dir) / "workspace"
            database_path = project_root / "runtime" / "jobs_ai.db"
            env = {"JOBS_AI_DB_PATH": str(database_path)}

            with patch("jobs_ai.workspace.discover_project_root", return_value=project_root):
                with patch(
                    "jobs_ai.sources.discover_ats.load_company_slug_candidates",
                    return_value=("acme", "northwind"),
                ):
                    with patch(
                        "jobs_ai.sources.discover_ats._fetch_text_allow_http_errors",
                        side_effect=_mapping_fetcher(
                            {
                                "https://boards-api.greenhouse.io/v1/boards/acme/jobs": _response(
                                    "https://boards-api.greenhouse.io/v1/boards/acme/jobs",
                                    json.dumps({"jobs": [{"title": "Data Engineer"}]}),
                                    content_type="application/json; charset=utf-8",
                                ),
                                "https://api.lever.co/v0/postings/northwind": _response(
                                    "https://api.lever.co/v0/postings/northwind",
                                    json.dumps(
                                        [
                                            {
                                                "text": "Platform Engineer",
                                                "hostedUrl": "https://jobs.lever.co/northwind/lever-data-1",
                                            }
                                        ]
                                    ),
                                    content_type="application/json; charset=utf-8",
                                ),
                            }
                        ),
                    ):
                        result = RUNNER.invoke(
                            app,
                            [
                                "sources",
                                "discover-ats",
                                "--limit",
                                "2",
                                "--output",
                                "discovered_boards.txt",
                            ],
                            env=env,
                        )

            self.assertEqual(result.exit_code, 0)
            self.assertIn("providers: greenhouse, lever, ashby", result.stdout)
            self.assertIn("provider probe counts:", result.stdout)
            self.assertIn("greenhouse boards found: 1", result.stdout)
            self.assertIn("lever boards found: 1", result.stdout)
            self.assertIn("registry created: 2", result.stdout)
            output_path = project_root / "discovered_boards.txt"
            self.assertTrue(output_path.exists())
            self.assertCountEqual(
                output_path.read_text(encoding="utf-8").splitlines(),
                [
                    "https://boards.greenhouse.io/acme",
                    "https://jobs.lever.co/northwind",
                ],
            )

    def test_cli_sources_expand_registry_dedupes_across_direct_site_and_probe_lanes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            project_root = Path(tmp_dir) / "workspace"
            database_path = project_root / "runtime" / "jobs_ai.db"
            inputs_path = project_root / "inputs.txt"
            inputs_path.parent.mkdir(parents=True, exist_ok=True)
            inputs_path.write_text(
                "\n".join(
                    [
                        "Acme Data | https://boards.greenhouse.io/acme",
                        "acme.example",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            env = {"JOBS_AI_DB_PATH": str(database_path)}

            with patch("jobs_ai.workspace.discover_project_root", return_value=project_root):
                with patch(
                    "jobs_ai.sources.seeding.fetch_text",
                    side_effect=_mapping_fetcher(
                        {
                            "https://boards.greenhouse.io/acme": _fixture_text("greenhouse_board.html"),
                        }
                    ),
                ):
                    with patch(
                        "jobs_ai.sources.detect_sites.fetch_text",
                        side_effect=_mapping_fetcher(
                            {
                                "https://acme.example": (
                                    "<!doctype html><html><body>"
                                    '<a href="https://boards.greenhouse.io/acme/jobs/12345?gh_src=linkedin">Careers</a>'
                                    "</body></html>"
                                ),
                                "https://boards.greenhouse.io/acme": _fixture_text("greenhouse_board.html"),
                            }
                        ),
                    ):
                        with patch(
                            "jobs_ai.sources.discover_ats.load_company_slug_candidates",
                            return_value=("acme",),
                        ):
                            with patch(
                                "jobs_ai.sources.discover_ats._fetch_text_allow_http_errors",
                                side_effect=_mapping_fetcher(
                                    {
                                        "https://boards-api.greenhouse.io/v1/boards/acme/jobs": _response(
                                            "https://boards-api.greenhouse.io/v1/boards/acme/jobs",
                                            json.dumps({"jobs": [{"title": "Data Engineer"}]}),
                                            content_type="application/json; charset=utf-8",
                                        ),
                                    }
                                ),
                            ):
                                result = RUNNER.invoke(
                                    app,
                                    [
                                        "sources",
                                        "expand-registry",
                                        "--from-file",
                                        str(inputs_path),
                                        "--detect-sites",
                                        "--discover-ats",
                                        "--provider",
                                        "greenhouse",
                                        "--limit",
                                        "5",
                                    ],
                                    env=env,
                                )

            self.assertEqual(result.exit_code, 0)
            self.assertIn("seed lane ran: yes", result.stdout)
            self.assertIn("detect-sites lane ran: yes", result.stdout)
            self.assertIn("discover-ats lane ran: yes", result.stdout)
            self.assertIn("provider probe counts:", result.stdout)

            entries = list_registry_sources(database_path)
            self.assertEqual(len(entries), 1)
            self.assertEqual(entries[0].status, "active")
            self.assertEqual(entries[0].source_url, "https://boards.greenhouse.io/acme")


if __name__ == "__main__":
    unittest.main()
