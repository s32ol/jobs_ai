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
from jobs_ai.collect.fetch import FetchError, FetchRequest, FetchResponse
from jobs_ai.source_seed.cli import run_seed_sources_command
from jobs_ai.source_seed.harness import run_source_seeding
from jobs_ai.source_seed.infer import build_source_candidates, infer_slug_candidates, parse_company_inputs
from jobs_ai.source_seed.models import CompanySeedInput, SourceCandidate
from jobs_ai.source_seed.verify import verify_source_candidate
from jobs_ai.workspace import build_workspace_paths

FIXTURE_ROOT = Path(__file__).resolve().parent / "fixtures" / "collect"
RUNNER = CliRunner()
FIXED_CREATED_AT = datetime(2026, 3, 13, 19, 45, 0, tzinfo=timezone.utc)
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


class SourceSeedTest(unittest.TestCase):
    def test_parse_company_inputs_ignores_comments_and_supports_domain_and_notes(self) -> None:
        parsed = parse_company_inputs(
            [
                "# curated targets",
                "OpenAI | https://www.openai.com/careers | priority",
                "",
                "ACME, Inc.",
                "Northwind Talent | northwind.com",
            ]
        )

        self.assertEqual(len(parsed), 3)
        self.assertEqual(parsed[0].index, 1)
        self.assertEqual(parsed[0].company, "OpenAI")
        self.assertEqual(parsed[0].domain, "www.openai.com")
        self.assertEqual(parsed[0].notes, "priority")
        self.assertEqual(parsed[1].index, 2)
        self.assertEqual(parsed[1].company, "ACME, Inc.")
        self.assertIsNone(parsed[1].domain)
        self.assertEqual(parsed[2].index, 3)
        self.assertEqual(parsed[2].company, "Northwind Talent")
        self.assertEqual(parsed[2].domain, "northwind.com")

    def test_infer_slug_candidates_are_conservative_and_deterministic(self) -> None:
        company_input = CompanySeedInput(
            index=1,
            raw_value="ACME, Inc. | https://www.acme-data.com",
            company="ACME, Inc.",
            domain="www.acme-data.com",
            notes=None,
        )

        slug_candidates = infer_slug_candidates(company_input)

        self.assertEqual(
            [(candidate.slug, candidate.slug_source, candidate.confidence) for candidate in slug_candidates],
            [
                ("acme-data", "domain_label", "high"),
                ("acmedata", "domain_label_compact", "medium"),
                ("acme", "company_compact", "medium"),
            ],
        )

    def test_build_source_candidates_expands_slugs_across_supported_portals(self) -> None:
        company_input = CompanySeedInput(
            index=1,
            raw_value="OpenAI | openai.com",
            company="OpenAI",
            domain="openai.com",
            notes=None,
        )

        candidates = build_source_candidates(company_input)

        self.assertEqual(
            [candidate.url for candidate in candidates],
            [
                "https://boards.greenhouse.io/openai",
                "https://jobs.lever.co/openai",
                "https://jobs.ashbyhq.com/openai",
            ],
        )

    def test_verify_source_candidate_confirms_supported_board_roots(self) -> None:
        cases = (
            (
                CompanySeedInput(1, "Acme Data", "Acme Data", None, None),
                SourceCandidate(1, "greenhouse", "acme", "https://boards.greenhouse.io/acme", "company_compact", "medium"),
                _fixture_text("greenhouse_board.html"),
                "Acme Data",
            ),
            (
                CompanySeedInput(1, "Northwind Talent", "Northwind Talent", None, None),
                SourceCandidate(1, "lever", "northwind", "https://jobs.lever.co/northwind", "company_primary_token", "low"),
                _fixture_text("lever_board.html"),
                "Northwind Talent",
            ),
            (
                CompanySeedInput(1, "Signal Ops", "Signal Ops", None, None),
                SourceCandidate(1, "ashby", "signalops", "https://jobs.ashbyhq.com/signalops", "company_compact", "medium"),
                _fixture_text("ashby_board.html"),
                "Signal Ops",
            ),
        )

        for company_input, candidate, html_text, expected_company in cases:
            with self.subTest(portal=candidate.portal_type):
                result = verify_source_candidate(
                    company_input,
                    candidate,
                    timeout_seconds=10.0,
                    fetcher=_mapping_fetcher({candidate.url: html_text}),
                )

                self.assertEqual(result.outcome, "confirmed")
                self.assertEqual(result.reason_code, "confirmed_board_root")
                self.assertEqual(result.detected_company, expected_company)
                self.assertEqual(result.confirmed_url, candidate.url)
                assert result.evidence is not None
                self.assertEqual(result.evidence.final_url, candidate.url)

    def test_verify_source_candidate_marks_ambiguous_board_as_manual_review(self) -> None:
        company_input = CompanySeedInput(1, "Acme Data", "Acme Data", None, None)
        candidate = SourceCandidate(
            1,
            "greenhouse",
            "acme",
            "https://boards.greenhouse.io/acme",
            "company_compact",
            "medium",
        )

        result = verify_source_candidate(
            company_input,
            candidate,
            timeout_seconds=10.0,
            fetcher=_mapping_fetcher({candidate.url: _fixture_text("greenhouse_board_partial.html")}),
        )

        self.assertEqual(result.outcome, "manual_review")
        self.assertEqual(result.reason_code, "greenhouse_parse_ambiguous")
        self.assertIn("manual review required", result.reason)

    def test_seed_source_handling_skips_fetch_failures_non_html_and_bad_input(self) -> None:
        fetch_failure_candidate = SourceCandidate(
            1,
            "greenhouse",
            "acme",
            "https://boards.greenhouse.io/acme",
            "company_compact",
            "medium",
        )
        fetch_failure_result = verify_source_candidate(
            CompanySeedInput(1, "Acme Data", "Acme Data", None, None),
            fetch_failure_candidate,
            timeout_seconds=3.0,
            fetcher=_mapping_fetcher(
                {
                    fetch_failure_candidate.url: FetchError(
                        "timed out after 3.0s while fetching https://boards.greenhouse.io/acme"
                    )
                }
            ),
        )
        self.assertEqual(fetch_failure_result.outcome, "skipped")
        self.assertEqual(fetch_failure_result.reason_code, "fetch_failed")

        non_html_candidate = SourceCandidate(
            1,
            "lever",
            "northwind",
            "https://jobs.lever.co/northwind",
            "company_primary_token",
            "low",
        )
        non_html_result = verify_source_candidate(
            CompanySeedInput(1, "Northwind Talent", "Northwind Talent", None, None),
            non_html_candidate,
            timeout_seconds=10.0,
            fetcher=_mapping_fetcher(
                {
                    non_html_candidate.url: _response(
                        non_html_candidate.url,
                        "%PDF-1.4",
                        content_type="application/pdf",
                    )
                }
            ),
        )
        self.assertEqual(non_html_result.outcome, "skipped")
        self.assertEqual(non_html_result.reason_code, "non_html_content")

        run = run_source_seeding(
            [
                CompanySeedInput(
                    index=1,
                    raw_value="| openai.com",
                    company=None,
                    domain="openai.com",
                    notes=None,
                )
            ],
            timeout_seconds=10.0,
            created_at=FIXED_CREATED_AT,
            fetcher=_mapping_fetcher({}),
        )
        self.assertEqual(run.report.skipped_count, 1)
        self.assertEqual(run.report.company_results[0].reason_code, "missing_company_name")

    def test_write_source_seed_artifacts_are_deterministic(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            project_root = Path(tmp_dir) / "workspace"
            project_root.mkdir(parents=True, exist_ok=True)
            output_dir = project_root / "out" / "seed"
            paths = build_workspace_paths(Path("data/jobs_ai.db"), project_root=project_root)

            run = run_seed_sources_command(
                paths,
                companies=("Acme Data | acme.com", "Northwind Analytics | northwind.com"),
                from_file=None,
                out_dir=output_dir,
                label="seed-batch",
                timeout_seconds=5.0,
                report_only=False,
                created_at=FIXED_CREATED_AT,
                fetcher=_mapping_fetcher(
                    {
                        "https://boards.greenhouse.io/acme": _fixture_text("greenhouse_board.html"),
                        "https://jobs.lever.co/northwind": _fixture_text("lever_board.html"),
                    }
                ),
            )

            artifact_paths = run.report.artifact_paths
            assert artifact_paths is not None
            confirmed_text = artifact_paths.confirmed_sources_path.read_text(encoding="utf-8")
            manual_review_payload = json.loads(
                artifact_paths.manual_review_sources_path.read_text(encoding="utf-8")
            )
            report_payload = json.loads(artifact_paths.seed_report_path.read_text(encoding="utf-8"))

        self.assertEqual(run.report.run_id, "seed-sources-seed-batch-20260313T194500000000Z")
        self.assertEqual(confirmed_text, "https://boards.greenhouse.io/acme\n")
        self.assertEqual(manual_review_payload["run_id"], "seed-sources-seed-batch-20260313T194500000000Z")
        self.assertEqual(manual_review_payload["finished_at"], FIXED_TIMESTAMP)
        self.assertEqual(manual_review_payload["item_count"], 1)
        self.assertEqual(manual_review_payload["items"][0]["company"], "Northwind Analytics")
        self.assertEqual(
            manual_review_payload["items"][0]["attempted_candidates"][3]["reason_code"],
            "company_name_mismatch",
        )
        self.assertEqual(report_payload["run_id"], "seed-sources-seed-batch-20260313T194500000000Z")
        self.assertEqual(report_payload["created_at"], FIXED_TIMESTAMP)
        self.assertEqual(report_payload["finished_at"], FIXED_TIMESTAMP)
        self.assertEqual(
            report_payload["totals"],
            {
                "input_companies": 2,
                "confirmed": 1,
                "manual_review_needed": 1,
                "skipped": 0,
                "confirmed_sources": 1,
            },
        )
        self.assertEqual(
            report_payload["companies"][0]["confirmed_sources"],
            ["https://boards.greenhouse.io/acme"],
        )

    def test_cli_seed_sources_prints_summary(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            project_root = Path(tmp_dir) / "workspace"
            project_root.mkdir(parents=True, exist_ok=True)
            paths = build_workspace_paths(Path("data/jobs_ai.db"), project_root=project_root)
            companies_path = project_root / "companies.txt"
            companies_path.write_text(
                "\n".join(
                    [
                        "# seed batch",
                        "Acme Data | acme.com",
                        "Northwind Analytics | northwind.com",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            output_dir = project_root / "artifacts"

            with patch("jobs_ai.cli._load_runtime", return_value=(None, paths)):
                with patch(
                    "jobs_ai.source_seed.cli.fetch_text",
                    _mapping_fetcher(
                        {
                            "https://boards.greenhouse.io/acme": _fixture_text("greenhouse_board.html"),
                            "https://jobs.lever.co/northwind": _fixture_text("lever_board.html"),
                        }
                    ),
                ):
                    result = RUNNER.invoke(
                        app,
                        [
                            "seed-sources",
                            "--from-file",
                            str(companies_path),
                            "--out-dir",
                            str(output_dir),
                            "--label",
                            "cli-seed",
                        ],
                    )

        self.assertEqual(result.exit_code, 0)
        self.assertIn("jobs_ai seed-sources", result.stdout)
        self.assertIn("confirmed: 1", result.stdout)
        self.assertIn("manual review: 1", result.stdout)
        self.assertIn("skipped: 0", result.stdout)
        self.assertIn(f"seed report: {output_dir / 'seed_report.json'}", result.stdout)
        self.assertIn(f"confirmed sources artifact: {output_dir / 'confirmed_sources.txt'}", result.stdout)


if __name__ == "__main__":
    unittest.main()
