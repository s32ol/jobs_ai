from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import json
import sys
import tempfile
import unittest

SRC_PATH = Path(__file__).resolve().parents[1] / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

from jobs_ai.collect.fetch import FetchError, FetchRequest, FetchResponse
from jobs_ai.source_seed_fast import (
    _assess_company_match,
    build_ats_candidate_urls,
    build_seed_list,
    classify_company_result,
    infer_slug_candidates,
    normalize_company_domain,
)

FIXTURE_ROOT = Path(__file__).resolve().parent / "fixtures" / "collect"
FIXED_CREATED_AT = datetime(2026, 3, 14, 18, 30, 0, tzinfo=timezone.utc)


def _fixture_text(name: str) -> str:
    return (FIXTURE_ROOT / name).read_text(encoding="utf-8")


def _build_fetcher(payloads: dict[str, str]):
    def fetcher(request: FetchRequest) -> FetchResponse:
        if request.url not in payloads:
            raise FetchError(f"unable to fetch {request.url}: fixture not found")
        return FetchResponse(
            url=request.url,
            final_url=request.url,
            status_code=200,
            content_type="text/html; charset=utf-8",
            text=payloads[request.url],
        )

    return fetcher


class SourceSeedFastTest(unittest.TestCase):
    def test_normalize_company_domain_handles_urls_and_multilabel_suffixes(self) -> None:
        self.assertEqual(normalize_company_domain("https://www.OpenAI.com/careers"), "openai.com")
        self.assertEqual(normalize_company_domain("careers.northwind.co.uk/jobs"), "northwind.co.uk")
        self.assertEqual(normalize_company_domain("https://roles.signalops.com.sg/openings"), "signalops.com.sg")
        self.assertIsNone(normalize_company_domain("not-a-domain"))

    def test_infer_slug_candidates_prioritizes_domain_then_name(self) -> None:
        self.assertEqual(
            infer_slug_candidates("signalops.io", "Signal Ops Labs"),
            ("signalops", "signal-ops", "signal-ops-labs", "signalopslabs"),
        )

    def test_build_ats_candidate_urls_generates_expected_roots(self) -> None:
        candidates = build_ats_candidate_urls(("openai",))
        self.assertEqual(
            [candidate.url for candidate in candidates],
            [
                "https://boards.greenhouse.io/openai",
                "https://job-boards.greenhouse.io/openai",
                "https://jobs.lever.co/openai",
                "https://jobs.ashbyhq.com/openai",
            ],
        )

    def test_classify_company_result_prefers_confirmed_then_manual_review_then_skipped(self) -> None:
        confirmed = classify_company_result(
            "signalops.io",
            "Signal Ops",
            (
                {
                    "outcome": "confirmed",
                    "portal_type": "ashby",
                    "confirmed_root": "https://jobs.ashbyhq.com/signalops",
                    "reason_code": "company_match_exact",
                    "reason": "matched",
                },
            ),
        )
        self.assertEqual(
            confirmed,
            (
                "confirmed",
                "confirmed_board_root",
                "confirmed ashby board root",
                "https://jobs.ashbyhq.com/signalops",
                None,
            ),
        )

        manual_review = classify_company_result(
            "acme.ai",
            "Acme AI",
            (
                {
                    "outcome": "manual_review",
                    "portal_type": "greenhouse",
                    "confirmed_root": None,
                    "reason_code": "company_match_ambiguous",
                    "reason": "looked plausible",
                },
            ),
        )
        self.assertEqual(manual_review[0], "manual_review")
        self.assertEqual(manual_review[1], "company_match_ambiguous")

        skipped = classify_company_result(
            "orbital.dev",
            None,
            (
                {
                    "outcome": "skipped",
                    "portal_type": "lever",
                    "confirmed_root": None,
                    "reason_code": "fetch_failed",
                    "reason": "unable to fetch",
                },
            ),
        )
        self.assertEqual(skipped[0], "skipped")
        self.assertEqual(skipped[1], "no_confirmed_candidates")

    def test_assess_company_match_keeps_prefix_and_marketing_suffix_matches_manual(self) -> None:
        self.assertEqual(
            _assess_company_match("stripe.com", None, ("Stripe Inc",)),
            ("confirmed", "company_match_exact", "board company matched stripe.com"),
        )
        self.assertEqual(
            _assess_company_match("scale.com", None, ("Scale AI",)),
            (
                "manual_review",
                "company_match_ambiguous",
                "board company 'Scale AI' looked plausible but did not match 'scale.com' strongly enough",
            ),
        )
        self.assertEqual(
            _assess_company_match("northwind.com", None, ("Northwind Talent",)),
            (
                "manual_review",
                "company_match_ambiguous",
                "board company 'Northwind Talent' looked plausible but did not match 'northwind.com' strongly enough",
            ),
        )

    def test_build_seed_list_writes_expected_artifacts_deterministically(self) -> None:
        seed_html = """
<!doctype html>
<html>
  <body>
    <a href="https://signalops.io">Signal Ops</a>
    <a href="https://acme.ai">Acme AI</a>
    <a href="https://portfolio.example.com/internal">Internal</a>
    <a href="https://www.linkedin.com/company/ignore-me">LinkedIn</a>
  </body>
</html>
""".strip()
        fetcher = _build_fetcher(
            {
                "https://portfolio.example.com/companies": seed_html,
                "https://jobs.ashbyhq.com/signalops": _fixture_text("ashby_board.html"),
                "https://boards.greenhouse.io/acme": _fixture_text("greenhouse_board.html"),
            }
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            seed_pages_file = temp_root / "seed_pages.txt"
            domains_file = temp_root / "domains.txt"
            out_dir = temp_root / "seed-output"

            seed_pages_file.write_text(
                "# seed pages\nhttps://portfolio.example.com/companies\n",
                encoding="utf-8",
            )
            domains_file.write_text("orbital.dev\n", encoding="utf-8")

            result = build_seed_list(
                seed_pages_file,
                domains_file=domains_file,
                out_dir=out_dir,
                timeout_seconds=1.0,
                max_workers=1,
                created_at=FIXED_CREATED_AT,
                fetcher=fetcher,
            )

            self.assertEqual(result.confirmed_roots, ("https://jobs.ashbyhq.com/signalops",))
            self.assertEqual([item.company_domain for item in result.company_results], ["acme.ai", "orbital.dev", "signalops.io"])
            self.assertEqual([item.outcome for item in result.company_results], ["manual_review", "skipped", "confirmed"])

            ats_roots_text = result.ats_roots_path.read_text(encoding="utf-8")
            manual_review_text = result.manual_review_path.read_text(encoding="utf-8")
            seed_report_text = result.seed_report_path.read_text(encoding="utf-8")

            self.assertEqual(ats_roots_text, "https://jobs.ashbyhq.com/signalops\n")

            manual_review_payload = json.loads(manual_review_text)
            self.assertEqual(len(manual_review_payload), 1)
            self.assertEqual(manual_review_payload[0]["company_domain"], "acme.ai")
            self.assertEqual(manual_review_payload[0]["reason_code"], "company_match_ambiguous")

            seed_report_payload = json.loads(seed_report_text)
            self.assertEqual(seed_report_payload["run_id"], "seed-20260314T183000000000Z")
            self.assertEqual(seed_report_payload["confirmed_count"], 1)
            self.assertEqual(seed_report_payload["manual_review_count"], 1)
            self.assertEqual(seed_report_payload["skipped_count"], 1)
            self.assertEqual(
                seed_report_payload["artifacts"]["ats_roots_path"],
                str(out_dir / "ats_roots.txt"),
            )

            second_result = build_seed_list(
                seed_pages_file,
                domains_file=domains_file,
                out_dir=out_dir,
                timeout_seconds=1.0,
                max_workers=1,
                created_at=FIXED_CREATED_AT,
                fetcher=fetcher,
            )
            self.assertEqual(second_result.seed_report_path.read_text(encoding="utf-8"), seed_report_text)
            self.assertEqual(second_result.manual_review_path.read_text(encoding="utf-8"), manual_review_text)


if __name__ == "__main__":
    unittest.main()
