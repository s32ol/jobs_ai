from __future__ import annotations

from pathlib import Path
import sys
import unittest

from typer.testing import CliRunner

SRC_PATH = Path(__file__).resolve().parents[1] / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

from jobs_ai.cli import app
from jobs_ai.portal_support import build_portal_support, detect_portal_type

RUNNER = CliRunner()


class PortalSupportTest(unittest.TestCase):
    def test_detect_portal_type_supports_representative_urls(self) -> None:
        self.assertEqual(
            detect_portal_type("https://boards.greenhouse.io/acme/jobs/12345?gh_jid=12345"),
            "greenhouse",
        )
        self.assertEqual(
            detect_portal_type("https://jobs.lever.co/acme/abcdef?lever-source=LinkedIn"),
            "lever",
        )
        self.assertEqual(
            detect_portal_type(
                "https://jobs.ashbyhq.com/acme?jobId=123e4567-e89b-12d3-a456-426614174000"
            ),
            "ashby",
        )
        self.assertEqual(
            detect_portal_type(
                "https://acme.wd5.myworkdayjobs.com/en-US/External/job/Remote-USA/Data-Engineer_R12345"
            ),
            "workday",
        )

    def test_build_portal_support_extracts_greenhouse_company_link(self) -> None:
        portal_support = build_portal_support(
            "https://boards.greenhouse.io/acme?gh_jid=12345&gh_src=linkedin"
        )

        self.assertIsNotNone(portal_support)
        assert portal_support is not None
        self.assertEqual(portal_support.portal_type, "greenhouse")
        self.assertEqual(
            portal_support.normalized_apply_url,
            "https://boards.greenhouse.io/acme?gh_jid=12345",
        )
        self.assertEqual(
            portal_support.company_apply_url,
            "https://boards.greenhouse.io/acme/jobs/12345",
        )

    def test_build_portal_support_normalizes_lever_tracking_params(self) -> None:
        portal_support = build_portal_support(
            "https://jobs.lever.co/acme/abcdef?lever-source=LinkedIn&utm_campaign=spring"
        )

        self.assertIsNotNone(portal_support)
        assert portal_support is not None
        self.assertEqual(portal_support.portal_type, "lever")
        self.assertEqual(
            portal_support.normalized_apply_url,
            "https://jobs.lever.co/acme/abcdef",
        )
        self.assertIsNone(portal_support.company_apply_url)

    def test_build_portal_support_extracts_ashby_company_link(self) -> None:
        portal_support = build_portal_support(
            "https://jobs.ashbyhq.com/acme?jobId=123e4567-e89b-12d3-a456-426614174000&utm_source=linkedin"
        )

        self.assertIsNotNone(portal_support)
        assert portal_support is not None
        self.assertEqual(portal_support.portal_type, "ashby")
        self.assertEqual(
            portal_support.company_apply_url,
            "https://jobs.ashbyhq.com/acme/123e4567-e89b-12d3-a456-426614174000",
        )

    def test_cli_portal_hint_reports_supported_portal_details(self) -> None:
        result = RUNNER.invoke(
            app,
            [
                "portal-hint",
                "https://boards.greenhouse.io/acme?gh_jid=12345&gh_src=linkedin",
            ],
        )

        self.assertEqual(result.exit_code, 0)
        self.assertIn("jobs_ai portal-hint", result.stdout)
        self.assertIn("portal type: Greenhouse", result.stdout)
        self.assertIn(
            "normalized apply_url: https://boards.greenhouse.io/acme?gh_jid=12345",
            result.stdout,
        )
        self.assertIn(
            "company apply_url: https://boards.greenhouse.io/acme/jobs/12345",
            result.stdout,
        )
        self.assertIn("status: supported", result.stdout)

    def test_cli_portal_hint_handles_unsupported_urls_cleanly(self) -> None:
        result = RUNNER.invoke(app, ["portal-hint", "https://example.com/jobs/123"])

        self.assertEqual(result.exit_code, 0)
        self.assertIn("status: no supported portal helper available", result.stdout)
        self.assertIn("supported portals: greenhouse, lever, ashby, workday", result.stdout)
        self.assertIn("tip: use --portal-type only when you already know the hosting portal", result.stdout)


if __name__ == "__main__":
    unittest.main()
