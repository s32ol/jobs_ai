from __future__ import annotations

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

from jobs_ai.application_prefill import run_application_prefill
from jobs_ai.cli import app
from jobs_ai.prefill_browser import (
    BrowserFieldOption,
    BrowserFieldSnapshot,
    BrowserPageSnapshot,
    FixturePrefillBrowserBackend,
)

RUNNER = CliRunner()


class ApplicationPrefillTest(unittest.TestCase):
    def test_run_application_prefill_greenhouse_fills_safe_fields_uploads_resume_and_flags_unresolved_required_fields(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            manifest_path = tmp_path / "session.json"
            profile_path = tmp_path / ".jobs_ai_applicant_profile.json"
            resume_dir = tmp_path / "resumes"
            resume_dir.mkdir(parents=True, exist_ok=True)
            resume_path = resume_dir / "data-engineering.pdf"
            resume_path.write_text("resume", encoding="utf-8")
            _write_manifest(
                manifest_path,
                {
                    "created_at": "2026-03-16T01:00:00Z",
                    "item_count": 1,
                    "items": [
                        _complete_item(
                            rank=1,
                            company="Acme Data",
                            title="Data Engineer",
                            apply_url="https://boards.greenhouse.io/acme/jobs/12345",
                            portal_type="greenhouse",
                            resume_key="data-engineering",
                            resume_label="Data Engineering Resume",
                            snippet_key="pipeline-delivery",
                            snippet_label="Pipeline Delivery",
                            snippet_text="Python-first pipeline delivery across SQL warehouses.",
                        ),
                    ],
                },
            )
            _write_profile(
                profile_path,
                {
                    "full_name": "Pat Example",
                    "email": "pat@example.com",
                    "phone": "555-0100",
                    "location": "Remote",
                    "linkedin": "https://linkedin.com/in/patexample",
                    "authorized_to_work_in_us": True,
                    "resume_paths": {
                        "data-engineering": str(resume_path),
                    },
                    "use_recommended_profile_snippet": True,
                },
            )
            browser = FixturePrefillBrowserBackend(
                {
                    "https://boards.greenhouse.io/acme/jobs/12345": BrowserPageSnapshot(
                        url="https://boards.greenhouse.io/acme/jobs/12345",
                        title="Job Application for Data Engineer at Acme Data",
                        fields=(
                            _field("First Name", selector="[data-jobs-ai-selector='first_name']", required=True),
                            _field("Last Name", selector="[data-jobs-ai-selector='last_name']", required=True),
                            _field("Email", control_type="email", selector="[data-jobs-ai-selector='email']", required=True),
                            _field("Phone", control_type="tel", selector="[data-jobs-ai-selector='phone']"),
                            _field("LinkedIn", control_type="url", selector="[data-jobs-ai-selector='linkedin']"),
                            _field(
                                "Are you authorized to work in the United States",
                                control_type="select",
                                selector="[data-jobs-ai-selector='auth']",
                                required=True,
                                options=(BrowserFieldOption("Yes", "yes"), BrowserFieldOption("No", "no")),
                            ),
                            _field("Resume", control_type="file", selector="[data-jobs-ai-selector='resume']", required=True),
                            _field(
                                "Cover Letter",
                                control_type="textarea",
                                selector="[data-jobs-ai-selector='cover_letter']",
                            ),
                            _field(
                                "Current Employer",
                                selector="[data-jobs-ai-selector='employer']",
                                required=True,
                            ),
                        ),
                        submit_controls=("Submit application",),
                    )
                }
            )

            result = run_application_prefill(
                manifest_path,
                project_root=tmp_path,
                applicant_profile_path=profile_path,
                launch_order=None,
                browser_backend=browser,
            )

        self.assertEqual(result.portal_label, "Greenhouse")
        self.assertEqual(result.status, "partial")
        self.assertTrue(result.stopped_before_submit)
        self.assertEqual(result.resolved_resume_path, resume_path)
        self.assertEqual(
            [field.field_key for field in result.filled_fields],
            [
                "first_name",
                "last_name",
                "email",
                "phone",
                "linkedin_url",
                "authorized_to_work_in_us",
                "resume",
                "short_text",
            ],
        )
        self.assertIn("Current Employer", result.unresolved_required_fields)

    def test_run_application_prefill_greenhouse_matches_resume_by_identifier_and_ignores_hidden_required_helpers(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            manifest_path = tmp_path / "session.json"
            profile_path = tmp_path / ".jobs_ai_applicant_profile.json"
            resume_path = tmp_path / "resume.pdf"
            resume_path.write_text("resume", encoding="utf-8")
            _write_manifest(
                manifest_path,
                {
                    "created_at": "2026-03-16T01:00:00Z",
                    "item_count": 1,
                    "items": [
                        _complete_item(
                            rank=1,
                            company="Care Access",
                            title="Enterprise Performance Analytics Engineer",
                            apply_url="https://job-boards.greenhouse.io/careaccess/jobs/4052147009",
                            portal_type="greenhouse",
                            resume_key="analytics-engineering",
                            resume_label="Analytics Engineering Resume",
                            snippet_key="analytics-modeling",
                            snippet_label="Analytics Modeling",
                            snippet_text="Analytics engineering work centered on SQL modeling.",
                        ),
                    ],
                },
            )
            _write_profile(
                profile_path,
                {
                    "full_name": "Robert Morales",
                    "email": "robert.morales.eng@gmail.com",
                    "phone": "415-900-2819",
                    "linkedin": "https://linkedin.com/in/s32ol",
                    "resume_paths": {
                        "analytics-engineering": str(resume_path),
                    },
                },
            )
            browser = FixturePrefillBrowserBackend(
                {
                    "https://job-boards.greenhouse.io/careaccess/jobs/4052147009": BrowserPageSnapshot(
                        url="https://job-boards.greenhouse.io/careaccess/jobs/4052147009",
                        title="Job Application for Enterprise Performance Analytics Engineer at Care Access",
                        fields=(
                            _field(
                                "First Name*",
                                selector="[data-jobs-ai-selector='jobs-ai-1']",
                                name="first_name",
                                required=True,
                            ),
                            _field(
                                "Last Name*",
                                selector="[data-jobs-ai-selector='jobs-ai-2']",
                                name="last_name",
                                required=True,
                            ),
                            _field(
                                "Email*",
                                selector="[data-jobs-ai-selector='jobs-ai-4']",
                                name="email",
                                required=True,
                            ),
                            _field(
                                "Phone",
                                selector="[data-jobs-ai-selector='jobs-ai-6']",
                                control_type="tel",
                                name="phone",
                            ),
                            _field(
                                "Attach",
                                selector="[data-jobs-ai-selector='jobs-ai-7']",
                                control_type="file",
                                name="resume",
                                required=True,
                            ),
                            _field(
                                "LinkedIn Profile",
                                selector="[data-jobs-ai-selector='jobs-ai-21']",
                                name="question_4371762009",
                            ),
                            _field(
                                "",
                                selector="[data-jobs-ai-selector='jobs-ai-10']",
                                name=None,
                                required=True,
                                visible=False,
                            ),
                            _field(
                                "",
                                selector="[data-jobs-ai-selector='jobs-ai-23']",
                                name=None,
                                required=True,
                                visible=False,
                            ),
                        ),
                        submit_controls=("Submit application",),
                    )
                }
            )

            result = run_application_prefill(
                manifest_path,
                project_root=tmp_path,
                applicant_profile_path=profile_path,
                launch_order=None,
                browser_backend=browser,
            )

        self.assertEqual(result.status, "success")
        self.assertEqual(result.unresolved_required_fields, ())
        self.assertEqual(
            [field.field_key for field in result.filled_fields],
            ["first_name", "last_name", "email", "phone", "linkedin_url", "resume"],
        )

    def test_run_application_prefill_lever_uses_canned_answers_and_profile_short_text(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            manifest_path = tmp_path / "session.json"
            profile_path = tmp_path / ".jobs_ai_applicant_profile.json"
            resume_path = tmp_path / "lever.pdf"
            resume_path.write_text("resume", encoding="utf-8")
            _write_manifest(
                manifest_path,
                {
                    "created_at": "2026-03-16T01:00:00Z",
                    "item_count": 1,
                    "items": [
                        _complete_item(
                            rank=1,
                            company="Northwind",
                            title="Data Engineer",
                            apply_url="https://jobs.lever.co/northwind/abc123",
                            portal_type="lever",
                            resume_key="data-engineering",
                            resume_label="Data Engineering Resume",
                            snippet_key="pipeline-delivery",
                            snippet_label="Pipeline Delivery",
                            snippet_text="unused snippet",
                        ),
                    ],
                },
            )
            _write_profile(
                profile_path,
                {
                    "full_name": "Pat Example",
                    "email": "pat@example.com",
                    "phone": "555-0100",
                    "short_text": "Short intro",
                    "resume_paths": {
                        "data-engineering": str(resume_path),
                    },
                    "canned_answers": {
                        "How did you hear about us": "Referral",
                    },
                },
            )
            browser = FixturePrefillBrowserBackend(
                {
                    "https://jobs.lever.co/northwind/abc123": BrowserPageSnapshot(
                        url="https://jobs.lever.co/northwind/abc123",
                        title="Apply to Northwind",
                        fields=(
                            _field("Full Name", selector="[data-jobs-ai-selector='full_name']", required=True),
                            _field("Email", control_type="email", selector="[data-jobs-ai-selector='email']", required=True),
                            _field("Resume", control_type="file", selector="[data-jobs-ai-selector='resume']", required=True),
                            _field(
                                "Additional Information",
                                control_type="textarea",
                                selector="[data-jobs-ai-selector='info']",
                            ),
                            _field(
                                "How did you hear about us",
                                control_type="select",
                                selector="[data-jobs-ai-selector='source']",
                                options=(
                                    BrowserFieldOption("Referral", "referral"),
                                    BrowserFieldOption("LinkedIn", "linkedin"),
                                ),
                            ),
                        ),
                        submit_controls=("Submit application",),
                    )
                }
            )

            result = run_application_prefill(
                manifest_path,
                project_root=tmp_path,
                applicant_profile_path=profile_path,
                launch_order=None,
                browser_backend=browser,
            )

        self.assertEqual(result.portal_label, "Lever")
        self.assertEqual(result.status, "success")
        self.assertEqual(result.unresolved_required_fields, ())
        self.assertEqual(
            [field.field_key for field in result.filled_fields],
            ["full_name", "email", "resume", "short_text", "canned_answer"],
        )

    def test_run_application_prefill_workday_stays_manual_handoff(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            manifest_path = tmp_path / "session.json"
            profile_path = tmp_path / ".jobs_ai_applicant_profile.json"
            _write_manifest(
                manifest_path,
                {
                    "created_at": "2026-03-16T01:00:00Z",
                    "item_count": 1,
                    "items": [
                        _complete_item(
                            rank=1,
                            company="Contoso",
                            title="Platform Engineer",
                            apply_url="https://acme.wd5.myworkdayjobs.com/en-US/External/job/Remote/Platform-Engineer_R12345",
                            portal_type="workday",
                            resume_key="data-engineering",
                            resume_label="Data Engineering Resume",
                            snippet_key="pipeline-delivery",
                            snippet_label="Pipeline Delivery",
                            snippet_text="snippet",
                        ),
                    ],
                },
            )
            _write_profile(
                profile_path,
                {
                    "full_name": "Pat Example",
                    "email": "pat@example.com",
                    "phone": "555-0100",
                },
            )
            browser = FixturePrefillBrowserBackend(
                {
                    "https://acme.wd5.myworkdayjobs.com/en-US/External/job/Remote/Platform-Engineer_R12345": BrowserPageSnapshot(
                        url="https://acme.wd5.myworkdayjobs.com/en-US/External/job/Remote/Platform-Engineer_R12345",
                        title="Workday Apply",
                        fields=(),
                        submit_controls=("Submit",),
                    )
                }
            )

            result = run_application_prefill(
                manifest_path,
                project_root=tmp_path,
                applicant_profile_path=profile_path,
                launch_order=None,
                browser_backend=browser,
            )

        self.assertEqual(result.portal_label, "Workday")
        self.assertEqual(result.status, "manual_handoff")
        self.assertEqual(result.filled_fields, ())
        self.assertIn("manual-review only", " ".join(result.notes))

    def test_cli_application_assist_prefill_prints_report_and_stops_before_submit(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            manifest_path = tmp_path / "session.json"
            profile_path = tmp_path / ".jobs_ai_applicant_profile.json"
            resume_path = tmp_path / "resume.pdf"
            resume_path.write_text("resume", encoding="utf-8")
            _write_manifest(
                manifest_path,
                {
                    "created_at": "2026-03-16T01:00:00Z",
                    "item_count": 1,
                    "items": [
                        _complete_item(
                            rank=1,
                            company="Acme Data",
                            title="Data Engineer",
                            apply_url="https://boards.greenhouse.io/acme/jobs/12345",
                            portal_type="greenhouse",
                            resume_key="data-engineering",
                            resume_label="Data Engineering Resume",
                            snippet_key="pipeline-delivery",
                            snippet_label="Pipeline Delivery",
                            snippet_text="Python-first pipeline delivery across SQL warehouses.",
                        ),
                    ],
                },
            )
            _write_profile(
                profile_path,
                {
                    "full_name": "Pat Example",
                    "email": "pat@example.com",
                    "phone": "555-0100",
                    "resume_paths": {
                        "data-engineering": str(resume_path),
                    },
                },
            )
            browser = FixturePrefillBrowserBackend(
                {
                    "https://boards.greenhouse.io/acme/jobs/12345": BrowserPageSnapshot(
                        url="https://boards.greenhouse.io/acme/jobs/12345",
                        title="Apply",
                        fields=(
                            _field("First Name", selector="[data-jobs-ai-selector='first_name']", required=True),
                            _field("Last Name", selector="[data-jobs-ai-selector='last_name']", required=True),
                            _field("Email", control_type="email", selector="[data-jobs-ai-selector='email']", required=True),
                            _field("Resume", control_type="file", selector="[data-jobs-ai-selector='resume']", required=True),
                        ),
                        submit_controls=("Submit application",),
                    )
                }
            )

            with patch("jobs_ai.cli.create_prefill_browser_backend", return_value=browser):
                result = RUNNER.invoke(
                    app,
                    [
                        "application-assist",
                        "--prefill",
                        "--no-hold-open",
                        "--applicant-profile",
                        str(profile_path),
                        str(manifest_path),
                    ],
                    env={"JOBS_AI_DB_PATH": str(tmp_path / "data" / "jobs_ai.db")},
                )

        self.assertEqual(result.exit_code, 0)
        self.assertIn("mode: review-first prefill", result.stdout)
        self.assertIn("portal: Greenhouse", result.stdout)
        self.assertIn("STOPPED BEFORE SUBMIT: yes", result.stdout)
        self.assertIn("filled fields:", result.stdout)


def _field(
    label: str,
    *,
    selector: str,
    control_type: str = "text",
    name: str | None = None,
    required: bool = False,
    visible: bool = True,
    options: tuple[BrowserFieldOption, ...] = (),
) -> BrowserFieldSnapshot:
    return BrowserFieldSnapshot(
        selector=selector,
        control_type=control_type,
        label=label,
        name=name,
        placeholder=None,
        required=required,
        visible=visible,
        options=options,
    )


def _complete_item(
    *,
    rank: int,
    company: str,
    title: str,
    apply_url: str | None,
    portal_type: str | None,
    resume_key: str,
    resume_label: str,
    snippet_key: str,
    snippet_label: str,
    snippet_text: str,
) -> dict[str, object | None]:
    return {
        "rank": rank,
        "company": company,
        "title": title,
        "apply_url": apply_url,
        "portal_type": portal_type,
        "recommended_resume_variant": {
            "key": resume_key,
            "label": resume_label,
        },
        "recommended_profile_snippet": {
            "key": snippet_key,
            "label": snippet_label,
            "text": snippet_text,
        },
    }


def _write_manifest(manifest_path: Path, payload: dict[str, object]) -> None:
    manifest_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def _write_profile(profile_path: Path, payload: dict[str, object]) -> None:
    profile_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    unittest.main()
