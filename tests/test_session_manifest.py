from __future__ import annotations

from contextlib import closing
from datetime import datetime, timezone
import json
from pathlib import Path
import sys
import tempfile
import unittest

from typer.testing import CliRunner

SRC_PATH = Path(__file__).resolve().parents[1] / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

from jobs_ai.cli import app
from jobs_ai.db import connect_database, initialize_schema, insert_job
from jobs_ai.session_export import export_launch_preview_session
from jobs_ai.session_manifest import load_session_manifest

RUNNER = CliRunner()


def _job_record(
    *,
    source: str,
    company: str,
    title: str,
    location: str,
    raw_payload: dict[str, object] | None = None,
    apply_url: str | None = None,
) -> dict[str, object]:
    return {
        "source": source,
        "source_job_id": None,
        "company": company,
        "title": title,
        "location": location,
        "apply_url": apply_url,
        "portal_type": None,
        "salary_text": None,
        "posted_at": None,
        "found_at": "2026-03-13T08:00:00Z",
        "raw_json": json.dumps(raw_payload or {}, ensure_ascii=True),
    }


class SessionManifestTest(unittest.TestCase):
    def test_valid_manifest_loads_successfully(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            database_path = Path(tmp_dir) / "runtime" / "jobs_ai.db"
            exports_dir = Path(tmp_dir) / "data" / "exports"
            initialize_schema(database_path)

            with closing(connect_database(database_path)) as connection:
                insert_job(
                    connection,
                    _job_record(
                        source="staffing recruiter",
                        company="Northwind Talent",
                        title="Senior Data Engineer",
                        location="Remote",
                        apply_url="https://agency.example/jobs/2",
                        raw_payload={"description": "Python BigQuery GCP pipelines"},
                    ),
                )
                connection.commit()

            export_result = export_launch_preview_session(
                database_path,
                exports_dir,
                created_at=datetime(2026, 3, 13, 17, 30, 45, tzinfo=timezone.utc),
            )

            manifest = load_session_manifest(export_result.export_path)

            self.assertEqual(manifest.created_at, "2026-03-13T17:30:45Z")
            self.assertEqual(manifest.item_count, 1)
            self.assertEqual(manifest.warning_count, 0)

            item = manifest.items[0]
            self.assertIsInstance(item.job_id, int)
            self.assertEqual(item.company, "Northwind Talent")
            self.assertEqual(item.title, "Senior Data Engineer")
            self.assertEqual(item.apply_url, "https://agency.example/jobs/2")
            self.assertEqual(item.recommended_resume_variant.key, "data-engineering")
            self.assertEqual(item.recommended_resume_variant.label, "Data Engineering Resume")
            self.assertEqual(item.recommended_profile_snippet.key, "pipeline-delivery")
            self.assertEqual(item.recommended_profile_snippet.label, "Pipeline Delivery")
            self.assertEqual(
                item.recommended_profile_snippet.text,
                "Python-first pipeline delivery across SQL warehouses, BigQuery/GCP, and production data systems.",
            )

    def test_cli_preflight_rejects_invalid_manifest_shape_clearly(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            manifest_path = Path(tmp_dir) / "invalid-session.json"
            manifest_path.write_text(
                json.dumps(
                    {
                        "created_at": "2026-03-13T17:30:45Z",
                        "item_count": 1,
                        "items": {"company": "Northwind Talent"},
                    },
                    indent=2,
                )
                + "\n",
                encoding="utf-8",
            )

            result = RUNNER.invoke(app, ["preflight", str(manifest_path)])

            self.assertEqual(result.exit_code, 1)
            self.assertIn("jobs_ai preflight", result.stdout)
            self.assertIn("status: failed", result.stdout)
            self.assertIn("manifest.items must be a list", result.stdout)

    def test_cli_preflight_shows_expected_fields_and_item_warnings(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            manifest_path = Path(tmp_dir) / "session.json"
            manifest_path.write_text(
                json.dumps(
                    {
                        "created_at": "2026-03-13T17:30:45Z",
                        "item_count": 1,
                        "items": [
                            {
                                "rank": 1,
                                "job_id": 7,
                                "company": "Northwind Talent",
                                "title": "Senior Data Engineer",
                                "location": "Remote",
                                "source": "staffing recruiter",
                                "apply_url": None,
                                "score": 17,
                                "recommended_resume_variant": {
                                    "key": "data-engineering",
                                    "label": "Data Engineering Resume",
                                },
                                "recommended_profile_snippet": {
                                    "key": None,
                                    "label": None,
                                    "text": None,
                                },
                                "explanation": "matched data engineering signals from title or stack",
                            }
                        ],
                    },
                    indent=2,
                )
                + "\n",
                encoding="utf-8",
            )

            result = RUNNER.invoke(app, ["preflight", str(manifest_path)])

            self.assertEqual(result.exit_code, 0)
            self.assertIn("created_at: 2026-03-13T17:30:45Z", result.stdout)
            self.assertIn("item count: 1", result.stdout)
            self.assertIn("1. Northwind Talent | Senior Data Engineer", result.stdout)
            self.assertIn("apply_url: apply_url missing", result.stdout)
            self.assertIn(
                "recommended resume variant: data-engineering (Data Engineering Resume)",
                result.stdout,
            )
            self.assertIn("recommended profile snippet: profile snippet missing", result.stdout)
            self.assertIn(
                "warnings: apply_url missing; recommended_profile_snippet incomplete",
                result.stdout,
            )


if __name__ == "__main__":
    unittest.main()
