from __future__ import annotations

from contextlib import closing
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

from jobs_ai.application_tracking import get_application_status
from jobs_ai.cli import app
from jobs_ai.db import connect_database, initialize_schema, insert_job

RUNNER = CliRunner()


def _job_record(
    *,
    source: str,
    company: str,
    title: str,
    location: str,
    apply_url: str | None = None,
    portal_type: str | None = None,
    raw_payload: dict[str, object] | None = None,
) -> dict[str, object]:
    return {
        "source": source,
        "source_job_id": None,
        "company": company,
        "title": title,
        "location": location,
        "apply_url": apply_url,
        "portal_type": portal_type,
        "salary_text": None,
        "posted_at": None,
        "found_at": "2026-03-13T08:00:00Z",
        "raw_json": json.dumps(raw_payload or {}, ensure_ascii=True),
    }


def _write_manifest(
    manifest_path: Path,
    *,
    job_id: int | None,
    company: str,
    title: str,
    apply_url: str | None,
) -> Path:
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(
        json.dumps(
            {
                "created_at": "2026-03-15T12:00:00Z",
                "item_count": 1,
                "items": [
                    {
                        "rank": 1,
                        "job_id": job_id,
                        "company": company,
                        "title": title,
                        "apply_url": apply_url,
                    }
                ],
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    return manifest_path


def _set_status(connection, *, job_id: int, status: str) -> None:
    connection.execute(
        "UPDATE jobs SET status = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
        (status, job_id),
    )


class JobReferenceCliTest(unittest.TestCase):
    def test_cli_check_url_without_inspect_remains_exact_match_only(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            database_path = Path(tmp_dir) / "runtime" / "jobs_ai.db"
            env = {"JOBS_AI_DB_BACKEND": "sqlite", "JOBS_AI_DB_PATH": str(database_path)}
            exact_apply_url = "https://boards.greenhouse.io/qualifiedhealth/jobs/5153317008"
            input_url = "https://boards.greenhouse.io/qualifiedhealth?gh_jid=5153317008&utm_source=test"
            with patch.dict("os.environ", env, clear=False):
                initialize_schema(database_path)
                with closing(connect_database(database_path)) as connection:
                    insert_job(
                        connection,
                        _job_record(
                            source="manual",
                            company="Qualified Health",
                            title="Platform Engineer",
                            location="Remote",
                            apply_url=exact_apply_url,
                            portal_type="greenhouse",
                        ),
                    )
                    connection.commit()

            result = RUNNER.invoke(app, ["check-url", input_url], env=env)

            self.assertEqual(result.exit_code, 0)
            self.assertIn("jobs_ai check-url", result.stdout)
            self.assertIn("status: no job found for exact apply_url", result.stdout)
            self.assertIn(f"apply_url: {input_url}", result.stdout)
            self.assertNotIn("normalized apply_url:", result.stdout)
            self.assertNotIn("matched rows:", result.stdout)

    def test_cli_apply_url_marks_exact_single_match_applied(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            database_path = Path(tmp_dir) / "runtime" / "jobs_ai.db"
            env = {"JOBS_AI_DB_PATH": str(database_path)}
            apply_url = "https://boards.greenhouse.io/qualifiedhealth/jobs/5153317008"
            initialize_schema(database_path)

            with closing(connect_database(database_path)) as connection:
                job_id = insert_job(
                    connection,
                    _job_record(
                        source="manual",
                        company="Qualified Health",
                        title="Platform Engineer",
                        location="Remote",
                        apply_url=apply_url,
                        portal_type="greenhouse",
                    ),
                )
                connection.commit()

            result = RUNNER.invoke(app, ["apply-url", apply_url], env=env)

            self.assertEqual(result.exit_code, 0)
            self.assertIn("status: applied", result.stdout)
            self.assertIn(f"job id: {job_id}", result.stdout)
            self.assertIn("company: Qualified Health", result.stdout)
            self.assertIn("title: Platform Engineer", result.stdout)

            detail = get_application_status(database_path, job_id=job_id)
            self.assertEqual(detail.snapshot.current_status, "applied")
            self.assertEqual([entry.status for entry in detail.history], ["applied"])

    def test_cli_apply_url_resolves_canonical_url_for_single_match(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            database_path = Path(tmp_dir) / "runtime" / "jobs_ai.db"
            env = {"JOBS_AI_DB_PATH": str(database_path)}
            tracking_url = "https://boards.greenhouse.io/qualifiedhealth?gh_jid=5153317008&utm_source=test"
            canonical_url = "https://boards.greenhouse.io/qualifiedhealth/jobs/5153317008"
            initialize_schema(database_path)

            with closing(connect_database(database_path)) as connection:
                job_id = insert_job(
                    connection,
                    _job_record(
                        source="manual",
                        company="Qualified Health",
                        title="Senior Platform Engineer",
                        location="Remote",
                        apply_url=canonical_url,
                        portal_type="greenhouse",
                    ),
                )
                connection.commit()

            result = RUNNER.invoke(app, ["apply-url", tracking_url], env=env)

            self.assertEqual(result.exit_code, 0)
            self.assertIn("status: applied", result.stdout)
            self.assertIn(f"job id: {job_id}", result.stdout)
            self.assertIn("company: Qualified Health", result.stdout)
            self.assertIn("title: Senior Platform Engineer", result.stdout)
            self.assertEqual(
                get_application_status(database_path, job_id=job_id).snapshot.current_status,
                "applied",
            )

    def test_cli_apply_url_reports_no_matches_found(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            database_path = Path(tmp_dir) / "runtime" / "jobs_ai.db"
            env = {"JOBS_AI_DB_PATH": str(database_path)}
            apply_url = "https://boards.greenhouse.io/example/jobs/0000000000"
            initialize_schema(database_path)

            result = RUNNER.invoke(app, ["apply-url", apply_url], env=env)

            self.assertEqual(result.exit_code, 1)
            self.assertIn("status: no matches found", result.stdout)
            self.assertIn(f"apply_url: {apply_url}", result.stdout)

    def test_cli_apply_url_requires_job_id_for_multiple_matches(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            database_path = Path(tmp_dir) / "runtime" / "jobs_ai.db"
            env = {"JOBS_AI_DB_PATH": str(database_path)}
            apply_url = "https://boards.greenhouse.io/qualifiedhealth/jobs/5153317008"
            initialize_schema(database_path)

            with closing(connect_database(database_path)) as connection:
                first_job_id = insert_job(
                    connection,
                    _job_record(
                        source="manual",
                        company="Qualified Health",
                        title="Platform Engineer",
                        location="Remote",
                        apply_url=apply_url,
                        portal_type="greenhouse",
                    ),
                )
                second_job_id = insert_job(
                    connection,
                    _job_record(
                        source="manual",
                        company="Qualified Health",
                        title="Senior Platform Engineer",
                        location="Remote",
                        apply_url=apply_url,
                        portal_type="greenhouse",
                    ),
                )
                connection.commit()

            result = RUNNER.invoke(app, ["apply-url", apply_url], env=env)

            self.assertEqual(result.exit_code, 1)
            self.assertIn("status: multiple matches found", result.stdout)
            self.assertIn(
                f"  - job id: {first_job_id} | Qualified Health | Platform Engineer",
                result.stdout,
            )
            self.assertIn(
                f"  - job id: {second_job_id} | Qualified Health | Senior Platform Engineer",
                result.stdout,
            )
            self.assertIn(
                f"  jobs-ai apply-url {apply_url} --job-id <id>",
                result.stdout,
            )
            self.assertEqual(
                get_application_status(database_path, job_id=first_job_id).snapshot.current_status,
                "new",
            )
            self.assertEqual(
                get_application_status(database_path, job_id=second_job_id).snapshot.current_status,
                "new",
            )

    def test_cli_apply_url_marks_requested_job_id_for_multiple_matches(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            database_path = Path(tmp_dir) / "runtime" / "jobs_ai.db"
            env = {"JOBS_AI_DB_PATH": str(database_path)}
            apply_url = "https://boards.greenhouse.io/qualifiedhealth/jobs/5153317008"
            initialize_schema(database_path)

            with closing(connect_database(database_path)) as connection:
                first_job_id = insert_job(
                    connection,
                    _job_record(
                        source="manual",
                        company="Qualified Health",
                        title="Platform Engineer",
                        location="Remote",
                        apply_url=apply_url,
                        portal_type="greenhouse",
                    ),
                )
                second_job_id = insert_job(
                    connection,
                    _job_record(
                        source="manual",
                        company="Qualified Health",
                        title="Senior Platform Engineer",
                        location="Remote",
                        apply_url=apply_url,
                        portal_type="greenhouse",
                    ),
                )
                connection.commit()

            result = RUNNER.invoke(
                app,
                ["apply-url", apply_url, "--job-id", str(second_job_id)],
                env=env,
            )

            self.assertEqual(result.exit_code, 0)
            self.assertIn("status: applied", result.stdout)
            self.assertIn(f"job id: {second_job_id}", result.stdout)
            self.assertEqual(
                get_application_status(database_path, job_id=first_job_id).snapshot.current_status,
                "superseded",
            )
            selected_detail = get_application_status(database_path, job_id=second_job_id)
            self.assertEqual(selected_detail.snapshot.current_status, "applied")
            self.assertEqual([entry.status for entry in selected_detail.history], ["applied"])

    def test_cli_apply_url_reports_already_applied_cleanly(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            database_path = Path(tmp_dir) / "runtime" / "jobs_ai.db"
            env = {"JOBS_AI_DB_PATH": str(database_path)}
            apply_url = "https://boards.greenhouse.io/qualifiedhealth/jobs/5153317008"
            initialize_schema(database_path)

            with closing(connect_database(database_path)) as connection:
                job_id = insert_job(
                    connection,
                    _job_record(
                        source="manual",
                        company="Qualified Health",
                        title="Platform Engineer",
                        location="Remote",
                        apply_url=apply_url,
                        portal_type="greenhouse",
                    ),
                )
                _set_status(connection, job_id=job_id, status="applied")
                connection.commit()

            result = RUNNER.invoke(app, ["apply-url", apply_url], env=env)

            self.assertEqual(result.exit_code, 0)
            self.assertIn("status: already applied", result.stdout)
            self.assertIn(f"job id: {job_id}", result.stdout)
            self.assertIn("company: Qualified Health", result.stdout)
            self.assertIn("title: Platform Engineer", result.stdout)
            detail = get_application_status(database_path, job_id=job_id)
            self.assertEqual(detail.snapshot.current_status, "applied")
            self.assertEqual(detail.history, ())

    def test_cli_apply_url_help_mentions_auto_resolution(self) -> None:
        result = RUNNER.invoke(app, ["apply-url", "--help"])

        self.assertEqual(result.exit_code, 0)
        self.assertIn("Mark a job as applied using its apply URL.", result.stdout)
        self.assertIn("Automatically resolves when only one match exists.", result.stdout)

    def test_cli_inspect_by_job_id_reports_enriched_profile(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            database_path = Path(tmp_dir) / "runtime" / "jobs_ai.db"
            env = {"JOBS_AI_DB_PATH": str(database_path)}
            initialize_schema(database_path)

            with closing(connect_database(database_path)) as connection:
                job_id = insert_job(
                    connection,
                    _job_record(
                        source="manual",
                        company="Northwind Talent",
                        title="Senior Data Engineer",
                        location="Remote",
                        apply_url="https://example.com/jobs/200",
                        raw_payload={"description": "Python BigQuery GCP pipelines"},
                    ),
                )
                _set_status(connection, job_id=job_id, status="applied")
                connection.commit()

            result = RUNNER.invoke(app, ["inspect", str(job_id)], env=env)

            self.assertEqual(result.exit_code, 0)
            self.assertIn("jobs_ai inspect", result.stdout)
            self.assertIn(f"job id: {job_id}", result.stdout)
            self.assertIn("company: Northwind Talent", result.stdout)
            self.assertIn("job status: applied", result.stdout)
            self.assertIn("actionable in queue/session flow: no", result.stdout)
            self.assertIn("launchable: no", result.stdout)
            self.assertIn("skip reasons: status applied is excluded from the normal queue/session flow", result.stdout)
            self.assertIn("recommended resume variant: data-engineering (Data Engineering Resume)", result.stdout)
            self.assertIn("recommended profile snippet: pipeline-delivery (Pipeline Delivery)", result.stdout)

    def test_cli_inspect_by_url_resolves_canonical_group_and_prefers_applied_row(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            database_path = Path(tmp_dir) / "runtime" / "jobs_ai.db"
            env = {"JOBS_AI_DB_PATH": str(database_path)}
            tracking_url = "https://boards.greenhouse.io/qualifiedhealth?gh_jid=5153317008&utm_source=test"
            canonical_url = "https://boards.greenhouse.io/qualifiedhealth/jobs/5153317008"
            initialize_schema(database_path)

            with closing(connect_database(database_path)) as connection:
                sibling_job_id = insert_job(
                    connection,
                    _job_record(
                        source="manual",
                        company="Qualified Health",
                        title="Platform Engineer",
                        location="Remote",
                        apply_url=tracking_url,
                        portal_type="greenhouse",
                    ),
                )
                preferred_job_id = insert_job(
                    connection,
                    _job_record(
                        source="manual",
                        company="Qualified Health",
                        title="Senior Platform Engineer",
                        location="Remote",
                        apply_url=canonical_url,
                        portal_type="greenhouse",
                    ),
                )
                _set_status(connection, job_id=preferred_job_id, status="applied")
                connection.commit()

            result = RUNNER.invoke(app, ["inspect", tracking_url], env=env)

            self.assertEqual(result.exit_code, 0)
            self.assertIn("jobs_ai inspect", result.stdout)
            self.assertIn(f"reference: {tracking_url}", result.stdout)
            self.assertIn(f"normalized apply_url: {tracking_url}", result.stdout)
            self.assertIn(f"canonical lookup url: {canonical_url}", result.stdout)
            self.assertIn(f"job id: {preferred_job_id}", result.stdout)
            self.assertIn("job status: applied", result.stdout)
            self.assertIn("matched rows: 2", result.stdout)
            self.assertIn(f"preferred row: [job {preferred_job_id}] Qualified Health | Senior Platform Engineer | Remote | status applied", result.stdout)
            self.assertIn(f"- [job {sibling_job_id}] Qualified Health | Platform Engineer | Remote | status new", result.stdout)

    def test_cli_check_url_inspect_resolves_canonical_apply_url(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            database_path = Path(tmp_dir) / "runtime" / "jobs_ai.db"
            env = {"JOBS_AI_DB_PATH": str(database_path)}
            tracking_url = "https://boards.greenhouse.io/qualifiedhealth?gh_jid=5153317008&utm_source=test"
            canonical_url = "https://boards.greenhouse.io/qualifiedhealth/jobs/5153317008"
            initialize_schema(database_path)

            with closing(connect_database(database_path)) as connection:
                insert_job(
                    connection,
                    _job_record(
                        source="manual",
                        company="Qualified Health",
                        title="Platform Engineer",
                        location="Remote",
                        apply_url=canonical_url,
                        portal_type="greenhouse",
                    ),
                )
                connection.commit()

            result = RUNNER.invoke(app, ["check-url", tracking_url, "--inspect"], env=env)

            self.assertEqual(result.exit_code, 0)
            self.assertIn("jobs_ai check-url", result.stdout)
            self.assertIn("inspect mode: yes", result.stdout)
            self.assertIn("total matches: 1", result.stdout)
            self.assertIn(f"apply_url: {tracking_url}", result.stdout)
            self.assertIn(f"canonical_apply_url: {canonical_url}", result.stdout)
            self.assertIn("recommended resume variant:", result.stdout)

    def test_cli_open_by_job_id_opens_exact_row_url(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            database_path = Path(tmp_dir) / "runtime" / "jobs_ai.db"
            env = {"JOBS_AI_DB_PATH": str(database_path)}
            initialize_schema(database_path)

            with closing(connect_database(database_path)) as connection:
                job_id = insert_job(
                    connection,
                    _job_record(
                        source="manual",
                        company="Fabrikam",
                        title="Backend Engineer",
                        location="Remote",
                        apply_url="https://example.com/jobs/300",
                    ),
                )
                connection.commit()

            with patch("jobs_ai.launch_executor.webbrowser.open", return_value=True) as open_browser:
                result = RUNNER.invoke(app, ["open", str(job_id)], env=env)

            self.assertEqual(result.exit_code, 0)
            self.assertIn("jobs_ai open", result.stdout)
            self.assertIn(f"resolved job id: {job_id}", result.stdout)
            self.assertIn("note: direct open leaves application status unchanged", result.stdout)
            open_browser.assert_called_once_with("https://example.com/jobs/300", new=2)

    def test_cli_open_by_url_uses_preferred_canonical_row(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            database_path = Path(tmp_dir) / "runtime" / "jobs_ai.db"
            env = {"JOBS_AI_DB_PATH": str(database_path)}
            tracking_url = "https://boards.greenhouse.io/qualifiedhealth?gh_jid=5153317008&utm_source=test"
            canonical_url = "https://boards.greenhouse.io/qualifiedhealth/jobs/5153317008"
            initialize_schema(database_path)

            with closing(connect_database(database_path)) as connection:
                sibling_job_id = insert_job(
                    connection,
                    _job_record(
                        source="manual",
                        company="Qualified Health",
                        title="Platform Engineer",
                        location="Remote",
                        apply_url=tracking_url,
                        portal_type="greenhouse",
                    ),
                )
                preferred_job_id = insert_job(
                    connection,
                    _job_record(
                        source="manual",
                        company="Qualified Health",
                        title="Senior Platform Engineer",
                        location="Remote",
                        apply_url=canonical_url,
                        portal_type="greenhouse",
                    ),
                )
                _set_status(connection, job_id=preferred_job_id, status="applied")
                connection.commit()

            with patch("jobs_ai.launch_executor.webbrowser.open", return_value=True) as open_browser:
                result = RUNNER.invoke(app, ["open", tracking_url], env=env)

            self.assertEqual(result.exit_code, 0)
            self.assertIn("jobs_ai open", result.stdout)
            self.assertIn(f"resolved job id: {preferred_job_id}", result.stdout)
            self.assertIn("matched rows: 2", result.stdout)
            self.assertIn(f"- [job {sibling_job_id}] Qualified Health | Platform Engineer | Remote | status new", result.stdout)
            open_browser.assert_called_once_with(canonical_url, new=2)

    def test_cli_open_by_job_id_errors_clearly_when_apply_url_is_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            database_path = Path(tmp_dir) / "runtime" / "jobs_ai.db"
            env = {"JOBS_AI_DB_PATH": str(database_path)}
            initialize_schema(database_path)

            with closing(connect_database(database_path)) as connection:
                job_id = insert_job(
                    connection,
                    _job_record(
                        source="manual",
                        company="Contoso",
                        title="Analytics Engineer",
                        location="Remote",
                        apply_url=None,
                    ),
                )
                connection.commit()

            with patch("jobs_ai.launch_executor.webbrowser.open") as open_browser:
                result = RUNNER.invoke(app, ["open", str(job_id)], env=env)

            self.assertEqual(result.exit_code, 1)
            self.assertIn("jobs_ai open", result.stdout)
            self.assertIn(f"reference: {job_id}", result.stdout)
            self.assertIn(f"error: job id {job_id} is missing apply_url", result.stdout)
            open_browser.assert_not_called()

    def test_cli_open_manifest_mode_still_marks_applied(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            project_root = Path(tmp_dir) / "workspace"
            database_path = project_root / "runtime" / "jobs_ai.db"
            env = {"JOBS_AI_DB_PATH": str(database_path)}
            initialize_schema(database_path)

            with closing(connect_database(database_path)) as connection:
                job_id = insert_job(
                    connection,
                    _job_record(
                        source="manual",
                        company="Northwind Talent",
                        title="Platform Data Engineer",
                        location="Remote",
                        apply_url="https://example.com/jobs/123",
                    ),
                )
                connection.commit()

            manifest_path = _write_manifest(
                project_root / "data" / "exports" / "direct-open.json",
                job_id=job_id,
                company="Northwind Talent",
                title="Platform Data Engineer",
                apply_url="https://example.com/jobs/123",
            )

            with patch("jobs_ai.launch_executor.webbrowser.open", return_value=True) as open_browser:
                result = RUNNER.invoke(
                    app,
                    ["open", "--manifest", str(manifest_path), "--index", "1"],
                    env=env,
                    input="y\n",
                )

            self.assertEqual(result.exit_code, 0)
            self.assertIn("Opened: [1] Northwind Talent - Platform Data Engineer", result.stdout)
            self.assertIn("requested status: applied", result.stdout)
            open_browser.assert_called_once_with("https://example.com/jobs/123", new=2)
            self.assertEqual(
                get_application_status(database_path, job_id=job_id).snapshot.current_status,
                "applied",
            )


if __name__ == "__main__":
    unittest.main()
