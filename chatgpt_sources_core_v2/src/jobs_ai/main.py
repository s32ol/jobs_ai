from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
import json
import webbrowser

from .collect.census import BoardCensusRun
from .collect.models import CollectRunReport
from .discover.models import DiscoverRun, DiscoverRunReport
from .source_seed.models import SourceSeedRunReport
from .application_tracking import (
    APPLICATION_STATUSES,
    ApplicationStatusDetail,
    ApplicationStatusSnapshot,
)
from .application_assist import ApplicationAssist
from .application_prefill import ApplicationPrefillResult
from .config import GEOGRAPHY_PRIORITY, SEARCH_PRIORITY, TARGET_ROLES, Settings
from .db import REQUIRED_TABLES, SessionHistoryEntry
from .launch_dry_run import LaunchDryRunStep
from .launch_executor import LaunchExecutionReport
from .maintenance import BackfillResult
from .jobs.importer import JobImportResult
from .jobs.fast_apply import FastApplySelection
from .launch_plan import LaunchPlan
from .jobs.queue import QueuedJob
from .launch_preview import LaunchPreview, select_launch_preview
from .jobs.scoring import ScoredJob
from .portal_support import SUPPORTED_PORTAL_TYPES, PortalSupport, build_portal_support
from .resume.recommendations import QueueRecommendation
from .run_workflow import RunWorkflowResult
from .session_export import SessionExportResult
from .session_history import SessionInspection, SessionReopenResult
from .session_mark import SessionMarkResult
from .session_manifest import ManifestSelection, SessionManifest, SessionSelectionScope
from .session_open import SessionOpenResult
from .session_start import SessionStartResult
from .sources.jobposting_parser import JobPostingExtractionResult
from .sources.models import (
    SourceRegistryHarvestCompaniesResult,
    SourceRegistryATSDiscoveryResult,
    SourceRegistryDetectSitesResult,
    SourceRegistryCollectResult,
    SourceRegistryEntry,
    SourceRegistryExpandResult,
    SourceRegistryImportResult,
    SourceRegistryMutationResult,
    SourceRegistrySeedBulkResult,
    SourceRegistryVerificationResult,
)
from .stats import OperatorStats
from .workspace import WorkspacePaths

CLI_EXAMPLE_PREFIX = "python -m jobs_ai"
APPLY_DEFAULT_LIMIT = 5
APPLY_HARD_MAX_LIMIT = 20


@dataclass(frozen=True, slots=True)
class ApplyItem:
    job_id: int
    company: str
    title: str
    location: str | None
    apply_url: str
    source_rank: int


@dataclass(frozen=True, slots=True)
class ApplyResult:
    query: str
    requested_limit: int
    effective_limit: int
    print_only: bool
    items: tuple[ApplyItem, ...]
    matching_count: int
    skipped_missing_apply_url_count: int
    warning: str | None

    @property
    def selected_count(self) -> int:
        return len(self.items)


def _render_bullets(title: str, values: Sequence[str | Path]) -> list[str]:
    lines = [title]
    lines.extend(f"- {value}" for value in values)
    return lines


def _append_guidance(lines: list[str], title: str, values: Sequence[str]) -> None:
    if not values:
        return
    lines.append("")
    lines.append(title)
    lines.extend(f"- {value}" for value in values)


def _cli_example(command: str) -> str:
    command_text = command.strip()
    if not command_text:
        return CLI_EXAMPLE_PREFIX
    return f"{CLI_EXAMPLE_PREFIX} {command_text}"


def _command_with_optional_limit(command: str, limit: int | None) -> str:
    if limit is None:
        return _cli_example(command)
    return _cli_example(f"{command} --limit {limit}")


def run_apply_workflow(
    database_path: Path,
    *,
    query: str,
    limit: int = APPLY_DEFAULT_LIMIT,
    print_only: bool = False,
) -> ApplyResult:
    normalized_query = query.strip()
    if not normalized_query:
        raise ValueError("query must not be blank")

    effective_limit, warning = _resolve_apply_limit(limit)
    previews = select_launch_preview(
        database_path,
        limit=None,
        query_text=normalized_query,
    )

    launchable_items: list[ApplyItem] = []
    skipped_missing_apply_url_count = 0
    for preview in previews:
        normalized_apply_url = _normalize_apply_url(preview.apply_url)
        if normalized_apply_url is None:
            skipped_missing_apply_url_count += 1
            continue
        launchable_items.append(
            ApplyItem(
                job_id=preview.job_id,
                company=preview.company,
                title=preview.title,
                location=preview.location,
                apply_url=normalized_apply_url,
                source_rank=preview.rank,
            )
        )

    selected_items = tuple(launchable_items[:effective_limit])
    if not print_only:
        for item in selected_items:
            webbrowser.open(item.apply_url)

    return ApplyResult(
        query=normalized_query,
        requested_limit=limit,
        effective_limit=effective_limit,
        print_only=print_only,
        items=selected_items,
        matching_count=len(previews),
        skipped_missing_apply_url_count=skipped_missing_apply_url_count,
        warning=warning,
    )


def _resolve_apply_limit(limit: int) -> tuple[int, str | None]:
    if limit < 1:
        raise ValueError("limit must be at least 1")
    if limit <= APPLY_HARD_MAX_LIMIT:
        return limit, None
    return (
        APPLY_HARD_MAX_LIMIT,
        (
            f"requested limit {limit} exceeds hard max {APPLY_HARD_MAX_LIMIT}; "
            f"clamped to {APPLY_HARD_MAX_LIMIT}"
        ),
    )


def _normalize_apply_url(value: str | None) -> str | None:
    if value is None:
        return None
    normalized_value = value.strip()
    return normalized_value or None


def render_status_report(settings: Settings, paths: WorkspacePaths) -> str:
    return "\n".join(
        [
            "jobs_ai control tower",
            f"environment: {settings.environment}",
            f"profile: {settings.profile}",
            f"project root: {paths.project_root}",
            f"database path: {paths.database_path}",
            "",
            "current focus: milestone 11 operational polish",
            "",
            *_render_bullets(
                "workspace paths:",
                (
                    f"raw leads inbox: {paths.raw_dir}",
                    f"normalized outputs: {paths.processed_dir}",
                    f"exports: {paths.exports_dir}",
                    f"session logs: {paths.sessions_dir}",
                    f"runtime logs: {paths.logs_dir}",
                ),
            ),
            "",
            *_render_bullets("target roles:", TARGET_ROLES),
            "",
            *_render_bullets("search priority:", SEARCH_PRIORITY),
            "",
            *_render_bullets("geography priority:", GEOGRAPHY_PRIORITY),
            "",
            'tip: preferred daily entrypoint is jobs-ai run "python backend engineer remote".',
        ]
    )


def render_init_report(paths: WorkspacePaths, created_paths: Sequence[Path]) -> str:
    lines = [
        "jobs_ai workspace bootstrap",
        f"database path: {paths.database_path}",
    ]
    if created_paths:
        lines.append("created directories:")
        lines.extend(f"- {path}" for path in created_paths)
    else:
        lines.append("workspace already initialized")
    _append_guidance(
        lines,
        "next:",
        (
            _cli_example("doctor"),
            _cli_example("db init"),
        ),
    )
    return "\n".join(lines)


def render_doctor_report(paths: WorkspacePaths, missing_paths: Sequence[Path]) -> str:
    lines = [
        "jobs_ai workspace doctor",
        f"database path: {paths.database_path}",
    ]
    if missing_paths:
        lines.append("missing directories:")
        lines.extend(f"- {path}" for path in missing_paths)
        _append_guidance(lines, "fix:", (_cli_example("init"),))
    else:
        lines.append("workspace looks ready for database setup")
        _append_guidance(lines, "next:", (_cli_example("db status"),))
    return "\n".join(lines)


def render_db_init_report(paths: WorkspacePaths, created_paths: Sequence[Path]) -> str:
    lines = [
        "jobs_ai database init",
        f"database path: {paths.database_path}",
        "schema: ready",
    ]
    if created_paths:
        lines.append("created directories:")
        lines.extend(f"- {path}" for path in created_paths)
    else:
        lines.append("workspace directories already existed")
    lines.append("required tables:")
    lines.extend(f"- {table_name}" for table_name in REQUIRED_TABLES)
    _append_guidance(
        lines,
        "next:",
        (
            _cli_example("import data/raw/sample_job_leads.json"),
            _cli_example("score"),
        ),
    )
    return "\n".join(lines)


def render_db_status_report(paths: WorkspacePaths, missing_tables: Sequence[str]) -> str:
    lines = [
        "jobs_ai database status",
        f"database path: {paths.database_path}",
        f"database file: {'present' if paths.database_path.exists() else 'missing'}",
    ]
    if missing_tables:
        lines.append("schema: missing")
        lines.append("missing required tables:")
        lines.extend(f"- {table_name}" for table_name in missing_tables)
        _append_guidance(lines, "fix:", (_cli_example("db init"),))
    else:
        lines.append("schema: ready")
        lines.append("required tables present:")
        lines.extend(f"- {table_name}" for table_name in REQUIRED_TABLES)
        _append_guidance(lines, "next:", (_cli_example("import data/raw/sample_job_leads.json"),))
    return "\n".join(lines)


def render_maintenance_backfill_report(
    paths: WorkspacePaths,
    result: BackfillResult,
) -> str:
    action_label = "would update" if result.dry_run else "updated"
    lines = [
        "jobs_ai maintenance backfill",
        f"database path: {paths.database_path}",
        f"dry run: {'yes' if result.dry_run else 'no'}",
        f"limit: {result.limit if result.limit is not None else 'none'}",
        f"jobs inspected: {result.total_jobs}",
        f"candidate jobs: {result.candidate_jobs}",
        f"{action_label} jobs: {result.updated_jobs}",
        f"skipped jobs: {result.skipped_jobs}",
        f"deferred by limit: {result.deferred_jobs}",
        f"status: {'success' if result.candidate_jobs or result.missing_tables or result.missing_job_columns else 'no backfill needed'}",
    ]
    if result.missing_tables:
        lines.append("missing tables before run:")
        lines.extend(f"- {table_name}" for table_name in result.missing_tables)
    if result.missing_job_columns:
        lines.append("missing job columns before run:")
        lines.extend(f"- {column_name}" for column_name in result.missing_job_columns)
    if result.field_counts:
        lines.append("field updates:")
        lines.extend(f"- {entry.field_name}: {entry.count}" for entry in result.field_counts)
    if result.job_updates:
        lines.append("job updates:")
        lines.extend(
            f"- [job {entry.job_id}] {entry.company} | {entry.title} | {', '.join(entry.changed_fields)}"
            for entry in result.job_updates
        )
    _append_guidance(
        lines,
        "next:",
        (
            _cli_example("stats"),
            _cli_example("session recent"),
        ),
    )
    return "\n".join(lines)


def render_maintenance_backfill_error_report(paths: WorkspacePaths, error: str) -> str:
    return "\n".join(
        [
            "jobs_ai maintenance backfill",
            f"database path: {paths.database_path}",
            "status: failed",
            f"error: {error}",
        ]
    )


def render_import_report(
    paths: WorkspacePaths,
    input_path: Path,
    result: JobImportResult,
) -> str:
    lines = [
        "jobs_ai import",
        f"database path: {paths.database_path}",
        f"input file: {input_path}",
        f"batch id: {result.batch_id or 'none'}",
        f"inserted: {result.inserted_count}",
        f"duplicates skipped: {result.duplicate_count}",
        f"invalid records: {result.error_count}",
        f"skipped: {result.skipped_count}",
    ]
    if result.source_query is not None:
        lines.append(f"source query: {result.source_query}")
    if result.skipped:
        lines.append("skipped records:")
        lines.extend(f"- {message}" for message in result.skipped)
    if result.errors:
        lines.append("status: completed with errors" if result.inserted_count else "status: failed")
        lines.append("errors:")
        lines.extend(f"- {error}" for error in result.errors)
        if result.inserted_count:
            lines.append("note: valid records were still committed")
            _append_guidance(
                lines,
                "next:",
                _import_next_commands(result),
            )
        else:
            _append_guidance(
                lines,
                "next:",
                (f"Fix {input_path.name} and rerun {_cli_example(f'import {input_path}')}",),
            )
    else:
        lines.append("status: success")
        if result.inserted_count:
            _append_guidance(
                lines,
                "next:",
                _import_next_commands(result),
            )
        elif result.skipped:
            lines.append("note: database contents were unchanged")
            _append_guidance(
                lines,
                "next:",
                (f"Import a fresh lead batch with {_cli_example(f'import {input_path}')}",),
            )
    return "\n".join(lines)


def render_collect_report(report: CollectRunReport) -> str:
    artifact_paths = report.artifact_paths
    assert artifact_paths is not None

    status = "success"
    if report.skipped_count and not (report.collected_count or report.manual_review_count):
        status = "completed with skips"
    elif report.skipped_count:
        status = "success with skips"

    lines = [
        "jobs_ai collect",
        f"run id: {report.run_id or artifact_paths.output_dir.name}",
        f"output dir: {artifact_paths.output_dir}",
        f"created_at: {report.created_at}",
        f"finished_at: {report.finished_at or report.created_at}",
        f"input sources: {report.input_source_count}",
        f"collected automatically: {report.collected_count}",
        f"manual review needed: {report.manual_review_count}",
        f"skipped: {report.skipped_count}",
        f"timeout seconds: {report.timeout_seconds:.1f}",
        f"report only: {'yes' if report.report_only else 'no'}",
        f"status: {status}",
        f"run report: {artifact_paths.run_report_path}",
    ]
    if artifact_paths.leads_path is None:
        lines.append("leads artifact: not written (--report-only)")
    else:
        lines.append(f"leads artifact: {artifact_paths.leads_path}")
    if artifact_paths.manual_review_path is None:
        lines.append("manual review artifact: not written (--report-only)")
    else:
        lines.append(f"manual review artifact: {artifact_paths.manual_review_path}")

    if report.collected_count == 0:
        lines.append("note: no importable leads were collected")
        if artifact_paths.leads_path is not None:
            lines.append(
                "note: leads.import.json contains an empty array; jobs-ai import rejects empty batches, so there is nothing to import from this run"
            )

    manual_review_results = [
        result
        for result in report.source_results
        if result.outcome == "manual_review"
    ]
    if manual_review_results:
        lines.append("manual review needed:")
        lines.extend(
            _format_collect_result_line(result)
            for result in manual_review_results
        )

    skipped_results = [
        result
        for result in report.source_results
        if result.outcome == "skipped"
    ]
    if skipped_results:
        lines.append("skipped sources:")
        lines.extend(
            _format_collect_result_line(result)
            for result in skipped_results
        )

    if artifact_paths.leads_path is not None and report.collected_count:
        _append_guidance(
            lines,
            "next:",
            (_cli_example(f"import {artifact_paths.leads_path}"),),
        )
    return "\n".join(lines)


def render_board_census_report(run: BoardCensusRun) -> str:
    portal_totals = {total.portal_type: total.job_count for total in run.portal_totals}
    lines = [
        f"Boards configured: {run.unique_board_count}",
        f"Boards counted: {run.counted_board_count}",
        f"Boards failed: {run.failed_count}",
        "",
        "Per-board counts",
    ]
    if run.counted_boards:
        lines.extend(
            f"{item.portal_label} | {item.board_root_url} | {item.available_job_count}"
            for item in run.counted_boards
        )
    else:
        lines.append("(none)")
    lines.append("")
    if run.failed_boards:
        lines.append("Failed boards")
        lines.extend(
            (
                f"{item.portal_label} | {(item.board_root_url or item.input_urls[0])} | "
                f"{item.reason_code} | {item.reason}"
            )
            for item in run.failed_boards
        )
        lines.append("")
    lines.extend(
        [
            "Portal totals",
            f"Greenhouse: {portal_totals.get('greenhouse', 0)}",
            f"Lever: {portal_totals.get('lever', 0)}",
            f"Ashby: {portal_totals.get('ashby', 0)}",
            "Workday: not supported in board-census yet",
            "",
            f"Grand total: {run.grand_total}",
        ]
    )
    return "\n".join(lines)


def _format_collect_result_line(result) -> str:
    line = f"- {result.source.source_url} | {result.reason_code} | {result.reason}"
    if result.suggested_next_action:
        line = f"{line} | next: {result.suggested_next_action}"
    return line


def render_collect_error_report(error: str) -> str:
    return "\n".join(
        [
            "jobs_ai collect",
            "status: failed",
            f"error: {error}",
            "",
            "next:",
            f"- {_cli_example('collect --from-file sources.txt')}",
        ]
    )


def render_board_census_error_report(error: str) -> str:
    return "\n".join(
        [
            "jobs_ai board-census",
            "status: failed",
            f"error: {error}",
            "",
            "next:",
            f"- {_cli_example('board-census --from-file boards.txt')}",
        ]
    )


def render_source_registry_list_report(
    paths: WorkspacePaths,
    entries: Sequence[SourceRegistryEntry],
) -> str:
    lines = [
        "jobs_ai sources list",
        f"database path: {paths.database_path}",
        f"registry entries: {len(entries)}",
        f"status: {'empty' if not entries else 'success'}",
    ]
    if not entries:
        _append_guidance(
            lines,
            "next:",
            (
                _cli_example("sources add https://boards.greenhouse.io/example"),
                _cli_example("seed-sources --add-to-registry \"Example | example.com\""),
            ),
        )
        return "\n".join(lines)

    for entry in entries:
        descriptor = entry.company or entry.label or "label missing"
        lines.append(
            f"- [{entry.source_id}] {entry.status} | {entry.portal_type or 'portal unknown'} | {descriptor} | {entry.source_url}"
        )
        verification_text = entry.last_verified_at or "never"
        lines.append(f"  last verified: {verification_text}")
        if entry.provenance is not None:
            lines.append(f"  provenance: {entry.provenance}")
    return "\n".join(lines)


def render_source_registry_add_report(
    paths: WorkspacePaths,
    result: SourceRegistryMutationResult,
) -> str:
    entry = result.entry
    lines = [
        "jobs_ai sources add",
        f"database path: {paths.database_path}",
        f"action: {result.action}",
        f"source id: {entry.source_id}",
        f"source URL: {entry.source_url}",
        f"portal type: {entry.portal_type or 'unknown'}",
        f"company/label: {entry.company or entry.label or 'unknown'}",
        f"status: {entry.status}",
    ]
    if result.source_result is not None:
        lines.append(
            f"verification: {result.source_result.outcome} | {result.source_result.reason_code}"
        )
    _append_guidance(
        lines,
        "next:",
        (
            _cli_example("sources list"),
            _cli_example("sources collect --import"),
        ),
    )
    return "\n".join(lines)


def render_source_registry_import_report(
    paths: WorkspacePaths,
    input_path: Path,
    result: SourceRegistryImportResult,
) -> str:
    lines = [
        "jobs_ai sources import",
        f"database path: {paths.database_path}",
        f"input file: {input_path}",
        f"created: {result.created_count}",
        f"updated: {result.updated_count}",
        f"unchanged: {result.unchanged_count}",
        f"errors: {result.error_count}",
        f"status: {'completed with errors' if result.errors else 'success'}",
    ]
    if result.results:
        lines.append("registry updates:")
        lines.extend(
            f"- [{mutation.entry.source_id}] {mutation.action} | {mutation.entry.status} | {mutation.entry.source_url}"
            for mutation in result.results
        )
    if result.errors:
        lines.append("errors:")
        lines.extend(f"- {error}" for error in result.errors)
    _append_guidance(
        lines,
        "next:",
        (
            _cli_example("sources list"),
            _cli_example("sources verify"),
        ),
    )
    return "\n".join(lines)


def render_source_registry_verify_report(
    paths: WorkspacePaths,
    results: Sequence[SourceRegistryVerificationResult],
) -> str:
    lines = [
        "jobs_ai sources verify",
        f"database path: {paths.database_path}",
        f"verified sources: {len(results)}",
        f"active: {sum(1 for result in results if result.after.status == 'active')}",
        f"manual review: {sum(1 for result in results if result.after.status == 'manual_review')}",
        f"inactive: {sum(1 for result in results if result.after.status == 'inactive')}",
        f"status: {'nothing selected' if not results else 'success'}",
    ]
    for result in results:
        lines.append(
            f"- [{result.after.source_id}] {result.before.status} -> {result.after.status} | {result.after.source_url} | {result.source_result.reason_code}"
        )
    return "\n".join(lines)


def render_source_registry_deactivate_report(
    paths: WorkspacePaths,
    entry: SourceRegistryEntry,
) -> str:
    return "\n".join(
        [
            "jobs_ai sources deactivate",
            f"database path: {paths.database_path}",
            f"source id: {entry.source_id}",
            f"source URL: {entry.source_url}",
            "status: inactive",
        ]
    )


def render_source_registry_collect_report(
    paths: WorkspacePaths,
    result: SourceRegistryCollectResult,
) -> str:
    collect_report = result.collect_run.report
    artifact_paths = collect_report.artifact_paths
    assert artifact_paths is not None
    lines = [
        "jobs_ai sources collect",
        f"database path: {paths.database_path}",
        f"selected registry sources: {result.selected_source_count}",
        f"verified before collect: {result.verified_source_count}",
        f"collect run id: {collect_report.run_id or artifact_paths.output_dir.name}",
        f"collect output dir: {artifact_paths.output_dir}",
        f"collected jobs: {collect_report.collected_count}",
        f"manual review jobs: {collect_report.manual_review_count}",
        f"skipped sources: {collect_report.skipped_count}",
        f"import requested: {'yes' if result.import_requested else 'no'}",
        f"status: {'success' if collect_report.collected_count or collect_report.manual_review_count else 'completed with skips'}",
    ]
    if result.import_result is not None:
        lines.append(f"imported jobs: {result.import_result.inserted_count}")
        lines.append(f"import batch id: {result.import_result.batch_id or 'none'}")
    _append_guidance(
        lines,
        "next:",
        (
            _cli_example(f"import {artifact_paths.leads_path}") if artifact_paths.leads_path is not None and not result.import_requested else _cli_example("session start --limit 25"),
        ),
    )
    return "\n".join(lines)


def render_source_registry_extract_jobposting_report(
    paths: WorkspacePaths,
    result: JobPostingExtractionResult,
) -> str:
    lines = [
        "jobs_ai sources extract-jobposting",
        f"database path: {paths.database_path}",
        f"batch id: {result.batch_id}",
        f"inputs processed: {result.input_count}",
        f"matched pages: {result.matched_page_count}",
        f"no-match pages: {result.no_match_count}",
        f"failed inputs: {result.failed_count}",
        f"JSON-LD blocks found: {result.json_ld_block_count}",
        f"JobPosting objects found: {result.jobposting_count}",
        f"imported jobs: {result.inserted_count}",
        f"duplicates skipped: {result.duplicate_count}",
        f"invalid job postings: {result.invalid_count}",
        f"registry links: {result.registry_link_count}",
        f"status: {'completed with errors' if result.failed_count else 'success'}",
    ]

    matched_results = [
        item
        for item in result.target_results
        if item.outcome == "success"
    ]
    if matched_results:
        lines.append("matched pages:")
        lines.extend(
            (
                f"- {item.raw_input} | inserted {item.inserted_count} | "
                f"duplicates {item.duplicate_count} | invalid {item.invalid_count} | "
                f"links {item.registry_link_count} | {item.final_url or item.resolved_url}"
            )
            for item in matched_results
        )

    no_match_results = [
        item
        for item in result.target_results
        if item.outcome == "no_match"
    ]
    if no_match_results:
        lines.append("no-match pages:")
        lines.extend(
            f"- {item.raw_input} | {item.reason_code} | {item.reason}"
            for item in no_match_results
        )

    failed_results = [
        item
        for item in result.target_results
        if item.outcome == "failed"
    ]
    if failed_results:
        lines.append("failed inputs:")
        lines.extend(
            f"- {item.raw_input} | {item.reason_code} | {item.reason}"
            for item in failed_results
        )

    _append_guidance(
        lines,
        "next:",
        (
            _cli_example(f"session start --batch-id {result.batch_id} --limit 25"),
            _cli_example("queue --limit 25"),
        ),
    )
    return "\n".join(lines)


def render_source_registry_discover_ats_report(
    paths: WorkspacePaths,
    result: SourceRegistryATSDiscoveryResult,
) -> str:
    lines = [
        "jobs_ai sources discover-ats",
        f"database path: {paths.database_path}",
        f"candidate slugs available: {result.candidate_slug_count}",
        f"candidate slugs tested: {result.tested_slug_count}",
        f"providers: {', '.join(result.providers)}",
        f"active boards found: {result.active_count}",
        f"greenhouse boards found: {result.greenhouse_count}",
        f"lever boards found: {result.lever_count}",
        f"ashby boards found: {result.ashby_count}",
        f"manual review boards: {result.manual_review_count}",
        f"ignored probe results: {result.ignored_count}",
        f"registry created: {result.created_count}",
        f"registry updated: {result.updated_count}",
        f"registry unchanged: {result.unchanged_count}",
        f"limit: {result.limit}",
        f"max concurrency: {result.max_concurrency}",
        f"max requests per second: {result.max_requests_per_second:.1f}",
        f"status: {'success with follow-up' if result.manual_review_count else 'success'}",
    ]
    if result.output_path is not None:
        lines.append(f"output file: {result.output_path}")

    lines.append("provider probe counts:")
    lines.extend(
        (
            f"- {item.provider} | active {item.active} | "
            f"manual_review {item.manual_review} | ignored {item.ignored}"
        )
        for item in result.provider_counts
    )

    manual_review_items = result.manual_review_item_results
    if manual_review_items:
        lines.append("manual review sources:")
        lines.extend(
            (
                f"- [{item.mutation.entry.source_id}] {item.action} | "
                f"{item.portal_type} | {item.source_url} | {item.reason_code}"
            )
            for item in manual_review_items
        )

    _append_guidance(
        lines,
        "next:",
        (
            _cli_example("sources collect --import"),
            _cli_example('run --use-registry "python"'),
        ),
    )
    return "\n".join(lines)


def render_source_registry_harvest_companies_report(
    paths: WorkspacePaths,
    result: SourceRegistryHarvestCompaniesResult,
) -> str:
    lines = [
        "jobs_ai sources harvest-companies",
        f"database path: {paths.database_path}",
        f"run id: {result.run_id}",
        f"output dir: {result.artifact_paths.output_dir}",
        f"sources: {', '.join(result.source_names)}",
        f"Discovered {result.harvested_domain_count} company domains",
        f"directory pages fetched: {result.directory_page_count - result.failed_page_count}",
        f"directory pages failed: {result.failed_page_count}",
        f"harvested domains artifact: {result.artifact_paths.harvested_domains_path}",
        f"harvest report: {result.artifact_paths.harvest_report_path}",
        f"confirmed registry sources: {result.detect_sites_result.confirmed_count}",
        f"manual review registry sources: {result.detect_sites_result.manual_review_count}",
        f"failed company domains: {result.detect_sites_result.failed_count}",
        f"registry created: {result.detect_sites_result.created_count}",
        f"registry updated: {result.detect_sites_result.updated_count}",
        f"registry unchanged: {result.detect_sites_result.unchanged_count}",
        f"timeout seconds: {result.timeout_seconds:.1f}",
        f"max requests per second: {result.max_requests_per_second:.1f}",
        (
            "status: success with follow-up"
            if result.failed_page_count
            or result.detect_sites_result.manual_review_count
            or result.detect_sites_result.failed_count
            else "status: success"
        ),
    ]

    failed_pages = [
        page_result
        for page_result in result.page_results
        if page_result.error is not None
    ]
    if failed_pages:
        lines.append("failed directory pages:")
        lines.extend(
            f"- {page_result.source_name} | {page_result.directory_url} | {page_result.error}"
            for page_result in failed_pages
        )

    manual_review_items = list(
        {
            item.mutation.entry.source_id: item
            for item in result.detect_sites_result.item_results
            if item.outcome == "manual_review" and item.mutation is not None
        }.values()
    )
    if manual_review_items:
        lines.append("manual review sources:")
        lines.extend(
            (
                f"- [{item.mutation.entry.source_id}] {item.mutation.action} | "
                f"{item.portal_type or 'unknown'} | {item.source_url} | {item.reason_code}"
            )
            for item in manual_review_items
        )

    failed_inputs = [
        input_result
        for input_result in result.detect_sites_result.input_results
        if input_result.outcome == "failed"
    ]
    if failed_inputs:
        lines.append("failed company domains:")
        lines.extend(
            (
                f"- {input_result.raw_input} | {input_result.reason_code} | "
                f"{input_result.reason}"
            )
            for input_result in failed_inputs
        )

    _append_guidance(
        lines,
        "next:",
        (
            _cli_example("sources collect --import"),
            _cli_example('run --use-registry "python"'),
        ),
    )
    return "\n".join(lines)


def render_source_registry_sync_report(
    command_label: str,
    results: Sequence[SourceRegistryMutationResult],
) -> str:
    created_count = sum(1 for result in results if result.action == "created")
    updated_count = sum(1 for result in results if result.action == "updated")
    unchanged_count = sum(1 for result in results if result.action == "unchanged")
    lines = [
        command_label,
        f"registry created: {created_count}",
        f"registry updated: {updated_count}",
        f"registry unchanged: {unchanged_count}",
        f"status: {'no confirmed sources' if not results else 'success'}",
    ]
    if results:
        lines.extend(
            f"- [{result.entry.source_id}] {result.action} | {result.entry.source_url}"
            for result in results
        )
    return "\n".join(lines)


def render_source_registry_seed_bulk_report(
    paths: WorkspacePaths,
    result: SourceRegistrySeedBulkResult,
) -> str:
    lines = [
        "jobs_ai sources seed-bulk",
        f"database path: {paths.database_path}",
        f"input companies: {result.input_company_count}",
        f"starter lists: {', '.join(result.starter_lists) if result.starter_lists else 'none'}",
        f"confirmed registry sources: {result.confirmed_count}",
        f"manual review registry sources: {result.manual_review_count}",
        f"failed inputs: {result.failed_count}",
        f"registry created: {result.created_count}",
        f"registry updated: {result.updated_count}",
        f"registry unchanged: {result.unchanged_count}",
        f"status: {'success with follow-up' if result.manual_review_count or result.failed_count else 'success'}",
    ]
    manual_review_items = list(
        {
            item.mutation.entry.source_id: item
            for item in result.item_results
            if item.outcome == "manual_review" and item.mutation is not None
        }.values()
    )
    if manual_review_items:
        lines.append("manual review sources:")
        lines.extend(
            (
                f"- [{item.mutation.entry.source_id}] {item.mutation.action} | "
                f"{item.portal_type or 'unknown'} | {item.source_url} | {item.reason_code}"
            )
            for item in manual_review_items
        )

    failed_items = [
        item
        for item in result.item_results
        if item.outcome == "failed"
    ]
    if failed_items:
        lines.append("failed inputs:")
        lines.extend(
            f"- {item.raw_input} | {item.reason_code} | {item.reason}"
            for item in failed_items
        )

    _append_guidance(
        lines,
        "next:",
        (
            _cli_example("sources collect --import"),
            _cli_example('run --use-registry "python"'),
        ),
    )
    return "\n".join(lines)


def render_source_registry_detect_sites_report(
    paths: WorkspacePaths,
    result: SourceRegistryDetectSitesResult,
) -> str:
    lines = [
        "jobs_ai sources detect-sites",
        f"database path: {paths.database_path}",
        f"inputs processed: {result.input_count}",
        f"starter lists: {', '.join(result.starter_lists) if result.starter_lists else 'none'}",
        f"confirmed registry sources: {result.confirmed_count}",
        f"manual review registry sources: {result.manual_review_count}",
        f"failed inputs: {result.failed_count}",
        f"registry created: {result.created_count}",
        f"registry updated: {result.updated_count}",
        f"registry unchanged: {result.unchanged_count}",
        f"status: {'success with follow-up' if result.manual_review_count or result.failed_count else 'success'}",
    ]

    manual_review_items = list(
        {
            item.mutation.entry.source_id: item
            for item in result.item_results
            if item.outcome == "manual_review" and item.mutation is not None
        }.values()
    )
    if manual_review_items:
        lines.append("manual review sources:")
        lines.extend(
            (
                f"- [{item.mutation.entry.source_id}] {item.mutation.action} | "
                f"{item.portal_type or 'unknown'} | {item.source_url} | {item.reason_code}"
            )
            for item in manual_review_items
        )

    failed_inputs = [
        input_result
        for input_result in result.input_results
        if input_result.outcome == "failed"
    ]
    if failed_inputs:
        lines.append("failed inputs:")
        lines.extend(
            (
                f"- {input_result.raw_input} | {input_result.reason_code} | "
                f"{input_result.reason}"
            )
            for input_result in failed_inputs
        )

    _append_guidance(
        lines,
        "next:",
        (
            _cli_example("sources collect --import"),
            _cli_example('run --use-registry "python"'),
        ),
    )
    return "\n".join(lines)


def render_source_registry_expand_report(
    paths: WorkspacePaths,
    result: SourceRegistryExpandResult,
) -> str:
    lines = [
        "jobs_ai sources expand-registry",
        f"database path: {paths.database_path}",
        f"seed lane ran: {'yes' if result.seed_result is not None else 'no'}",
        f"detect-sites lane ran: {'yes' if result.detect_sites_result is not None else 'no'}",
        f"discover-ats lane ran: {'yes' if result.discover_ats_result is not None else 'no'}",
        f"structured clues enabled: {'yes' if result.structured_clues_enabled else 'no'}",
        f"active sources touched: {result.active_source_count}",
        f"manual review sources touched: {result.manual_review_source_count}",
        f"failed inputs: {result.failed_input_count}",
        f"ignored probe results: {result.ignored_probe_count}",
        f"registry created: {result.created_count}",
        f"registry updated: {result.updated_count}",
        f"registry unchanged: {result.unchanged_count}",
        f"status: {'success with follow-up' if result.manual_review_source_count or result.failed_input_count else 'success'}",
    ]

    if result.seed_result is not None:
        lines.append(
            "seed lane:"
            f" inputs {result.seed_result.input_company_count}"
            f" | active {result.seed_result.confirmed_count}"
            f" | manual_review {result.seed_result.manual_review_count}"
            f" | failed {result.seed_result.failed_count}"
        )

    if result.detect_sites_result is not None:
        lines.append(
            "detect-sites lane:"
            f" inputs {result.detect_sites_result.input_count}"
            f" | active {result.detect_sites_result.confirmed_count}"
            f" | manual_review {result.detect_sites_result.manual_review_count}"
            f" | failed {result.detect_sites_result.failed_count}"
        )

    if result.discover_ats_result is not None:
        lines.append(
            "discover-ats lane:"
            f" slugs tested {result.discover_ats_result.tested_slug_count}"
            f" | active {result.discover_ats_result.active_count}"
            f" | manual_review {result.discover_ats_result.manual_review_count}"
            f" | ignored {result.discover_ats_result.ignored_count}"
        )
        lines.append("provider probe counts:")
        lines.extend(
            (
                f"- {item.provider} | active {item.active} | "
                f"manual_review {item.manual_review} | ignored {item.ignored}"
            )
            for item in result.discover_ats_result.provider_counts
        )

    _append_guidance(
        lines,
        "next:",
        (
            _cli_example("sources collect --import"),
            _cli_example('run --use-registry "python" --limit 25'),
        ),
    )
    return "\n".join(lines)


def render_seed_sources_report(report: SourceSeedRunReport) -> str:
    artifact_paths = report.artifact_paths
    assert artifact_paths is not None

    status = "success"
    if report.confirmed_count == 0 and report.manual_review_count == 0 and report.skipped_count:
        status = "completed with skips"
    elif report.manual_review_count or report.skipped_count:
        status = "success with follow-up"

    lines = [
        "jobs_ai seed-sources",
        f"run id: {report.run_id or artifact_paths.output_dir.name}",
        f"output dir: {artifact_paths.output_dir}",
        f"created_at: {report.created_at}",
        f"finished_at: {report.finished_at or report.created_at}",
        f"input companies: {report.input_company_count}",
        f"confirmed: {report.confirmed_count}",
        f"manual review: {report.manual_review_count}",
        f"skipped: {report.skipped_count}",
        f"confirmed source URLs: {report.confirmed_source_count}",
        f"timeout seconds: {report.timeout_seconds:.1f}",
        f"report only: {'yes' if report.report_only else 'no'}",
        f"status: {status}",
        f"seed report: {artifact_paths.seed_report_path}",
    ]
    if artifact_paths.confirmed_sources_path is None:
        lines.append("confirmed sources artifact: not written (--report-only)")
    else:
        lines.append(f"confirmed sources artifact: {artifact_paths.confirmed_sources_path}")
    if artifact_paths.manual_review_sources_path is None:
        lines.append("manual review artifact: not written (--report-only)")
    else:
        lines.append(f"manual review artifact: {artifact_paths.manual_review_sources_path}")

    if report.confirmed_count == 0:
        lines.append("note: no ATS board roots were auto-confirmed")

    manual_review_results = [
        result
        for result in report.company_results
        if result.outcome == "manual_review"
    ]
    if manual_review_results:
        lines.append("manual review companies:")
        lines.extend(_format_seed_result_line(result) for result in manual_review_results)

    skipped_results = [
        result
        for result in report.company_results
        if result.outcome == "skipped"
    ]
    if skipped_results:
        lines.append("skipped companies:")
        lines.extend(_format_seed_result_line(result) for result in skipped_results)

    if artifact_paths.confirmed_sources_path is not None and report.confirmed_source_count:
        _append_guidance(
            lines,
            "next:",
            (_cli_example(f"collect --from-file {artifact_paths.confirmed_sources_path}"),),
        )
    return "\n".join(lines)


def _format_seed_result_line(result) -> str:
    company_label = result.company_input.company or result.company_input.raw_value
    line = f"- {company_label} | {result.reason_code} | {result.reason}"
    if result.suggested_next_action:
        line = f"{line} | next: {result.suggested_next_action}"
    return line


def render_seed_sources_error_report(error: str) -> str:
    return "\n".join(
        [
            "jobs_ai seed-sources",
            "status: failed",
            f"error: {error}",
            "",
            "next:",
            f"- {_cli_example('seed-sources --from-file companies.txt')}",
        ]
    )


def render_discover_report(report: DiscoverRunReport) -> str:
    artifact_paths = report.artifact_paths
    assert artifact_paths is not None

    status = "success"
    if report.has_fatal_search_failure:
        status = "failed"
    elif report.raw_hit_count == 0 and not report.has_search_failures:
        status = "completed with zero search hits"
    elif report.confirmed_count == 0 and report.manual_review_count == 0 and report.skipped_count:
        status = "completed with skips"
    elif report.manual_review_count or report.skipped_count:
        status = "success with follow-up"

    lines = [
        "jobs_ai discover",
        f"run id: {report.run_id or artifact_paths.output_dir.name}",
        f"output dir: {artifact_paths.output_dir}",
        f"created_at: {report.created_at}",
        f"finished_at: {report.finished_at or report.created_at}",
        f"query: {report.query}",
        f"search queries: {len(report.search_results)}",
        f"raw hits: {report.raw_hit_count}",
        f"candidate sources found: {report.candidate_source_count}",
        f"candidate sources verified: {report.verified_candidate_count}",
        f"confirmed: {report.confirmed_count}",
        f"manual review: {report.manual_review_count}",
        f"skipped: {report.skipped_count}",
        f"timeout seconds: {report.timeout_seconds:.1f}",
        f"report only: {'yes' if report.report_only else 'no'}",
        f"collect requested: {'yes' if report.collect_requested else 'no'}",
        f"import requested: {'yes' if report.import_requested else 'no'}",
        f"status: {status}",
        f"discover report: {artifact_paths.discover_report_path}",
    ]
    if artifact_paths.confirmed_sources_path is None:
        lines.append("confirmed sources artifact: not written (--report-only)")
    else:
        lines.append(f"confirmed sources artifact: {artifact_paths.confirmed_sources_path}")
    if artifact_paths.manual_review_sources_path is None:
        lines.append("manual review artifact: not written (--report-only)")
    else:
        lines.append(f"manual review artifact: {artifact_paths.manual_review_sources_path}")
    if artifact_paths.search_artifact_dir is not None:
        lines.append(f"search artifacts dir: {artifact_paths.search_artifact_dir}")
    lines.extend(_render_discovery_summary_lines(report))
    lines.extend(_render_search_summary_lines(report))

    search_outcomes = [result for result in report.search_results if result.status != "success"]
    if search_outcomes:
        lines.append("search query outcomes:")
        lines.extend(_format_search_execution_line(result) for result in search_outcomes)

    manual_review_results = [
        result
        for result in report.candidate_results
        if result.outcome == "manual_review"
    ]
    if manual_review_results:
        lines.append("manual review sources:")
        lines.extend(_format_discover_result_line(result) for result in manual_review_results)

    skipped_results = [
        result
        for result in report.candidate_results
        if result.outcome == "skipped"
    ]
    if skipped_results:
        lines.append("skipped candidates:")
        lines.extend(_format_discover_result_line(result) for result in skipped_results)

    collect_summary = report.collect_summary
    if collect_summary is not None and collect_summary.requested:
        lines.append(
            f"collect step: {collect_summary.status} | collected {collect_summary.collected_count} | manual review {collect_summary.manual_review_count}"
        )
        if collect_summary.run_report_path is not None:
            lines.append(f"collect run report: {collect_summary.run_report_path}")
        if collect_summary.leads_path is not None:
            lines.append(f"collect leads artifact: {collect_summary.leads_path}")

    import_summary = report.import_summary
    if import_summary is not None and import_summary.requested:
        lines.append(
            f"import step: {import_summary.status} | inserted {import_summary.inserted_count} | skipped {import_summary.skipped_count}"
        )
        if import_summary.input_path is not None:
            lines.append(f"import input: {import_summary.input_path}")
        if import_summary.batch_id is not None:
            lines.append(f"import batch id: {import_summary.batch_id}")
        if import_summary.source_query is not None:
            lines.append(f"import source query: {import_summary.source_query}")
        if import_summary.errors:
            lines.append("import errors:")
            lines.extend(f"- {error}" for error in import_summary.errors)

    if report.has_fatal_search_failure:
        lines.append("note: no ATS sources were auto-confirmed because the search provider returned anomalous or unusable responses")
    elif report.confirmed_count == 0:
        lines.append("note: no ATS sources were auto-confirmed from this query")

    next_steps: list[str] = []
    if (
        not report.collect_requested
        and artifact_paths.confirmed_sources_path is not None
        and report.confirmed_count
    ):
        next_steps.append(_cli_example(f"collect --from-file {artifact_paths.confirmed_sources_path}"))
    if collect_summary is not None and collect_summary.leads_path is not None and not report.import_requested:
        next_steps.append(_cli_example(f"import {collect_summary.leads_path}"))
    if import_summary is not None and import_summary.executed and not import_summary.errors:
        next_steps.extend(_import_summary_next_commands(import_summary))
        next_steps.append(_command_with_optional_limit("queue", 25))
    _append_guidance(lines, "next:", tuple(dict.fromkeys(next_steps)))
    return "\n".join(lines)


def _format_discover_result_line(result) -> str:
    candidate_url = _discover_result_url(result)
    if result.outcome == "manual_review" and result.candidate.portal_type == "workday":
        line = f"- manual review (Workday portal) | {candidate_url} | {result.reason_code} | {result.reason}"
    else:
        line = f"- {candidate_url} | {result.reason_code} | {result.reason}"
    if result.suggested_next_action:
        line = f"{line} | next: {result.suggested_next_action}"
    return line


def _format_search_execution_line(result) -> str:
    line = (
        f"- {result.plan.site_filter} | {result.status} | attempts {result.attempt_count} | "
        f"hits {result.hit_count}"
    )
    if result.error:
        line = f"{line} | {result.error}"
    elif result.status == "zero_results":
        line = f"{line} | provider returned an explicit zero-results page"
    evidence = result.evidence
    if evidence is not None and evidence.detected_patterns:
        line = f"{line} | patterns: {', '.join(evidence.detected_patterns)}"
    if result.raw_artifact_paths:
        line = f"{line} | artifacts: {len(result.raw_artifact_paths)}"
    return line


def _render_discovery_summary_lines(report: DiscoverRunReport) -> list[str]:
    confirmed_counts = {
        "greenhouse": 0,
        "lever": 0,
        "ashby": 0,
    }
    workday_manual_review_count = 0

    for result in report.candidate_results:
        portal_type = result.candidate.portal_type
        if result.outcome == "confirmed" and portal_type in confirmed_counts:
            confirmed_counts[portal_type] += 1
        if result.outcome == "manual_review" and portal_type == "workday":
            workday_manual_review_count += 1

    return [
        "discovery summary:",
        f"- greenhouse sources: {confirmed_counts['greenhouse']}",
        f"- lever sources: {confirmed_counts['lever']}",
        f"- ashby sources: {confirmed_counts['ashby']}",
        f"- workday sources (manual review): {workday_manual_review_count}",
    ]


def _render_search_summary_lines(report: DiscoverRunReport) -> list[str]:
    status_counts = {
        "success": 0,
        "zero_results": 0,
        "provider_anomaly": 0,
        "parse_failure": 0,
        "fetch_failure": 0,
    }
    recovered_after_retry = 0
    for result in report.search_results:
        status_counts[result.status] += 1
        if result.status == "success" and result.attempt_count > 1:
            recovered_after_retry += 1
    return [
        "search summary:",
        f"- success: {status_counts['success']}",
        f"- zero_results: {status_counts['zero_results']}",
        f"- provider_anomaly: {status_counts['provider_anomaly']}",
        f"- parse_failure: {status_counts['parse_failure']}",
        f"- fetch_failure: {status_counts['fetch_failure']}",
        f"- recovered_after_retry: {recovered_after_retry}",
    ]


def _discover_result_url(result) -> str:
    candidate = result.candidate
    if candidate.source_url is not None:
        return candidate.source_url
    if candidate.normalized_url is not None:
        return candidate.normalized_url
    if candidate.supporting_results:
        return candidate.supporting_results[0].target_url
    return "<unknown>"


def _discover_workday_manual_review_count(report: DiscoverRunReport) -> int:
    return sum(
        1
        for result in report.candidate_results
        if result.outcome == "manual_review" and result.candidate.portal_type == "workday"
    )


def render_discover_error_report(error: str) -> str:
    return "\n".join(
        [
            "jobs_ai discover",
            "status: failed",
            f"error: {error}",
            "",
            "next:",
            f"- {_cli_example('discover \"python backend engineer remote\"')}",
        ]
    )


def render_score_report(paths: WorkspacePaths, scored_jobs: Sequence[ScoredJob]) -> str:
    lines = [
        "jobs_ai score",
        f"database path: {paths.database_path}",
        f"jobs scored: {len(scored_jobs)}",
    ]
    if not scored_jobs:
        lines.append("status: no jobs to score")
        _append_guidance(lines, "next:", (_cli_example("import data/raw/sample_job_leads.json"),))
        return "\n".join(lines)

    lines.append("status: success")
    for index, job in enumerate(scored_jobs, start=1):
        location = job.location or "location missing"
        lines.extend(
            [
                "",
                f"{index}. score {job.total_score} | {job.company} | {job.title} | {location}",
                f"   job id: {job.job_id}",
                f"   role: {job.role_reason} (+{job.role_score})",
                f"   stack: {job.stack_reason} (+{job.stack_score})",
                f"   geography: {job.geography_reason} (+{job.geography_score})",
                f"   source: {job.source_reason} (+{job.source_score})",
                f"   actionability: {job.actionability_reason} ({job.actionability_score:+d})",
            ]
        )
        if job.apply_url:
            lines.append(f"   apply_url: {job.apply_url}")
    _append_guidance(lines, "next:", (_cli_example("queue"),))
    return "\n".join(lines)


def render_stats_report(paths: WorkspacePaths, stats: OperatorStats) -> str:
    lines = [
        "jobs_ai stats",
        f"database path: {paths.database_path}",
        f"days: {stats.days}",
        f"total jobs: {stats.total_jobs}",
        *(
            f"{status}: {stats.status_count(status)}"
            for status in APPLICATION_STATUSES
        ),
        f"recent imports ({stats.days}d): {stats.recent_imported_jobs}",
        f"recent import batches ({stats.days}d): {stats.recent_import_batches}",
        f"total sessions started: {stats.total_sessions_started}",
        f"recent sessions started ({stats.days}d): {stats.recent_sessions_started}",
        f"total tracking events: {stats.total_tracking_events}",
        f"recent tracking events ({stats.days}d): {stats.recent_tracking_events}",
    ]
    if stats.portal_counts:
        lines.append("portal counts:")
        lines.extend(f"- {entry.label}: {entry.count}" for entry in stats.portal_counts)
    if stats.top_companies:
        lines.append("top companies:")
        lines.extend(f"- {entry.label}: {entry.count}" for entry in stats.top_companies)
    lines.append("status: success")
    _append_guidance(
        lines,
        "next:",
        (
            _cli_example('run "python backend engineer remote" --limit 25 --open'),
            _cli_example("track list --status applied"),
        ),
    )
    return "\n".join(lines)


def render_stats_json(paths: WorkspacePaths, stats: OperatorStats) -> str:
    payload = {
        "command": "jobs_ai stats",
        "database_path": str(paths.database_path),
        "days": stats.days,
        "total_jobs": stats.total_jobs,
        "status_counts": {entry.label: entry.count for entry in stats.status_counts},
        "recent_imported_jobs": stats.recent_imported_jobs,
        "recent_import_batches": stats.recent_import_batches,
        "total_sessions_started": stats.total_sessions_started,
        "recent_sessions_started": stats.recent_sessions_started,
        "total_tracking_events": stats.total_tracking_events,
        "recent_tracking_events": stats.recent_tracking_events,
        "portal_counts": {entry.label: entry.count for entry in stats.portal_counts},
        "top_companies": [
            {"company": entry.label, "count": entry.count}
            for entry in stats.top_companies
        ],
    }
    return json.dumps(payload, indent=2, ensure_ascii=True)


def render_queue_report(
    paths: WorkspacePaths,
    queued_jobs: Sequence[QueuedJob],
    *,
    limit: int | None = None,
) -> str:
    lines = [
        "jobs_ai queue",
        f"database path: {paths.database_path}",
        f"working set size: {len(queued_jobs)}",
        f"limit: {limit if limit is not None else 'none'}",
    ]
    if not queued_jobs:
        lines.append("status: no new jobs in queue")
        lines.append("tip: queue only shows jobs where status = 'new'")
        _append_guidance(lines, "next:", (_cli_example("import data/raw/sample_job_leads.json"),))
        return "\n".join(lines)

    lines.append("status: success")
    for job in queued_jobs:
        location = job.location or "location missing"
        lines.extend(
            [
                "",
                f"{job.rank}. score {job.score} | {job.company} | {job.title} | {location} | {job.source}",
                f"   reason: {job.reason_summary}",
            ]
        )
    _append_guidance(lines, "next:", (_command_with_optional_limit("recommend", limit),))
    return "\n".join(lines)


def render_apply_report(
    paths: WorkspacePaths,
    result: ApplyResult,
) -> str:
    lines = [
        "jobs_ai apply",
        f"database path: {paths.database_path}",
        f"query: {result.query}",
        f"requested limit: {result.requested_limit}",
        f"effective limit: {result.effective_limit}",
        f"print only: {'yes' if result.print_only else 'no'}",
        f"matching jobs: {result.matching_count}",
        f"launchable jobs: {result.selected_count}",
    ]
    if result.warning is not None:
        lines.append(f"warning: {result.warning}")
    if result.skipped_missing_apply_url_count:
        lines.append(
            "matching jobs without apply_url: "
            f"{result.skipped_missing_apply_url_count}"
        )
    if not result.items:
        lines.append("status: no launchable jobs matched query")
        return "\n".join(lines)

    lines.append("status: success")
    lines.append("")
    lines.append(
        f"{'Printing' if result.print_only else 'Opening'} "
        f"{result.selected_count} applications..."
    )
    for index, item in enumerate(result.items, start=1):
        lines.extend(
            [
                "",
                f"{index}. {item.title} - {item.company}",
                (
                    f"   {'Apply URL' if result.print_only else 'Opening'}: "
                    f"{item.apply_url}"
                ),
            ]
        )
    return "\n".join(lines)


def render_apply_error_report(paths: WorkspacePaths, error: str) -> str:
    return "\n".join(
        [
            "jobs_ai apply",
            f"database path: {paths.database_path}",
            "status: failed",
            f"error: {error}",
        ]
    )


def render_recommendation_report(
    paths: WorkspacePaths,
    recommendations: Sequence[QueueRecommendation],
    *,
    limit: int | None = None,
) -> str:
    lines = [
        "jobs_ai recommend",
        f"database path: {paths.database_path}",
        f"recommendations: {len(recommendations)}",
        f"limit: {limit if limit is not None else 'none'}",
    ]
    if not recommendations:
        lines.append("status: no new jobs in queue")
        lines.append("tip: recommendations follow the same ranked queue used by jobs_ai queue")
        _append_guidance(lines, "next:", (_cli_example("queue"),))
        return "\n".join(lines)

    lines.append("status: success")
    for recommendation in recommendations:
        location = recommendation.location or "location missing"
        lines.extend(
            [
                "",
                (
                    f"{recommendation.rank}. score {recommendation.score} | "
                    f"{recommendation.company} | {recommendation.title} | {location} | {recommendation.source}"
                ),
                (
                    f"   resume variant: {recommendation.resume_variant_key} "
                    f"({recommendation.resume_variant_label})"
                ),
                (
                    f"   profile snippet: {recommendation.snippet_key} "
                    f"({recommendation.snippet_label})"
                ),
                f"   snippet text: {recommendation.snippet_text}",
                f"   explanation: {recommendation.explanation}",
            ]
        )
    _append_guidance(lines, "next:", (_command_with_optional_limit("launch-preview", limit),))
    return "\n".join(lines)


def render_launch_preview_report(
    paths: WorkspacePaths,
    previews: Sequence[LaunchPreview],
    *,
    limit: int | None = None,
    show_portal_hints: bool = False,
) -> str:
    lines = [
        "jobs_ai launch-preview",
        f"database path: {paths.database_path}",
        f"preview size: {len(previews)}",
        f"limit: {limit if limit is not None else 'none'}",
    ]
    if not previews:
        lines.append("status: no new jobs in queue")
        _append_guidance(lines, "next:", (_cli_example("queue"),))
        return "\n".join(lines)

    lines.append("status: success")
    for preview in previews:
        location = preview.location or "location missing"
        apply_url = preview.apply_url or "apply_url missing"
        lines.extend(
            [
                "",
                f"{preview.rank}. score {preview.score} | {preview.company} | {preview.title} | {location}",
                f"   apply_url: {apply_url}",
                (
                    f"   recommended resume variant: {preview.resume_variant_key} "
                    f"({preview.resume_variant_label})"
                ),
                (
                    f"   recommended profile snippet: {preview.snippet_key} "
                    f"({preview.snippet_label})"
                ),
                f"   snippet text: {preview.snippet_text}",
                f"   explanation: {preview.explanation}",
            ]
        )
        if show_portal_hints:
            lines.extend(_render_portal_support_lines(build_portal_support(preview.apply_url)))
    if not show_portal_hints:
        lines.append("tip: rerun with --portal-hints when links look portal-hosted")
    _append_guidance(lines, "next:", (_command_with_optional_limit("export-session", limit),))
    return "\n".join(lines)


def render_fast_apply_report(
    paths: WorkspacePaths,
    result: SessionStartResult,
    selections: Sequence[FastApplySelection],
    *,
    limit: int,
    query_text: str | None,
    families: Sequence[str],
    remote_only: bool,
    easy_apply_first: bool,
    show_portal_hints: bool = False,
) -> str:
    lines = [
        "jobs_ai fast-apply",
        f"database path: {paths.database_path}",
        f"manifest path: {result.export_result.export_path}",
        f"created_at: {result.export_result.created_at}",
        f"shortlisted jobs: {result.selected_count}",
        f"launchable jobs: {result.plan.launchable_items}",
        f"limit: {limit}",
        f"query: {query_text or 'none'}",
        f"families: {', '.join(families) if families else 'all likely matches'}",
        f"remote only: {'yes' if remote_only else 'no'}",
        f"easy apply first: {'yes' if easy_apply_first else 'no'}",
        f"status: {'no likely fast-apply matches found' if not selections else 'success'}",
    ]
    insert_at = 4
    if result.selection_scope is not None:
        scope_lines = [f"selection scope: {_describe_selection_scope(result.selection_scope)}"]
        if result.selection_scope.source_query is not None:
            scope_lines.append(f"source query: {result.selection_scope.source_query}")
        scope_lines.extend(_selection_scope_detail_lines(result.selection_scope))
        lines[insert_at:insert_at] = scope_lines

    if not selections:
        _append_guidance(
            lines,
            "next:",
            (
                _cli_example("queue --limit 20"),
                _cli_example('run "python backend engineer remote" --limit 20'),
            ),
        )
        return "\n".join(lines)

    for item, selection in zip(result.items, selections):
        preview = item.preview
        flags = []
        if selection.matched_families:
            flags.append(f"families={', '.join(selection.matched_families)}")
        if selection.requested_family_hit:
            flags.append("family-priority")
        if selection.easy_apply_supported:
            flags.append("easy-apply")
        flags.append(f"resume={preview.resume_variant_key}")
        lines.extend(
            [
                "",
                (
                    f"{preview.rank}. [job {preview.job_id}] score {preview.score} | "
                    f"{preview.company} | {preview.title} | {preview.location or 'location missing'}"
                ),
                f"   fast-apply: {' | '.join(flags)}",
                f"   apply_url: {preview.apply_url or 'apply_url missing'}",
                f"   fit: {selection.fit_reason}",
                f"   signals: {selection.queue_reason_summary}",
                f"   explanation: {preview.explanation}",
            ]
        )
        if show_portal_hints:
            lines.extend(_render_portal_support_lines(item.portal_support))

    _append_guidance(
        lines,
        "next:",
        (
            _cli_example(
                f"application-assist{' --portal-hints' if show_portal_hints else ''} {result.export_result.export_path}"
            ),
            _cli_example(
                f"session mark applied --manifest {result.export_result.export_path} --index 1"
            ),
            _cli_example(
                f"session mark skipped --manifest {result.export_result.export_path} --index 2"
            ),
        ),
    )
    return "\n".join(lines)


def render_fast_apply_error_report(paths: WorkspacePaths, error: str) -> str:
    lines = [
        "jobs_ai fast-apply",
        f"database path: {paths.database_path}",
        "status: failed",
        f"error: {error}",
    ]
    _append_guidance(lines, "next:", (_cli_example("queue --limit 20"),))
    return "\n".join(lines)


def render_export_session_report(
    paths: WorkspacePaths,
    result: SessionExportResult,
) -> str:
    lines = [
        "jobs_ai export-session",
        f"database path: {paths.database_path}",
        f"export path: {result.export_path}",
        f"created_at: {result.created_at}",
        f"item count: {result.item_count}",
        f"limit: {result.limit if result.limit is not None else 'none'}",
        "status: success",
    ]
    _append_guidance(lines, "next:", (_cli_example(f"preflight {result.export_path}"),))
    return "\n".join(lines)


def render_session_start_report(
    paths: WorkspacePaths,
    result: SessionStartResult,
    *,
    show_portal_hints: bool = False,
) -> str:
    lines = [
        "jobs_ai session start",
        f"database path: {paths.database_path}",
        f"manifest path: {result.export_result.export_path}",
        f"created_at: {result.export_result.created_at}",
        f"selected jobs: {result.selected_count}",
        f"launchable jobs: {result.plan.launchable_items}",
        f"skipped jobs: {result.plan.skipped_items}",
        f"resume recommendations: {result.resume_recommendation_count}",
        f"resume files resolved: {result.resolved_resume_count}",
        f"portal hints: {result.portal_hint_count}",
        f"limit: {result.limit}",
        f"status: {'no new jobs in queue' if not result.items else 'success'}",
    ]
    insert_at = 4
    if result.export_result.label is not None:
        lines.insert(insert_at, f"label: {result.export_result.label}")
        insert_at += 1
    if result.selection_scope is not None:
        scope_lines = [f"selection scope: {_describe_selection_scope(result.selection_scope)}"]
        if result.selection_scope.source_query is not None:
            scope_lines.append(f"source query: {result.selection_scope.source_query}")
        scope_lines.extend(_selection_scope_detail_lines(result.selection_scope))
        lines[insert_at:insert_at] = scope_lines

    if result.open_requested:
        assert result.executor_mode is not None
        lines.append(f"open executor: {result.executor_mode}")
        lines.append(f"open actions: {len(result.execution_reports)}")
        if result.execution_reports:
            lines.append(
                f"opened in browser: {_count_launch_execution_status(result.execution_reports, 'opened')}"
            )
            lines.append(
                f"dry run only: {_count_launch_execution_status(result.execution_reports, 'noop')}"
            )
            lines.append(
                "skipped missing url: "
                f"{_count_launch_execution_status(result.execution_reports, 'skipped_missing_url')}"
            )

    if not result.items:
        _append_guidance(
            lines,
            "next:",
            (
                _cli_example("discover \"python backend engineer remote\" --collect --import"),
                _cli_example("track list"),
            ),
        )
        return "\n".join(lines)

    for item in result.items:
        preview = item.preview
        location = preview.location or "location missing"
        lines.extend(
            [
                "",
                (
                    f"{preview.rank}. [job {preview.job_id}] score {preview.score} | "
                    f"{preview.company} | {preview.title} | {location}"
                ),
                f"   apply_url: {preview.apply_url or 'apply_url missing'}",
                (
                    "   resume: "
                    f"{preview.resume_variant_key} ({preview.resume_variant_label})"
                ),
                f"   resume focus: {item.resume_variant_summary}",
                _format_resume_file_line(
                    item.resolved_resume_path,
                    item.resume_fallback_reason,
                ),
                (
                    "   profile snippet: "
                    f"{preview.snippet_key} ({preview.snippet_label})"
                ),
                f"   explanation: {preview.explanation}",
            ]
        )
        if show_portal_hints:
            lines.extend(_render_portal_support_lines(item.portal_support))

    _append_guidance(
        lines,
        "next:",
        _session_start_next_commands(
            manifest_path=result.export_result.export_path,
            opened_in_browser=_reports_include_opened(result.execution_reports),
            show_portal_hints=show_portal_hints,
        ),
    )
    return "\n".join(lines)


def render_session_start_error_report(paths: WorkspacePaths, error: str) -> str:
    lines = [
        "jobs_ai session start",
        f"database path: {paths.database_path}",
        "status: failed",
        f"error: {error}",
    ]
    _append_guidance(lines, "next:", (_cli_example("queue"), _cli_example("launch-preview")))
    return "\n".join(lines)


def render_session_recent_report(
    paths: WorkspacePaths,
    sessions: Sequence[SessionHistoryEntry],
    *,
    limit: int,
) -> str:
    lines = [
        "jobs_ai session recent",
        f"database path: {paths.database_path}",
        f"sessions listed: {len(sessions)}",
        f"limit: {limit}",
    ]
    if not sessions:
        lines.append("status: no recorded sessions")
        _append_guidance(lines, "next:", (_cli_example("session start --limit 25"),))
        return "\n".join(lines)

    lines.append("status: success")
    for index, entry in enumerate(sessions, start=1):
        lines.extend(
            [
                "",
                (
                    f"{index}. [session {entry.session_id}] {entry.created_at} | "
                    f"selected {entry.item_count} | launchable {entry.launchable_count}"
                ),
                f"   batch id: {entry.batch_id or 'none'}",
                f"   query: {entry.source_query or 'unknown'}",
                f"   manifest path: {entry.manifest_path}",
            ]
        )
    _append_guidance(
        lines,
        "next:",
        (
            _cli_example(f"session inspect {sessions[0].session_id}"),
            _cli_example(f"session reopen {sessions[0].session_id}"),
        ),
    )
    return "\n".join(lines)


def render_session_inspect_report(
    paths: WorkspacePaths,
    inspection: SessionInspection,
) -> str:
    manifest = inspection.resolved.manifest
    lines = [
        "jobs_ai session inspect",
        f"database path: {paths.database_path}",
        f"manifest path: {inspection.resolved.manifest_path}",
        f"created_at: {manifest.created_at}",
        f"items: {manifest.item_count}",
        f"launchable items: {inspection.plan.launchable_items}",
        f"skipped items: {inspection.plan.skipped_items}",
        f"status: {'success with skipped items' if inspection.plan.skipped_items else 'success'}",
    ]
    if inspection.resolved.session_history_entry is not None:
        lines.insert(2, f"session id: {inspection.resolved.session_history_entry.session_id}")
    if manifest.label is not None:
        lines.insert(4, f"label: {manifest.label}")
    if manifest.selection_scope is not None:
        scope_lines = [f"selection scope: {_describe_selection_scope(manifest.selection_scope)}"]
        if manifest.selection_scope.source_query is not None:
            scope_lines.append(f"source query: {manifest.selection_scope.source_query}")
        scope_lines.extend(_selection_scope_detail_lines(manifest.selection_scope))
        lines[4:4] = scope_lines
    if inspection.status_counts:
        lines.append("current tracked statuses:")
        lines.extend(
            f"- {entry.label}: {entry.count}"
            for entry in inspection.status_counts
        )
    for item in inspection.items:
        job_label = f"[job {item.job_id}]" if item.job_id is not None else "[job missing]"
        lines.extend(
            [
                "",
                f"{item.index}. {job_label} {item.company or 'company missing'} | {item.title or 'title missing'}",
                f"   manifest status: {'launchable' if item.launchable else 'skipped'}",
                f"   current tracking status: {item.current_status or 'unknown'}",
            ]
        )
        if item.warnings:
            lines.append(f"   warnings: {'; '.join(item.warnings)}")
    _append_guidance(
        lines,
        "next:",
        (
            _cli_example(f"session reopen {inspection.resolved.reference_text}"),
            _cli_example(f"preflight {inspection.resolved.manifest_path}"),
        ),
    )
    return "\n".join(lines)


def render_session_inspect_error_report(paths: WorkspacePaths, error: str) -> str:
    return "\n".join(
        [
            "jobs_ai session inspect",
            f"database path: {paths.database_path}",
            "status: failed",
            f"error: {error}",
        ]
    )


def render_session_reopen_report(
    paths: WorkspacePaths,
    result: SessionReopenResult,
) -> str:
    lines = [
        "jobs_ai session reopen",
        f"database path: {paths.database_path}",
        f"manifest path: {result.resolved.manifest_path}",
        f"executor mode: {result.executor_mode}",
        f"launchable items: {result.plan.launchable_items}",
        f"reopen actions: {len(result.execution_reports)}",
        f"opened in browser: {_count_launch_execution_status(result.execution_reports, 'opened')}",
        f"dry run only: {_count_launch_execution_status(result.execution_reports, 'noop')}",
        f"skipped missing url: {_count_launch_execution_status(result.execution_reports, 'skipped_missing_url')}",
        f"status: {'no launchable items to reopen' if not result.execution_reports else 'success'}",
    ]
    if result.resolved.session_history_entry is not None:
        lines.insert(2, f"session id: {result.resolved.session_history_entry.session_id}")
    _append_guidance(
        lines,
        "next:",
        (
            _cli_example(f"session inspect {result.resolved.reference_text}"),
            _cli_example(f"session mark opened --manifest {result.resolved.manifest_path} --all"),
        ),
    )
    return "\n".join(lines)


def render_session_reopen_error_report(paths: WorkspacePaths, error: str) -> str:
    return "\n".join(
        [
            "jobs_ai session reopen",
            f"database path: {paths.database_path}",
            "status: failed",
            f"error: {error}",
        ]
    )


def render_open_prompt(result: SessionOpenResult) -> str:
    item = result.selected_item
    return "\n".join(
        [
            (
                f"Opened: [{item.index}] "
                f"{_format_launch_dry_run_text(item.company, fallback='<missing company>')} - "
                f"{_format_launch_dry_run_text(item.title, fallback='<missing role title>')}"
            ),
            "What happened?",
            "[y] applied",
            "[s] skipped",
            "[l] later",
            "[q] quit",
        ]
    )


def render_open_unchanged_report(result: SessionOpenResult) -> str:
    item = result.selected_item
    return (
        "Status left unchanged for "
        f"[{item.index}] "
        f"{_format_launch_dry_run_text(item.company, fallback='<missing company>')} - "
        f"{_format_launch_dry_run_text(item.title, fallback='<missing role title>')}."
    )


def render_open_error_report(
    paths: WorkspacePaths,
    manifest_path: Path,
    error: str,
) -> str:
    return "\n".join(
        [
            "jobs_ai open",
            f"database path: {paths.database_path}",
            f"manifest path: {manifest_path}",
            "status: failed",
            f"error: {error}",
        ]
    )


def render_run_report(
    paths: WorkspacePaths,
    result: RunWorkflowResult,
    *,
    show_portal_hints: bool = False,
) -> str:
    if result.intake_mode == "registry":
        lines = [
            "jobs_ai run",
            f"database path: {paths.database_path}",
            f"query: {result.query}",
            "intake mode: registry",
            f"workflow dir: {result.output_dir}",
            f"registry sources selected: {result.confirmed_source_count}",
            f"registry sources verified: {result.registry_verified_source_count}",
            f"collected sources: {len(result.collected_sources)}",
            f"imported jobs: {result.imported_jobs_count}",
            f"import batch id: {result.import_result.batch_id if result.import_result is not None else (result.collect_run.report.run_id if result.collect_run is not None else result.output_dir.name)}",
            f"manifest path: {result.manifest_path}",
            f"selected jobs: {result.session_result.selected_count}",
            f"launchable jobs: {result.session_result.plan.launchable_items}",
            f"recommendations: {result.recommendation_count}",
            f"resume files resolved: {result.session_result.resolved_resume_count}",
            f"portal hints: {result.portal_hint_count}",
            f"session limit: {result.session_limit}",
            f"status: {'no matching new jobs in queue' if not result.session_result.items else 'success'}",
        ]
        if result.label is not None:
            lines.insert(5, f"label: {result.label}")
        if result.session_result.selection_scope is not None:
            scope_lines = [
                f"session scope: {_describe_selection_scope(result.session_result.selection_scope)}"
            ]
            scope_lines.extend(_selection_scope_detail_lines(result.session_result.selection_scope))
            insert_at = 6 if result.label is not None else 5
            lines[insert_at:insert_at] = scope_lines
        if result.open_requested:
            lines.append(f"open executor: {result.executor_mode or 'browser_stub'}")
        if result.import_result is not None and result.import_result.errors:
            lines.append("import errors:")
            lines.extend(f"- {error}" for error in result.import_result.errors)

        resume_lines = _render_run_resume_lines(result)
        if resume_lines:
            lines.append("resume variants:")
            lines.extend(resume_lines)

        if show_portal_hints:
            portal_lines = _render_run_portal_lines(result)
            if portal_lines:
                lines.append("portal hint details:")
                lines.extend(portal_lines)

        _append_guidance(
            lines,
            "next:",
            _session_start_next_commands(
                manifest_path=result.manifest_path,
                opened_in_browser=_reports_include_opened(result.session_result.execution_reports),
                show_portal_hints=show_portal_hints,
            ),
        )
        return "\n".join(lines)

    collect_suffix = ""
    if result.collect_limit is not None and result.confirmed_source_count > result.collect_limit:
        collect_suffix = f" of {result.confirmed_source_count} confirmed"

    lines = [
        "jobs_ai run",
        f"database path: {paths.database_path}",
        f"query: {result.query}",
        "intake mode: discover",
        f"workflow dir: {result.output_dir}",
        (
            "discover report: "
            f"{result.discover_run.report.artifact_paths.discover_report_path}"
        ),
        f"confirmed sources: {result.confirmed_source_count}",
        f"workday sources (manual review): {_discover_workday_manual_review_count(result.discover_run.report)}",
        f"collected sources: {len(result.collected_sources)}{collect_suffix}",
        f"imported jobs: {result.imported_jobs_count}",
        f"import batch id: {result.import_result.batch_id if result.import_result is not None else (result.discover_run.report.run_id or result.output_dir.name)}",
        f"manifest path: {result.manifest_path}",
        f"selected jobs: {result.session_result.selected_count}",
        f"launchable jobs: {result.session_result.plan.launchable_items}",
        f"recommendations: {result.recommendation_count}",
        f"resume files resolved: {result.session_result.resolved_resume_count}",
        f"portal hints: {result.portal_hint_count}",
        f"session limit: {result.session_limit}",
        f"status: {'no new jobs in queue' if not result.session_result.items else 'success'}",
    ]
    if result.label is not None:
        lines.insert(4, f"label: {result.label}")
    if result.session_result.selection_scope is not None:
        scope_lines = [
            f"session scope: {_describe_selection_scope(result.session_result.selection_scope)}"
        ]
        scope_lines.extend(_selection_scope_detail_lines(result.session_result.selection_scope))
        insert_at = 5 if result.label is not None else 4
        lines[insert_at:insert_at] = scope_lines
    if result.open_requested:
        lines.append(f"open executor: {result.executor_mode or 'browser_stub'}")
    if result.discover_run.report.has_search_failures:
        issue_count = sum(
            1
            for search_result in result.discover_run.report.search_results
            if search_result.status in {"provider_anomaly", "parse_failure", "fetch_failure"}
        )
        lines.append(f"search issues: {issue_count} (see discover report)")

    if result.import_result is not None and result.import_result.errors:
        lines.append("import errors:")
        lines.extend(f"- {error}" for error in result.import_result.errors)

    resume_lines = _render_run_resume_lines(result)
    if resume_lines:
        lines.append("resume variants:")
        lines.extend(resume_lines)

    if show_portal_hints:
        portal_lines = _render_run_portal_lines(result)
        if portal_lines:
            lines.append("portal hint details:")
            lines.extend(portal_lines)

    _append_guidance(
        lines,
        "next:",
        _session_start_next_commands(
            manifest_path=result.manifest_path,
            opened_in_browser=_reports_include_opened(result.session_result.execution_reports),
            show_portal_hints=show_portal_hints,
        ),
    )
    return "\n".join(lines)


def render_run_error_report(paths: WorkspacePaths, error: str) -> str:
    lines = [
        "jobs_ai run",
        f"database path: {paths.database_path}",
        "status: failed",
        f"error: {error}",
    ]
    _append_guidance(
        lines,
        "next:",
        (
            'jobs-ai run "python backend engineer remote" --limit 25 --open',
            _cli_example('discover "python backend engineer remote" --collect --import'),
        ),
    )
    return "\n".join(lines)


def render_run_discover_failure_report(
    paths: WorkspacePaths,
    result: DiscoverRun,
) -> str:
    artifact_paths = result.report.artifact_paths
    assert artifact_paths is not None
    lines = [
        "jobs_ai run",
        f"database path: {paths.database_path}",
        "status: failed",
        "error: discovery search failed before collection, import, or session start",
        f"query: {result.report.query}",
        f"workflow dir: {artifact_paths.output_dir}",
        f"discover report: {artifact_paths.discover_report_path}",
        f"confirmed sources: {result.report.confirmed_count}",
    ]
    if artifact_paths.search_artifact_dir is not None:
        lines.append(f"search artifacts dir: {artifact_paths.search_artifact_dir}")
    lines.append("search query issues:")
    lines.extend(
        _format_search_execution_line(search_result)
        for search_result in result.report.search_results
        if search_result.status in {"provider_anomaly", "parse_failure", "fetch_failure"}
    )
    _append_guidance(
        lines,
        "next:",
        (
            _cli_example(
                f'discover "{result.report.query}" --capture-search-artifacts'
            ),
        ),
    )
    return "\n".join(lines)


def build_run_report_payload(
    paths: WorkspacePaths,
    result: RunWorkflowResult,
    *,
    show_portal_hints: bool = False,
) -> dict[str, object]:
    payload: dict[str, object] = {
        "command": "jobs_ai run",
        "database_path": str(paths.database_path),
        "query": result.query,
        "intake_mode": result.intake_mode,
        "workflow_dir": str(result.output_dir),
        "label": result.label,
        "confirmed_source_count": result.confirmed_source_count,
        "collected_source_count": len(result.collected_sources),
        "collect_limit": result.collect_limit,
        "imported_jobs_count": result.imported_jobs_count,
        "import_batch_id": result.import_result.batch_id if result.import_result is not None else (
            result.discover_run.report.run_id
            if result.discover_run is not None
            else (result.collect_run.report.run_id if result.collect_run is not None else result.output_dir.name)
        ),
        "manifest_path": str(result.manifest_path),
        "selected_jobs_count": result.session_result.selected_count,
        "launchable_jobs_count": result.session_result.plan.launchable_items,
        "recommendation_count": result.recommendation_count,
        "resolved_resume_count": result.session_result.resolved_resume_count,
        "portal_hint_count": result.portal_hint_count,
        "session_limit": result.session_limit,
        "open_requested": result.open_requested,
        "executor_mode": result.executor_mode,
        "selection_scope": _selection_scope_payload(result.session_result.selection_scope),
        "next_commands": list(
            _session_start_next_commands(
                manifest_path=result.manifest_path,
                opened_in_browser=_reports_include_opened(result.session_result.execution_reports),
                show_portal_hints=show_portal_hints,
            )
        ),
        "resume_variants": [
            _run_resume_payload_line(item)
            for item in _unique_run_resume_items(result)
        ],
    }
    if result.discover_run is not None:
        payload["discover_report_path"] = str(result.discover_run.report.artifact_paths.discover_report_path)
        payload["workday_manual_review_count"] = _discover_workday_manual_review_count(result.discover_run.report)
    if result.registry_collect_result is not None:
        payload["registry_verified_source_count"] = result.registry_verified_source_count
    if result.import_result is not None:
        payload["import_errors"] = list(result.import_result.errors)
    if show_portal_hints:
        payload["portal_hint_details"] = [
            _run_portal_payload_line(item)
            for item in result.session_result.items
            if item.portal_support is not None
        ]
    return payload


def render_run_json(
    paths: WorkspacePaths,
    result: RunWorkflowResult,
    *,
    show_portal_hints: bool = False,
) -> str:
    return json.dumps(
        build_run_report_payload(
            paths,
            result,
            show_portal_hints=show_portal_hints,
        ),
        indent=2,
        ensure_ascii=True,
    )


def render_preflight_report(manifest: SessionManifest) -> str:
    lines = [
        "jobs_ai preflight",
        f"manifest path: {manifest.manifest_path}",
        f"created_at: {manifest.created_at}",
        f"item count: {manifest.item_count}",
        f"warnings: {manifest.warning_count}",
        f"status: {'success with warnings' if manifest.warning_count else 'success'}",
    ]
    if manifest.label is not None:
        lines.insert(3, f"label: {manifest.label}")
    if manifest.selection_scope is not None:
        scope_lines = [f"selection scope: {_describe_selection_scope(manifest.selection_scope)}"]
        if manifest.selection_scope.source_query is not None:
            scope_lines.append(f"source query: {manifest.selection_scope.source_query}")
        scope_lines.extend(_selection_scope_detail_lines(manifest.selection_scope))
        lines[3:3] = scope_lines
    if not manifest.items:
        lines.append("preview: empty manifest")
        _append_guidance(lines, "next:", (_cli_example("export-session"),))
        return "\n".join(lines)

    for item in manifest.items:
        rank = item.rank if item.rank is not None else item.index
        lines.extend(
            [
                "",
                f"{rank}. {item.company or 'company missing'} | {item.title or 'title missing'}",
                f"   apply_url: {item.apply_url or 'apply_url missing'}",
                (
                    "   recommended resume variant: "
                    f"{_format_manifest_selection(item.recommended_resume_variant, fallback='resume variant missing')}"
                ),
                (
                    "   recommended profile snippet: "
                    f"{_format_manifest_selection(item.recommended_profile_snippet, fallback='profile snippet missing')}"
                ),
            ]
        )
        if item.warnings:
            lines.append(f"   warnings: {'; '.join(item.warnings)}")
    if manifest.warning_count:
        lines.append("tip: warnings are safe, but incomplete items will be skipped downstream")
    _append_guidance(lines, "next:", (_cli_example(f"launch-plan {manifest.manifest_path}"),))
    return "\n".join(lines)


def render_preflight_error_report(manifest_path: Path, error: str) -> str:
    lines = [
        "jobs_ai preflight",
        f"manifest path: {manifest_path}",
        "status: failed",
        f"error: {error}",
    ]
    _append_guidance(lines, "next:", (_cli_example("export-session"),))
    return "\n".join(lines)


def render_launch_plan_report(plan: LaunchPlan) -> str:
    lines = [
        "jobs_ai launch-plan",
        f"manifest path: {plan.manifest_path}",
        f"created_at: {plan.created_at}",
        f"total items: {plan.total_items}",
        f"launchable items: {plan.launchable_items}",
        f"skipped items: {plan.skipped_items}",
        f"status: {'success with skipped items' if plan.skipped_items else 'success'}",
    ]
    if plan.label is not None:
        lines.insert(3, f"label: {plan.label}")
    if plan.selection_scope is not None:
        lines.insert(3, f"selection scope: {_describe_selection_scope(plan.selection_scope)}")
        if plan.selection_scope.source_query is not None:
            lines.insert(4, f"source query: {plan.selection_scope.source_query}")
    if not plan.items:
        lines.append("plan: empty manifest")
        _append_guidance(lines, "next:", (_cli_example("export-session"),))
        return "\n".join(lines)

    for item in plan.items:
        lines.extend(
            [
                "",
                (
                    f"{item.manifest_index}. launch order "
                    f"{item.launch_order if item.launch_order is not None else 'skipped'} | "
                    f"{item.company or 'company missing'} | {item.title or 'title missing'}"
                ),
                f"   apply_url: {item.apply_url or 'apply_url missing'}",
                (
                    "   recommended resume variant: "
                    f"{_format_manifest_selection(item.recommended_resume_variant, fallback='resume variant missing')}"
                ),
                (
                    "   recommended profile snippet: "
                    f"{_format_manifest_selection(item.recommended_profile_snippet, fallback='profile snippet missing')}"
                ),
            ]
        )
        if item.skip_reasons:
            lines.append(f"   status: skipped ({'; '.join(item.skip_reasons)})")
    if plan.skipped_items:
        lines.append("tip: skipped items are left untouched and will not be launched")
    if plan.launchable_items:
        _append_guidance(
            lines,
            "next:",
            (
                _cli_example(f"application-assist {plan.manifest_path}"),
                _cli_example(f"launch-dry-run --confirm --executor browser_stub {plan.manifest_path}"),
            ),
        )
    else:
        _append_guidance(lines, "next:", (_cli_example(f"preflight {plan.manifest_path}"),))
    return "\n".join(lines)


def render_launch_plan_error_report(manifest_path: Path, error: str) -> str:
    lines = [
        "jobs_ai launch-plan",
        f"manifest path: {manifest_path}",
        "status: failed",
        f"error: {error}",
    ]
    _append_guidance(lines, "next:", (_cli_example(f"preflight {manifest_path}"),))
    return "\n".join(lines)


def render_launch_dry_run_report(reports: Sequence[LaunchExecutionReport]) -> str:
    ordered_reports = sorted(reports, key=_launch_dry_run_sort_key)
    if not ordered_reports:
        return "No launchable actions. Run python -m jobs_ai launch-plan <manifest_path> to inspect skipped items."

    return "\n\n".join(
        "\n".join(
            [
                (
                    f"[{report.launch_order}] "
                    f"{_format_launch_dry_run_text(report.company, fallback='<missing company>')} | "
                    f"{_format_launch_dry_run_text(report.title, fallback='<missing role title>')}"
                ),
                f"URL: {_format_launch_dry_run_url(report.apply_url)}",
                f"Executor: {report.executor_mode}",
                f"Action: {report.action_label}",
                f"Result: {_format_launch_execution_status(report.status)}",
            ]
        )
        for report in ordered_reports
    )


def render_launch_execution_summary(steps: Sequence[LaunchDryRunStep]) -> str:
    application_label = "application" if len(steps) == 1 else "applications"
    lines = [f"Launching {len(steps)} {application_label}:"]
    lines.extend(
        (
            f"[{step.launch_order}] "
            f"{_format_launch_dry_run_text(step.company, fallback='<missing company>')} | "
            f"{_format_launch_dry_run_text(step.title, fallback='<missing role title>')}"
        )
        for step in steps
    )
    return "\n".join(lines)


def render_application_assist_report(
    assist: ApplicationAssist,
    *,
    show_portal_hints: bool = False,
) -> str:
    if not assist.assist_items:
        lines = [
            "jobs_ai application-assist",
            f"manifest path: {assist.manifest_path}",
            "launchable items: 0",
            "status: no launchable application assists",
        ]
        _append_guidance(lines, "next:", (_cli_example(f"launch-plan {assist.manifest_path}"),))
        return "\n".join(lines)

    lines = [
        "jobs_ai application-assist",
        f"manifest path: {assist.manifest_path}",
        f"launchable items: {len(assist.assist_items)}",
        "status: success",
    ]
    if not show_portal_hints:
        lines.append("tip: rerun with --portal-hints for portal-specific guidance")

    body = "\n\n".join(
        "\n".join(
            [
                (
                    f"[{entry.launch_order}] "
                    f"{_format_launch_dry_run_text(entry.company, fallback='<missing company>')} | "
                    f"{_format_launch_dry_run_text(entry.title, fallback='<missing role title>')}"
                ),
                f"URL: {_format_launch_dry_run_url(entry.apply_url)}",
                (
                    "Resume: "
                    f"{_format_manifest_selection(entry.recommended_resume_variant, fallback='<missing>')}"
                ),
                (
                    "Snippet: "
                    f"{_format_manifest_selection(entry.recommended_profile_snippet, fallback='<missing>')}"
                ),
                (
                    "Text: "
                    f"{entry.recommended_profile_snippet.text or '<missing>'}"
                ),
                *(
                    _render_portal_support_lines(build_portal_support(entry.apply_url))
                    if show_portal_hints
                    else []
                ),
            ]
        )
        for entry in assist.assist_items
    )
    lines.extend(["", body])
    _append_guidance(
        lines,
        "next:",
        (_cli_example(f"launch-dry-run --confirm --executor browser_stub {assist.manifest_path}"),),
    )
    return "\n".join(lines)


def render_application_assist_error_report(manifest_path: Path, error: str) -> str:
    lines = [
        "jobs_ai application-assist",
        f"manifest path: {manifest_path}",
        "status: failed",
        f"error: {error}",
    ]
    _append_guidance(lines, "next:", (_cli_example(f"preflight {manifest_path}"),))
    return "\n".join(lines)


def render_application_prefill_report(
    result: ApplicationPrefillResult,
) -> str:
    lines = [
        "jobs_ai application-assist",
        f"manifest path: {result.manifest_path}",
        "mode: review-first prefill",
        f"launch order: {result.launch_order}",
        (
            "job: "
            f"{_format_launch_dry_run_text(result.company, fallback='<missing company>')} | "
            f"{_format_launch_dry_run_text(result.title, fallback='<missing role title>')}"
        ),
        f"portal: {result.portal_label}",
        f"portal support: {result.support_level}",
        f"browser backend: {result.browser_backend}",
        f"applicant profile: {result.applicant_profile_path}",
        f"original apply_url: {result.original_apply_url}",
        f"opened url: {result.opened_url}",
        f"recommended resume: {_format_manifest_selection(result.recommended_resume_variant, fallback='<missing>')}",
        (
            "recommended snippet: "
            f"{_format_manifest_selection(result.recommended_profile_snippet, fallback='<missing>')}"
        ),
        f"resume path: {result.resolved_resume_path or 'unresolved'}",
        f"page title: {result.page_title or '<unknown>'}",
        f"status: {result.status}",
        f"STOPPED BEFORE SUBMIT: {'yes' if result.stopped_before_submit else 'no'}",
    ]
    if result.filled_fields:
        lines.append("filled fields:")
        lines.extend(
            f"- {field.action_type} | {field.field_label} | {field.value}"
            for field in result.filled_fields
        )
    else:
        lines.append("filled fields: none")
    if result.skipped_fields:
        lines.append("skipped fields:")
        lines.extend(
            f"- {field.field_label}: {field.reason}"
            for field in result.skipped_fields
        )
    if result.unresolved_required_fields:
        lines.append("unresolved required fields:")
        lines.extend(f"- {field_label}" for field_label in result.unresolved_required_fields)
    else:
        lines.append("unresolved required fields: none")
    if result.submit_controls:
        lines.append("submit controls detected:")
        lines.extend(f"- {label}" for label in result.submit_controls)
    if result.notes:
        lines.append("notes:")
        lines.extend(f"- {note}" for note in result.notes)
    lines.append("next: review the browser page manually and click submit yourself if everything looks correct.")
    return "\n".join(lines)


def render_portal_hint_report(apply_url: str, portal_support: PortalSupport | None) -> str:
    lines = [
        "jobs_ai portal-hint",
        f"input apply_url: {apply_url}",
    ]
    if portal_support is None:
        lines.append("status: no supported portal helper available")
        lines.append(f"supported portals: {', '.join(SUPPORTED_PORTAL_TYPES)}")
        lines.append("tip: use --portal-type only when you already know the hosting portal")
        return "\n".join(lines)

    lines.extend(
        [
            f"portal type: {portal_support.portal_label}",
            f"normalized apply_url: {portal_support.normalized_apply_url}",
        ]
    )
    if portal_support.company_apply_url is not None:
        lines.append(f"company apply_url: {portal_support.company_apply_url}")
    lines.append("hints:")
    lines.extend(f"- {hint}" for hint in portal_support.hints)
    lines.append("status: supported")
    return "\n".join(lines)


def render_application_tracking_mark_report(
    paths: WorkspacePaths,
    snapshot: ApplicationStatusSnapshot,
) -> str:
    return "\n".join(
        [
            "jobs_ai track mark",
            f"database path: {paths.database_path}",
            f"job id: {snapshot.job_id}",
            f"job: {snapshot.company} | {snapshot.title} | {snapshot.location or 'location missing'}",
            f"recorded status: {snapshot.current_status}",
            f"timestamp: {snapshot.latest_timestamp or 'none'}",
            "status: success",
        ]
    )


def render_application_tracking_list_report(
    paths: WorkspacePaths,
    snapshots: Sequence[ApplicationStatusSnapshot],
    *,
    status_filter: str | None = None,
) -> str:
    lines = [
        "jobs_ai track list",
        f"database path: {paths.database_path}",
        f"jobs listed: {len(snapshots)}",
        f"filter status: {status_filter if status_filter is not None else 'none'}",
    ]
    if not snapshots:
        lines.append("status: no jobs found")
        return "\n".join(lines)

    lines.append("status: success")
    for index, snapshot in enumerate(snapshots, start=1):
        lines.extend(
            [
                "",
                (
                    f"{index}. [job {snapshot.job_id}] {snapshot.company} | "
                    f"{snapshot.title} | {snapshot.location or 'location missing'}"
                ),
                f"   current status: {snapshot.current_status}",
                f"   latest timestamp: {snapshot.latest_timestamp or 'none'}",
            ]
        )
    return "\n".join(lines)


def render_session_mark_report(
    paths: WorkspacePaths,
    result: SessionMarkResult,
) -> str:
    status = "success"
    if result.skipped:
        status = "success with skips" if result.updated else "failed"
    elif not result.updated:
        status = "failed"

    lines = [
        "jobs_ai session mark",
        f"database path: {paths.database_path}",
        f"requested status: {result.requested_status}",
        f"target source: {result.source_label}",
        f"target scope: {result.scope_label}",
        f"updated jobs: {len(result.updated)}",
        f"skipped targets: {len(result.skipped)}",
        f"status: {status}",
    ]
    if result.manifest_path is not None:
        lines.insert(2, f"manifest path: {result.manifest_path}")
    if result.manifest_item_count is not None:
        lines.append(f"manifest items: {result.manifest_item_count}")
    if result.manifest_launchable_count is not None:
        lines.append(f"manifest launchable items: {result.manifest_launchable_count}")

    if result.updated:
        lines.append("updated:")
        lines.extend(
            (
                f"- [job {snapshot.job_id}] {snapshot.company} | "
                f"{snapshot.title} | {snapshot.location or 'location missing'}"
            )
            for snapshot in result.updated
        )

    if result.skipped:
        lines.append("skipped:")
        lines.extend(f"- {issue.target}: {issue.reason}" for issue in result.skipped)

    _append_guidance(
        lines,
        "next:",
        (_cli_example(f"track list --status {result.requested_status}"),),
    )
    return "\n".join(lines)


def render_application_tracking_status_report(
    paths: WorkspacePaths,
    detail: ApplicationStatusDetail,
) -> str:
    lines = [
        "jobs_ai track status",
        f"database path: {paths.database_path}",
        f"job id: {detail.snapshot.job_id}",
        (
            f"job: {detail.snapshot.company} | {detail.snapshot.title} | "
            f"{detail.snapshot.location or 'location missing'}"
        ),
        f"current status: {detail.snapshot.current_status}",
        f"latest timestamp: {detail.snapshot.latest_timestamp or 'none'}",
        f"tracking entries: {len(detail.history)}",
    ]
    if not detail.history:
        lines.append("history: no tracked updates yet")
        return "\n".join(lines)

    lines.append("history:")
    lines.extend(f"- {entry.timestamp} | {entry.status}" for entry in detail.history)
    return "\n".join(lines)


def render_application_tracking_error_report(
    command_name: str,
    paths: WorkspacePaths,
    error: str,
) -> str:
    return "\n".join(
        [
            f"jobs_ai track {command_name}",
            f"database path: {paths.database_path}",
            "status: failed",
            f"error: {error}",
        ]
    )


def render_session_mark_error_report(
    paths: WorkspacePaths,
    error: str,
    *,
    manifest_path: Path | None = None,
) -> str:
    lines = [
        "jobs_ai session mark",
        f"database path: {paths.database_path}",
        "status: failed",
        f"error: {error}",
    ]
    if manifest_path is not None:
        lines.insert(2, f"manifest path: {manifest_path}")
    return "\n".join(lines)


def render_launch_dry_run_error_report(manifest_path: Path, error: str) -> str:
    lines = [
        "jobs_ai launch-dry-run",
        f"manifest path: {manifest_path}",
        "status: failed",
        f"error: {error}",
    ]
    _append_guidance(lines, "next:", (_cli_example(f"preflight {manifest_path}"),))
    return "\n".join(lines)


def _format_launch_execution_status(value: str) -> str:
    if value == "opened":
        return "opened in browser"
    if value == "noop":
        return "dry run only"
    if value == "skipped_missing_url":
        return "skipped (missing URL)"
    return value.replace("_", " ")


def _format_manifest_selection(selection: ManifestSelection | None, *, fallback: str) -> str:
    if selection is None:
        return fallback
    if selection.key and selection.label:
        return f"{selection.key} ({selection.label})"
    if selection.key:
        return selection.key
    if selection.label:
        return selection.label
    return fallback


def _count_launch_execution_status(
    reports: Sequence[LaunchExecutionReport],
    status: str,
) -> int:
    return sum(1 for report in reports if report.status == status)


def _reports_include_opened(reports: Sequence[LaunchExecutionReport]) -> bool:
    return any(report.status == "opened" for report in reports)


def _format_resume_file_line(
    resolved_resume_path: Path | None,
    fallback_reason: str | None,
) -> str:
    if resolved_resume_path is not None:
        return f"   resume file: {resolved_resume_path}"
    if fallback_reason is not None:
        return f"   resume file: unresolved ({fallback_reason})"
    return "   resume file: unresolved"


def _unique_run_resume_items(result: RunWorkflowResult) -> tuple[tuple[str, str, Path | None, str | None], ...]:
    seen_keys: set[str] = set()
    ordered_items: list[tuple[str, str, Path | None, str | None]] = []
    for item in result.session_result.items:
        key = item.preview.resume_variant_key
        if key in seen_keys:
            continue
        seen_keys.add(key)
        ordered_items.append(
            (
                key,
                item.preview.resume_variant_label,
                item.resolved_resume_path,
                item.resume_fallback_reason,
            )
        )
    return tuple(ordered_items)


def _render_run_resume_lines(result: RunWorkflowResult) -> list[str]:
    lines: list[str] = []
    for key, label, resolved_resume_path, fallback_reason in _unique_run_resume_items(result):
        if resolved_resume_path is not None:
            lines.append(f"- {key} ({label}): {resolved_resume_path}")
        elif fallback_reason is not None:
            lines.append(f"- {key} ({label}): unresolved | {fallback_reason}")
        else:
            lines.append(f"- {key} ({label}): unresolved")
    return lines


def _run_resume_payload_line(
    item: tuple[str, str, Path | None, str | None],
) -> dict[str, str | None]:
    key, label, resolved_resume_path, fallback_reason = item
    return {
        "variant_key": key,
        "variant_label": label,
        "resolved_resume_path": str(resolved_resume_path) if resolved_resume_path is not None else None,
        "fallback_reason": fallback_reason,
    }


def _render_run_portal_lines(result: RunWorkflowResult) -> list[str]:
    return [
        _run_portal_line(item)
        for item in result.session_result.items
        if item.portal_support is not None
    ]


def _run_portal_line(item) -> str:
    portal_support = item.portal_support
    assert portal_support is not None
    launch_url = portal_support.company_apply_url or portal_support.normalized_apply_url
    return (
        f"- [job {item.preview.job_id}] {portal_support.portal_label} | "
        f"{launch_url} | {portal_support.hints[0]}"
    )


def _run_portal_payload_line(item) -> dict[str, object]:
    portal_support = item.portal_support
    assert portal_support is not None
    return {
        "job_id": item.preview.job_id,
        "portal_label": portal_support.portal_label,
        "launch_url": portal_support.company_apply_url or portal_support.normalized_apply_url,
        "hints": list(portal_support.hints),
    }


def _import_next_commands(result: JobImportResult) -> tuple[str, ...]:
    commands = []
    if result.batch_id is not None:
        commands.append(_cli_example(f"session start --batch-id {result.batch_id} --limit 25"))
    commands.append(_cli_example("score"))
    return tuple(commands)


def _import_summary_next_commands(import_summary) -> tuple[str, ...]:
    if import_summary.batch_id is not None:
        return (_cli_example(f"session start --batch-id {import_summary.batch_id} --limit 25"),)
    return (_cli_example("session start --limit 25"),)


def _describe_selection_scope(selection_scope: SessionSelectionScope) -> str:
    if selection_scope.batch_id is not None:
        return f"batch {selection_scope.batch_id}"
    if selection_scope.source_query is not None:
        return f'query "{selection_scope.source_query}" across global new-job pool'
    return "global new-job pool"


def _selection_scope_payload(selection_scope: SessionSelectionScope | None) -> dict[str, str | None] | None:
    if selection_scope is None:
        return None
    return {
        "batch_id": selection_scope.batch_id,
        "source_query": selection_scope.source_query,
        "import_source": selection_scope.import_source,
        "selection_mode": selection_scope.selection_mode,
        "refresh_batch_id": selection_scope.refresh_batch_id,
    }


def _selection_scope_detail_lines(selection_scope: SessionSelectionScope) -> tuple[str, ...]:
    lines: list[str] = []
    selection_mode_detail = _selection_mode_detail(selection_scope.selection_mode)
    if selection_mode_detail is not None:
        lines.append(f"selection source: {selection_mode_detail}")
    if (
        selection_scope.refresh_batch_id is not None
        and selection_scope.refresh_batch_id != selection_scope.batch_id
    ):
        lines.append(f"refresh batch id: {selection_scope.refresh_batch_id}")
    return tuple(lines)


def _selection_mode_detail(selection_mode: str | None) -> str | None:
    if selection_mode == "fast_apply":
        return "fast-apply shortlist from likely resume-matching launchable jobs"
    if selection_mode == "registry_new_imports":
        return "newly imported jobs from this registry refresh"
    if selection_mode == "registry_refresh_empty_reused_existing":
        return (
            "existing eligible jobs reused from prior imports because this registry "
            "refresh imported 0 new jobs"
        )
    return None


def _session_start_next_commands(
    *,
    manifest_path: Path,
    opened_in_browser: bool,
    show_portal_hints: bool,
) -> tuple[str, ...]:
    application_assist_command = _cli_example(
        f"application-assist{' --portal-hints' if show_portal_hints else ''} {manifest_path}"
    )
    if opened_in_browser:
        return (
            application_assist_command,
            _cli_example(f"session mark opened --manifest {manifest_path} --all"),
            _cli_example("track list"),
        )
    return (
        _cli_example(f"preflight {manifest_path}"),
        application_assist_command,
        _cli_example(f"launch-dry-run --executor browser_stub {manifest_path}"),
        _cli_example("track list"),
    )


def _launch_dry_run_sort_key(report: LaunchExecutionReport) -> tuple[int, str, str, str, str, str]:
    return (
        report.launch_order,
        _format_launch_dry_run_text(report.company, fallback="<missing company>"),
        _format_launch_dry_run_text(report.title, fallback="<missing role title>"),
        _format_launch_dry_run_url(report.apply_url),
        report.executor_mode,
        report.action_label,
    )


def _render_portal_support_lines(portal_support: PortalSupport | None) -> list[str]:
    if portal_support is None:
        return []

    lines = [f"   portal: {portal_support.portal_label}"]
    if portal_support.company_apply_url is not None:
        lines.append(f"   company apply_url: {portal_support.company_apply_url}")
    elif portal_support.normalized_apply_url != portal_support.original_apply_url:
        lines.append(f"   normalized apply_url: {portal_support.normalized_apply_url}")
    lines.append(f"   portal hints: {'; '.join(portal_support.hints)}")
    return lines


def _format_launch_dry_run_text(value: str | None, *, fallback: str) -> str:
    if value is None:
        return fallback
    text = value.strip()
    return text or fallback


def _format_launch_dry_run_url(value: str | None) -> str:
    if value is None:
        return "<missing>"
    text = value.strip()
    return text or "<missing>"
