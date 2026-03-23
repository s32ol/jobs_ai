from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
import sqlite3

import click
import typer

from .collect.census import run_board_census_command
from .collect.cli import run_collect_command
from .discover.cli import run_discover_command
from .source_seed.cli import run_seed_sources_command
from .application_assist import build_application_assist
from .application_log import (
    APPLICATION_LOG_STATUSES,
    normalize_application_log_status,
    write_application_log,
)
from .application_prefill import ApplicationPrefillResult, run_application_prefill
from .application_tracking import (
    APPLICATION_STATUSES,
    SESSION_MARK_APPLICATION_STATUSES,
    get_application_status,
    list_application_statuses,
    record_application_status,
)
from .config import load_settings
from .db import initialize_schema, missing_required_tables
from .db_merge import merge_sqlite_databases
from .db_postgres import build_backend_status, migrate_sqlite_to_postgres, ping_database_target
from .db_runtime import database_exists, resolve_database_runtime
from .maintenance import backfill_jobs_metadata
from .jobs.importer import JobImportResult, import_jobs_from_file
from .jobs.fast_apply import (
    DEFAULT_FAST_APPLY_LIMIT,
    parse_fast_apply_families,
    select_fast_apply_selections,
)
from .launch_dry_run import LaunchDryRun, LaunchDryRunStep, build_launch_dry_run
from .jobs.queue import select_apply_queue
from .jobs.scoring import score_jobs_from_database
from .launch_executor import (
    BROWSER_STUB_EXECUTOR_MODE,
    collect_launch_execution_reports_for_steps,
    NO_OP_EXECUTOR_MODE,
    select_launch_executor,
    SUPPORTED_EXECUTOR_MODES,
)
from .launch_preview import select_launch_preview
from .launch_plan import build_launch_plan
from .main import (
    APPLY_DEFAULT_LIMIT,
    APPLY_HARD_MAX_LIMIT,
    render_apply_error_report,
    render_apply_report,
    render_application_assist_error_report,
    render_application_log_error_report,
    render_application_log_report,
    render_application_prefill_report,
    render_application_assist_report,
    render_board_census_error_report,
    render_board_census_report,
    render_collect_error_report,
    render_collect_report,
    render_discover_error_report,
    render_discover_report,
    render_application_tracking_error_report,
    render_application_tracking_list_report,
    render_application_tracking_mark_report,
    render_application_tracking_status_report,
    render_run_discover_failure_report,
    render_run_error_report,
    render_run_json,
    render_run_report,
    render_db_init_report,
    render_db_backend_status_report,
    render_db_merge_error_report,
    render_db_merge_report,
    render_db_migrate_to_postgres_error_report,
    render_db_migrate_to_postgres_report,
    render_db_ping_report,
    render_db_status_report,
    render_doctor_report,
    render_maintenance_backfill_error_report,
    render_maintenance_backfill_report,
    render_launch_dry_run_report,
    render_launch_dry_run_error_report,
    render_launch_execution_summary,
    render_launch_plan_error_report,
    render_launch_plan_report,
    render_open_error_report,
    render_open_prompt,
    render_open_unchanged_report,
    render_export_session_report,
    render_fast_apply_report,
    render_fast_apply_error_report,
    render_import_report,
    render_init_report,
    render_launch_preview_report,
    render_portal_hint_report,
    render_preflight_error_report,
    render_preflight_report,
    render_queue_report,
    render_recommendation_report,
    render_session_mark_error_report,
    render_session_mark_report,
    render_session_inspect_error_report,
    render_session_inspect_report,
    render_session_recent_report,
    render_session_reopen_error_report,
    render_session_reopen_report,
    render_session_start_error_report,
    render_session_start_report,
    render_seed_sources_error_report,
    render_seed_sources_report,
    render_source_registry_add_report,
    render_source_registry_collect_report,
    render_source_registry_discover_ats_report,
    render_source_registry_detect_sites_report,
    render_source_registry_deactivate_report,
    render_source_registry_expand_report,
    render_source_registry_extract_jobposting_report,
    render_source_registry_harvest_companies_report,
    render_source_registry_import_report,
    render_source_registry_list_report,
    render_source_registry_seed_bulk_report,
    render_source_registry_sync_report,
    render_source_registry_verify_report,
    render_score_report,
    render_stats_json,
    render_stats_report,
    render_status_report,
    run_apply_workflow,
)
from .session_open import open_manifest_item
from .prefill_browser import SUPPORTED_PREFILL_BROWSER_BACKENDS, create_prefill_browser_backend
from .portal_support import build_portal_support
from .run_workflow import DEFAULT_RUN_DISCOVER_LIMIT, DiscoverSearchWorkflowError, run_operator_workflow
from .sources.detect_sites import (
    detect_registry_sources_from_sites,
    detect_sites_starter_help_text,
)
from .sources.jobposting_parser import (
    DEFAULT_JOBPOSTING_MAX_REQUESTS_PER_SECOND,
    DEFAULT_JOBPOSTING_TIMEOUT_SECONDS,
    extract_jobposting_sources,
)
from .sources.company_harvester import (
    DEFAULT_COMPANY_HARVEST_MAX_REQUESTS_PER_SECOND,
    DEFAULT_COMPANY_HARVEST_TIMEOUT_SECONDS,
    company_harvest_sources_help_text,
    harvest_companies_from_sources,
)
from .sources.expand_registry import expand_registry_sources
from .sources.discover_ats import (
    DEFAULT_DISCOVER_ATS_LIMIT,
    DEFAULT_DISCOVER_ATS_TIMEOUT_SECONDS,
    SUPPORTED_DISCOVER_ATS_PROVIDERS,
    discover_registry_ats_sources,
)
from .sources.registry import (
    SOURCE_REGISTRY_STATUSES,
    deactivate_registry_source,
    import_registry_sources,
    list_registry_sources,
    register_source,
    register_verified_source,
    verify_registry_sources,
)
from .sources.seeding import (
    seed_bulk_starter_help_text,
    seed_registry_bulk,
)
from .sources.workflow import collect_registry_sources
from .session_history import (
    DEFAULT_SESSION_RECENT_LIMIT,
    inspect_session,
    recent_sessions,
    reopen_session,
)
from .session_mark import mark_session_jobs
from .session_manifest import load_session_manifest
from .resume.recommendations import select_queue_recommendations
from .stats import gather_operator_stats
from .session_export import export_launch_preview_session
from .session_start import (
    DEFAULT_SESSION_START_LIMIT,
    resolve_selection_scope,
    start_session,
    start_session_from_previews,
)
from .workspace import build_workspace_paths, ensure_workspace, missing_workspace_paths


app = typer.Typer(
    add_completion=False,
    help=(
        "Local CLI for the jobs_ai job-search exoskeleton.\n\n"
        "Preferred daily flow: run or fast-apply -> open -> session mark.\n\n"
        "Preferred modular flow: discover --collect --import -> session start -> "
        "open -> session mark.\n\n"
        "Advanced manual building blocks: score -> queue -> recommend -> "
        "launch-preview -> apply -> open -> export-session -> preflight -> application-assist -> application-log -> "
        "launch-dry-run -> track -> stats -> maintenance."
    ),
)
db_app = typer.Typer(
    help=(
        "Manage SQLite and Postgres database backends.\n\n"
        "Use db backend-status/db ping to inspect the active backend, then db init or db migrate-to-postgres as needed."
    )
)
track_app = typer.Typer(
    help=(
        "Manage manual application tracking.\n\n"
        "Use track mark after you open, apply to, or reach a downstream outcome "
        "such as interview, rejected, or offer."
    )
)
session_app = typer.Typer(
    help=(
        "Freeze and start operator-ready application batches.\n\n"
        "Use jobs-ai run for the daily happy path, or use session start in the modular "
        "flow after discover/import. Use --batch-id when you want one explicit "
        "recent import or discover run. Use session recent/inspect/reopen for prior "
        "manifests, then use session mark to record outcomes with direct job ids or a manifest."
    )
)
maintenance_app = typer.Typer(
    help=(
        "Run small operator maintenance utilities.\n\n"
        "Use maintenance backfill to upgrade older rows without rewriting existing history."
    )
)
sources_app = typer.Typer(
    help=(
        "Manage the durable ATS source registry.\n\n"
        "Use sources harvest-companies/expand-registry/discover-ats/detect-sites/seed-bulk/add/import/verify to maintain confirmed board roots, "
        "use sources extract-jobposting for schema.org JobPosting pages, then use sources collect or run --use-registry for the daily registry-first flow."
    )
)
app.add_typer(db_app, name="db")
app.add_typer(track_app, name="track")
app.add_typer(session_app, name="session")
app.add_typer(maintenance_app, name="maintenance")
app.add_typer(sources_app, name="sources")


def _load_runtime():
    settings = load_settings()
    paths = build_workspace_paths(settings.database_path)
    return settings, paths


def _resolve_discover_query(
    query_argument: str | None,
    query_option: str | None,
) -> str:
    query_values = [
        value.strip()
        for value in (query_argument, query_option)
        if value is not None and value.strip()
    ]
    if not query_values:
        raise ValueError("provide a discover query as a positional argument or with --query")
    if len(dict.fromkeys(query_values)) > 1:
        raise ValueError("provide the discover query only once; positional QUERY and --query must match")
    return query_values[0]


def _select_launch_execution_steps(
    dry_run: LaunchDryRun,
    *,
    executor_mode: str,
    limit: int | None,
) -> tuple[LaunchDryRunStep, ...]:
    if executor_mode != BROWSER_STUB_EXECUTOR_MODE or limit is None:
        return dry_run.steps
    return dry_run.steps[:limit]


def _build_launch_confirmation_prompt(
    steps: Sequence[LaunchDryRunStep],
    *,
    executor_mode: str,
) -> str:
    tab_count = len(steps)
    tab_label = "application tab" if tab_count == 1 else "application tabs"
    return f"Open {tab_count} {tab_label} in {executor_mode} mode?"


_APPLICATION_ASSIST_PROMPT_LOG_STATUSES = ("applied", "skipped", "opened", "failed")


def _resolve_application_assist_log_options(
    *,
    prefill: bool,
    log_outcome: bool,
    log_status: str | None,
    log_notes: str | None,
) -> tuple[str | None, str | None, bool]:
    normalized_log_notes = log_notes.strip() or None if log_notes is not None else None
    logging_requested = log_outcome or log_status is not None or normalized_log_notes is not None
    if logging_requested and not prefill:
        raise ValueError("outcome logging options require --prefill")
    if log_outcome and log_status is not None:
        raise ValueError("use either --log-outcome or --log-status, not both")
    if normalized_log_notes is not None and log_status is None and not log_outcome:
        raise ValueError("--log-notes requires --log-status or --log-outcome")

    normalized_log_status = (
        normalize_application_log_status(log_status)
        if log_status is not None
        else None
    )
    return normalized_log_status, normalized_log_notes, log_outcome


def _prompt_application_assist_log_status() -> str | None:
    prompt_text = "Status? " f"[{'/'.join(_APPLICATION_ASSIST_PROMPT_LOG_STATUSES)}]"
    while True:
        response = click.prompt(
            prompt_text,
            default="",
            show_default=False,
        ).strip().lower()
        if not response:
            return None
        if response in _APPLICATION_ASSIST_PROMPT_LOG_STATUSES:
            return response
        typer.echo(
            "Invalid status. Choose one of: "
            f"{', '.join(_APPLICATION_ASSIST_PROMPT_LOG_STATUSES)}. "
            "Press Enter to skip."
        )


def _maybe_log_application_assist_outcome(
    project_root: Path,
    *,
    result: ApplicationPrefillResult,
    prompt_for_outcome: bool,
    log_status: str | None,
    log_notes: str | None,
) -> None:
    if log_status is None and not prompt_for_outcome:
        return

    resolved_log_status = log_status
    resolved_log_notes = log_notes
    if resolved_log_status is None:
        resolved_log_status = _prompt_application_assist_log_status()
        if resolved_log_status is None:
            typer.echo("Outcome log skipped.")
            return
        resolved_log_notes = (
            click.prompt(
                "Notes? (optional, press Enter to skip)",
                default=resolved_log_notes or "",
                show_default=False,
            ).strip()
            or None
        )

    try:
        log_result = write_application_log(
            project_root,
            company=result.company,
            role=result.title,
            portal=result.portal_type,
            apply_url=result.original_apply_url,
            status=resolved_log_status,
            notes=resolved_log_notes,
            manifest_path=result.manifest_path,
            launch_order=result.launch_order,
        )
    except ValueError as exc:
        typer.echo("Outcome logging failed after application-assist completed successfully.")
        typer.echo(
            render_application_log_error_report(
                str(exc),
                manifest_path=result.manifest_path,
            )
        )
        return

    typer.echo(render_application_log_report(log_result))


@app.callback(invoke_without_command=True)
def entrypoint(ctx: typer.Context) -> None:
    if ctx.invoked_subcommand is not None:
        return
    settings, paths = _load_runtime()
    typer.echo(render_status_report(settings, paths))


@app.command("run")
def run_command(
    query: str = typer.Argument(
        ...,
        help='Search query such as "python backend engineer remote".',
    ),
    limit: int = typer.Option(
        DEFAULT_SESSION_START_LIMIT,
        "--limit",
        "--session-limit",
        min=1,
        help="Maximum number of ranked new jobs to freeze into the session manifest.",
    ),
    discover_limit: int = typer.Option(
        DEFAULT_RUN_DISCOVER_LIMIT,
        "--discover-limit",
        min=1,
        help="Maximum number of unique ATS source candidates to verify during discovery.",
    ),
    collect_limit: int | None = typer.Option(
        None,
        "--collect-limit",
        min=1,
        help="Optional cap on confirmed sources to collect/import after discovery.",
    ),
    portal_hints: bool = typer.Option(
        False,
        "--portal-hints",
        help="Show portal detection details for launchable jobs when available.",
    ),
    open_: bool = typer.Option(
        False,
        "--open/--no-open",
        help="Reuse the current safe browser-open behavior after freezing the session.",
    ),
    executor: str | None = typer.Option(
        None,
        "--executor",
        help=(
            "Launch executor mode for --open. Allowed values: "
            f"{', '.join(SUPPORTED_EXECUTOR_MODES)}."
        ),
    ),
    label: str | None = typer.Option(
        None,
        "--label",
        help="Optional short label to reuse across discover, collect, and session artifacts.",
    ),
    out_dir: Path | None = typer.Option(
        None,
        "--out-dir",
        help="Optional workflow output directory for discover artifacts, collect artifacts, and the manifest.",
    ),
    capture_search_artifacts: bool = typer.Option(
        False,
        "--capture-search-artifacts",
        help="Save raw search HTML for anomalous or parse-failed search responses.",
    ),
    use_registry: bool = typer.Option(
        False,
        "--use-registry",
        help=(
            "Use the source registry as the primary intake path: collect/import from active "
            "registry sources, then rank and freeze a session from jobs matching the query."
        ),
    ),
    json_output: bool = typer.Option(
        False,
        "--json",
        help="Print the final run summary as JSON.",
    ),
) -> None:
    """Preferred operator entrypoint: discover, collect/import, and start one ready-to-apply session."""
    settings, paths = _load_runtime()
    del settings
    ensure_workspace(paths)
    try:
        result = run_operator_workflow(
            paths,
            query=query,
            discover_limit=discover_limit,
            collect_limit=collect_limit,
            session_limit=limit,
            out_dir=out_dir,
            label=label,
            open_urls=open_,
            executor_mode=executor,
            capture_search_artifacts=capture_search_artifacts,
            use_registry=use_registry,
        )
    except DiscoverSearchWorkflowError as exc:
        typer.echo(render_run_discover_failure_report(paths, exc.discover_run))
        raise typer.Exit(code=1)
    except ValueError as exc:
        typer.echo(render_run_error_report(paths, str(exc)))
        raise typer.Exit(code=1)

    typer.echo(
        render_run_json(
            paths,
            result,
            show_portal_hints=portal_hints,
        )
        if json_output
        else render_run_report(
            paths,
            result,
            show_portal_hints=portal_hints,
        )
    )
    if result.import_result is not None and result.import_result.errors:
        raise typer.Exit(code=1)


@app.command()
def apply(
    query: str = typer.Argument(
        ...,
        help='Search query such as "python backend engineer".',
    ),
    limit: int = typer.Option(
        APPLY_DEFAULT_LIMIT,
        "--limit",
        min=1,
        help=(
            "Maximum number of launchable matching jobs to open. "
            f"Default {APPLY_DEFAULT_LIMIT}; hard max {APPLY_HARD_MAX_LIMIT}."
        ),
    ),
    print_only: bool = typer.Option(
        False,
        "--print-only",
        help="Print matching launchable jobs and apply URLs without opening a browser.",
    ),
) -> None:
    """Open apply URLs for the current ranked query matches without exporting a session manifest."""
    settings, paths = _load_runtime()
    del settings
    ensure_workspace(paths)
    initialize_schema(paths.database_path)
    try:
        result = run_apply_workflow(
            paths.database_path,
            query=query,
            limit=limit,
            print_only=print_only,
        )
    except ValueError as exc:
        typer.echo(render_apply_error_report(paths, str(exc)))
        raise typer.Exit(code=1)
    typer.echo(render_apply_report(paths, result))


@app.command("fast-apply")
def fast_apply(
    limit: int = typer.Option(
        DEFAULT_FAST_APPLY_LIMIT,
        "--limit",
        min=1,
        help="Maximum number of likely-fit launchable jobs to shortlist.",
    ),
    batch_id: str | None = typer.Option(
        None,
        "--batch-id",
        "--run-id",
        help="Optional import/discover batch id to scope fast apply to one recent run.",
    ),
    query: str | None = typer.Option(
        None,
        "--query",
        help="Optional text query to narrow the eligible new-job pool before fast-apply heuristics.",
    ),
    families: str | None = typer.Option(
        None,
        "--families",
        help="Optional preferred families, comma-separated: data,backend,software.",
    ),
    remote_only: bool = typer.Option(
        False,
        "--remote-only",
        help="Only shortlist jobs whose location explicitly looks remote.",
    ),
    easy_apply_first: bool = typer.Option(
        False,
        "--easy-apply-first",
        help="Prefer supported hosted portals such as Greenhouse, Lever, and Ashby.",
    ),
    portal_hints: bool = typer.Option(
        False,
        "--portal-hints",
        help="Show portal detection details for shortlisted jobs when available.",
    ),
    open_: bool = typer.Option(
        False,
        "--open",
        help="After exporting the shortlist manifest, reuse the current dry-run launch behavior.",
    ),
    executor: str | None = typer.Option(
        None,
        "--executor",
        help=(
            "Launch executor mode for --open. Allowed values: "
            f"{', '.join(SUPPORTED_EXECUTOR_MODES)}."
        ),
    ),
) -> None:
    """Build a compact likely-fit shortlist from the existing collected job pool."""
    settings, paths = _load_runtime()
    del settings
    ensure_workspace(paths)
    initialize_schema(paths.database_path)
    normalized_query = query.strip() if query is not None and query.strip() else None
    try:
        requested_families = parse_fast_apply_families(families)
        selections = select_fast_apply_selections(
            paths.database_path,
            limit=limit,
            ingest_batch_id=batch_id,
            query_text=normalized_query,
            families=requested_families,
            remote_only=remote_only,
            easy_apply_first=easy_apply_first,
        )
        selection_scope = resolve_selection_scope(
            paths.database_path,
            ingest_batch_id=batch_id,
            source_query=normalized_query,
            selection_mode="fast_apply",
        )
        result = start_session_from_previews(
            paths.database_path,
            project_root=paths.project_root,
            default_exports_dir=paths.exports_dir,
            previews=tuple(selection.preview for selection in selections),
            limit=limit,
            open_urls=open_,
            executor_mode=executor,
            selection_scope=selection_scope,
        )
    except ValueError as exc:
        typer.echo(render_fast_apply_error_report(paths, str(exc)))
        raise typer.Exit(code=1)

    typer.echo(
        render_fast_apply_report(
            paths,
            result,
            selections,
            limit=limit,
            query_text=normalized_query,
            families=requested_families,
            remote_only=remote_only,
            easy_apply_first=easy_apply_first,
            show_portal_hints=portal_hints,
        )
    )


@app.command("open")
def open_command(
    manifest: Path = typer.Option(
        ...,
        "--manifest",
        help="Path to any JSON session manifest created by jobs-ai.",
    ),
    index: int = typer.Option(
        ...,
        "--index",
        help="1-based manifest index to open.",
    ),
    executor: str = typer.Option(
        BROWSER_STUB_EXECUTOR_MODE,
        "--executor",
        help=(
            "Launch executor mode for opening one manifest item. Allowed values: "
            f"{', '.join(SUPPORTED_EXECUTOR_MODES)}."
        ),
    ),
) -> None:
    """Open one manifest item's apply URL, then optionally record applied/skipped."""
    settings, paths = _load_runtime()
    del settings
    ensure_workspace(paths)
    initialize_schema(paths.database_path)
    try:
        open_result = open_manifest_item(
            manifest,
            index=index,
            executor_mode=executor,
        )
    except ValueError as exc:
        typer.echo(render_open_error_report(paths, manifest, str(exc)))
        raise typer.Exit(code=1)

    typer.echo(render_open_prompt(open_result))
    response = click.prompt("Choice", default="", show_default=False).strip().lower()
    status = {"y": "applied", "s": "skipped"}.get(response)
    if status is None:
        typer.echo(render_open_unchanged_report(open_result))
        return

    mark_result = mark_session_jobs(
        paths.database_path,
        status=status,
        job_ids=(),
        manifest_path=open_result.manifest_path,
        indexes=(open_result.selected_item.index,),
    )
    typer.echo(render_session_mark_report(paths, mark_result))
    if not mark_result.updated:
        raise typer.Exit(code=1)


@app.command()
def status() -> None:
    """Show the local control tower status."""
    settings, paths = _load_runtime()
    typer.echo(render_status_report(settings, paths))


@app.command()
def init() -> None:
    """Create the local folders used by upcoming milestones."""
    settings, paths = _load_runtime()
    del settings
    created_paths = ensure_workspace(paths)
    typer.echo(render_init_report(paths, created_paths))


@app.command()
def doctor() -> None:
    """Validate that the local workspace folders exist."""
    settings, paths = _load_runtime()
    del settings
    missing_paths = missing_workspace_paths(paths)
    typer.echo(render_doctor_report(paths, missing_paths))
    if missing_paths:
        raise typer.Exit(code=1)


@app.command()
def discover(
    query_argument: str | None = typer.Argument(
        None,
        help="Search query such as 'python backend engineer remote'.",
    ),
    query: str | None = typer.Option(
        None,
        "--query",
        help="Search query such as 'python backend engineer remote'.",
    ),
    limit: int = typer.Option(
        20,
        "--limit",
        min=1,
        help="Maximum number of unique ATS source candidates to verify.",
    ),
    out_dir: Path | None = typer.Option(
        None,
        "--out-dir",
        help=(
            "Optional output directory for discover_report.json, confirmed_sources.txt, "
            "and manual_review_sources.json."
        ),
    ),
    label: str | None = typer.Option(
        None,
        "--label",
        help="Optional short label to include in the default run id and output directory name.",
    ),
    timeout: float = typer.Option(
        10.0,
        "--timeout",
        min=0.1,
        help="Per-request timeout in seconds for search and ATS verification fetches.",
    ),
    report_only: bool = typer.Option(
        False,
        "--report-only",
        help="Write only discover_report.json and skip confirmed_sources.txt plus manual_review_sources.json.",
    ),
    collect: bool = typer.Option(
        False,
        "--collect",
        help="After discovery, run collect on the confirmed sources in the same output bundle.",
    ),
    import_results: bool = typer.Option(
        False,
        "--import",
        help="After discovery, continue through collect and import the resulting leads into the database.",
    ),
    capture_search_artifacts: bool = typer.Option(
        False,
        "--capture-search-artifacts",
        help="Save raw search HTML for anomalous or parse-failed search responses.",
    ),
    add_to_registry: bool = typer.Option(
        False,
        "--add-to-registry",
        help="Add confirmed ATS roots from this discover run into the durable source registry.",
    ),
) -> None:
    """Modular upstream path: search ATS portals, confirm supported sources, and surface Workday for manual review."""
    settings, paths = _load_runtime()
    del settings
    ensure_workspace(paths)
    try:
        resolved_query = _resolve_discover_query(query_argument, query)
        run = run_discover_command(
            paths,
            query=resolved_query,
            limit=limit,
            out_dir=out_dir,
            label=label,
            timeout_seconds=timeout,
            report_only=report_only,
            collect=collect,
            import_results=import_results,
            capture_search_artifacts=capture_search_artifacts,
        )
    except ValueError as exc:
        typer.echo(render_discover_error_report(str(exc)))
        raise typer.Exit(code=1)
    typer.echo(render_discover_report(run.report))
    if add_to_registry and run.confirmed_sources:
        initialize_schema(paths.database_path)
        mutation_results = tuple(
            register_verified_source(
                paths.database_path,
                source_url=result.confirmed_source,
                portal_type=result.candidate.portal_type,
                provenance=f'discover query "{run.report.query}"',
                verification_reason_code="confirmed_via_discover",
                verification_reason=f'confirmed from discover query "{run.report.query}"',
            )
            for result in run.report.candidate_results
            if result.outcome == "confirmed" and result.confirmed_source is not None
        )
        typer.echo(
            render_source_registry_sync_report(
                "jobs_ai discover registry sync",
                mutation_results,
            )
        )
    if run.report.has_fatal_search_failure:
        raise typer.Exit(code=1)
    import_summary = run.report.import_summary
    if import_summary is not None and import_summary.executed and import_summary.errors:
        raise typer.Exit(code=1)


@app.command()
def collect(
    sources: list[str] = typer.Argument(
        None,
        help=(
            "One or more source URLs to collect. Supported Greenhouse, Lever, "
            "and Ashby pages are collected automatically."
        ),
    ),
    from_file: Path | None = typer.Option(
        None,
        "--from-file",
        exists=True,
        dir_okay=False,
        file_okay=True,
        readable=True,
        resolve_path=True,
        help=(
            "Optional text file containing one source URL per line. Blank lines "
            "and lines starting with # are ignored."
        ),
    ),
    out_dir: Path | None = typer.Option(
        None,
        "--out-dir",
        help=(
            "Optional output directory for run_report.json, leads.import.json, "
            "and manual_review.json."
        ),
    ),
    label: str | None = typer.Option(
        None,
        "--label",
        help="Optional short label to include in the default run id and output directory name.",
    ),
    timeout: float = typer.Option(
        10.0,
        "--timeout",
        min=0.1,
        help="Per-source fetch timeout in seconds.",
    ),
    report_only: bool = typer.Option(
        False,
        "--report-only",
        help="Write only run_report.json and skip leads.import.json plus manual_review.json.",
    ),
) -> None:
    """Low-level/manual source collection step for confirmed ATS URLs."""
    settings, paths = _load_runtime()
    del settings
    ensure_workspace(paths)
    try:
        run = run_collect_command(
            paths,
            sources=sources or (),
            from_file=from_file,
            out_dir=out_dir,
            label=label,
            timeout_seconds=timeout,
            report_only=report_only,
        )
    except ValueError as exc:
        typer.echo(render_collect_error_report(str(exc)))
        raise typer.Exit(code=1)
    typer.echo(render_collect_report(run.report))


@app.command("board-census")
def board_census(
    from_file: Path = typer.Option(
        ...,
        "--from-file",
        exists=True,
        dir_okay=False,
        file_okay=True,
        readable=True,
        resolve_path=True,
        help=(
            "Text file containing one Greenhouse, Lever, or Ashby board URL per line. "
            "Blank lines and lines starting with # are ignored."
        ),
    ),
    out_dir: Path | None = typer.Option(
        None,
        "--out-dir",
        help="Optional output directory for board_census.json and board_census.csv.",
    ),
    label: str | None = typer.Option(
        None,
        "--label",
        help="Optional short label to include in the default run id and output directory name.",
    ),
    timeout: float = typer.Option(
        10.0,
        "--timeout",
        min=0.1,
        help="Per-board fetch timeout in seconds.",
    ),
) -> None:
    """Fetch each unique supported board root once and report live posting counts."""
    settings, paths = _load_runtime()
    del settings
    ensure_workspace(paths)
    try:
        run = run_board_census_command(
            paths,
            from_file=from_file,
            out_dir=out_dir,
            label=label,
            timeout_seconds=timeout,
        )
    except ValueError as exc:
        typer.echo(render_board_census_error_report(str(exc)))
        raise typer.Exit(code=1)
    typer.echo(render_board_census_report(run))


@app.command("seed-sources")
def seed_sources(
    companies: list[str] = typer.Argument(
        None,
        help=(
            "Optional company seed entries. Use plain company names or "
            "'Company Name | company.com | notes'."
        ),
    ),
    from_file: Path | None = typer.Option(
        None,
        "--from-file",
        exists=True,
        dir_okay=False,
        file_okay=True,
        readable=True,
        resolve_path=True,
        help=(
            "Optional text file containing one company seed entry per line. "
            "Blank lines and lines starting with # are ignored."
        ),
    ),
    out_dir: Path | None = typer.Option(
        None,
        "--out-dir",
        help=(
            "Optional output directory for seed_report.json, confirmed_sources.txt, "
            "and manual_review_sources.json."
        ),
    ),
    label: str | None = typer.Option(
        None,
        "--label",
        help="Optional short label to include in the default run id and output directory name.",
    ),
    timeout: float = typer.Option(
        10.0,
        "--timeout",
        min=0.1,
        help="Per-candidate fetch timeout in seconds.",
    ),
    report_only: bool = typer.Option(
        False,
        "--report-only",
        help="Write only seed_report.json and skip confirmed_sources.txt plus manual_review_sources.json.",
    ),
    add_to_registry: bool = typer.Option(
        False,
        "--add-to-registry",
        help="Add confirmed ATS roots from this seed run into the durable source registry.",
    ),
) -> None:
    """Infer and verify reusable ATS board-root sources from company inputs."""
    settings, paths = _load_runtime()
    del settings
    ensure_workspace(paths)
    try:
        run = run_seed_sources_command(
            paths,
            companies=companies or (),
            from_file=from_file,
            out_dir=out_dir,
            label=label,
            timeout_seconds=timeout,
            report_only=report_only,
        )
    except ValueError as exc:
        typer.echo(render_seed_sources_error_report(str(exc)))
        raise typer.Exit(code=1)
    typer.echo(render_seed_sources_report(run.report))
    if add_to_registry and run.confirmed_sources:
        initialize_schema(paths.database_path)
        mutation_results = []
        for result in run.report.company_results:
            if result.outcome != "confirmed":
                continue
            for confirmed_source in result.confirmed_sources:
                mutation_results.append(
                    register_verified_source(
                        paths.database_path,
                        source_url=confirmed_source,
                        company=result.company_input.company,
                        label=result.company_input.company,
                        provenance=f'seed-sources input "{result.company_input.raw_value}"',
                        verification_reason_code="confirmed_via_seed_sources",
                        verification_reason=(
                            f'confirmed from seed-sources input "{result.company_input.raw_value}"'
                        ),
                    )
                )
        typer.echo(
            render_source_registry_sync_report(
                "jobs_ai seed-sources registry sync",
                tuple(mutation_results),
            )
        )


@app.command("import")
def import_file(
    input_path: Path = typer.Argument(
        ...,
        exists=True,
        dir_okay=False,
        file_okay=True,
        readable=True,
        resolve_path=True,
        help="Path to a local JSON file containing job lead records.",
    ),
    batch_id: str | None = typer.Option(
        None,
        "--batch-id",
        "--run-id",
        help="Optional import batch id to tag inserted jobs for later session scoping.",
    ),
    source_query: str | None = typer.Option(
        None,
        "--source-query",
        help="Optional source query to attach to the imported batch for reporting and session scoping.",
    ),
) -> None:
    """Import local job leads from a JSON file into the jobs table."""
    settings, paths = _load_runtime()
    del settings
    ensure_workspace(paths)
    initialize_schema(paths.database_path)
    try:
        result = import_jobs_from_file(
            paths.database_path,
            input_path,
            batch_id=batch_id,
            source_query=source_query,
        )
    except ValueError as exc:
        result = JobImportResult(inserted_count=0, skipped_count=0, errors=(str(exc),))
    typer.echo(render_import_report(paths, input_path, result))
    if result.errors:
        raise typer.Exit(code=1)


@app.command()
def score() -> None:
    """Score stored jobs using lightweight rule-based ranking."""
    settings, paths = _load_runtime()
    del settings
    ensure_workspace(paths)
    initialize_schema(paths.database_path)
    scored_jobs = score_jobs_from_database(paths.database_path)
    typer.echo(render_score_report(paths, scored_jobs))


@app.command()
def queue(
    limit: int | None = typer.Option(
        None,
        "--limit",
        min=1,
        help="Maximum number of ranked new jobs to show in the working set.",
    ),
) -> None:
    """Advanced/manual queue view for top ranked new jobs."""
    settings, paths = _load_runtime()
    del settings
    ensure_workspace(paths)
    initialize_schema(paths.database_path)
    queued_jobs = select_apply_queue(paths.database_path, limit=limit)
    typer.echo(render_queue_report(paths, queued_jobs, limit=limit))


@app.command()
def recommend(
    limit: int | None = typer.Option(
        None,
        "--limit",
        min=1,
        help="Maximum number of ranked new jobs to recommend resume/profile selections for.",
    ),
) -> None:
    """Advanced/manual recommendation view for the current queue."""
    settings, paths = _load_runtime()
    del settings
    ensure_workspace(paths)
    initialize_schema(paths.database_path)
    recommendations = select_queue_recommendations(paths.database_path, limit=limit)
    typer.echo(render_recommendation_report(paths, recommendations, limit=limit))


@app.command("launch-preview")
def launch_preview(
    limit: int | None = typer.Option(
        None,
        "--limit",
        min=1,
        help="Maximum number of queued jobs to preview for a read-only launch session.",
    ),
    portal_hints: bool = typer.Option(
        False,
        "--portal-hints",
        help="Show portal detection, hints, and normalized/apply link suggestions when available.",
    ),
) -> None:
    """Advanced/manual preview of queued application inputs without launching a browser."""
    settings, paths = _load_runtime()
    del settings
    ensure_workspace(paths)
    initialize_schema(paths.database_path)
    previews = select_launch_preview(paths.database_path, limit=limit)
    typer.echo(
        render_launch_preview_report(
            paths,
            previews,
            limit=limit,
            show_portal_hints=portal_hints,
        )
    )


@app.command("export-session")
def export_session(
    limit: int | None = typer.Option(
        None,
        "--limit",
        min=1,
        help="Maximum number of queued jobs to export from the current read-only launch preview set.",
    ),
) -> None:
    """Advanced/manual export of the current launch-preview working set to a JSON manifest."""
    settings, paths = _load_runtime()
    del settings
    ensure_workspace(paths)
    initialize_schema(paths.database_path)
    result = export_launch_preview_session(paths.database_path, paths.exports_dir, limit=limit)
    typer.echo(render_export_session_report(paths, result))


@session_app.command("start")
def session_start(
    limit: int = typer.Option(
        DEFAULT_SESSION_START_LIMIT,
        "--limit",
        min=1,
        help="Maximum number of ranked new jobs to freeze into the session manifest.",
    ),
    portal_hints: bool = typer.Option(
        False,
        "--portal-hints",
        help="Show portal detection, hints, and normalized/apply link suggestions when available.",
    ),
    open_: bool = typer.Option(
        False,
        "--open",
        help="After exporting the session manifest, reuse the current dry-run launch behavior.",
    ),
    executor: str | None = typer.Option(
        None,
        "--executor",
        help=(
            "Launch executor mode for --open. Allowed values: "
            f"{', '.join(SUPPORTED_EXECUTOR_MODES)}."
        ),
    ),
    out_dir: Path | None = typer.Option(
        None,
        "--out-dir",
        help="Optional output directory for the exported session manifest.",
    ),
    label: str | None = typer.Option(
        None,
        "--label",
        help="Optional short label to include in the exported session filename.",
    ),
    batch_id: str | None = typer.Option(
        None,
        "--batch-id",
        "--run-id",
        help="Optional import/discover batch id to scope selection to one recent run instead of the global new-job pool.",
    ),
) -> None:
    """Preferred modular batch-freeze step and optional safe browser open."""
    settings, paths = _load_runtime()
    del settings
    ensure_workspace(paths)
    initialize_schema(paths.database_path)
    try:
        result = start_session(
            paths.database_path,
            project_root=paths.project_root,
            default_exports_dir=paths.exports_dir,
            limit=limit,
            out_dir=out_dir,
            label=label,
            open_urls=open_,
            executor_mode=executor,
            ingest_batch_id=batch_id,
        )
    except ValueError as exc:
        typer.echo(render_session_start_error_report(paths, str(exc)))
        raise typer.Exit(code=1)
    typer.echo(
        render_session_start_report(
            paths,
            result,
            show_portal_hints=portal_hints,
        )
    )


@app.command("stats")
def stats(
    days: int = typer.Option(
        7,
        "--days",
        min=1,
        help="Recent activity window in days for import, session, and tracking summaries.",
    ),
    json_output: bool = typer.Option(
        False,
        "--json",
        help="Print the stats summary as JSON.",
    ),
) -> None:
    """Show a compact operator throughput and outcome summary."""
    settings, paths = _load_runtime()
    del settings
    ensure_workspace(paths)
    initialize_schema(paths.database_path)
    stats_result = gather_operator_stats(paths.database_path, days=days)
    typer.echo(
        render_stats_json(paths, stats_result)
        if json_output
        else render_stats_report(paths, stats_result)
    )


@sources_app.command("list")
def sources_list(
    status: list[str] = typer.Option(
        None,
        "--status",
        help=f"Optional status filter. Allowed values: {', '.join(SOURCE_REGISTRY_STATUSES)}.",
    ),
) -> None:
    """List durable ATS sources in the local registry."""
    settings, paths = _load_runtime()
    del settings
    ensure_workspace(paths)
    initialize_schema(paths.database_path)
    try:
        entries = list_registry_sources(
            paths.database_path,
            statuses=status or None,
        )
    except ValueError as exc:
        typer.echo(render_collect_error_report(str(exc)))
        raise typer.Exit(code=1)
    typer.echo(render_source_registry_list_report(paths, entries))


@sources_app.command("add")
def sources_add(
    source_url: str = typer.Argument(..., help="Source URL to add to the registry."),
    company: str | None = typer.Option(
        None,
        "--company",
        help="Optional company name to attach to this source.",
    ),
    label: str | None = typer.Option(
        None,
        "--label",
        help="Optional operator label to attach to this source.",
    ),
    notes: str | None = typer.Option(
        None,
        "--notes",
        help="Optional note to store on the registry entry.",
    ),
    provenance: str | None = typer.Option(
        "manual_add",
        "--provenance",
        help="Optional provenance string for this source.",
    ),
    portal_type: str | None = typer.Option(
        None,
        "--portal-type",
        help="Optional explicit portal type override.",
    ),
    verify: bool = typer.Option(
        True,
        "--verify/--no-verify",
        help="Verify the source before storing it. --no-verify stores it as manual_review by default.",
    ),
    timeout: float = typer.Option(
        10.0,
        "--timeout",
        min=0.1,
        help="Per-source fetch timeout in seconds when verification is enabled.",
    ),
) -> None:
    """Add one ATS source into the durable registry."""
    settings, paths = _load_runtime()
    del settings
    ensure_workspace(paths)
    initialize_schema(paths.database_path)
    try:
        result = register_source(
            paths.database_path,
            source_url=source_url,
            portal_type=portal_type,
            company=company,
            label=label,
            notes=notes,
            provenance=provenance,
            verify=verify,
            timeout_seconds=timeout,
        )
    except ValueError as exc:
        typer.echo(render_collect_error_report(str(exc)))
        raise typer.Exit(code=1)
    typer.echo(render_source_registry_add_report(paths, result))


@sources_app.command("discover-ats")
def sources_discover_ats(
    limit: int = typer.Option(
        DEFAULT_DISCOVER_ATS_LIMIT,
        "--limit",
        min=1,
        help="Maximum number of verified ATS boards to add as active registry sources.",
    ),
    provider: list[str] = typer.Option(
        None,
        "--provider",
        help=(
            "Optional provider filter. Repeat to probe a subset of providers. "
            f"Allowed values: {', '.join(SUPPORTED_DISCOVER_ATS_PROVIDERS)}."
        ),
    ),
    output: Path | None = typer.Option(
        None,
        "--output",
        dir_okay=False,
        file_okay=True,
        writable=True,
        resolve_path=False,
        help="Optional text file to write discovered active ATS board roots.",
    ),
    timeout: float = typer.Option(
        DEFAULT_DISCOVER_ATS_TIMEOUT_SECONDS,
        "--timeout",
        min=0.1,
        help="Per-request timeout in seconds while probing ATS discovery endpoints.",
    ),
) -> None:
    """Discover ATS board roots at scale from public Greenhouse, Lever, and Ashby endpoints."""
    settings, paths = _load_runtime()
    del settings
    ensure_workspace(paths)
    try:
        result = discover_registry_ats_sources(
            paths,
            limit=limit,
            output_path=output,
            providers=tuple(provider or ()),
            timeout_seconds=timeout,
        )
    except ValueError as exc:
        typer.echo(render_collect_error_report(str(exc)))
        raise typer.Exit(code=1)
    typer.echo(render_source_registry_discover_ats_report(paths, result))


@sources_app.command("harvest-companies")
def sources_harvest_companies(
    source: list[str] = typer.Option(
        None,
        "--source",
        help=company_harvest_sources_help_text(),
    ),
    out_dir: Path | None = typer.Option(
        None,
        "--out-dir",
        help="Optional output directory for harvested domains and the harvest report.",
    ),
    label: str | None = typer.Option(
        None,
        "--label",
        help="Optional short label to include in the default run id and output directory name.",
    ),
    timeout: float = typer.Option(
        DEFAULT_COMPANY_HARVEST_TIMEOUT_SECONDS,
        "--timeout",
        min=0.1,
        help="Per-fetch timeout in seconds while harvesting directory pages and inspecting company sites.",
    ),
    max_requests_per_second: float = typer.Option(
        DEFAULT_COMPANY_HARVEST_MAX_REQUESTS_PER_SECOND,
        "--max-requests-per-second",
        min=0.1,
        help="Bound the combined harvest and detect-sites fetch rate.",
    ),
) -> None:
    """Harvest company domains from curated public directory pages and feed them into detect-sites."""
    settings, paths = _load_runtime()
    del settings
    ensure_workspace(paths)
    try:
        result = harvest_companies_from_sources(
            paths,
            sources=tuple(source or ()),
            out_dir=out_dir,
            label=label,
            timeout_seconds=timeout,
            max_requests_per_second=max_requests_per_second,
        )
    except ValueError as exc:
        typer.echo(render_collect_error_report(str(exc)))
        raise typer.Exit(code=1)
    typer.echo(render_source_registry_harvest_companies_report(paths, result))


@sources_app.command("seed-bulk")
def sources_seed_bulk(
    companies: list[str] = typer.Argument(
        None,
        help=(
            "Optional bulk seed entries. Prefer direct Greenhouse, Lever, or Ashby board URLs. "
            "Also accepts company names, domains, careers URLs, or "
            "'Company Name | https://jobs.example.com | notes'."
        ),
    ),
    from_file: Path | None = typer.Option(
        None,
        "--from-file",
        exists=True,
        dir_okay=False,
        file_okay=True,
        readable=True,
        resolve_path=True,
        help=(
            "Optional text file containing one bulk seed entry per line. Blank lines "
            "and lines starting with # are ignored."
        ),
    ),
    starter: list[str] = typer.Option(
        None,
        "--starter",
        help=seed_bulk_starter_help_text(),
    ),
    timeout: float = typer.Option(
        10.0,
        "--timeout",
        min=0.1,
        help="Per-fetch timeout in seconds while probing supported ATS roots.",
    ),
) -> None:
    """Seed the durable registry from ATS URLs, company names, domains, or careers URLs."""
    settings, paths = _load_runtime()
    del settings
    ensure_workspace(paths)
    try:
        result = seed_registry_bulk(
            paths,
            companies=tuple(companies or ()),
            from_file=from_file,
            starter_lists=tuple(starter or ()),
            timeout_seconds=timeout,
        )
    except ValueError as exc:
        typer.echo(render_collect_error_report(str(exc)))
        raise typer.Exit(code=1)
    typer.echo(render_source_registry_seed_bulk_report(paths, result))


@sources_app.command("detect-sites")
def sources_detect_sites(
    companies: list[str] = typer.Argument(
        None,
        help=(
            "Optional company-site inputs. Accepts domains, homepage URLs, careers URLs, direct ATS URLs, or "
            "'Company Name | https://company.example | notes'. Company-name-only inputs stay conservative."
        ),
    ),
    from_file: Path | None = typer.Option(
        None,
        "--from-file",
        exists=True,
        dir_okay=False,
        file_okay=True,
        readable=True,
        resolve_path=True,
        help=(
            "Optional text file containing one company-site input per line. Blank lines "
            "and lines starting with # are ignored."
        ),
    ),
    starter: list[str] = typer.Option(
        None,
        "--starter",
        help=detect_sites_starter_help_text(),
    ),
    timeout: float = typer.Option(
        10.0,
        "--timeout",
        min=0.1,
        help="Per-fetch timeout in seconds while inspecting company sites and verifying supported ATS roots.",
    ),
    structured_clues: bool = typer.Option(
        True,
        "--structured-clues/--no-structured-clues",
        help="Inspect already fetched public pages for JSON-LD, embedded ATS URLs, and bounded machine-readable clues.",
    ),
) -> None:
    """Inspect employer sites, detect supported ATS boards, and upsert them into the registry."""
    settings, paths = _load_runtime()
    del settings
    ensure_workspace(paths)
    try:
        result = detect_registry_sources_from_sites(
            paths,
            companies=tuple(companies or ()),
            from_file=from_file,
            starter_lists=tuple(starter or ()),
            timeout_seconds=timeout,
            use_structured_clues=structured_clues,
        )
    except ValueError as exc:
        typer.echo(render_collect_error_report(str(exc)))
        raise typer.Exit(code=1)
    typer.echo(render_source_registry_detect_sites_report(paths, result))


@sources_app.command("extract-jobposting")
def sources_extract_jobposting(
    targets: list[str] = typer.Argument(
        None,
        help=(
            "Company domains, homepages, or direct careers page URLs to scan for "
            "schema.org JobPosting JSON-LD."
        ),
    ),
    from_file: Path | None = typer.Option(
        None,
        "--from-file",
        exists=True,
        dir_okay=False,
        file_okay=True,
        readable=True,
        resolve_path=True,
        help=(
            "Optional text file containing one domain or page URL per line. Blank "
            "lines and lines starting with # are ignored."
        ),
    ),
    timeout: float = typer.Option(
        DEFAULT_JOBPOSTING_TIMEOUT_SECONDS,
        "--timeout",
        min=0.1,
        help="Per-page fetch timeout in seconds.",
    ),
    max_requests_per_second: float = typer.Option(
        DEFAULT_JOBPOSTING_MAX_REQUESTS_PER_SECOND,
        "--max-requests-per-second",
        min=0.1,
        help="Maximum fetch rate across scanned pages.",
    ),
    batch_id: str | None = typer.Option(
        None,
        "--batch-id",
        "--run-id",
        help="Optional import batch id to attach to imported jobs.",
    ),
) -> None:
    """Extract embedded JobPosting JSON-LD from company pages and import jobs directly."""
    settings, paths = _load_runtime()
    del settings
    ensure_workspace(paths)
    try:
        result = extract_jobposting_sources(
            paths,
            targets=tuple(targets or ()),
            from_file=from_file,
            timeout_seconds=timeout,
            max_requests_per_second=max_requests_per_second,
            batch_id=batch_id,
        )
    except ValueError as exc:
        typer.echo(render_collect_error_report(str(exc)))
        raise typer.Exit(code=1)
    typer.echo(render_source_registry_extract_jobposting_report(paths, result))
    if result.failed_count:
        raise typer.Exit(code=1)


@sources_app.command("expand-registry")
def sources_expand_registry(
    companies: list[str] = typer.Argument(
        None,
        help=(
            "Optional discovery inputs. Accepts direct ATS URLs, domains, homepage URLs, careers URLs, or "
            "'Company Name | https://company.example | notes'."
        ),
    ),
    from_file: Path | None = typer.Option(
        None,
        "--from-file",
        exists=True,
        dir_okay=False,
        file_okay=True,
        readable=True,
        resolve_path=True,
        help=(
            "Optional text file containing one discovery input per line. Blank lines "
            "and lines starting with # are ignored."
        ),
    ),
    starter: list[str] = typer.Option(
        None,
        "--starter",
        help=seed_bulk_starter_help_text(),
    ),
    detect_sites: bool = typer.Option(
        False,
        "--detect-sites/--no-detect-sites",
        help="Inspect company sites for ATS footprints in addition to ingesting direct ATS inputs and starter entries.",
    ),
    discover_ats: bool = typer.Option(
        False,
        "--discover-ats/--no-discover-ats",
        help="Probe bundled Greenhouse, Lever, and Ashby public surfaces for more registry candidates.",
    ),
    structured_clues: bool = typer.Option(
        True,
        "--structured-clues/--no-structured-clues",
        help="Use JSON-LD, embedded ATS URLs, and machine-readable job-feed clues during company-site detection.",
    ),
    provider: list[str] = typer.Option(
        None,
        "--provider",
        help=(
            "Optional discover-ats provider filter. Repeat to keep ATS probing focused. "
            f"Allowed values: {', '.join(SUPPORTED_DISCOVER_ATS_PROVIDERS)}."
        ),
    ),
    limit: int = typer.Option(
        DEFAULT_DISCOVER_ATS_LIMIT,
        "--limit",
        min=1,
        help="Maximum number of active ATS sources to accept from the discover-ats probing lane.",
    ),
    timeout: float = typer.Option(
        DEFAULT_DISCOVER_ATS_TIMEOUT_SECONDS,
        "--timeout",
        min=0.1,
        help="Per-fetch timeout in seconds across all enabled discovery lanes.",
    ),
) -> None:
    """Run the practical multi-lane workflow for growing the durable source registry."""
    settings, paths = _load_runtime()
    del settings
    ensure_workspace(paths)
    try:
        result = expand_registry_sources(
            paths,
            companies=tuple(companies or ()),
            from_file=from_file,
            starter_lists=tuple(starter or ()),
            detect_sites=detect_sites,
            discover_ats=discover_ats,
            structured_clues=structured_clues,
            discover_ats_limit=limit,
            discover_ats_providers=tuple(provider or ()),
            timeout_seconds=timeout,
        )
    except ValueError as exc:
        typer.echo(render_collect_error_report(str(exc)))
        raise typer.Exit(code=1)
    typer.echo(render_source_registry_expand_report(paths, result))


@sources_app.command("import")
def sources_import(
    from_file: Path = typer.Option(
        ...,
        "--from-file",
        exists=True,
        dir_okay=False,
        file_okay=True,
        readable=True,
        resolve_path=True,
        help="Text or JSON file containing registry sources to import.",
    ),
    verify: bool = typer.Option(
        True,
        "--verify/--no-verify",
        help="Verify imported sources before storing them.",
    ),
    timeout: float = typer.Option(
        10.0,
        "--timeout",
        min=0.1,
        help="Per-source fetch timeout in seconds when verification is enabled.",
    ),
    notes: str | None = typer.Option(
        None,
        "--notes",
        help="Optional note to append to every imported source.",
    ),
    provenance: str | None = typer.Option(
        None,
        "--provenance",
        help="Optional provenance string to append to every imported source.",
    ),
) -> None:
    """Import ATS sources into the durable registry from a text or JSON file."""
    settings, paths = _load_runtime()
    del settings
    ensure_workspace(paths)
    initialize_schema(paths.database_path)
    try:
        result = import_registry_sources(
            paths.database_path,
            input_path=from_file,
            verify=verify,
            timeout_seconds=timeout,
            notes=notes,
            provenance=provenance,
        )
    except ValueError as exc:
        typer.echo(render_collect_error_report(str(exc)))
        raise typer.Exit(code=1)
    typer.echo(render_source_registry_import_report(paths, from_file, result))
    if result.errors:
        raise typer.Exit(code=1)


@sources_app.command("verify")
def sources_verify(
    source_ids: list[int] = typer.Argument(
        None,
        help="Optional source ids to verify. Defaults to active and manual_review sources.",
    ),
    include_inactive: bool = typer.Option(
        False,
        "--include-inactive",
        help="Include inactive sources when no ids are provided.",
    ),
    timeout: float = typer.Option(
        10.0,
        "--timeout",
        min=0.1,
        help="Per-source fetch timeout in seconds.",
    ),
) -> None:
    """Verify registry sources and refresh their status."""
    settings, paths = _load_runtime()
    del settings
    ensure_workspace(paths)
    initialize_schema(paths.database_path)
    try:
        results = verify_registry_sources(
            paths.database_path,
            source_ids=source_ids or None,
            include_inactive=include_inactive,
            timeout_seconds=timeout,
        )
    except ValueError as exc:
        typer.echo(render_collect_error_report(str(exc)))
        raise typer.Exit(code=1)
    typer.echo(render_source_registry_verify_report(paths, results))


@sources_app.command("deactivate")
def sources_deactivate(
    source_id: int = typer.Argument(..., help="Registry source id to deactivate."),
    note: str | None = typer.Option(
        None,
        "--note",
        help="Optional note to append while deactivating the source.",
    ),
) -> None:
    """Deactivate one registry source."""
    settings, paths = _load_runtime()
    del settings
    ensure_workspace(paths)
    initialize_schema(paths.database_path)
    try:
        entry = deactivate_registry_source(
            paths.database_path,
            source_id=source_id,
            note=note,
        )
    except ValueError as exc:
        typer.echo(render_collect_error_report(str(exc)))
        raise typer.Exit(code=1)
    typer.echo(render_source_registry_deactivate_report(paths, entry))


@sources_app.command("collect")
def sources_collect(
    source_ids: list[int] = typer.Argument(
        None,
        help="Optional registry source ids to collect. Defaults to all active sources.",
    ),
    limit: int | None = typer.Option(
        None,
        "--limit",
        min=1,
        help="Optional cap on how many active registry sources to collect.",
    ),
    out_dir: Path | None = typer.Option(
        None,
        "--out-dir",
        help="Optional output directory for normal collect artifacts.",
    ),
    label: str | None = typer.Option(
        None,
        "--label",
        help="Optional short label for the collect run id and artifacts.",
    ),
    timeout: float = typer.Option(
        10.0,
        "--timeout",
        min=0.1,
        help="Per-source fetch timeout in seconds.",
    ),
    verify: bool = typer.Option(
        True,
        "--verify/--no-verify",
        help="Verify selected registry entries if they have not been verified yet or are not active.",
    ),
    force_verify: bool = typer.Option(
        False,
        "--force-verify",
        help="Reverify every selected source before collecting.",
    ),
    import_results: bool = typer.Option(
        False,
        "--import",
        help="Import collected leads automatically after collection completes.",
    ),
) -> None:
    """Collect jobs directly from the durable source registry."""
    settings, paths = _load_runtime()
    del settings
    ensure_workspace(paths)
    try:
        result = collect_registry_sources(
            paths,
            source_ids=tuple(source_ids or ()),
            limit=limit,
            out_dir=out_dir,
            label=label,
            timeout_seconds=timeout,
            verify_if_needed=verify,
            force_verify=force_verify,
            import_results=import_results,
        )
    except ValueError as exc:
        typer.echo(render_collect_error_report(str(exc)))
        raise typer.Exit(code=1)
    typer.echo(render_source_registry_collect_report(paths, result))
    if result.import_result is not None and result.import_result.errors:
        raise typer.Exit(code=1)


@maintenance_app.command("backfill")
def maintenance_backfill(
    limit: int | None = typer.Option(
        None,
        "--limit",
        min=1,
        help="Optional cap on how many candidate jobs to backfill in one run.",
    ),
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        help="Preview candidate updates without modifying the database.",
    ),
) -> None:
    """Populate missing derived job metadata on older rows without inventing history."""
    settings, paths = _load_runtime()
    del settings
    ensure_workspace(paths)
    try:
        result = backfill_jobs_metadata(
            paths.database_path,
            limit=limit,
            dry_run=dry_run,
        )
    except ValueError as exc:
        typer.echo(render_maintenance_backfill_error_report(paths, str(exc)))
        raise typer.Exit(code=1)
    typer.echo(render_maintenance_backfill_report(paths, result))


@session_app.command("recent")
def session_recent(
    limit: int = typer.Option(
        DEFAULT_SESSION_RECENT_LIMIT,
        "--limit",
        min=1,
        help="Maximum number of recent recorded sessions to show.",
    ),
) -> None:
    """List recent session manifests recorded in session history."""
    settings, paths = _load_runtime()
    del settings
    ensure_workspace(paths)
    initialize_schema(paths.database_path)
    typer.echo(
        render_session_recent_report(
            paths,
            recent_sessions(paths.database_path, limit=limit),
            limit=limit,
        )
    )


@session_app.command("inspect")
def session_inspect(
    reference: str = typer.Argument(
        ...,
        help="Session id from session history or a direct manifest path.",
    ),
) -> None:
    """Inspect one prior session manifest and its current tracked statuses."""
    settings, paths = _load_runtime()
    del settings
    ensure_workspace(paths)
    initialize_schema(paths.database_path)
    try:
        result = inspect_session(paths.database_path, reference=reference)
    except ValueError as exc:
        typer.echo(render_session_inspect_error_report(paths, str(exc)))
        raise typer.Exit(code=1)
    typer.echo(render_session_inspect_report(paths, result))


@session_app.command("reopen")
def session_reopen(
    reference: str = typer.Argument(
        ...,
        help="Session id from session history or a direct manifest path.",
    ),
    executor: str = typer.Option(
        BROWSER_STUB_EXECUTOR_MODE,
        "--executor",
        help=(
            "Launch executor mode for reopening session URLs. Allowed values: "
            f"{', '.join(SUPPORTED_EXECUTOR_MODES)}."
        ),
    ),
) -> None:
    """Reopen launchable URLs from a prior session manifest using the current safe executor."""
    settings, paths = _load_runtime()
    del settings
    ensure_workspace(paths)
    initialize_schema(paths.database_path)
    try:
        result = reopen_session(
            paths.database_path,
            reference=reference,
            executor_mode=executor,
        )
    except ValueError as exc:
        typer.echo(render_session_reopen_error_report(paths, str(exc)))
        raise typer.Exit(code=1)
    typer.echo(render_session_reopen_report(paths, result))


@session_app.command("mark")
def session_mark(
    status: str = typer.Argument(
        ...,
        help=(
            "Operator status to record: "
            f"{', '.join(SESSION_MARK_APPLICATION_STATUSES)}."
        ),
    ),
    job_ids: list[int] = typer.Argument(
        None,
        metavar="JOB_ID...",
        help="One or more job ids from the jobs table.",
    ),
    manifest: Path | None = typer.Option(
        None,
        "--manifest",
        help="Optional exported session manifest to mark against.",
    ),
    all_: bool = typer.Option(
        False,
        "--all",
        help="When used with --manifest, mark all launchable manifest items.",
    ),
    indexes: list[int] = typer.Option(
        None,
        "--index",
        "--indexes",
        metavar="N",
        help="Manifest index to mark. Repeat --indexes for multiple entries.",
    ),
) -> None:
    """Record batch outcomes after a session using job ids or an exported manifest."""
    settings, paths = _load_runtime()
    del settings
    ensure_workspace(paths)
    initialize_schema(paths.database_path)
    try:
        result = mark_session_jobs(
            paths.database_path,
            status=status,
            job_ids=job_ids or (),
            manifest_path=manifest,
            all_items=all_,
            indexes=indexes or (),
        )
    except ValueError as exc:
        typer.echo(
            render_session_mark_error_report(
                paths,
                str(exc),
                manifest_path=manifest,
            )
        )
        raise typer.Exit(code=1)

    typer.echo(render_session_mark_report(paths, result))
    if not result.updated:
        raise typer.Exit(code=1)


@app.command()
def preflight(
    manifest_path: Path = typer.Argument(
        ...,
        exists=True,
        dir_okay=False,
        file_okay=True,
        readable=True,
        resolve_path=True,
        help="Path to a JSON session manifest created by jobs-ai export-session.",
    ),
) -> None:
    """Validate and preview an exported read-only session manifest."""
    try:
        manifest = load_session_manifest(manifest_path)
    except ValueError as exc:
        typer.echo(render_preflight_error_report(manifest_path, str(exc)))
        raise typer.Exit(code=1)
    typer.echo(render_preflight_report(manifest))


@app.command("launch-plan")
def launch_plan(
    manifest_path: Path = typer.Argument(
        ...,
        exists=True,
        dir_okay=False,
        file_okay=True,
        readable=True,
        resolve_path=True,
        help="Path to a JSON session manifest created by jobs-ai export-session.",
    ),
) -> None:
    """Build a read-only launch plan from an exported session manifest."""
    try:
        manifest = load_session_manifest(manifest_path)
    except ValueError as exc:
        typer.echo(render_launch_plan_error_report(manifest_path, str(exc)))
        raise typer.Exit(code=1)
    typer.echo(render_launch_plan_report(build_launch_plan(manifest)))


@app.command("launch-dry-run")
def launch_dry_run(
    manifest_path: Path = typer.Argument(
        ...,
        exists=True,
        dir_okay=False,
        file_okay=True,
        readable=True,
        resolve_path=True,
        help="Path to a JSON session manifest created by jobs-ai export-session.",
    ),
    executor: str = typer.Option(
        NO_OP_EXECUTOR_MODE,
        "--executor",
        help=f"Launch executor mode. Allowed values: {', '.join(SUPPORTED_EXECUTOR_MODES)}.",
    ),
    limit: int | None = typer.Option(
        None,
        "--limit",
        min=1,
        help="Optional safety cap for browser launches. Applies only to browser_stub mode.",
    ),
    confirm: bool = typer.Option(
        False,
        "--confirm",
        help="Ask for confirmation before opening browser tabs in browser_stub mode.",
    ),
) -> None:
    """Print a compact launch summary and run the selected launch executor."""
    try:
        manifest = load_session_manifest(manifest_path)
        plan = build_launch_plan(manifest)
        dry_run = build_launch_dry_run(plan)
        execution_steps = _select_launch_execution_steps(
            dry_run,
            executor_mode=executor,
            limit=limit,
        )
        if executor == BROWSER_STUB_EXECUTOR_MODE and execution_steps:
            typer.echo(render_launch_execution_summary(execution_steps))
            if confirm and not click.confirm(
                _build_launch_confirmation_prompt(execution_steps, executor_mode=executor),
                default=False,
            ):
                typer.echo("Launch cancelled. No browser tabs were opened.")
                return
        reports = collect_launch_execution_reports_for_steps(
            execution_steps,
            select_launch_executor(executor),
        )
    except ValueError as exc:
        typer.echo(render_launch_dry_run_error_report(manifest_path, str(exc)))
        raise typer.Exit(code=1)
    typer.echo(render_launch_dry_run_report(reports))


@app.command("application-assist")
def application_assist(
    manifest_path: Path = typer.Argument(
        ...,
        exists=True,
        dir_okay=False,
        file_okay=True,
        readable=True,
        resolve_path=True,
        help="Path to a JSON session manifest created by jobs-ai export-session.",
    ),
    portal_hints: bool = typer.Option(
        False,
        "--portal-hints",
        help="Show portal detection, hints, and normalized/apply link suggestions when available.",
    ),
    prefill: bool = typer.Option(
        False,
        "--prefill",
        help="Open one application page, fill safe fields, upload the recommended resume, and stop before submit.",
    ),
    launch_order: int | None = typer.Option(
        None,
        "--launch-order",
        min=1,
        help="Launch order from the manifest to prefill. Required when multiple launchable items exist.",
    ),
    applicant_profile: Path | None = typer.Option(
        None,
        "--applicant-profile",
        help="Optional applicant profile JSON path. Defaults to .jobs_ai_applicant_profile.json in the project root.",
    ),
    browser_backend: str = typer.Option(
        "playwright",
        "--browser-backend",
        help=(
            "Browser automation backend for --prefill. Allowed values: "
            f"{', '.join(SUPPORTED_PREFILL_BROWSER_BACKENDS)}."
        ),
    ),
    hold_open: bool = typer.Option(
        True,
        "--hold-open/--no-hold-open",
        help="Keep the automation browser open after prefilling so the operator can review and submit manually.",
    ),
    log_outcome: bool = typer.Option(
        False,
        "--log-outcome",
        help="After the browser closes, prompt for a lightweight application log.",
    ),
    log_status: str | None = typer.Option(
        None,
        "--log-status",
        help=(
            "Write this application log automatically after the browser closes. "
            f"Allowed values: {', '.join(APPLICATION_LOG_STATUSES)}."
        ),
    ),
    log_notes: str | None = typer.Option(
        None,
        "--log-notes",
        help="Optional notes for post-run outcome logging.",
    ),
) -> None:
    """Show read-only guidance or run review-first browser prefilling for one application."""
    try:
        manifest = load_session_manifest(manifest_path)
        plan = build_launch_plan(manifest)
        assist = build_application_assist(plan)
        normalized_log_status, normalized_log_notes, prompt_for_log_outcome = (
            _resolve_application_assist_log_options(
                prefill=prefill,
                log_outcome=log_outcome,
                log_status=log_status,
                log_notes=log_notes,
            )
        )
    except ValueError as exc:
        typer.echo(render_application_assist_error_report(manifest_path, str(exc)))
        raise typer.Exit(code=1)
    if not prefill:
        typer.echo(
            render_application_assist_report(
                assist,
                show_portal_hints=portal_hints,
            )
        )
        return

    settings, paths = _load_runtime()
    del settings
    ensure_workspace(paths)
    try:
        browser = create_prefill_browser_backend(browser_backend)
    except ValueError as exc:
        typer.echo(render_application_assist_error_report(manifest_path, str(exc)))
        raise typer.Exit(code=1)
    try:
        result = run_application_prefill(
            manifest_path,
            project_root=paths.project_root,
            applicant_profile_path=applicant_profile,
            launch_order=launch_order,
            browser_backend=browser,
        )
    except ValueError as exc:
        browser.close()
        typer.echo(render_application_assist_error_report(manifest_path, str(exc)))
        raise typer.Exit(code=1)
    typer.echo(render_application_prefill_report(result))
    try:
        if hold_open:
            typer.echo(
                "Browser remains open for manual review. Click submit manually if you choose, then press Enter to close the automation browser."
            )
            click.prompt("", prompt_suffix="", default="", show_default=False)
    finally:
        browser.close()
    _maybe_log_application_assist_outcome(
        paths.project_root,
        result=result,
        prompt_for_outcome=prompt_for_log_outcome,
        log_status=normalized_log_status,
        log_notes=normalized_log_notes,
    )


@app.command("application-log")
def application_log(
    company: str | None = typer.Option(
        None,
        "--company",
        help="Company name. Required unless populated from --manifest.",
    ),
    role: str | None = typer.Option(
        None,
        "--role",
        help="Role or title. Required unless populated from --manifest.",
    ),
    portal: str | None = typer.Option(
        None,
        "--portal",
        help="Portal name or type. Required unless inferred from --apply-url or populated from --manifest.",
    ),
    apply_url: str | None = typer.Option(
        None,
        "--apply-url",
        help="Application URL. Required unless populated from --manifest.",
    ),
    status: str = typer.Option(
        ...,
        "--status",
        help=f"Application outcome to log: {', '.join(APPLICATION_LOG_STATUSES)}.",
    ),
    notes: str | None = typer.Option(
        None,
        "--notes",
        help="Optional notes about manual fixes, blockers, or submit outcome.",
    ),
    manifest: Path | None = typer.Option(
        None,
        "--manifest",
        exists=True,
        dir_okay=False,
        file_okay=True,
        readable=True,
        resolve_path=True,
        help="Optional session manifest path to populate company, role, portal, and apply_url.",
    ),
    launch_order: int | None = typer.Option(
        None,
        "--launch-order",
        min=1,
        help="Launch order from the manifest to log.",
    ),
) -> None:
    """Write one JSON application log under data/applications for a manual outcome."""
    settings, paths = _load_runtime()
    del settings
    ensure_workspace(paths)
    try:
        result = write_application_log(
            paths.project_root,
            company=company,
            role=role,
            portal=portal,
            apply_url=apply_url,
            status=status,
            notes=notes,
            manifest_path=manifest,
            launch_order=launch_order,
        )
    except ValueError as exc:
        typer.echo(
            render_application_log_error_report(
                str(exc),
                manifest_path=manifest,
            )
        )
        raise typer.Exit(code=1)
    typer.echo(render_application_log_report(result))


@app.command("portal-hint")
def portal_hint(
    apply_url: str = typer.Argument(
        ...,
        help="Job application URL to inspect for supported portal hints.",
    ),
    portal_type: str | None = typer.Option(
        None,
        "--portal-type",
        help="Optional explicit portal type hint: greenhouse, lever, ashby, or workday.",
    ),
) -> None:
    """Inspect one apply URL for supported portal hints and safe link normalization."""
    typer.echo(
        render_portal_hint_report(
            apply_url,
            build_portal_support(apply_url, portal_type=portal_type),
        )
    )


@track_app.command("mark")
def track_mark(
    job_id: int = typer.Argument(..., min=1, help="Job id from the jobs table."),
    status: str = typer.Argument(
        ...,
        help=f"Manual application status: {', '.join(APPLICATION_STATUSES)}.",
    ),
) -> None:
    """Record a manual application status update for one job."""
    settings, paths = _load_runtime()
    del settings
    ensure_workspace(paths)
    initialize_schema(paths.database_path)
    try:
        snapshot = record_application_status(paths.database_path, job_id=job_id, status=status)
    except ValueError as exc:
        typer.echo(render_application_tracking_error_report("mark", paths, str(exc)))
        raise typer.Exit(code=1)
    typer.echo(render_application_tracking_mark_report(paths, snapshot))


def _run_track_list(status: str | None) -> None:
    settings, paths = _load_runtime()
    del settings
    ensure_workspace(paths)
    initialize_schema(paths.database_path)
    try:
        snapshots = list_application_statuses(paths.database_path, status=status)
    except ValueError as exc:
        typer.echo(render_application_tracking_error_report("list", paths, str(exc)))
        raise typer.Exit(code=1)
    typer.echo(render_application_tracking_list_report(paths, snapshots, status_filter=status))


@app.command("applied")
def applied() -> None:
    """Shortcut for viewing jobs currently tracked as applied."""
    _run_track_list(status="applied")


@track_app.command("list")
def track_list(
    status: str | None = typer.Option(
        None,
        "--status",
        help=f"Optional current-status filter: {', '.join(APPLICATION_STATUSES)}.",
    ),
) -> None:
    """List current application statuses in deterministic job order."""
    _run_track_list(status=status)


@track_app.command("status")
def track_status(
    job_id: int = typer.Argument(..., min=1, help="Job id from the jobs table."),
) -> None:
    """Show the current manual tracking status and history for one job."""
    settings, paths = _load_runtime()
    del settings
    ensure_workspace(paths)
    initialize_schema(paths.database_path)
    try:
        detail = get_application_status(paths.database_path, job_id=job_id)
    except ValueError as exc:
        typer.echo(render_application_tracking_error_report("status", paths, str(exc)))
        raise typer.Exit(code=1)
    typer.echo(render_application_tracking_status_report(paths, detail))


@db_app.command("init")
def db_init() -> None:
    """Create the schema for the active database backend."""
    settings, paths = _load_runtime()
    runtime = resolve_database_runtime(paths.database_path, settings=settings)
    created_paths = ensure_workspace(paths)
    try:
        initialize_schema(paths.database_path)
    except Exception as exc:
        typer.echo(
            "\n".join(
                [
                    "jobs_ai database init",
                    f"database backend: {runtime.backend}",
                    f"database target: {runtime.target_label}",
                    "status: failed",
                    f"error: {exc}",
                ]
            )
        )
        raise typer.Exit(code=1)
    typer.echo(
        render_db_init_report(
            paths,
            created_paths,
            backend=runtime.backend,
            target_label=runtime.target_label,
        )
    )


@db_app.command("status")
def db_status() -> None:
    """Show whether the active database schema is ready."""
    settings, paths = _load_runtime()
    runtime = resolve_database_runtime(paths.database_path, settings=settings)
    try:
        present = database_exists(paths.database_path, settings=settings)
        missing_tables = missing_required_tables(paths.database_path)
    except Exception as exc:
        typer.echo(
            "\n".join(
                [
                    "jobs_ai database status",
                    f"database backend: {runtime.backend}",
                    f"database target: {runtime.target_label}",
                    "status: failed",
                    f"error: {exc}",
                ]
            )
        )
        raise typer.Exit(code=1)
    typer.echo(
        render_db_status_report(
            paths,
            missing_tables,
            backend=runtime.backend,
            target_label=runtime.target_label,
            database_present=present,
        )
    )
    if not present or missing_tables:
        raise typer.Exit(code=1)


@db_app.command("backend-status")
def db_backend_status() -> None:
    """Show the resolved database backend configuration and schema readiness."""
    settings, paths = _load_runtime()
    del paths
    result = build_backend_status(settings)
    typer.echo(render_db_backend_status_report(result))
    if not result.reachable or result.missing_tables:
        raise typer.Exit(code=1)


@db_app.command("ping")
def db_ping() -> None:
    """Check connectivity to the active database backend."""
    settings, paths = _load_runtime()
    del paths
    result = ping_database_target(settings)
    typer.echo(render_db_ping_report(result))
    if not result.ok:
        raise typer.Exit(code=1)


@db_app.command("migrate-to-postgres")
def db_migrate_to_postgres(
    source_sqlite: Path | None = typer.Option(
        None,
        "--source-sqlite",
        help="Optional source SQLite path. Defaults to the resolved local canonical SQLite path.",
    ),
    database_url: str | None = typer.Option(
        None,
        "--database-url",
        help="Optional Postgres/Neon connection string. Defaults to DATABASE_URL from the environment.",
    ),
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        help="Run the SQLite-to-Postgres migration inside a rollback-only transaction.",
    ),
) -> None:
    """Copy the canonical SQLite database on this machine into Neon/Postgres."""
    settings, paths = _load_runtime()
    ensure_workspace(paths)
    source_path = paths.database_path if source_sqlite is None else source_sqlite.expanduser()
    effective_database_url = settings.database_url if database_url is None else database_url
    target_label = resolve_database_runtime(
        source_path,
        backend="postgres",
        database_url=effective_database_url,
    ).target_label
    try:
        result = migrate_sqlite_to_postgres(
            source_path,
            database_url=effective_database_url or "",
            dry_run=dry_run,
        )
    except Exception as exc:
        typer.echo(
            render_db_migrate_to_postgres_error_report(
                source_path,
                target_label,
                str(exc),
            )
        )
        raise typer.Exit(code=1)
    typer.echo(render_db_migrate_to_postgres_report(result))


@db_app.command("merge")
def db_merge(
    source_db: Path = typer.Argument(
        ...,
        exists=True,
        dir_okay=False,
        file_okay=True,
        readable=True,
        resolve_path=True,
        help="SQLite database file to merge into the canonical target.",
    ),
    target: Path | None = typer.Option(
        None,
        "--target",
        help="Optional target SQLite database path. Defaults to JOBS_AI_SQLITE_PATH or data/jobs_ai.db.",
    ),
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        help="Preview the merge without modifying the target database.",
    ),
    backup: bool = typer.Option(
        False,
        "--backup",
        help="Create a timestamped backup of the target database before merging.",
    ),
    vacuum: bool = typer.Option(
        False,
        "--vacuum",
        help="Run VACUUM on the target database after a successful write merge.",
    ),
) -> None:
    """Merge one SQLite database into the canonical target without blindly overwriting it."""
    settings, default_paths = _load_runtime()
    effective_database_path = settings.database_path if target is None else target
    paths = build_workspace_paths(
        effective_database_path,
        project_root=default_paths.project_root,
    )
    ensure_workspace(paths)
    try:
        result = merge_sqlite_databases(
            paths.database_path,
            source_db,
            dry_run=dry_run,
            create_backup=backup,
            vacuum=vacuum,
        )
    except (ValueError, RuntimeError, sqlite3.Error) as exc:
        typer.echo(
            render_db_merge_error_report(
                paths.database_path,
                source_db,
                str(exc),
            )
        )
        raise typer.Exit(code=1)
    typer.echo(render_db_merge_report(paths, result))


def run(argv: Sequence[str] | None = None) -> int:
    try:
        result = app(
            args=list(argv) if argv is not None else None,
            prog_name="jobs-ai",
            standalone_mode=False,
        )
    except click.ClickException as exc:
        exc.show()
        return exc.exit_code
    except typer.Exit as exc:
        return exc.exit_code
    if isinstance(result, int):
        return result
    return 0


main = run


if __name__ == "__main__":
    raise SystemExit(run())
