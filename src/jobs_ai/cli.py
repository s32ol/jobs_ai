from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path

import click
import typer

from .collect.cli import run_collect_command
from .discover.cli import run_discover_command
from .source_seed.cli import run_seed_sources_command
from .application_assist import build_application_assist
from .application_tracking import (
    APPLICATION_STATUSES,
    SESSION_MARK_APPLICATION_STATUSES,
    get_application_status,
    list_application_statuses,
    record_application_status,
)
from .config import load_settings
from .db import initialize_schema, missing_required_tables
from .maintenance import backfill_jobs_metadata
from .jobs.importer import JobImportResult, import_jobs_from_file
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
    render_application_assist_error_report,
    render_application_assist_report,
    render_collect_error_report,
    render_collect_report,
    render_discover_error_report,
    render_discover_report,
    render_application_tracking_error_report,
    render_application_tracking_list_report,
    render_application_tracking_mark_report,
    render_application_tracking_status_report,
    render_run_error_report,
    render_run_json,
    render_run_report,
    render_db_init_report,
    render_db_status_report,
    render_doctor_report,
    render_maintenance_backfill_error_report,
    render_maintenance_backfill_report,
    render_launch_dry_run_report,
    render_launch_dry_run_error_report,
    render_launch_execution_summary,
    render_launch_plan_error_report,
    render_launch_plan_report,
    render_export_session_report,
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
    render_score_report,
    render_stats_json,
    render_stats_report,
    render_status_report,
)
from .portal_support import build_portal_support
from .run_workflow import DEFAULT_RUN_DISCOVER_LIMIT, run_operator_workflow
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
from .session_start import DEFAULT_SESSION_START_LIMIT, start_session
from .workspace import build_workspace_paths, ensure_workspace, missing_workspace_paths


app = typer.Typer(
    add_completion=False,
    help=(
        "Local CLI for the jobs_ai job-search exoskeleton.\n\n"
        "Preferred daily flow: run -> manual apply -> session mark.\n\n"
        "Preferred modular flow: discover --collect --import -> session start -> "
        "manual apply -> session mark.\n\n"
        "Advanced manual building blocks: score -> queue -> recommend -> "
        "launch-preview -> export-session -> preflight -> application-assist -> "
        "launch-dry-run -> track -> stats -> maintenance."
    ),
)
db_app = typer.Typer(
    help=(
        "Manage the local SQLite database.\n\n"
        "Use db init for a fresh workspace, then db status before importing leads."
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
app.add_typer(db_app, name="db")
app.add_typer(track_app, name="track")
app.add_typer(session_app, name="session")
app.add_typer(maintenance_app, name="maintenance")


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
        )
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
        )
    except ValueError as exc:
        typer.echo(render_discover_error_report(str(exc)))
        raise typer.Exit(code=1)
    typer.echo(render_discover_report(run.report))
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
) -> None:
    """Show read-only resume and snippet guidance for launchable applications."""
    try:
        manifest = load_session_manifest(manifest_path)
        plan = build_launch_plan(manifest)
        assist = build_application_assist(plan)
    except ValueError as exc:
        typer.echo(render_application_assist_error_report(manifest_path, str(exc)))
        raise typer.Exit(code=1)
    typer.echo(
        render_application_assist_report(
            assist,
            show_portal_hints=portal_hints,
        )
    )


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


@track_app.command("list")
def track_list(
    status: str | None = typer.Option(
        None,
        "--status",
        help=f"Optional current-status filter: {', '.join(APPLICATION_STATUSES)}.",
    ),
) -> None:
    """List current application statuses in deterministic job order."""
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
    """Create the local SQLite database and required tables."""
    settings, paths = _load_runtime()
    del settings
    created_paths = ensure_workspace(paths)
    initialize_schema(paths.database_path)
    typer.echo(render_db_init_report(paths, created_paths))


@db_app.command("status")
def db_status() -> None:
    """Show whether the local SQLite schema is ready."""
    settings, paths = _load_runtime()
    del settings
    missing_tables = missing_required_tables(paths.database_path)
    typer.echo(render_db_status_report(paths, missing_tables))
    if not paths.database_path.exists() or missing_tables:
        raise typer.Exit(code=1)


def run(argv: Sequence[str] | None = None) -> int:
    try:
        app(args=list(argv) if argv is not None else None, prog_name="jobs-ai", standalone_mode=False)
    except click.ClickException as exc:
        exc.show()
        return exc.exit_code
    except typer.Exit as exc:
        return exc.exit_code
    return 0


main = run
