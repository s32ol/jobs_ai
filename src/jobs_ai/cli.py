from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path

import click
import typer

from .application_assist import build_application_assist
from .application_tracking import (
    get_application_status,
    list_application_statuses,
    record_application_status,
)
from .config import load_settings
from .db import initialize_schema, missing_required_tables
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
    render_application_tracking_error_report,
    render_application_tracking_list_report,
    render_application_tracking_mark_report,
    render_application_tracking_status_report,
    render_db_init_report,
    render_db_status_report,
    render_doctor_report,
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
    render_score_report,
    render_status_report,
)
from .portal_support import build_portal_support
from .session_manifest import load_session_manifest
from .resume.recommendations import select_queue_recommendations
from .session_export import export_launch_preview_session
from .workspace import build_workspace_paths, ensure_workspace, missing_workspace_paths


app = typer.Typer(
    add_completion=False,
    help=(
        "Local CLI for the jobs_ai job-search exoskeleton.\n\n"
        "Typical sprint flow: init -> db init -> import -> score -> queue -> "
        "recommend -> launch-preview -> export-session -> preflight -> "
        "application-assist -> launch-dry-run -> track."
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
        "Use track mark after you open, apply to, or intentionally skip a job."
    )
)
app.add_typer(db_app, name="db")
app.add_typer(track_app, name="track")


def _load_runtime():
    settings = load_settings()
    paths = build_workspace_paths(settings.database_path)
    return settings, paths


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
) -> None:
    """Import local job leads from a JSON file into the jobs table."""
    settings, paths = _load_runtime()
    del settings
    ensure_workspace(paths)
    initialize_schema(paths.database_path)
    try:
        result = import_jobs_from_file(paths.database_path, input_path)
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
    """Show a deterministic read-only working set of top ranked new jobs."""
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
    """Recommend resume variants and profile snippets for the current queue."""
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
    """Preview queued application session inputs without launching a browser."""
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
    """Export the current read-only launch preview working set to a JSON manifest."""
    settings, paths = _load_runtime()
    del settings
    ensure_workspace(paths)
    initialize_schema(paths.database_path)
    result = export_launch_preview_session(paths.database_path, paths.exports_dir, limit=limit)
    typer.echo(render_export_session_report(paths, result))


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
        help="Manual application status: new, opened, applied, or skipped.",
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
        help="Optional current-status filter: new, opened, applied, or skipped.",
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
