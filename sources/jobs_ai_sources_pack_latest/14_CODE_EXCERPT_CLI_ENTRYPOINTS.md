# Code Excerpt: CLI Entrypoints

Exact excerpts from the current repo for the canonical CLI entrypoints and the operator-facing commands that matter most.

## Typer app setup and command groups
Source: `src/jobs_ai/cli.py` lines 196-249

```python
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
```

## Primary operator entrypoint: run_command
Source: `src/jobs_ai/cli.py` lines 399-512

```python
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
```

## Session freeze command: session_start
Source: `src/jobs_ai/cli.py` lines 1202-1273

```python
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
```

## Registry-first collection command: sources_collect
Source: `src/jobs_ai/cli.py` lines 1864-1931

```python
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
```

## Application assist command
Source: `src/jobs_ai/cli.py` lines 2196-2322

```python
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
```

## Database backend inspection commands
Source: `src/jobs_ai/cli.py` lines 2543-2564

```python
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
```

## CLI run()/main entrypoint wiring
Source: `src/jobs_ai/cli.py` lines 2671-2692

```python
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
```

## Module entrypoint
Source: `src/jobs_ai/__main__.py` lines 1-7

```python
from __future__ import annotations

from .cli import run


if __name__ == "__main__":
    raise SystemExit(run())
```
