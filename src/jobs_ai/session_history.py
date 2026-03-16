from __future__ import annotations

from collections import Counter
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path

from .db import (
    SessionHistoryEntry,
    connect_database,
    get_session_history_entry,
    list_recent_session_history,
)
from .launch_dry_run import LaunchDryRun, build_launch_dry_run
from .launch_executor import (
    BROWSER_STUB_EXECUTOR_MODE,
    LaunchExecutionReport,
    collect_launch_execution_reports_for_steps,
    select_launch_executor,
)
from .launch_plan import LaunchPlan, build_launch_plan
from .session_manifest import SessionManifest, load_session_manifest

DEFAULT_SESSION_RECENT_LIMIT = 10


@dataclass(frozen=True, slots=True)
class SessionInspectionItem:
    index: int
    job_id: int | None
    company: str | None
    title: str | None
    launchable: bool
    warnings: tuple[str, ...]
    current_status: str | None


@dataclass(frozen=True, slots=True)
class SessionStatusCount:
    label: str
    count: int


@dataclass(frozen=True, slots=True)
class ResolvedSessionReference:
    reference_text: str
    manifest_path: Path
    manifest: SessionManifest
    session_history_entry: SessionHistoryEntry | None


@dataclass(frozen=True, slots=True)
class SessionInspection:
    resolved: ResolvedSessionReference
    plan: LaunchPlan
    items: tuple[SessionInspectionItem, ...]
    status_counts: tuple[SessionStatusCount, ...]


@dataclass(frozen=True, slots=True)
class SessionReopenResult:
    resolved: ResolvedSessionReference
    plan: LaunchPlan
    dry_run: LaunchDryRun
    executor_mode: str
    execution_reports: tuple[LaunchExecutionReport, ...]


def recent_sessions(
    database_path: Path,
    *,
    limit: int = DEFAULT_SESSION_RECENT_LIMIT,
) -> tuple[SessionHistoryEntry, ...]:
    return list_recent_session_history(database_path, limit=limit)


def inspect_session(
    database_path: Path,
    *,
    reference: str,
) -> SessionInspection:
    resolved = resolve_session_reference(database_path, reference=reference)
    plan = build_launch_plan(resolved.manifest)
    current_statuses = _load_current_job_statuses(database_path, resolved.manifest)
    items = tuple(
        SessionInspectionItem(
            index=item.index,
            job_id=item.job_id,
            company=item.company,
            title=item.title,
            launchable=not item.warnings,
            warnings=item.warnings,
            current_status=(
                current_statuses.get(item.job_id)
                if item.job_id is not None
                else None
            ),
        )
        for item in resolved.manifest.items
    )
    status_counts_counter = Counter(
        item.current_status
        for item in items
        if item.current_status is not None
    )
    return SessionInspection(
        resolved=resolved,
        plan=plan,
        items=items,
        status_counts=tuple(
            SessionStatusCount(label=label, count=status_counts_counter[label])
            for label in sorted(status_counts_counter)
        ),
    )


def reopen_session(
    database_path: Path,
    *,
    reference: str,
    executor_mode: str = BROWSER_STUB_EXECUTOR_MODE,
) -> SessionReopenResult:
    resolved = resolve_session_reference(database_path, reference=reference)
    plan = build_launch_plan(resolved.manifest)
    dry_run = build_launch_dry_run(plan)
    execution_reports = collect_launch_execution_reports_for_steps(
        dry_run.steps,
        select_launch_executor(executor_mode),
    )
    return SessionReopenResult(
        resolved=resolved,
        plan=plan,
        dry_run=dry_run,
        executor_mode=executor_mode,
        execution_reports=execution_reports,
    )


def resolve_session_reference(
    database_path: Path,
    *,
    reference: str,
) -> ResolvedSessionReference:
    normalized_reference = reference.strip()
    if not normalized_reference:
        raise ValueError("reference must be a manifest path or session id")

    session_history_entry = None
    manifest_path = Path(normalized_reference).expanduser()
    if normalized_reference.isdigit():
        session_history_entry = get_session_history_entry(
            database_path,
            session_id=int(normalized_reference),
        )
        if session_history_entry is not None:
            manifest_path = session_history_entry.manifest_path

    manifest = load_session_manifest(manifest_path)
    return ResolvedSessionReference(
        reference_text=normalized_reference,
        manifest_path=manifest_path,
        manifest=manifest,
        session_history_entry=session_history_entry,
    )


def _load_current_job_statuses(
    database_path: Path,
    manifest: SessionManifest,
) -> dict[int, str]:
    job_ids = tuple(
        item.job_id
        for item in manifest.items
        if item.job_id is not None
    )
    if not job_ids:
        return {}

    placeholders = ", ".join("?" for _ in job_ids)
    with closing(connect_database(database_path)) as connection:
        rows = connection.execute(
            f"SELECT id, status FROM jobs WHERE id IN ({placeholders})",
            job_ids,
        ).fetchall()
    return {
        int(row["id"]): str(row["status"])
        for row in rows
    }
