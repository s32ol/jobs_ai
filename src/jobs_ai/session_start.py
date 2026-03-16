from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from .db import get_ingest_batch_summary, record_session_history
from .launch_dry_run import LaunchDryRun, build_launch_dry_run
from .launch_executor import (
    BROWSER_STUB_EXECUTOR_MODE,
    LaunchExecutionReport,
    collect_launch_execution_reports_for_steps,
    select_launch_executor,
)
from .launch_plan import LaunchPlan, build_launch_plan
from .launch_preview import LaunchPreview, select_launch_preview
from .portal_support import PortalSupport, build_portal_support
from .resume.config import resolve_resume_variant
from .session_export import SessionExportResult, export_launch_previews_session
from .session_manifest import SessionManifest, SessionSelectionScope, load_session_manifest

DEFAULT_SESSION_START_LIMIT = 25


@dataclass(frozen=True, slots=True)
class SessionStartItem:
    preview: LaunchPreview
    portal_support: PortalSupport | None
    resume_variant_summary: str
    resolved_resume_path: Path | None
    resume_fallback_reason: str | None


@dataclass(frozen=True, slots=True)
class SessionStartResult:
    export_result: SessionExportResult
    manifest: SessionManifest
    plan: LaunchPlan
    dry_run: LaunchDryRun
    items: tuple[SessionStartItem, ...]
    limit: int
    open_requested: bool
    executor_mode: str | None
    selection_scope: SessionSelectionScope | None
    session_history_id: int | None
    execution_reports: tuple[LaunchExecutionReport, ...]

    @property
    def selected_count(self) -> int:
        return len(self.items)

    @property
    def resume_recommendation_count(self) -> int:
        return sum(
            1
            for item in self.items
            if item.preview.resume_variant_key.strip() and item.preview.resume_variant_label.strip()
        )

    @property
    def portal_hint_count(self) -> int:
        return sum(1 for item in self.items if item.portal_support is not None)

    @property
    def resolved_resume_count(self) -> int:
        return sum(1 for item in self.items if item.resolved_resume_path is not None)


def start_session(
    database_path: Path,
    *,
    project_root: Path,
    default_exports_dir: Path,
    limit: int = DEFAULT_SESSION_START_LIMIT,
    out_dir: Path | None = None,
    label: str | None = None,
    open_urls: bool = False,
    executor_mode: str | None = None,
    created_at: datetime | None = None,
    ingest_batch_id: str | None = None,
    source_query: str | None = None,
) -> SessionStartResult:
    if executor_mode is not None and not open_urls:
        raise ValueError("--executor can only be used together with --open")

    export_dir = _resolve_output_dir(
        project_root=project_root,
        default_exports_dir=default_exports_dir,
        out_dir=out_dir,
    )
    selection_scope = _resolve_selection_scope(
        database_path,
        ingest_batch_id=ingest_batch_id,
        source_query=source_query,
    )
    previews = select_launch_preview(
        database_path,
        limit=limit,
        ingest_batch_id=selection_scope.batch_id if selection_scope is not None else None,
    )
    items = tuple(
        _build_session_start_item(
            preview,
            project_root=project_root,
        )
        for preview in previews
    )
    export_result = export_launch_previews_session(
        previews,
        export_dir,
        limit=limit,
        created_at=created_at,
        label=label,
        selection_scope=selection_scope,
    )
    manifest = load_session_manifest(export_result.export_path)
    plan = build_launch_plan(manifest)
    dry_run = build_launch_dry_run(plan)
    session_history_id = record_session_history(
        database_path,
        manifest_path=export_result.export_path,
        item_count=len(previews),
        launchable_count=plan.launchable_items,
        batch_id=selection_scope.batch_id if selection_scope is not None else None,
        source_query=selection_scope.source_query if selection_scope is not None else None,
        created_at=export_result.created_at,
    )

    resolved_executor_mode = None
    execution_reports: tuple[LaunchExecutionReport, ...] = ()
    if open_urls:
        resolved_executor_mode = (
            BROWSER_STUB_EXECUTOR_MODE if executor_mode is None else executor_mode
        )
        execution_reports = collect_launch_execution_reports_for_steps(
            dry_run.steps,
            select_launch_executor(resolved_executor_mode),
        )

    return SessionStartResult(
        export_result=export_result,
        manifest=manifest,
        plan=plan,
        dry_run=dry_run,
        items=items,
        limit=limit,
        open_requested=open_urls,
        executor_mode=resolved_executor_mode,
        selection_scope=selection_scope,
        session_history_id=session_history_id,
        execution_reports=execution_reports,
    )


def _resolve_output_dir(
    *,
    project_root: Path,
    default_exports_dir: Path,
    out_dir: Path | None,
) -> Path:
    if out_dir is None:
        return default_exports_dir
    if out_dir.is_absolute():
        return out_dir
    return (project_root / out_dir).resolve()


def _build_session_start_item(
    preview: LaunchPreview,
    *,
    project_root: Path,
) -> SessionStartItem:
    resolved_resume_variant = resolve_resume_variant(
        preview.resume_variant_key,
        project_root=project_root,
    )
    return SessionStartItem(
        preview=preview,
        portal_support=build_portal_support(preview.apply_url),
        resume_variant_summary=resolved_resume_variant.summary,
        resolved_resume_path=resolved_resume_variant.resolved_path,
        resume_fallback_reason=resolved_resume_variant.fallback_reason,
    )


def _resolve_selection_scope(
    database_path: Path,
    *,
    ingest_batch_id: str | None,
    source_query: str | None,
) -> SessionSelectionScope | None:
    if ingest_batch_id is None and source_query is None:
        return None

    batch_summary = (
        get_ingest_batch_summary(database_path, batch_id=ingest_batch_id)
        if ingest_batch_id is not None
        else None
    )
    resolved_source_query = (
        source_query
        or (batch_summary.source_query if batch_summary is not None else None)
    )
    return SessionSelectionScope(
        batch_id=ingest_batch_id,
        source_query=resolved_source_query,
        import_source=batch_summary.import_source if batch_summary is not None else None,
    )
