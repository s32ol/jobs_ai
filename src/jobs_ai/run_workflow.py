from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from .collect.cli import run_collect_command
from .collect.fetch import Fetcher, fetch_text
from .collect.models import CollectRun
from .db import initialize_schema
from .discover.cli import run_discover_command
from .discover.models import DiscoverRun
from .jobs.importer import JobImportResult, import_jobs_from_file
from .launch_preview import select_launch_preview
from .maintenance import InvalidLocationMarkResult, mark_invalid_location_jobs
from .session_manifest import SessionSelectionScope
from .session_start import SessionStartResult, start_session
from .sources.models import SourceRegistryCollectResult
from .sources.workflow import collect_registry_sources
from .workspace import WorkspacePaths

DEFAULT_RUN_DISCOVER_LIMIT = 20


class DiscoverSearchWorkflowError(RuntimeError):
    def __init__(self, discover_run: DiscoverRun) -> None:
        super().__init__("discovery search failed before collection, import, or session start")
        self.discover_run = discover_run


@dataclass(frozen=True, slots=True)
class RunWorkflowResult:
    query: str
    output_dir: Path
    intake_mode: str
    discover_run: DiscoverRun | None
    registry_collect_result: SourceRegistryCollectResult | None
    collected_sources: tuple[str, ...]
    collect_run: CollectRun | None
    import_result: JobImportResult | None
    session_result: SessionStartResult
    discover_limit: int
    collect_limit: int | None
    session_limit: int
    label: str | None
    open_requested: bool
    executor_mode: str | None
    us_only: bool
    location_guard_result: InvalidLocationMarkResult | None

    @property
    def confirmed_source_count(self) -> int:
        if self.discover_run is not None:
            return len(self.discover_run.confirmed_sources)
        if self.registry_collect_result is not None:
            return self.registry_collect_result.selected_source_count
        return 0

    @property
    def registry_verified_source_count(self) -> int:
        if self.registry_collect_result is None:
            return 0
        return self.registry_collect_result.verified_source_count

    @property
    def imported_jobs_count(self) -> int:
        if self.import_result is None:
            return 0
        return self.import_result.inserted_count

    @property
    def recommendation_count(self) -> int:
        return self.session_result.resume_recommendation_count

    @property
    def portal_hint_count(self) -> int:
        return self.session_result.portal_hint_count

    @property
    def manifest_path(self) -> Path:
        return self.session_result.export_result.export_path


def run_operator_workflow(
    paths: WorkspacePaths,
    *,
    query: str,
    discover_limit: int = DEFAULT_RUN_DISCOVER_LIMIT,
    collect_limit: int | None = None,
    session_limit: int,
    out_dir: Path | None,
    label: str | None,
    timeout_seconds: float = 10.0,
    open_urls: bool = False,
    executor_mode: str | None = None,
    capture_search_artifacts: bool = False,
    use_registry: bool = False,
    us_only: bool = False,
    created_at: datetime | None = None,
    fetcher: Fetcher | None = None,
) -> RunWorkflowResult:
    effective_fetcher = fetch_text if fetcher is None else fetcher
    effective_created_at = _normalize_created_at(created_at)
    if use_registry:
        registry_collect_result = collect_registry_sources(
            paths,
            source_ids=(),
            limit=collect_limit,
            out_dir=out_dir,
            label=label,
            timeout_seconds=timeout_seconds,
            verify_if_needed=True,
            force_verify=False,
            import_results=True,
            source_query=query,
            created_at=effective_created_at,
            fetcher=effective_fetcher,
        )
        collect_run = registry_collect_result.collect_run
        collect_artifacts = collect_run.report.artifact_paths
        assert collect_artifacts is not None
        workflow_dir = collect_artifacts.output_dir
        batch_id = (
            registry_collect_result.import_result.batch_id
            if registry_collect_result.import_result is not None
            else (collect_run.report.run_id or workflow_dir.name)
        )
        session_batch_id = batch_id
        session_selection_scope: SessionSelectionScope | None = None
        if registry_collect_result.import_result is not None:
            import_result = registry_collect_result.import_result
            if import_result.inserted_count == 0:
                session_batch_id, session_selection_scope = _registry_reuse_existing_selection_scope(
                    query=query,
                    import_result=import_result,
                    selection_mode="registry_refresh_empty_reused_existing",
                )
            else:
                session_selection_scope = SessionSelectionScope(
                    batch_id=import_result.batch_id,
                    source_query=query,
                    import_source=import_result.import_source,
                    selection_mode="registry_new_imports",
                    refresh_batch_id=import_result.batch_id,
                )
        location_guard_result: InvalidLocationMarkResult | None = None
        if us_only:
            location_guard_result = mark_invalid_location_jobs(
                paths.database_path,
                us_only=True,
                ingest_batch_id=session_batch_id,
                query_text=query if session_batch_id is None else None,
                actionable_only=True,
            )
        if (
            registry_collect_result.import_result is not None
            and session_batch_id is not None
            and not select_launch_preview(
                paths.database_path,
                limit=1,
                ingest_batch_id=session_batch_id,
                query_text=query,
                us_only=us_only,
            )
        ):
            session_batch_id, session_selection_scope = _registry_reuse_existing_selection_scope(
                query=query,
                import_result=registry_collect_result.import_result,
                selection_mode="registry_refresh_no_actionable_new_reused_existing",
            )
            if us_only:
                location_guard_result = mark_invalid_location_jobs(
                    paths.database_path,
                    us_only=True,
                    ingest_batch_id=None,
                    query_text=query,
                    actionable_only=True,
                )
        initialize_schema(paths.database_path)
        session_result = start_session(
            paths.database_path,
            project_root=paths.project_root,
            default_exports_dir=paths.exports_dir,
            limit=session_limit,
            out_dir=workflow_dir,
            label=label,
            open_urls=open_urls,
            executor_mode=executor_mode,
            created_at=effective_created_at,
            ingest_batch_id=session_batch_id,
            source_query=query,
            job_query=query,
            selection_scope=session_selection_scope,
            us_only=us_only,
        )
        return RunWorkflowResult(
            query=query,
            output_dir=workflow_dir,
            intake_mode="registry",
            discover_run=None,
            registry_collect_result=registry_collect_result,
            collected_sources=collect_run.report.input_sources,
            collect_run=collect_run,
            import_result=registry_collect_result.import_result,
            session_result=session_result,
            discover_limit=discover_limit,
            collect_limit=collect_limit,
            session_limit=session_limit,
            label=label,
            open_requested=open_urls,
            executor_mode=session_result.executor_mode,
            us_only=us_only,
            location_guard_result=location_guard_result,
        )

    discover_run = run_discover_command(
        paths,
        query=query,
        limit=discover_limit,
        out_dir=out_dir,
        label=label,
        timeout_seconds=timeout_seconds,
        report_only=False,
        collect=False,
        import_results=False,
        capture_search_artifacts=capture_search_artifacts,
        created_at=effective_created_at,
        fetcher=effective_fetcher,
    )
    artifact_paths = discover_run.report.artifact_paths
    assert artifact_paths is not None
    if discover_run.report.has_fatal_search_failure:
        raise DiscoverSearchWorkflowError(discover_run)
    workflow_dir = artifact_paths.output_dir
    workflow_batch_id = discover_run.report.run_id or workflow_dir.name

    collected_sources = _select_collection_sources(
        discover_run.confirmed_sources,
        collect_limit=collect_limit,
    )
    collect_run: CollectRun | None = None
    import_result: JobImportResult | None = None

    location_guard_result: InvalidLocationMarkResult | None = None
    if collected_sources:
        collect_run = run_collect_command(
            paths,
            sources=collected_sources,
            from_file=None,
            out_dir=workflow_dir / "collect",
            label=label,
            timeout_seconds=timeout_seconds,
            report_only=False,
            created_at=effective_created_at,
            fetcher=effective_fetcher,
        )
        collect_artifacts = collect_run.report.artifact_paths
        assert collect_artifacts is not None
        leads_path = collect_artifacts.leads_path
        if (
            leads_path is not None
            and leads_path.exists()
            and collect_run.report.collected_count > 0
        ):
            initialize_schema(paths.database_path)
            import_result = import_jobs_from_file(
                paths.database_path,
                leads_path,
                batch_id=workflow_batch_id,
                source_query=discover_run.report.query,
                import_source=str(leads_path),
                created_at=effective_created_at,
            )
            if us_only:
                location_guard_result = mark_invalid_location_jobs(
                    paths.database_path,
                    us_only=True,
                    ingest_batch_id=workflow_batch_id,
                    actionable_only=True,
                )

    initialize_schema(paths.database_path)
    session_result = start_session(
        paths.database_path,
        project_root=paths.project_root,
        default_exports_dir=paths.exports_dir,
        limit=session_limit,
        out_dir=workflow_dir,
        label=label,
        open_urls=open_urls,
        executor_mode=executor_mode,
        created_at=effective_created_at,
        ingest_batch_id=workflow_batch_id,
        source_query=discover_run.report.query,
        us_only=us_only,
    )

    return RunWorkflowResult(
        query=query,
        output_dir=workflow_dir,
        intake_mode="discover",
        discover_run=discover_run,
        registry_collect_result=None,
        collected_sources=collected_sources,
        collect_run=collect_run,
        import_result=import_result,
        session_result=session_result,
        discover_limit=discover_limit,
        collect_limit=collect_limit,
        session_limit=session_limit,
        label=label,
        open_requested=open_urls,
        executor_mode=session_result.executor_mode,
        us_only=us_only,
        location_guard_result=location_guard_result,
    )


def _select_collection_sources(
    confirmed_sources: tuple[str, ...],
    *,
    collect_limit: int | None,
) -> tuple[str, ...]:
    if collect_limit is None:
        return confirmed_sources
    return confirmed_sources[:collect_limit]


def _normalize_created_at(created_at: datetime | None) -> datetime:
    if created_at is None:
        return datetime.now(timezone.utc)
    if created_at.tzinfo is None:
        return created_at.replace(tzinfo=timezone.utc)
    return created_at.astimezone(timezone.utc)


def _registry_reuse_existing_selection_scope(
    *,
    query: str,
    import_result: JobImportResult,
    selection_mode: str,
) -> tuple[None, SessionSelectionScope]:
    return (
        None,
        SessionSelectionScope(
            batch_id=None,
            source_query=query,
            import_source=None,
            selection_mode=selection_mode,
            refresh_batch_id=import_result.batch_id,
        ),
    )
