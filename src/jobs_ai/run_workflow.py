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
from .session_start import SessionStartResult, start_session
from .workspace import WorkspacePaths

DEFAULT_RUN_DISCOVER_LIMIT = 20


@dataclass(frozen=True, slots=True)
class RunWorkflowResult:
    query: str
    output_dir: Path
    discover_run: DiscoverRun
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

    @property
    def confirmed_source_count(self) -> int:
        return len(self.discover_run.confirmed_sources)

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
    created_at: datetime | None = None,
    fetcher: Fetcher | None = None,
) -> RunWorkflowResult:
    effective_fetcher = fetch_text if fetcher is None else fetcher
    effective_created_at = _normalize_created_at(created_at)
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
        created_at=effective_created_at,
        fetcher=effective_fetcher,
    )
    artifact_paths = discover_run.report.artifact_paths
    assert artifact_paths is not None
    workflow_dir = artifact_paths.output_dir
    workflow_batch_id = discover_run.report.run_id or workflow_dir.name

    collected_sources = _select_collection_sources(
        discover_run.confirmed_sources,
        collect_limit=collect_limit,
    )
    collect_run: CollectRun | None = None
    import_result: JobImportResult | None = None

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
    )

    return RunWorkflowResult(
        query=query,
        output_dir=workflow_dir,
        discover_run=discover_run,
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
