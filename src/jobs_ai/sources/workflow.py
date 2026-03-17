from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from pathlib import Path

from ..collect.cli import run_collect_command
from ..collect.fetch import Fetcher, fetch_text
from ..db import initialize_schema
from ..jobs.importer import import_jobs_from_file
from ..workspace import WorkspacePaths
from .models import SourceRegistryCollectResult
from .registry import (
    get_registry_source,
    list_registry_sources,
    verify_registry_source,
)


def collect_registry_sources(
    paths: WorkspacePaths,
    *,
    source_ids: Sequence[int] = (),
    limit: int | None = None,
    out_dir: Path | None = None,
    label: str | None = None,
    timeout_seconds: float,
    verify_if_needed: bool,
    force_verify: bool,
    import_results: bool,
    source_query: str | None = None,
    created_at: datetime | None = None,
    fetcher: Fetcher | None = None,
) -> SourceRegistryCollectResult:
    initialize_schema(paths.database_path)
    selected_entries = _select_registry_entries(
        paths,
        source_ids=source_ids,
    )
    verification_results = []

    if force_verify or verify_if_needed:
        verification_targets = [
            entry
            for entry in selected_entries
            if force_verify or entry.status != "active" or entry.last_verified_at is None
        ]
        verification_results = [
            verify_registry_source(
                paths.database_path,
                source_id=entry.source_id,
                timeout_seconds=timeout_seconds,
                created_at=created_at,
                fetcher=fetcher,
            )
            for entry in verification_targets
        ]

    active_entries = _active_collection_entries(
        paths,
        source_ids=source_ids,
    )
    if limit is not None:
        active_entries = active_entries[:limit]
    if not active_entries:
        raise ValueError(
            "no active registry sources are ready to collect; add or verify sources first"
        )

    collect_run = run_collect_command(
        paths,
        sources=tuple(entry.source_url for entry in active_entries),
        from_file=None,
        out_dir=out_dir,
        label=label,
        timeout_seconds=timeout_seconds,
        report_only=False,
        created_at=created_at,
        fetcher=fetch_text if fetcher is None else fetcher,
    )

    import_result = None
    if import_results:
        artifact_paths = collect_run.report.artifact_paths
        assert artifact_paths is not None
        leads_path = artifact_paths.leads_path
        if (
            leads_path is not None
            and leads_path.exists()
            and collect_run.report.collected_count > 0
        ):
            import_result = import_jobs_from_file(
                paths.database_path,
                leads_path,
                batch_id=collect_run.report.run_id,
                source_query=source_query,
                import_source=str(leads_path),
                created_at=created_at,
            )

    return SourceRegistryCollectResult(
        selected_entries=active_entries,
        verification_results=tuple(verification_results),
        collect_run=collect_run,
        import_result=import_result,
        import_requested=import_results,
    )


def _select_registry_entries(
    paths: WorkspacePaths,
    *,
    source_ids: Sequence[int],
):
    if source_ids:
        entries = list_registry_sources(paths.database_path, source_ids=source_ids)
        found_ids = {entry.source_id for entry in entries}
        missing_ids = [source_id for source_id in source_ids if source_id not in found_ids]
        if missing_ids:
            missing_text = ", ".join(str(source_id) for source_id in missing_ids)
            raise ValueError(f"registry source ids were not found: {missing_text}")
        return entries

    entries = list_registry_sources(paths.database_path, statuses=("active",))
    if not entries:
        raise ValueError(
            "the source registry does not contain any active sources yet; add sources or sync from seed-sources/discover first"
        )
    return entries


def _active_collection_entries(
    paths: WorkspacePaths,
    *,
    source_ids: Sequence[int],
):
    if source_ids:
        entries = [
            get_registry_source(paths.database_path, source_id=source_id)
            for source_id in source_ids
        ]
        return tuple(
            entry
            for entry in entries
            if entry is not None and entry.status == "active"
        )
    return list_registry_sources(paths.database_path, statuses=("active",))
