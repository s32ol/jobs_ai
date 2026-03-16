from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import re

from ..collect.cli import run_collect_command
from ..collect.fetch import Fetcher, fetch_text
from ..db import initialize_schema
from ..jobs.importer import JobImportResult, import_jobs_from_file
from ..workspace import WorkspacePaths
from .harness import run_discovery
from .models import DiscoverCollectSummary, DiscoverImportSummary, DiscoverRun
from .writers import write_discover_artifacts

_LABEL_RE = re.compile(r"[^A-Za-z0-9._-]+")


def run_discover_command(
    paths: WorkspacePaths,
    *,
    query: str,
    limit: int,
    out_dir: Path | None,
    label: str | None,
    timeout_seconds: float,
    report_only: bool,
    collect: bool,
    import_results: bool,
    created_at: datetime | None = None,
    fetcher: Fetcher | None = None,
) -> DiscoverRun:
    normalized_query = _normalize_query(query)
    created_at_dt = _normalize_created_at(created_at)
    normalized_label = _normalize_label(label)
    run_id = _build_run_id(normalized_label, created_at_dt)
    output_dir = _resolve_output_dir(paths, out_dir=out_dir, run_id=run_id)
    effective_collect = collect or import_results

    run = run_discovery(
        normalized_query,
        limit=limit,
        timeout_seconds=timeout_seconds,
        label=normalized_label,
        report_only=report_only,
        collect_requested=effective_collect,
        import_requested=import_results,
        created_at=created_at_dt,
        fetcher=fetch_text if fetcher is None else fetcher,
    )

    collect_summary: DiscoverCollectSummary | None = None
    import_summary: DiscoverImportSummary | None = None
    if effective_collect:
        collect_summary, import_summary = _run_follow_on_steps(
            paths,
            run=run,
            run_id=run_id,
            output_dir=output_dir,
            label=normalized_label,
            timeout_seconds=timeout_seconds,
            created_at=created_at_dt,
            fetcher=fetch_text if fetcher is None else fetcher,
            import_results=import_results,
        )
    else:
        collect_summary = DiscoverCollectSummary(
            requested=False,
            executed=False,
            status="not_requested",
        )
        import_summary = DiscoverImportSummary(
            requested=False,
            executed=False,
            status="not_requested",
        )

    finalized_at = created_at_dt if created_at is not None else _current_utc_datetime()
    return write_discover_artifacts(
        output_dir,
        run,
        run_id=run_id,
        finished_at=_format_created_at(finalized_at),
        collect_summary=collect_summary,
        import_summary=import_summary,
    )


def _run_follow_on_steps(
    paths: WorkspacePaths,
    *,
    run: DiscoverRun,
    run_id: str,
    output_dir: Path,
    label: str | None,
    timeout_seconds: float,
    created_at: datetime,
    fetcher: Fetcher,
    import_results: bool,
) -> tuple[DiscoverCollectSummary, DiscoverImportSummary]:
    if not run.confirmed_sources:
        return (
            DiscoverCollectSummary(
                requested=True,
                executed=False,
                status="skipped_no_confirmed_sources",
            ),
            DiscoverImportSummary(
                requested=import_results,
                executed=False,
                status="skipped_no_confirmed_sources" if import_results else "not_requested",
            ),
        )

    collect_run = run_collect_command(
        paths,
        sources=run.confirmed_sources,
        from_file=None,
        out_dir=output_dir / "collect",
        label=label,
        timeout_seconds=timeout_seconds,
        report_only=False,
        created_at=created_at,
        fetcher=fetcher,
    )
    collect_artifacts = collect_run.report.artifact_paths
    assert collect_artifacts is not None
    collect_summary = DiscoverCollectSummary(
        requested=True,
        executed=True,
        status="success",
        output_dir=collect_artifacts.output_dir,
        run_report_path=collect_artifacts.run_report_path,
        leads_path=collect_artifacts.leads_path,
        manual_review_path=collect_artifacts.manual_review_path,
        collected_count=collect_run.report.collected_count,
        manual_review_count=collect_run.report.manual_review_count,
        skipped_count=collect_run.report.skipped_count,
    )

    if not import_results:
        return (
            collect_summary,
            DiscoverImportSummary(
                requested=False,
                executed=False,
                status="not_requested",
            ),
        )

    leads_path = collect_artifacts.leads_path
    if (
        leads_path is None
        or not leads_path.exists()
        or collect_run.report.collected_count == 0
    ):
        return (
            collect_summary,
            DiscoverImportSummary(
                requested=True,
                executed=False,
                status=(
                    "skipped_no_collected_leads"
                    if collect_run.report.collected_count == 0
                    else "skipped_no_leads_artifact"
                ),
            ),
        )

    initialize_schema(paths.database_path)
    result = import_jobs_from_file(
        paths.database_path,
        leads_path,
        batch_id=run_id,
        source_query=run.report.query,
        import_source=str(leads_path),
        created_at=created_at,
    )
    return (
        collect_summary,
        DiscoverImportSummary(
            requested=True,
            executed=True,
            status="success" if not result.errors else "completed_with_errors",
            input_path=leads_path,
            batch_id=result.batch_id,
            source_query=result.source_query,
            inserted_count=result.inserted_count,
            skipped_count=result.skipped_count,
            duplicate_count=result.duplicate_count,
            skipped=result.skipped,
            errors=result.errors,
        ),
    )


def _resolve_output_dir(
    paths: WorkspacePaths,
    *,
    out_dir: Path | None,
    run_id: str,
) -> Path:
    if out_dir is not None:
        if out_dir.is_absolute():
            return out_dir
        return (paths.project_root / out_dir).resolve()
    return paths.processed_dir / run_id


def _normalize_query(query: str) -> str:
    normalized_query = query.strip()
    if not normalized_query:
        raise ValueError("a non-empty discover query is required")
    return normalized_query


def _normalize_label(label: str | None) -> str | None:
    if label is None:
        return None
    normalized_label = _LABEL_RE.sub("-", label.strip()).strip("-.")
    if not normalized_label:
        raise ValueError("label must contain at least one letter or number")
    return normalized_label


def _normalize_created_at(created_at: datetime | None) -> datetime:
    if created_at is None:
        return _current_utc_datetime()
    if created_at.tzinfo is None:
        return created_at.replace(tzinfo=timezone.utc)
    return created_at.astimezone(timezone.utc)


def _current_utc_datetime() -> datetime:
    return datetime.now(timezone.utc)


def _build_run_id(label: str | None, created_at: datetime) -> str:
    stamp = created_at.strftime("%Y%m%dT%H%M%S%fZ")
    if label is None:
        return f"discover-{stamp}"
    return f"discover-{label}-{stamp}"


def _format_created_at(created_at: datetime) -> str:
    return created_at.isoformat(timespec="seconds").replace("+00:00", "Z")
