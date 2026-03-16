from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

from .application_tracking import (
    ApplicationStatusSnapshot,
    normalize_session_mark_status,
    record_application_statuses,
)
from .launch_plan import build_launch_plan
from .session_manifest import ManifestItem, SessionManifest, load_session_manifest


@dataclass(frozen=True, slots=True)
class SessionMarkIssue:
    target: str
    reason: str


@dataclass(frozen=True, slots=True)
class SessionMarkResult:
    requested_status: str
    source_label: str
    manifest_path: Path | None
    scope_label: str
    selected_job_ids: tuple[int, ...]
    selected_indexes: tuple[int, ...]
    manifest_item_count: int | None
    manifest_launchable_count: int | None
    updated: tuple[ApplicationStatusSnapshot, ...]
    skipped: tuple[SessionMarkIssue, ...]


@dataclass(frozen=True, slots=True)
class _ResolvedSessionTargets:
    source_label: str
    manifest_path: Path | None
    scope_label: str
    job_ids: tuple[int, ...]
    selected_indexes: tuple[int, ...]
    manifest_item_count: int | None
    manifest_launchable_count: int | None
    skipped: tuple[SessionMarkIssue, ...]


def mark_session_jobs(
    database_path: Path,
    *,
    status: str,
    job_ids: Sequence[int],
    manifest_path: Path | None = None,
    all_items: bool = False,
    indexes: Sequence[int] = (),
) -> SessionMarkResult:
    normalized_status = normalize_session_mark_status(status)
    targets = resolve_session_mark_targets(
        job_ids=job_ids,
        manifest_path=manifest_path,
        all_items=all_items,
        indexes=indexes,
    )
    batch_result = record_application_statuses(
        database_path,
        job_ids=targets.job_ids,
        status=normalized_status,
    )
    skipped = list(targets.skipped)
    skipped.extend(
        SessionMarkIssue(target=f"job {issue.job_id}", reason=issue.reason)
        for issue in batch_result.skipped
    )
    return SessionMarkResult(
        requested_status=normalized_status,
        source_label=targets.source_label,
        manifest_path=targets.manifest_path,
        scope_label=targets.scope_label,
        selected_job_ids=targets.job_ids,
        selected_indexes=targets.selected_indexes,
        manifest_item_count=targets.manifest_item_count,
        manifest_launchable_count=targets.manifest_launchable_count,
        updated=batch_result.updated,
        skipped=tuple(skipped),
    )


def resolve_session_mark_targets(
    *,
    job_ids: Sequence[int],
    manifest_path: Path | None,
    all_items: bool,
    indexes: Sequence[int],
) -> _ResolvedSessionTargets:
    if manifest_path is None:
        return _resolve_direct_job_ids(job_ids=job_ids, all_items=all_items, indexes=indexes)
    return _resolve_manifest_targets(
        manifest_path=manifest_path,
        job_ids=job_ids,
        all_items=all_items,
        indexes=indexes,
    )


def _resolve_direct_job_ids(
    *,
    job_ids: Sequence[int],
    all_items: bool,
    indexes: Sequence[int],
) -> _ResolvedSessionTargets:
    if all_items:
        raise ValueError("--all requires --manifest")
    if indexes:
        raise ValueError("--indexes requires --manifest")
    if not job_ids:
        raise ValueError("provide one or more job ids, or use --manifest with --all")

    normalized_job_ids = tuple(_require_positive_int(job_id, label="job id") for job_id in job_ids)
    return _ResolvedSessionTargets(
        source_label="job ids",
        manifest_path=None,
        scope_label="direct job ids",
        job_ids=normalized_job_ids,
        selected_indexes=(),
        manifest_item_count=None,
        manifest_launchable_count=None,
        skipped=(),
    )


def _resolve_manifest_targets(
    *,
    manifest_path: Path,
    job_ids: Sequence[int],
    all_items: bool,
    indexes: Sequence[int],
) -> _ResolvedSessionTargets:
    if job_ids:
        raise ValueError("choose one target mode: direct job ids or --manifest, not both")
    if all_items and indexes:
        raise ValueError("choose either --all or --indexes when using --manifest")
    if not all_items and not indexes:
        raise ValueError("with --manifest, provide --all or at least one --indexes value")

    manifest = load_session_manifest(manifest_path)
    plan = build_launch_plan(manifest)
    if all_items:
        return _resolve_all_manifest_targets(manifest=manifest, launchable_item_count=plan.launchable_items)
    return _resolve_manifest_index_targets(
        manifest=manifest,
        launchable_item_count=plan.launchable_items,
        indexes=indexes,
    )


def _resolve_all_manifest_targets(
    *,
    manifest: SessionManifest,
    launchable_item_count: int,
) -> _ResolvedSessionTargets:
    selected_items = tuple(item for item in manifest.items if not item.warnings)
    if not selected_items:
        raise ValueError("manifest has no launchable items to mark with --all")

    job_ids, skipped = _job_ids_from_manifest_items(selected_items)
    return _ResolvedSessionTargets(
        source_label="manifest",
        manifest_path=manifest.manifest_path,
        scope_label=f"all launchable items ({len(selected_items)} of {manifest.item_count})",
        job_ids=job_ids,
        selected_indexes=tuple(item.index for item in selected_items),
        manifest_item_count=manifest.item_count,
        manifest_launchable_count=launchable_item_count,
        skipped=skipped,
    )


def _resolve_manifest_index_targets(
    *,
    manifest: SessionManifest,
    launchable_item_count: int,
    indexes: Sequence[int],
) -> _ResolvedSessionTargets:
    selected_items = []
    skipped: list[SessionMarkIssue] = []
    selected_indexes: list[int] = []
    seen_indexes: set[int] = set()

    for raw_index in indexes:
        index = _require_positive_int(raw_index, label="manifest index")
        if index in seen_indexes:
            skipped.append(
                SessionMarkIssue(
                    target=f"manifest index {index}",
                    reason="duplicate manifest index ignored",
                )
            )
            continue
        seen_indexes.add(index)
        if index > manifest.item_count:
            skipped.append(
                SessionMarkIssue(
                    target=f"manifest index {index}",
                    reason=f"index exceeds manifest size {manifest.item_count}",
                )
            )
            continue
        selected_indexes.append(index)
        selected_items.append(manifest.items[index - 1])

    job_ids, item_issues = _job_ids_from_manifest_items(selected_items)
    skipped.extend(item_issues)
    return _ResolvedSessionTargets(
        source_label="manifest",
        manifest_path=manifest.manifest_path,
        scope_label=_format_manifest_index_scope(selected_indexes),
        job_ids=job_ids,
        selected_indexes=tuple(selected_indexes),
        manifest_item_count=manifest.item_count,
        manifest_launchable_count=launchable_item_count,
        skipped=tuple(skipped),
    )


def _job_ids_from_manifest_items(
    items: Sequence[ManifestItem],
) -> tuple[tuple[int, ...], tuple[SessionMarkIssue, ...]]:
    resolved_job_ids: list[int] = []
    skipped: list[SessionMarkIssue] = []
    seen_job_ids: set[int] = set()

    for item in items:
        job_id = item.job_id
        if job_id is None:
            skipped.append(
                SessionMarkIssue(
                    target=f"manifest index {item.index}",
                    reason="manifest item is missing job_id",
                )
            )
            continue
        if job_id in seen_job_ids:
            skipped.append(
                SessionMarkIssue(
                    target=f"manifest index {item.index}",
                    reason=f"job id {job_id} already selected",
                )
            )
            continue
        seen_job_ids.add(job_id)
        resolved_job_ids.append(job_id)

    return tuple(resolved_job_ids), tuple(skipped)


def _format_manifest_index_scope(indexes: Sequence[int]) -> str:
    if not indexes:
        return "manifest indexes none"
    return "manifest indexes " + ", ".join(str(index) for index in indexes)


def _require_positive_int(value: int, *, label: str) -> int:
    number = int(value)
    if number < 1:
        raise ValueError(f"{label} values must be greater than or equal to 1")
    return number
