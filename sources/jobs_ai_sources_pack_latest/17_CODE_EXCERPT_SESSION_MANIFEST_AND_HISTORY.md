# Code Excerpt: Session Manifest and History

Exact excerpts from the current repo for manifest export, manifest validation, inspect/reopen behavior, direct open behavior, and session mark resolution.

## Manifest export writer
Source: `src/jobs_ai/session_export.py` lines 46-107

```python
def export_launch_previews_session(
    previews: Sequence[LaunchPreview],
    exports_dir: Path,
    *,
    limit: int | None = None,
    created_at: datetime | None = None,
    label: str | None = None,
    selection_scope: SessionSelectionScope | None = None,
) -> SessionExportResult:
    created_at_dt = _normalize_created_at(created_at)
    created_at_text = _format_created_at(created_at_dt)
    normalized_label = _normalize_label(label)
    export_path = exports_dir / _build_export_filename(created_at_dt, label=normalized_label)
    payload = {
        "created_at": created_at_text,
        "item_count": len(previews),
        "items": [_record_from_preview(preview) for preview in previews],
    }
    if normalized_label is not None:
        payload["label"] = normalized_label
    if selection_scope is not None:
        payload["selection_scope"] = _selection_scope_payload(selection_scope)

    exports_dir.mkdir(parents=True, exist_ok=True)
    with export_path.open("w", encoding="utf-8") as output_file:
        json.dump(payload, output_file, indent=2, ensure_ascii=True)
        output_file.write("\n")

    return SessionExportResult(
        export_path=export_path,
        created_at=created_at_text,
        item_count=len(previews),
        limit=limit,
        label=normalized_label,
        selection_scope=selection_scope,
    )


def _record_from_preview(preview: LaunchPreview) -> dict[str, object | None]:
    return {
        "rank": preview.rank,
        "job_id": preview.job_id,
        "company": preview.company,
        "title": preview.title,
        "location": preview.location,
        "source": preview.source,
        "apply_url": preview.apply_url,
        "portal_type": preview.portal_type,
        "score": preview.score,
        "recommended_resume_variant": {
            "key": preview.resume_variant_key,
            "label": preview.resume_variant_label,
        },
        "recommended_profile_snippet": {
            "key": preview.snippet_key,
            "label": preview.snippet_label,
            "text": preview.snippet_text,
        },
        "explanation": preview.explanation,
    }
```

## Session manifest schema and validation
Source: `src/jobs_ai/session_manifest.py` lines 1-231

```python
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import json
from pathlib import Path


@dataclass(frozen=True, slots=True)
class ManifestSelection:
    key: str | None
    label: str | None
    text: str | None = None


@dataclass(frozen=True, slots=True)
class ManifestItem:
    index: int
    rank: int | None
    job_id: int | None
    company: str | None
    title: str | None
    apply_url: str | None
    portal_type: str | None
    recommended_resume_variant: ManifestSelection | None
    recommended_profile_snippet: ManifestSelection | None
    warnings: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class SessionSelectionScope:
    batch_id: str | None
    source_query: str | None
    import_source: str | None
    selection_mode: str | None = None
    refresh_batch_id: str | None = None


@dataclass(frozen=True, slots=True)
class SessionManifest:
    manifest_path: Path
    created_at: str
    label: str | None
    selection_scope: SessionSelectionScope | None
    item_count: int
    items: tuple[ManifestItem, ...]

    @property
    def warning_count(self) -> int:
        return sum(len(item.warnings) for item in self.items)


def load_session_manifest(manifest_path: Path) -> SessionManifest:
    payload = _load_manifest_payload(manifest_path)
    if not isinstance(payload, dict):
        raise ValueError("manifest must be a JSON object")

    created_at = _require_iso8601_timestamp(payload.get("created_at"), "manifest.created_at")
    label = _optional_string(payload.get("label"), "manifest.label")
    selection_scope = _selection_scope_from_payload(
        payload.get("selection_scope"),
        "manifest.selection_scope",
    )
    item_count = _require_int(payload.get("item_count"), "manifest.item_count")
    if item_count < 0:
        raise ValueError("manifest.item_count must be greater than or equal to 0")

    items_payload = payload.get("items")
    if not isinstance(items_payload, list):
        raise ValueError("manifest.items must be a list")

    items = tuple(
        _item_from_payload(index, item_payload)
        for index, item_payload in enumerate(items_payload, start=1)
    )
    if item_count != len(items):
        raise ValueError(
            f"manifest.item_count ({item_count}) does not match items length ({len(items)})"
        )

    return SessionManifest(
        manifest_path=manifest_path,
        created_at=created_at,
        label=label,
        selection_scope=selection_scope,
        item_count=item_count,
        items=items,
    )


def _load_manifest_payload(manifest_path: Path) -> object:
    try:
        with manifest_path.open("r", encoding="utf-8") as input_file:
            return json.load(input_file)
    except FileNotFoundError as exc:
        raise ValueError(f"manifest was not found: {manifest_path}") from exc
    except OSError as exc:
        raise ValueError(f"manifest could not be read: {exc}") from exc
    except json.JSONDecodeError as exc:
        raise ValueError(f"manifest is not valid JSON: {exc.msg}") from exc


def _item_from_payload(index: int, payload: object) -> ManifestItem:
    path = f"manifest.items[{index - 1}]"
    if not isinstance(payload, dict):
        raise ValueError(f"{path} must be a JSON object")

    resume_variant = _selection_from_payload(
        payload.get("recommended_resume_variant"),
        f"{path}.recommended_resume_variant",
    )
    profile_snippet = _selection_from_payload(
        payload.get("recommended_profile_snippet"),
        f"{path}.recommended_profile_snippet",
    )
    warnings = []
    job_id = _optional_int(payload.get("job_id"), f"{path}.job_id")
    if job_id is not None and job_id < 1:
        raise ValueError(f"{path}.job_id must be greater than or equal to 1")

    company = _optional_string(payload.get("company"), f"{path}.company")
    if company is None:
        warnings.append("company missing")

    title = _optional_string(payload.get("title"), f"{path}.title")
    if title is None:
        warnings.append("title missing")

    apply_url = _optional_string(payload.get("apply_url"), f"{path}.apply_url")
    if apply_url is None:
        warnings.append("apply_url missing")
    portal_type = _optional_string(payload.get("portal_type"), f"{path}.portal_type")

    if _selection_is_incomplete(resume_variant, require_text=False):
        warnings.append("recommended_resume_variant incomplete")
    if _selection_is_incomplete(profile_snippet, require_text=True):
        warnings.append("recommended_profile_snippet incomplete")

    return ManifestItem(
        index=index,
        rank=_optional_int(payload.get("rank"), f"{path}.rank"),
        job_id=job_id,
        company=company,
        title=title,
        apply_url=apply_url,
        portal_type=portal_type,
        recommended_resume_variant=resume_variant,
        recommended_profile_snippet=profile_snippet,
        warnings=tuple(warnings),
    )


def _selection_from_payload(payload: object, path: str) -> ManifestSelection | None:
    if payload is None:
        return None
    if not isinstance(payload, dict):
        raise ValueError(f"{path} must be a JSON object when present")

    return ManifestSelection(
        key=_optional_string(payload.get("key"), f"{path}.key"),
        label=_optional_string(payload.get("label"), f"{path}.label"),
        text=_optional_string(payload.get("text"), f"{path}.text"),
    )


def _selection_scope_from_payload(payload: object, path: str) -> SessionSelectionScope | None:
    if payload is None:
        return None
    if not isinstance(payload, dict):
        raise ValueError(f"{path} must be a JSON object when present")

    return SessionSelectionScope(
        batch_id=_optional_string(payload.get("batch_id"), f"{path}.batch_id"),
        source_query=_optional_string(payload.get("source_query"), f"{path}.source_query"),
        import_source=_optional_string(payload.get("import_source"), f"{path}.import_source"),
        selection_mode=_optional_string(payload.get("selection_mode"), f"{path}.selection_mode"),
        refresh_batch_id=_optional_string(
            payload.get("refresh_batch_id"),
            f"{path}.refresh_batch_id",
        ),
    )


def _selection_is_incomplete(selection: ManifestSelection | None, *, require_text: bool) -> bool:
    if selection is None:
        return True
    if selection.key is None or selection.label is None:
        return True
    if require_text and selection.text is None:
        return True
    return False


def _require_iso8601_timestamp(value: object, path: str) -> str:
    text = _require_string(value, path)
    try:
        datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{path} must be an ISO 8601 timestamp string") from exc
    return text


def _require_string(value: object, path: str) -> str:
    text = _optional_string(value, path)
    if text is None:
        raise ValueError(f"{path} must be a non-empty string")
    return text


def _optional_string(value: object, path: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError(f"{path} must be a string or null")
    text = value.strip()
    return text or None


def _require_int(value: object, path: str) -> int:
    number = _optional_int(value, path)
    if number is None:
        raise ValueError(f"{path} must be an integer")
    return number


def _optional_int(value: object, path: str) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{path} must be an integer or null")
    return value
```

## Session inspect and reopen helpers
Source: `src/jobs_ai/session_history.py` lines 1-188

```python
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
```

## Open one manifest item directly
Source: `src/jobs_ai/session_open.py` lines 1-70

```python
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from .launch_dry_run import OPEN_URL_ACTION
from .launch_executor import (
    BROWSER_STUB_EXECUTOR_MODE,
    LaunchExecutionReport,
    select_launch_executor,
)
from .session_manifest import ManifestItem, load_session_manifest


@dataclass(frozen=True, slots=True)
class SessionOpenResult:
    manifest_path: Path
    manifest_item_count: int
    selected_item: ManifestItem
    execution_report: LaunchExecutionReport


@dataclass(frozen=True, slots=True)
class _ManifestOpenStep:
    launch_order: int
    action_label: str
    company: str | None
    title: str | None
    apply_url: str


def open_manifest_item(
    manifest_path: Path,
    *,
    index: int,
    executor_mode: str = BROWSER_STUB_EXECUTOR_MODE,
) -> SessionOpenResult:
    manifest = load_session_manifest(manifest_path)
    resolved_index = _require_manifest_index(index)
    if resolved_index > manifest.item_count:
        raise ValueError(
            f"manifest index {resolved_index} exceeds manifest size {manifest.item_count}"
        )

    selected_item = manifest.items[resolved_index - 1]
    if selected_item.apply_url is None:
        raise ValueError(f"manifest index {resolved_index} is missing apply_url")

    execution_report = select_launch_executor(executor_mode).execute_step(
        _ManifestOpenStep(
            launch_order=selected_item.index,
            action_label=OPEN_URL_ACTION,
            company=selected_item.company,
            title=selected_item.title,
            apply_url=selected_item.apply_url,
        )
    )
    return SessionOpenResult(
        manifest_path=manifest.manifest_path,
        manifest_item_count=manifest.item_count,
        selected_item=selected_item,
        execution_report=execution_report,
    )


def _require_manifest_index(value: int) -> int:
    resolved_value = int(value)
    if resolved_value < 1:
        raise ValueError("manifest index must be at least 1")
    return resolved_value
```

## Session mark and manifest target resolution
Source: `src/jobs_ai/session_mark.py` lines 1-266

```python
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
```
