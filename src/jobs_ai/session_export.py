from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
import re
from collections.abc import Sequence

from .launch_preview import LaunchPreview, select_launch_preview
from .session_manifest import SessionSelectionScope

_LABEL_RE = re.compile(r"[^A-Za-z0-9._-]+")


@dataclass(frozen=True, slots=True)
class SessionExportResult:
    export_path: Path
    created_at: str
    item_count: int
    limit: int | None
    label: str | None
    selection_scope: SessionSelectionScope | None


def export_launch_preview_session(
    database_path: Path,
    exports_dir: Path,
    *,
    limit: int | None = None,
    created_at: datetime | None = None,
    label: str | None = None,
    selection_scope: SessionSelectionScope | None = None,
) -> SessionExportResult:
    previews = select_launch_preview(database_path, limit=limit)
    return export_launch_previews_session(
        previews,
        exports_dir,
        limit=limit,
        created_at=created_at,
        label=label,
        selection_scope=selection_scope,
    )


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


def _normalize_created_at(created_at: datetime | None) -> datetime:
    if created_at is None:
        return datetime.now(timezone.utc)
    if created_at.tzinfo is None:
        return created_at.replace(tzinfo=timezone.utc)
    return created_at.astimezone(timezone.utc)


def _format_created_at(created_at: datetime) -> str:
    return created_at.isoformat(timespec="seconds").replace("+00:00", "Z")


def _build_export_filename(created_at: datetime, *, label: str | None = None) -> str:
    if label is not None:
        return f"launch-preview-session-{label}-{created_at.strftime('%Y%m%dT%H%M%S%fZ')}.json"
    return f"launch-preview-session-{created_at.strftime('%Y%m%dT%H%M%S%fZ')}.json"


def _normalize_label(label: str | None) -> str | None:
    if label is None:
        return None
    normalized_label = _LABEL_RE.sub("-", label.strip()).strip("-.")
    if not normalized_label:
        raise ValueError("label must contain at least one letter or number")
    return normalized_label


def _selection_scope_payload(selection_scope: SessionSelectionScope) -> dict[str, str | None]:
    return {
        "batch_id": selection_scope.batch_id,
        "source_query": selection_scope.source_query,
        "import_source": selection_scope.import_source,
        "selection_mode": selection_scope.selection_mode,
        "refresh_batch_id": selection_scope.refresh_batch_id,
    }
