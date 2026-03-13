from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path

from .launch_preview import LaunchPreview, select_launch_preview


@dataclass(frozen=True, slots=True)
class SessionExportResult:
    export_path: Path
    created_at: str
    item_count: int
    limit: int | None


def export_launch_preview_session(
    database_path: Path,
    exports_dir: Path,
    *,
    limit: int | None = None,
    created_at: datetime | None = None,
) -> SessionExportResult:
    previews = select_launch_preview(database_path, limit=limit)
    created_at_dt = _normalize_created_at(created_at)
    created_at_text = _format_created_at(created_at_dt)
    export_path = exports_dir / _build_export_filename(created_at_dt)
    payload = {
        "created_at": created_at_text,
        "item_count": len(previews),
        "items": [_record_from_preview(preview) for preview in previews],
    }

    exports_dir.mkdir(parents=True, exist_ok=True)
    with export_path.open("w", encoding="utf-8") as output_file:
        json.dump(payload, output_file, indent=2, ensure_ascii=True)
        output_file.write("\n")

    return SessionExportResult(
        export_path=export_path,
        created_at=created_at_text,
        item_count=len(previews),
        limit=limit,
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


def _build_export_filename(created_at: datetime) -> str:
    return f"launch-preview-session-{created_at.strftime('%Y%m%dT%H%M%S%fZ')}.json"
