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
    recommended_resume_variant: ManifestSelection | None
    recommended_profile_snippet: ManifestSelection | None
    warnings: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class SessionSelectionScope:
    batch_id: str | None
    source_query: str | None
    import_source: str | None


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
