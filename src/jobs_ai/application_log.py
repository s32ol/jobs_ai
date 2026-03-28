from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
import json
from pathlib import Path
import re
import unicodedata

from .application_tracking import SESSION_MARK_APPLICATION_STATUSES
from .launch_plan import LaunchPlanItem, build_launch_plan
from .portal_support import detect_portal_type
from .session_manifest import load_session_manifest

APPLICATION_LOG_METHOD = "jobs_ai application-assist"
APPLICATION_LOG_TRACKING_STATUSES = tuple(
    status
    for status in SESSION_MARK_APPLICATION_STATUSES
    if status not in {"invalid_location", "superseded"}
)
APPLICATION_LOG_STATUSES = tuple(
    dict.fromkeys((*APPLICATION_LOG_TRACKING_STATUSES, "failed"))
)

_FILENAME_TOKEN_RE = re.compile(r"[^a-z0-9]+")
_HYPHEN_RE = re.compile(r"-{2,}")


@dataclass(frozen=True, slots=True)
class ApplicationLogRecord:
    company: str
    role: str
    portal: str
    apply_url: str
    status: str
    method: str
    notes: str | None
    timestamp: str


@dataclass(frozen=True, slots=True)
class ApplicationLogResult:
    log_path: Path
    record: ApplicationLogRecord
    manifest_path: Path | None
    launch_order: int | None


def normalize_application_log_status(value: str) -> str:
    normalized_value = value.strip().lower()
    if normalized_value not in APPLICATION_LOG_STATUSES:
        supported = ", ".join(APPLICATION_LOG_STATUSES)
        raise ValueError(f"invalid status '{value}'; expected one of: {supported}")
    return normalized_value


def write_application_log(
    project_root: Path,
    *,
    company: str | None,
    role: str | None,
    portal: str | None,
    apply_url: str | None,
    status: str,
    notes: str | None = None,
    manifest_path: Path | None = None,
    launch_order: int | None = None,
    created_at: datetime | None = None,
) -> ApplicationLogResult:
    if manifest_path is None and launch_order is not None:
        raise ValueError("--launch-order requires --manifest")

    manifest_item = None
    if manifest_path is not None:
        manifest_item = _select_manifest_launch_item(
            manifest_path,
            launch_order=launch_order,
        )

    resolved_company = _require_text(
        company if company is not None else _manifest_text(manifest_item, "company"),
        label="company",
    )
    resolved_role = _require_text(
        role if role is not None else _manifest_text(manifest_item, "title"),
        label="role",
    )
    resolved_apply_url = _require_text(
        apply_url if apply_url is not None else _manifest_text(manifest_item, "apply_url"),
        label="apply_url",
    )
    resolved_portal = _resolve_portal(
        portal if portal is not None else _manifest_text(manifest_item, "portal_type"),
        apply_url=resolved_apply_url,
    )
    resolved_status = normalize_application_log_status(status)
    normalized_notes = _normalize_optional_text(notes)

    local_timestamp = _resolve_local_datetime(created_at)
    record = ApplicationLogRecord(
        company=resolved_company,
        role=resolved_role,
        portal=resolved_portal,
        apply_url=resolved_apply_url,
        status=resolved_status,
        method=APPLICATION_LOG_METHOD,
        notes=normalized_notes,
        timestamp=local_timestamp.isoformat(timespec="seconds"),
    )
    log_dir = project_root / "data" / "applications"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / _build_log_filename(
        local_timestamp=local_timestamp,
        company=record.company,
        portal=record.portal,
    )
    _write_log_payload(log_path, record)

    return ApplicationLogResult(
        log_path=log_path,
        record=record,
        manifest_path=manifest_path,
        launch_order=manifest_item.launch_order if manifest_item is not None else None,
    )


def _select_manifest_launch_item(
    manifest_path: Path,
    *,
    launch_order: int | None,
) -> LaunchPlanItem:
    manifest = load_session_manifest(manifest_path)
    plan = build_launch_plan(manifest)
    launchable_items = tuple(
        item
        for item in plan.items
        if item.launchable and item.launch_order is not None
    )
    if not launchable_items:
        raise ValueError("manifest contains no launchable application items")
    if launch_order is None:
        if len(launchable_items) == 1:
            return launchable_items[0]
        raise ValueError(
            "provide --launch-order when the manifest contains more than one launchable application"
        )

    for item in launchable_items:
        if item.launch_order == launch_order:
            return item
    raise ValueError(f"launch order {launch_order} was not found in the manifest")


def _manifest_text(item: LaunchPlanItem | None, field_name: str) -> str | None:
    if item is None:
        return None
    value = getattr(item, field_name)
    return value if isinstance(value, str) else None


def _resolve_portal(value: str | None, *, apply_url: str) -> str:
    normalized_value = _normalize_optional_text(value)
    if normalized_value is None:
        detected_portal = detect_portal_type(apply_url)
        if detected_portal is None:
            raise ValueError("portal is required when it cannot be inferred from apply_url")
        return detected_portal
    return normalized_value.lower()


def _require_text(value: str | None, *, label: str) -> str:
    normalized_value = _normalize_optional_text(value)
    if normalized_value is None:
        raise ValueError(f"{label} is required")
    return normalized_value


def _normalize_optional_text(value: str | None) -> str | None:
    if value is None:
        return None
    normalized_value = value.strip()
    return normalized_value or None


def _resolve_local_datetime(created_at: datetime | None) -> datetime:
    if created_at is None:
        return _current_local_datetime()
    if created_at.tzinfo is None:
        return created_at.astimezone()
    return created_at.astimezone()


def _current_local_datetime() -> datetime:
    return datetime.now().astimezone()


def _build_log_filename(
    *,
    local_timestamp: datetime,
    company: str,
    portal: str,
) -> str:
    date_text = local_timestamp.date().isoformat()
    company_slug = _slugify_filename_part(company)
    portal_slug = _slugify_filename_part(portal)
    return f"{date_text}-{company_slug}-{portal_slug}.json"


def _slugify_filename_part(value: str) -> str:
    ascii_value = (
        unicodedata.normalize("NFKD", value)
        .encode("ascii", "ignore")
        .decode("ascii")
        .lower()
    )
    slug = _FILENAME_TOKEN_RE.sub("-", ascii_value).strip("-.")
    slug = _HYPHEN_RE.sub("-", slug)
    return slug or "unknown"


def _write_log_payload(log_path: Path, record: ApplicationLogRecord) -> None:
    payload = asdict(record)
    if log_path.exists():
        existing_payload = _load_existing_payload(log_path)
        if not _same_logged_application(existing_payload, payload):
            raise ValueError(
                "log file already exists for a different application: "
                f"{log_path}"
            )
    log_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def _load_existing_payload(log_path: Path) -> dict[str, object]:
    try:
        with log_path.open("r", encoding="utf-8") as input_file:
            payload = json.load(input_file)
    except json.JSONDecodeError as exc:
        raise ValueError(f"existing log file is not valid JSON: {log_path}") from exc
    except OSError as exc:
        raise ValueError(f"existing log file could not be read: {exc}") from exc
    if not isinstance(payload, dict):
        raise ValueError(f"existing log file must contain a JSON object: {log_path}")
    return payload


def _same_logged_application(
    existing_payload: dict[str, object],
    requested_payload: dict[str, object],
) -> bool:
    identity_fields = ("company", "role", "portal", "apply_url")
    return all(existing_payload.get(field_name) == requested_payload[field_name] for field_name in identity_fields)
