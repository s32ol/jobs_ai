from __future__ import annotations

from collections.abc import Sequence
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path
import json
from urllib.parse import urlparse

from ..collect.fetch import Fetcher, fetch_text
from ..collect.harness import run_collection
from ..collect.models import SourceResult
from ..db import connect_database
from ..portal_support import build_portal_support, extract_portal_board_root_url
from .models import (
    SourceRegistryEntry,
    SourceRegistryImportResult,
    SourceRegistryMutationResult,
    SourceRegistryVerificationResult,
)

SOURCE_REGISTRY_STATUSES = ("active", "inactive", "manual_review")
_DEFAULT_VERIFY_SELECTION = ("active", "manual_review")
_INACTIVE_REASON_CODES = frozenset(
    {
        "http_error_status",
        "missing_network_host",
        "non_html_content",
        "unsupported_url_scheme",
    }
)

_SOURCE_REGISTRY_SELECT_SQL = """
SELECT
    id,
    source_url,
    normalized_url,
    portal_type,
    company,
    label,
    status,
    first_seen_at,
    last_verified_at,
    notes,
    provenance,
    verification_reason_code,
    verification_reason,
    created_at,
    updated_at
FROM source_registry
"""


def list_registry_sources(
    database_path: Path,
    *,
    source_ids: Sequence[int] | None = None,
    statuses: Sequence[str] | None = None,
) -> tuple[SourceRegistryEntry, ...]:
    normalized_statuses = _normalize_status_filters(statuses)
    normalized_source_ids = tuple(dict.fromkeys(int(source_id) for source_id in source_ids or ()))
    where_clauses: list[str] = []
    parameters: list[object] = []

    if normalized_source_ids:
        placeholders = ", ".join("?" for _ in normalized_source_ids)
        where_clauses.append(f"id IN ({placeholders})")
        parameters.extend(normalized_source_ids)
    if normalized_statuses:
        placeholders = ", ".join("?" for _ in normalized_statuses)
        where_clauses.append(f"status IN ({placeholders})")
        parameters.extend(normalized_statuses)

    query = _SOURCE_REGISTRY_SELECT_SQL
    if where_clauses:
        query = f"{query} WHERE {' AND '.join(where_clauses)}"
    query = (
        f"{query} ORDER BY "
        "CASE status "
        "WHEN 'active' THEN 0 "
        "WHEN 'manual_review' THEN 1 "
        "ELSE 2 END, "
        "COALESCE(company, label, source_url), id"
    )

    with closing(connect_database(database_path)) as connection:
        rows = connection.execute(query, parameters).fetchall()
    return tuple(_entry_from_row(row) for row in rows)


def get_registry_source(
    database_path: Path,
    *,
    source_id: int,
) -> SourceRegistryEntry | None:
    entries = list_registry_sources(database_path, source_ids=(source_id,))
    if not entries:
        return None
    return entries[0]


def register_source(
    database_path: Path,
    *,
    source_url: str,
    portal_type: str | None = None,
    company: str | None = None,
    label: str | None = None,
    status: str | None = None,
    notes: str | None = None,
    provenance: str | None = None,
    verify: bool,
    timeout_seconds: float,
    created_at: datetime | None = None,
    fetcher: Fetcher | None = None,
) -> SourceRegistryMutationResult:
    created_at_text = _format_created_at(_normalize_created_at(created_at))
    normalized_url, detected_portal_type = normalize_registry_source_url(
        source_url,
        portal_type=portal_type,
    )
    source_result: SourceResult | None = None
    resolved_status = _normalize_status(status or ("manual_review" if not verify else "active"))
    resolved_portal_type = detected_portal_type
    resolved_company = _normalize_text(company)

    if verify:
        source_result = _verify_source_url(
            normalized_url,
            portal_type=resolved_portal_type,
            timeout_seconds=timeout_seconds,
            fetcher=fetch_text if fetcher is None else fetcher,
        )
        normalized_url = source_result.source.normalized_url or normalized_url
        resolved_portal_type = source_result.source.portal_type or resolved_portal_type
        resolved_company = _verified_company(source_result) or resolved_company
        resolved_status = _status_from_source_result(source_result)

    with closing(connect_database(database_path)) as connection:
        mutation = _upsert_registry_entry(
            connection,
            source_url=normalized_url,
            normalized_url=normalized_url,
            portal_type=resolved_portal_type,
            company=resolved_company,
            label=_normalize_text(label),
            status=resolved_status,
            notes=_normalize_text(notes),
            provenance=_normalize_text(provenance),
            last_verified_at=created_at_text if verify else None,
            verification_reason_code=(source_result.reason_code if source_result is not None else None),
            verification_reason=(source_result.reason if source_result is not None else None),
            created_at=created_at_text,
        )
        connection.commit()
    return SourceRegistryMutationResult(
        action=mutation.action,
        entry=mutation.entry,
        source_result=source_result,
    )


def register_verified_source(
    database_path: Path,
    *,
    source_url: str,
    portal_type: str | None = None,
    company: str | None = None,
    label: str | None = None,
    notes: str | None = None,
    provenance: str | None = None,
    verification_reason_code: str,
    verification_reason: str,
    created_at: datetime | None = None,
) -> SourceRegistryMutationResult:
    created_at_text = _format_created_at(_normalize_created_at(created_at))
    normalized_url, detected_portal_type = normalize_registry_source_url(
        source_url,
        portal_type=portal_type,
    )
    with closing(connect_database(database_path)) as connection:
        mutation = _upsert_registry_entry(
            connection,
            source_url=normalized_url,
            normalized_url=normalized_url,
            portal_type=detected_portal_type,
            company=_normalize_text(company),
            label=_normalize_text(label),
            status="active",
            notes=_normalize_text(notes),
            provenance=_normalize_text(provenance),
            last_verified_at=created_at_text,
            verification_reason_code=verification_reason_code,
            verification_reason=verification_reason,
            created_at=created_at_text,
        )
        connection.commit()
    return SourceRegistryMutationResult(
        action=mutation.action,
        entry=mutation.entry,
    )


def upsert_registry_source(
    database_path: Path,
    *,
    source_url: str,
    portal_type: str | None = None,
    company: str | None = None,
    label: str | None = None,
    status: str,
    notes: str | None = None,
    provenance: str | None = None,
    verification_reason_code: str | None = None,
    verification_reason: str | None = None,
    created_at: datetime | None = None,
    preserve_existing_active: bool = False,
    mark_verified_at: bool = False,
) -> SourceRegistryMutationResult:
    created_at_text = _format_created_at(_normalize_created_at(created_at))
    normalized_url, detected_portal_type = normalize_registry_source_url(
        source_url,
        portal_type=portal_type,
    )

    with closing(connect_database(database_path)) as connection:
        existing_entry = get_registry_source_by_normalized_url_from_connection(
            connection,
            normalized_url=normalized_url,
        )
        resolved_status = _normalize_status(status)
        resolved_last_verified_at = created_at_text if mark_verified_at else None
        resolved_reason_code = _normalize_text(verification_reason_code)
        resolved_reason = _normalize_text(verification_reason)
        if (
            preserve_existing_active
            and existing_entry is not None
            and existing_entry.status == "active"
            and resolved_status != "active"
        ):
            resolved_status = existing_entry.status
            resolved_last_verified_at = existing_entry.last_verified_at
            resolved_reason_code = existing_entry.verification_reason_code
            resolved_reason = existing_entry.verification_reason

        mutation = _upsert_registry_entry(
            connection,
            source_url=normalized_url,
            normalized_url=normalized_url,
            portal_type=detected_portal_type,
            company=_normalize_text(company),
            label=_normalize_text(label),
            status=resolved_status,
            notes=_normalize_text(notes),
            provenance=_normalize_text(provenance),
            last_verified_at=resolved_last_verified_at,
            verification_reason_code=resolved_reason_code,
            verification_reason=resolved_reason,
            created_at=created_at_text,
        )
        connection.commit()
    return SourceRegistryMutationResult(
        action=mutation.action,
        entry=mutation.entry,
    )


def import_registry_sources(
    database_path: Path,
    *,
    input_path: Path,
    verify: bool,
    timeout_seconds: float,
    notes: str | None = None,
    provenance: str | None = None,
    created_at: datetime | None = None,
    fetcher: Fetcher | None = None,
) -> SourceRegistryImportResult:
    created_at_dt = _normalize_created_at(created_at)
    results: list[SourceRegistryMutationResult] = []
    errors: list[str] = []
    for record_number, record in enumerate(load_registry_source_records(input_path), start=1):
        try:
            results.append(
                register_source(
                    database_path,
                    source_url=record["source_url"],
                    portal_type=record.get("portal_type"),
                    company=record.get("company"),
                    label=record.get("label"),
                    status=record.get("status"),
                    notes=_merge_optional_text(notes, record.get("notes")),
                    provenance=_merge_optional_text(provenance, record.get("provenance")),
                    verify=verify,
                    timeout_seconds=timeout_seconds,
                    created_at=created_at_dt,
                    fetcher=fetcher,
                )
            )
        except ValueError as exc:
            errors.append(f"record {record_number}: {exc}")

    return SourceRegistryImportResult(
        results=tuple(results),
        errors=tuple(errors),
    )


def verify_registry_sources(
    database_path: Path,
    *,
    source_ids: Sequence[int] | None = None,
    include_inactive: bool = False,
    timeout_seconds: float,
    created_at: datetime | None = None,
    fetcher: Fetcher | None = None,
) -> tuple[SourceRegistryVerificationResult, ...]:
    if source_ids:
        entries = list_registry_sources(database_path, source_ids=source_ids)
    else:
        statuses = SOURCE_REGISTRY_STATUSES if include_inactive else _DEFAULT_VERIFY_SELECTION
        entries = list_registry_sources(database_path, statuses=statuses)
    return tuple(
        verify_registry_source(
            database_path,
            source_id=entry.source_id,
            timeout_seconds=timeout_seconds,
            created_at=created_at,
            fetcher=fetcher,
        )
        for entry in entries
    )


def verify_registry_source(
    database_path: Path,
    *,
    source_id: int,
    timeout_seconds: float,
    created_at: datetime | None = None,
    fetcher: Fetcher | None = None,
) -> SourceRegistryVerificationResult:
    existing_entry = get_registry_source(database_path, source_id=source_id)
    if existing_entry is None:
        raise ValueError(f"registry source id {source_id} was not found")

    created_at_text = _format_created_at(_normalize_created_at(created_at))
    source_result = _verify_source_url(
        existing_entry.source_url,
        portal_type=existing_entry.portal_type,
        timeout_seconds=timeout_seconds,
        fetcher=fetch_text if fetcher is None else fetcher,
    )
    normalized_url = source_result.source.normalized_url or existing_entry.normalized_url
    resolved_status = _status_from_source_result(
        source_result,
        current_status=existing_entry.status,
    )

    with closing(connect_database(database_path)) as connection:
        mutation = _upsert_registry_entry(
            connection,
            source_url=normalized_url,
            normalized_url=normalized_url,
            portal_type=source_result.source.portal_type or existing_entry.portal_type,
            company=_verified_company(source_result) or existing_entry.company,
            label=existing_entry.label,
            status=resolved_status,
            notes=existing_entry.notes,
            provenance=existing_entry.provenance,
            last_verified_at=created_at_text,
            verification_reason_code=source_result.reason_code,
            verification_reason=source_result.reason,
            created_at=created_at_text,
        )
        connection.commit()

    return SourceRegistryVerificationResult(
        action=mutation.action,
        before=existing_entry,
        after=mutation.entry,
        source_result=source_result,
    )


def deactivate_registry_source(
    database_path: Path,
    *,
    source_id: int,
    note: str | None = None,
    created_at: datetime | None = None,
) -> SourceRegistryEntry:
    existing_entry = get_registry_source(database_path, source_id=source_id)
    if existing_entry is None:
        raise ValueError(f"registry source id {source_id} was not found")

    created_at_text = _format_created_at(_normalize_created_at(created_at))
    with closing(connect_database(database_path)) as connection:
        mutation = _upsert_registry_entry(
            connection,
            source_url=existing_entry.source_url,
            normalized_url=existing_entry.normalized_url,
            portal_type=existing_entry.portal_type,
            company=existing_entry.company,
            label=existing_entry.label,
            status="inactive",
            notes=_merge_optional_text(existing_entry.notes, note),
            provenance=existing_entry.provenance,
            last_verified_at=existing_entry.last_verified_at,
            verification_reason_code=existing_entry.verification_reason_code,
            verification_reason=existing_entry.verification_reason,
            created_at=created_at_text,
        )
        connection.commit()
    return mutation.entry


def load_registry_source_records(input_path: Path) -> tuple[dict[str, str | None], ...]:
    suffix = input_path.suffix.lower()
    if suffix == ".json":
        return _load_registry_source_records_from_json(input_path)
    return _load_registry_source_records_from_text(input_path)


def normalize_registry_source_url(
    source_url: str,
    *,
    portal_type: str | None = None,
) -> tuple[str, str | None]:
    normalized_source_url = _normalize_text(source_url)
    if normalized_source_url is None:
        raise ValueError("source URL must be a non-empty URL")

    parsed_url = urlparse(normalized_source_url)
    if parsed_url.scheme.lower() not in {"http", "https"}:
        raise ValueError(f"unsupported URL scheme: {parsed_url.scheme or '<missing>'}")
    if not parsed_url.netloc:
        raise ValueError("source URL is missing a network host")

    portal_support = build_portal_support(normalized_source_url, portal_type=portal_type)
    resolved_portal_type = portal_support.portal_type if portal_support is not None else None
    board_root_url = extract_portal_board_root_url(
        normalized_source_url,
        portal_type=resolved_portal_type,
    )
    if board_root_url is not None:
        return board_root_url, resolved_portal_type
    if portal_support is not None:
        return portal_support.normalized_apply_url, resolved_portal_type
    return parsed_url._replace(fragment="").geturl(), resolved_portal_type


def _load_registry_source_records_from_text(input_path: Path) -> tuple[dict[str, str | None], ...]:
    records: list[dict[str, str | None]] = []
    for line in input_path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        records.append({"source_url": stripped})
    return tuple(records)


def _load_registry_source_records_from_json(input_path: Path) -> tuple[dict[str, str | None], ...]:
    try:
        payload = json.loads(input_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(
            f"invalid JSON at line {exc.lineno}, column {exc.colno}: {exc.msg}"
        ) from exc

    if isinstance(payload, dict):
        payload = [payload]
    if not isinstance(payload, list):
        raise ValueError("registry source JSON must be an object or array of objects")

    records: list[dict[str, str | None]] = []
    for item in payload:
        if isinstance(item, str):
            records.append({"source_url": item})
            continue
        if not isinstance(item, dict):
            raise ValueError("registry source JSON items must be strings or objects")
        source_url = item.get("source_url") or item.get("url")
        if not isinstance(source_url, str):
            raise ValueError("registry source JSON object is missing source_url")
        records.append(
            {
                "source_url": source_url,
                "portal_type": _normalize_text(item.get("portal_type")),
                "company": _normalize_text(item.get("company")),
                "label": _normalize_text(item.get("label")),
                "status": _normalize_text(item.get("status")),
                "notes": _normalize_text(item.get("notes")),
                "provenance": _normalize_text(item.get("provenance")),
            }
        )
    return tuple(records)


def _verify_source_url(
    source_url: str,
    *,
    portal_type: str | None,
    timeout_seconds: float,
    fetcher: Fetcher,
) -> SourceResult:
    normalized_url, _ = normalize_registry_source_url(source_url, portal_type=portal_type)
    run = run_collection(
        (normalized_url,),
        timeout_seconds=timeout_seconds,
        fetcher=fetcher,
    )
    return run.report.source_results[0]


def _status_from_source_result(
    source_result: SourceResult,
    *,
    current_status: str | None = None,
) -> str:
    if source_result.outcome == "collected":
        return "active"
    if source_result.outcome == "manual_review":
        return "manual_review"
    evidence = source_result.evidence
    if evidence is not None and evidence.status_code in {404, 410}:
        return "inactive"
    if source_result.reason_code in _INACTIVE_REASON_CODES:
        return "inactive"
    if current_status == "active" and source_result.reason_code == "fetch_failed":
        return "active"
    return "manual_review"


def _verified_company(source_result: SourceResult) -> str | None:
    companies = tuple(
        dict.fromkeys(
            lead.company
            for lead in source_result.collected_leads
            if lead.company
        )
    )
    if len(companies) == 1:
        return companies[0]
    return None


class _Mutation:
    def __init__(self, action: str, entry: SourceRegistryEntry) -> None:
        self.action = action
        self.entry = entry


def _upsert_registry_entry(
    connection,
    *,
    source_url: str,
    normalized_url: str,
    portal_type: str | None,
    company: str | None,
    label: str | None,
    status: str,
    notes: str | None,
    provenance: str | None,
    last_verified_at: str | None,
    verification_reason_code: str | None,
    verification_reason: str | None,
    created_at: str,
) -> _Mutation:
    row = connection.execute(
        f"{_SOURCE_REGISTRY_SELECT_SQL} WHERE normalized_url = ? LIMIT 1",
        (normalized_url,),
    ).fetchone()
    if row is None:
        cursor = connection.execute(
            """
            INSERT INTO source_registry (
                source_url,
                normalized_url,
                portal_type,
                company,
                label,
                status,
                first_seen_at,
                last_verified_at,
                notes,
                provenance,
                verification_reason_code,
                verification_reason,
                created_at,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                source_url,
                normalized_url,
                portal_type,
                company,
                label,
                status,
                created_at,
                last_verified_at,
                notes,
                provenance,
                verification_reason_code,
                verification_reason,
                created_at,
                created_at,
            ),
        )
        entry = get_registry_source_from_connection(connection, source_id=int(cursor.lastrowid))
        assert entry is not None
        return _Mutation("created", entry)

    existing_entry = _entry_from_row(row)
    merged_notes = _merge_optional_text(existing_entry.notes, notes)
    merged_provenance = _merge_optional_text(existing_entry.provenance, provenance)
    updated_entry = SourceRegistryEntry(
        source_id=existing_entry.source_id,
        source_url=source_url,
        normalized_url=normalized_url,
        portal_type=portal_type or existing_entry.portal_type,
        company=company or existing_entry.company,
        label=label or existing_entry.label,
        status=_normalize_status(status),
        first_seen_at=existing_entry.first_seen_at,
        last_verified_at=last_verified_at or existing_entry.last_verified_at,
        notes=merged_notes,
        provenance=merged_provenance,
        verification_reason_code=verification_reason_code or existing_entry.verification_reason_code,
        verification_reason=verification_reason or existing_entry.verification_reason,
        created_at=existing_entry.created_at,
        updated_at=created_at,
    )
    if _entries_match(existing_entry, updated_entry):
        return _Mutation("unchanged", existing_entry)

    connection.execute(
        """
        UPDATE source_registry
        SET source_url = ?,
            normalized_url = ?,
            portal_type = ?,
            company = ?,
            label = ?,
            status = ?,
            last_verified_at = ?,
            notes = ?,
            provenance = ?,
            verification_reason_code = ?,
            verification_reason = ?,
            updated_at = ?
        WHERE id = ?
        """,
        (
            updated_entry.source_url,
            updated_entry.normalized_url,
            updated_entry.portal_type,
            updated_entry.company,
            updated_entry.label,
            updated_entry.status,
            updated_entry.last_verified_at,
            updated_entry.notes,
            updated_entry.provenance,
            updated_entry.verification_reason_code,
            updated_entry.verification_reason,
            updated_entry.updated_at,
            updated_entry.source_id,
        ),
    )
    entry = get_registry_source_from_connection(connection, source_id=updated_entry.source_id)
    assert entry is not None
    return _Mutation("updated", entry)


def get_registry_source_from_connection(connection, *, source_id: int) -> SourceRegistryEntry | None:
    row = connection.execute(
        f"{_SOURCE_REGISTRY_SELECT_SQL} WHERE id = ? LIMIT 1",
        (source_id,),
    ).fetchone()
    if row is None:
        return None
    return _entry_from_row(row)


def get_registry_source_by_normalized_url_from_connection(
    connection,
    *,
    normalized_url: str,
) -> SourceRegistryEntry | None:
    row = connection.execute(
        f"{_SOURCE_REGISTRY_SELECT_SQL} WHERE normalized_url = ? LIMIT 1",
        (normalized_url,),
    ).fetchone()
    if row is None:
        return None
    return _entry_from_row(row)


def _entry_from_row(row) -> SourceRegistryEntry:
    return SourceRegistryEntry(
        source_id=int(row["id"]),
        source_url=str(row["source_url"]),
        normalized_url=str(row["normalized_url"]),
        portal_type=_normalize_text(row["portal_type"]),
        company=_normalize_text(row["company"]),
        label=_normalize_text(row["label"]),
        status=_normalize_status(str(row["status"])),
        first_seen_at=str(row["first_seen_at"]),
        last_verified_at=_normalize_text(row["last_verified_at"]),
        notes=_normalize_text(row["notes"]),
        provenance=_normalize_text(row["provenance"]),
        verification_reason_code=_normalize_text(row["verification_reason_code"]),
        verification_reason=_normalize_text(row["verification_reason"]),
        created_at=str(row["created_at"]),
        updated_at=str(row["updated_at"]),
    )


def _normalize_status_filters(statuses: Sequence[str] | None) -> tuple[str, ...]:
    if statuses is None:
        return ()
    return tuple(dict.fromkeys(_normalize_status(status) for status in statuses))


def _normalize_status(status: str) -> str:
    normalized_status = status.strip().lower()
    if normalized_status not in SOURCE_REGISTRY_STATUSES:
        allowed_statuses = ", ".join(SOURCE_REGISTRY_STATUSES)
        raise ValueError(f"unsupported source registry status {status!r}; expected one of {allowed_statuses}")
    return normalized_status


def _merge_optional_text(left: str | None, right: str | None) -> str | None:
    normalized_values = [
        value
        for value in (_normalize_text(left), _normalize_text(right))
        if value is not None
    ]
    if not normalized_values:
        return None
    merged_values = tuple(dict.fromkeys(normalized_values))
    return "\n".join(merged_values)


def _normalize_text(value: object) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        value = str(value)
    normalized_value = value.strip()
    return normalized_value or None


def _entries_match(left: SourceRegistryEntry, right: SourceRegistryEntry) -> bool:
    return (
        left.source_url == right.source_url
        and left.normalized_url == right.normalized_url
        and left.portal_type == right.portal_type
        and left.company == right.company
        and left.label == right.label
        and left.status == right.status
        and left.first_seen_at == right.first_seen_at
        and left.last_verified_at == right.last_verified_at
        and left.notes == right.notes
        and left.provenance == right.provenance
        and left.verification_reason_code == right.verification_reason_code
        and left.verification_reason == right.verification_reason
        and left.created_at == right.created_at
    )


def _normalize_created_at(created_at: datetime | None) -> datetime:
    if created_at is None:
        return datetime.now(timezone.utc)
    if created_at.tzinfo is None:
        return created_at.replace(tzinfo=timezone.utc)
    return created_at.astimezone(timezone.utc)


def _format_created_at(created_at: datetime) -> str:
    return created_at.isoformat(timespec="seconds").replace("+00:00", "Z")
