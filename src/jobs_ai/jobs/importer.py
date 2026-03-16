from __future__ import annotations

from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path

from ..db import connect_database, find_duplicate_job_match, insert_job
from .identity import normalize_batch_id, normalize_optional_metadata
from .normalization import normalize_job_import_fields

REQUIRED_IMPORT_FIELDS = ("source", "company", "title", "location")
OPTIONAL_IMPORT_FIELDS = (
    "apply_url",
    "source_job_id",
    "portal_type",
    "salary_text",
    "posted_at",
    "found_at",
)
SUPPORTED_IMPORT_SUFFIXES = {".json"}


@dataclass(frozen=True, slots=True)
class JobImportResult:
    inserted_count: int
    skipped_count: int
    batch_id: str | None = None
    source_query: str | None = None
    import_source: str | None = None
    duplicate_count: int = 0
    error_count: int = 0
    skipped: tuple[str, ...] = ()
    errors: tuple[str, ...] = ()


def import_jobs_from_file(
    database_path: Path,
    input_path: Path,
    *,
    batch_id: str | None = None,
    source_query: str | None = None,
    import_source: str | None = None,
    created_at: datetime | None = None,
) -> JobImportResult:
    records = load_job_records(input_path)
    inserted_count = 0
    duplicate_count = 0
    skipped: list[str] = []
    errors: list[str] = []
    normalized_batch_id = _resolve_batch_id(batch_id, created_at=created_at)
    normalized_source_query = normalize_optional_metadata(source_query)
    normalized_import_source = (
        normalize_optional_metadata(import_source)
        if import_source is not None
        else str(input_path)
    )

    with closing(connect_database(database_path)) as connection:
        for record_number, record in enumerate(records, start=1):
            job_record, error = normalize_import_record(record)
            if error is not None:
                errors.append(f"record {record_number}: {error}")
                continue

            duplicate_match = find_duplicate_job_match(connection, job_record)
            if duplicate_match is not None:
                duplicate_count += 1
                skipped.append(
                    f"record {record_number}: duplicate skipped via "
                    f"{describe_duplicate_match(duplicate_match)} "
                    f"(existing job id {duplicate_match.job_id})"
                )
                continue

            insert_job(
                connection,
                {
                    **job_record,
                    "ingest_batch_id": normalized_batch_id,
                    "source_query": normalized_source_query,
                    "import_source": normalized_import_source,
                },
            )
            inserted_count += 1
        connection.commit()

    return JobImportResult(
        inserted_count=inserted_count,
        skipped_count=duplicate_count + len(errors),
        batch_id=normalized_batch_id,
        source_query=normalized_source_query,
        import_source=normalized_import_source,
        duplicate_count=duplicate_count,
        error_count=len(errors),
        skipped=tuple(skipped),
        errors=tuple(errors),
    )


def load_job_records(input_path: Path) -> list[object]:
    if input_path.suffix.lower() not in SUPPORTED_IMPORT_SUFFIXES:
        supported = ", ".join(sorted(SUPPORTED_IMPORT_SUFFIXES))
        raise ValueError(f"unsupported file type '{input_path.suffix or '<none>'}'; supported types: {supported}")

    try:
        payload = json.loads(input_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(
            f"invalid JSON at line {exc.lineno}, column {exc.colno}: {exc.msg}"
        ) from exc

    if isinstance(payload, list):
        if not payload:
            raise ValueError("input file does not contain any job records")
        return payload
    if isinstance(payload, dict):
        return [payload]

    raise ValueError("JSON input must be an object or an array of objects")


def normalize_import_record(record: object) -> tuple[dict[str, str | None], str | None]:
    if not isinstance(record, dict):
        return {}, "expected a JSON object"

    normalized_record = normalize_job_import_fields(
        record,
        REQUIRED_IMPORT_FIELDS + OPTIONAL_IMPORT_FIELDS,
    )
    missing_fields = [field for field in REQUIRED_IMPORT_FIELDS if normalized_record[field] is None]
    if missing_fields:
        return {}, f"missing required fields: {', '.join(missing_fields)}"

    normalized_record["raw_json"] = json.dumps(record, ensure_ascii=True)
    return normalized_record, None


def describe_duplicate_match(match) -> str:
    return f"{match.rule}: {match.matched_value}"


def _resolve_batch_id(
    batch_id: str | None,
    *,
    created_at: datetime | None,
) -> str:
    normalized_batch_id = normalize_batch_id(batch_id)
    if normalized_batch_id is not None:
        return normalized_batch_id
    created_at_dt = _normalize_created_at(created_at)
    return f"import-{created_at_dt.strftime('%Y%m%dT%H%M%S%fZ')}"


def _normalize_created_at(created_at: datetime | None) -> datetime:
    if created_at is None:
        return datetime.now(timezone.utc)
    if created_at.tzinfo is None:
        return created_at.replace(tzinfo=timezone.utc)
    return created_at.astimezone(timezone.utc)
