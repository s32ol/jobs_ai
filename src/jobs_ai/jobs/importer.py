from __future__ import annotations

from contextlib import closing
from dataclasses import dataclass
import json
from pathlib import Path

from ..db import connect_database, find_duplicate_job_id, insert_job
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
    skipped: tuple[str, ...] = ()
    errors: tuple[str, ...] = ()


def import_jobs_from_file(database_path: Path, input_path: Path) -> JobImportResult:
    records = load_job_records(input_path)
    inserted_count = 0
    skipped: list[str] = []
    errors: list[str] = []

    with closing(connect_database(database_path)) as connection:
        for record_number, record in enumerate(records, start=1):
            job_record, error = normalize_import_record(record)
            if error is not None:
                errors.append(f"record {record_number}: {error}")
                continue
            duplicate_job_id = find_duplicate_job_id(connection, job_record)
            if duplicate_job_id is not None:
                skipped.append(
                    f"record {record_number}: duplicate skipped via {describe_duplicate_rule(job_record)}"
                    f" (existing job id {duplicate_job_id})"
                )
                continue
            insert_job(connection, job_record)
            inserted_count += 1
        connection.commit()

    return JobImportResult(
        inserted_count=inserted_count,
        skipped_count=len(skipped) + len(errors),
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


def describe_duplicate_rule(job_record: dict[str, str | None]) -> str:
    if job_record["apply_url"] is not None:
        return f"exact apply_url match: {job_record['apply_url']}"
    return (
        "exact fallback key: "
        f"{job_record['source']} | {job_record['company']} | "
        f"{job_record['title']} | {job_record['location']}"
    )
