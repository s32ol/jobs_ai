from __future__ import annotations

from collections import Counter
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path

from .db import REQUIRED_TABLES, connect_database, initialize_schema
from .db_runtime import database_exists, table_columns_from_connection, table_names_from_connection
from .jobs.identity import build_job_identity, normalize_optional_metadata
from .portal_support import detect_portal_type

_BACKFILL_COLUMNS = (
    "portal_type",
    "canonical_apply_url",
    "identity_key",
)


@dataclass(frozen=True, slots=True)
class BackfillFieldCount:
    field_name: str
    count: int


@dataclass(frozen=True, slots=True)
class BackfillJobUpdate:
    job_id: int
    company: str
    title: str
    changed_fields: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class BackfillResult:
    dry_run: bool
    limit: int | None
    total_jobs: int
    candidate_jobs: int
    updated_jobs: int
    skipped_jobs: int
    deferred_jobs: int
    missing_tables: tuple[str, ...]
    missing_job_columns: tuple[str, ...]
    field_counts: tuple[BackfillFieldCount, ...]
    job_updates: tuple[BackfillJobUpdate, ...]


@dataclass(frozen=True, slots=True)
class _BackfillCandidate:
    job_id: int
    company: str
    title: str
    updates: dict[str, str]

    @property
    def changed_fields(self) -> tuple[str, ...]:
        return tuple(self.updates.keys())


def backfill_jobs_metadata(
    database_path: Path,
    *,
    limit: int | None = None,
    dry_run: bool = False,
) -> BackfillResult:
    if limit is not None and limit < 1:
        raise ValueError("limit must be at least 1")

    if dry_run and not database_exists(database_path):
        return BackfillResult(
            dry_run=True,
            limit=limit,
            total_jobs=0,
            candidate_jobs=0,
            updated_jobs=0,
            skipped_jobs=0,
            deferred_jobs=0,
            missing_tables=tuple(sorted(REQUIRED_TABLES)),
            missing_job_columns=(),
            field_counts=(),
            job_updates=(),
        )

    pre_assessment = _empty_assessment()
    if database_exists(database_path):
        with closing(connect_database(database_path)) as connection:
            pre_assessment = _assess_backfill_candidates(connection)
    elif not dry_run:
        pre_assessment = _BackfillAssessment(
            total_jobs=0,
            missing_tables=tuple(sorted(REQUIRED_TABLES)),
            missing_job_columns=(),
            candidates=(),
        )

    if not dry_run:
        initialize_schema(database_path, backfill_identity=False)

    with closing(connect_database(database_path)) as connection:
        assessment = _assess_backfill_candidates(connection)
        selected_candidates = (
            assessment.candidates[:limit]
            if limit is not None
            else assessment.candidates
        )

        if not dry_run and selected_candidates:
            for candidate in selected_candidates:
                _apply_backfill_candidate(connection, candidate)
            connection.commit()

    field_counter = Counter(
        field_name
        for candidate in selected_candidates
        for field_name in candidate.changed_fields
    )
    updated_jobs = len(selected_candidates)
    candidate_jobs = len(assessment.candidates)
    return BackfillResult(
        dry_run=dry_run,
        limit=limit,
        total_jobs=assessment.total_jobs,
        candidate_jobs=candidate_jobs,
        updated_jobs=updated_jobs,
        skipped_jobs=assessment.total_jobs - candidate_jobs,
        deferred_jobs=max(0, candidate_jobs - updated_jobs),
        missing_tables=pre_assessment.missing_tables,
        missing_job_columns=pre_assessment.missing_job_columns,
        field_counts=tuple(
            BackfillFieldCount(field_name=field_name, count=field_counter[field_name])
            for field_name in _BACKFILL_COLUMNS
            if field_counter[field_name] > 0
        ),
        job_updates=tuple(
            BackfillJobUpdate(
                job_id=candidate.job_id,
                company=candidate.company,
                title=candidate.title,
                changed_fields=candidate.changed_fields,
            )
            for candidate in selected_candidates
        ),
    )


@dataclass(frozen=True, slots=True)
class _BackfillAssessment:
    total_jobs: int
    missing_tables: tuple[str, ...]
    missing_job_columns: tuple[str, ...]
    candidates: tuple[_BackfillCandidate, ...]


def _empty_assessment() -> _BackfillAssessment:
    return _BackfillAssessment(
        total_jobs=0,
        missing_tables=(),
        missing_job_columns=(),
        candidates=(),
    )


def _assess_backfill_candidates(connection) -> _BackfillAssessment:
    existing_tables = table_names_from_connection(connection)
    missing_tables = tuple(sorted(set(REQUIRED_TABLES) - existing_tables))
    if "jobs" not in existing_tables:
        return _BackfillAssessment(
            total_jobs=0,
            missing_tables=missing_tables,
            missing_job_columns=(),
            candidates=(),
        )

    available_columns = set(table_columns_from_connection(connection, "jobs"))
    missing_job_columns = tuple(
        column_name
        for column_name in (
            "ingest_batch_id",
            "source_query",
            "import_source",
            "canonical_apply_url",
            "identity_key",
        )
        if column_name not in available_columns
    )

    rows = connection.execute(_backfill_select_sql(available_columns)).fetchall()
    candidates = tuple(
        candidate
        for row in rows
        if (candidate := _build_backfill_candidate(row)) is not None
    )
    return _BackfillAssessment(
        total_jobs=len(rows),
        missing_tables=missing_tables,
        missing_job_columns=missing_job_columns,
        candidates=candidates,
    )


def _backfill_select_sql(available_columns: set[str]) -> str:
    required_columns = (
        "id",
        "source",
        "source_job_id",
        "company",
        "title",
        "location",
        "apply_url",
        "portal_type",
        "canonical_apply_url",
        "identity_key",
    )
    select_parts = [
        column_name
        if column_name in available_columns
        else f"NULL AS {column_name}"
        for column_name in required_columns
    ]
    return "SELECT " + ", ".join(select_parts) + " FROM jobs ORDER BY id"


def _build_backfill_candidate(row: sqlite3.Row) -> _BackfillCandidate | None:
    apply_url = normalize_optional_metadata(row["apply_url"])
    current_portal_type = normalize_optional_metadata(row["portal_type"])
    current_canonical_apply_url = normalize_optional_metadata(row["canonical_apply_url"])
    current_identity_key = normalize_optional_metadata(row["identity_key"])

    updates: dict[str, str] = {}
    derived_portal_type = current_portal_type
    if current_portal_type is None and apply_url is not None:
        inferred_portal_type = detect_portal_type(apply_url)
        if inferred_portal_type is not None:
            derived_portal_type = inferred_portal_type
            updates["portal_type"] = inferred_portal_type

    identity = build_job_identity(
        {
            "source": row["source"],
            "source_job_id": row["source_job_id"],
            "company": row["company"],
            "title": row["title"],
            "location": row["location"],
            "apply_url": row["apply_url"],
            "portal_type": derived_portal_type,
        }
    )
    if current_canonical_apply_url is None and identity.canonical_apply_url is not None:
        updates["canonical_apply_url"] = identity.canonical_apply_url
    if current_identity_key is None:
        updates["identity_key"] = identity.identity_key

    if not updates:
        return None
    return _BackfillCandidate(
        job_id=int(row["id"]),
        company=str(row["company"]),
        title=str(row["title"]),
        updates=updates,
    )


def _apply_backfill_candidate(connection: sqlite3.Connection, candidate: _BackfillCandidate) -> None:
    assignments = [f"{field_name} = ?" for field_name in candidate.changed_fields]
    assignments.append("updated_at = CURRENT_TIMESTAMP")
    values = [candidate.updates[field_name] for field_name in candidate.changed_fields]
    values.append(candidate.job_id)
    connection.execute(
        f"UPDATE jobs SET {', '.join(assignments)} WHERE id = ?",
        values,
    )
