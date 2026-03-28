from __future__ import annotations

from collections import Counter
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path

from .application_tracking import ApplicationStatusSnapshot, record_application_statuses
from .db import (
    REQUIRED_TABLES,
    connect_database,
    initialize_schema,
    list_canonical_duplicate_apply_urls,
    resolve_canonical_duplicate_group,
)
from .db_runtime import database_exists, table_columns_from_connection, table_names_from_connection
from .jobs.identity import build_job_identity, normalize_optional_metadata
from .jobs.location_guard import classify_job_location
from .jobs.query_filter import job_matches_query
from .portal_support import detect_portal_type

_BACKFILL_COLUMNS = (
    "portal_type",
    "canonical_apply_url",
    "identity_key",
)
_LOCATION_GUARD_SCANNED_STATUSES = frozenset(
    {"new", "opened", "skipped", "superseded", "invalid_location"}
)
_LOCATION_GUARD_MUTABLE_STATUSES = frozenset({"new", "opened", "skipped", "superseded"})


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
class CanonicalDuplicateRepairGroup:
    canonical_apply_url: str
    group_size: int
    winner_job_id: int
    winner_status: str
    superseded_job_ids: tuple[int, ...]
    changed_job_ids: tuple[int, ...]


@dataclass(frozen=True, slots=True)
class CanonicalDuplicateRepairResult:
    dry_run: bool
    duplicate_groups: int
    repaired_groups: int
    changed_jobs: int
    superseded_jobs: int
    reactivated_jobs: int
    groups: tuple[CanonicalDuplicateRepairGroup, ...]


@dataclass(frozen=True, slots=True)
class InvalidLocationMarkJob:
    job_id: int
    company: str
    title: str
    location: str | None
    previous_status: str
    classification_reason: str


@dataclass(frozen=True, slots=True)
class InvalidLocationMarkResult:
    dry_run: bool
    us_only: bool
    limit: int | None
    ingest_batch_id: str | None
    query_text: str | None
    total_jobs: int
    candidate_jobs: int
    marked_jobs: int
    deferred_jobs: int
    already_invalid_jobs: int
    ambiguous_jobs: int
    us_allowed_jobs: int
    skipped_jobs: int
    job_updates: tuple[InvalidLocationMarkJob, ...]
    updated_snapshots: tuple[ApplicationStatusSnapshot, ...]


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


def repair_canonical_duplicate_statuses(
    database_path: Path,
    *,
    dry_run: bool = False,
) -> CanonicalDuplicateRepairResult:
    if not dry_run:
        initialize_schema(database_path)

    with closing(connect_database(database_path)) as connection:
        existing_tables = table_names_from_connection(connection)
        if "jobs" not in existing_tables:
            return CanonicalDuplicateRepairResult(
                dry_run=dry_run,
                duplicate_groups=0,
                repaired_groups=0,
                changed_jobs=0,
                superseded_jobs=0,
                reactivated_jobs=0,
                groups=(),
            )

        groups = []
        for canonical_apply_url in list_canonical_duplicate_apply_urls(connection):
            resolution = resolve_canonical_duplicate_group(
                connection,
                canonical_apply_url=canonical_apply_url,
                dry_run=dry_run,
            )
            if resolution is None:
                continue
            groups.append(
                CanonicalDuplicateRepairGroup(
                    canonical_apply_url=resolution.canonical_apply_url,
                    group_size=resolution.group_size,
                    winner_job_id=resolution.winner_job_id,
                    winner_status=resolution.winner_status,
                    superseded_job_ids=resolution.superseded_job_ids,
                    changed_job_ids=resolution.changed_job_ids,
                )
            )

        if not dry_run and any(group.changed_job_ids for group in groups):
            connection.commit()

    return CanonicalDuplicateRepairResult(
        dry_run=dry_run,
        duplicate_groups=len(groups),
        repaired_groups=sum(1 for group in groups if group.changed_job_ids),
        changed_jobs=sum(len(group.changed_job_ids) for group in groups),
        superseded_jobs=sum(
            len(
                tuple(
                    job_id
                    for job_id in group.superseded_job_ids
                    if job_id in group.changed_job_ids
                )
            )
            for group in groups
        ),
        reactivated_jobs=sum(
            max(0, len(group.changed_job_ids) - sum(job_id in group.superseded_job_ids for job_id in group.changed_job_ids))
            for group in groups
        ),
        groups=tuple(groups),
    )


def mark_invalid_location_jobs(
    database_path: Path,
    *,
    us_only: bool,
    limit: int | None = None,
    dry_run: bool = False,
    ingest_batch_id: str | None = None,
    query_text: str | None = None,
    actionable_only: bool = False,
) -> InvalidLocationMarkResult:
    if not us_only:
        raise ValueError("currently only --us-only is supported")
    if limit is not None and limit < 1:
        raise ValueError("limit must be at least 1")

    if dry_run and not database_exists(database_path):
        return InvalidLocationMarkResult(
            dry_run=True,
            us_only=True,
            limit=limit,
            ingest_batch_id=ingest_batch_id,
            query_text=query_text,
            total_jobs=0,
            candidate_jobs=0,
            marked_jobs=0,
            deferred_jobs=0,
            already_invalid_jobs=0,
            ambiguous_jobs=0,
            us_allowed_jobs=0,
            skipped_jobs=0,
            job_updates=(),
            updated_snapshots=(),
        )

    if not dry_run:
        initialize_schema(database_path)

    with closing(connect_database(database_path)) as connection:
        existing_tables = table_names_from_connection(connection)
        if "jobs" not in existing_tables:
            return InvalidLocationMarkResult(
                dry_run=dry_run,
                us_only=True,
                limit=limit,
                ingest_batch_id=ingest_batch_id,
                query_text=query_text,
                total_jobs=0,
                candidate_jobs=0,
                marked_jobs=0,
                deferred_jobs=0,
                already_invalid_jobs=0,
                ambiguous_jobs=0,
                us_allowed_jobs=0,
                skipped_jobs=0,
                job_updates=(),
                updated_snapshots=(),
            )

        available_columns = set(table_columns_from_connection(connection, "jobs"))
        rows = connection.execute(_location_guard_select_sql(available_columns)).fetchall()

    scoped_rows = [
        row
        for row in rows
        if _row_matches_location_guard_scope(
            row,
            ingest_batch_id=ingest_batch_id,
            query_text=query_text,
            actionable_only=actionable_only,
        )
    ]

    candidate_job_ids: list[int] = []
    candidate_updates: list[InvalidLocationMarkJob] = []
    already_invalid_jobs = 0
    ambiguous_jobs = 0
    us_allowed_jobs = 0
    skipped_jobs = 0

    for row in scoped_rows:
        current_status = _normalized_job_status(row["status"])
        if current_status not in _LOCATION_GUARD_SCANNED_STATUSES:
            skipped_jobs += 1
            continue

        classification = classify_job_location(_nullable_text(row["location"]))
        if current_status == "invalid_location":
            already_invalid_jobs += 1
            continue
        if classification.is_non_us:
            if current_status not in _LOCATION_GUARD_MUTABLE_STATUSES:
                skipped_jobs += 1
                continue
            candidate_job_ids.append(int(row["id"]))
            candidate_updates.append(
                InvalidLocationMarkJob(
                    job_id=int(row["id"]),
                    company=str(row["company"]),
                    title=str(row["title"]),
                    location=_nullable_text(row["location"]),
                    previous_status=current_status,
                    classification_reason=classification.reason,
                )
            )
            continue
        if classification.is_ambiguous:
            ambiguous_jobs += 1
            continue
        us_allowed_jobs += 1

    updated_snapshots: tuple[ApplicationStatusSnapshot, ...] = ()
    selected_job_ids = (
        candidate_job_ids[:limit]
        if limit is not None
        else candidate_job_ids
    )
    selected_updates = (
        candidate_updates[:limit]
        if limit is not None
        else candidate_updates
    )
    deferred_jobs = max(0, len(candidate_job_ids) - len(selected_job_ids))
    if not dry_run and selected_job_ids:
        batch_result = record_application_statuses(
            database_path,
            job_ids=tuple(selected_job_ids),
            status="invalid_location",
        )
        updated_snapshots = batch_result.updated
        skipped_jobs += len(batch_result.skipped)
        updated_job_ids = {snapshot.job_id for snapshot in updated_snapshots}
        selected_updates = [
            update
            for update in selected_updates
            if update.job_id in updated_job_ids
        ]

    return InvalidLocationMarkResult(
        dry_run=dry_run,
        us_only=True,
        limit=limit,
        ingest_batch_id=ingest_batch_id,
        query_text=query_text,
        total_jobs=len(scoped_rows),
        candidate_jobs=len(candidate_job_ids),
        marked_jobs=len(selected_job_ids) if dry_run else len(updated_snapshots),
        deferred_jobs=deferred_jobs,
        already_invalid_jobs=already_invalid_jobs,
        ambiguous_jobs=ambiguous_jobs,
        us_allowed_jobs=us_allowed_jobs,
        skipped_jobs=skipped_jobs,
        job_updates=tuple(selected_updates),
        updated_snapshots=updated_snapshots,
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


def _location_guard_select_sql(available_columns: set[str]) -> str:
    required_columns = (
        "id",
        "source",
        "company",
        "title",
        "location",
        "apply_url",
        "portal_type",
        "status",
        "raw_json",
        "ingest_batch_id",
    )
    select_parts = [
        column_name
        if column_name in available_columns
        else f"NULL AS {column_name}"
        for column_name in required_columns
    ]
    return "SELECT " + ", ".join(select_parts) + " FROM jobs ORDER BY id"


def _row_matches_location_guard_scope(
    row,
    *,
    ingest_batch_id: str | None,
    query_text: str | None,
    actionable_only: bool,
) -> bool:
    if actionable_only and _normalized_job_status(row["status"]) != "new":
        return False
    if ingest_batch_id is not None and _nullable_text(row["ingest_batch_id"]) != ingest_batch_id:
        return False
    if query_text is not None and not job_matches_query(row, query_text):
        return False
    return True


def _normalized_job_status(value: object) -> str:
    normalized_value = normalize_optional_metadata(value)
    if normalized_value is None:
        return "new"
    return normalized_value.lower()


def _nullable_text(value: object) -> str | None:
    return normalize_optional_metadata(value)
