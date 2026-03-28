from __future__ import annotations

from collections import Counter
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import shutil
import sqlite3
import tempfile

from .application_tracking import APPLICATION_STATUSES
from .db import REQUIRED_TABLES, find_duplicate_job_match, initialize_schema_connection
from .db_runtime import connect_sqlite_database
from .jobs.identity import normalize_optional_metadata

_REQUIRED_JOB_COLUMNS = (
    "ingest_batch_id",
    "source_query",
    "import_source",
    "source_registry_id",
    "canonical_apply_url",
    "identity_key",
    "applied_at",
)

_REGISTRY_STATUS_PRIORITY = {
    "inactive": 0,
    "manual_review": 1,
    "active": 2,
}


@dataclass(frozen=True, slots=True)
class DatabaseSchemaAssessment:
    database_exists: bool
    missing_tables: tuple[str, ...]
    missing_job_columns: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class TableMergeSummary:
    scanned_count: int
    inserted_count: int
    updated_count: int
    skipped_count: int


@dataclass(frozen=True, slots=True)
class JobMatchRuleCount:
    rule: str
    count: int


@dataclass(frozen=True, slots=True)
class JobMergeSummary:
    scanned_count: int
    inserted_count: int
    matched_count: int
    updated_existing_count: int
    reconciled_status_count: int
    rule_counts: tuple[JobMatchRuleCount, ...]


@dataclass(frozen=True, slots=True)
class DatabaseMergeResult:
    dry_run: bool
    target_path: Path
    source_path: Path
    backup_path: Path | None
    vacuumed: bool
    target_created: bool
    target_schema_before: DatabaseSchemaAssessment
    source_schema_before: DatabaseSchemaAssessment
    source_registry: TableMergeSummary
    jobs: JobMergeSummary
    applications: TableMergeSummary
    application_tracking: TableMergeSummary
    session_history: TableMergeSummary


@dataclass(frozen=True, slots=True)
class _MergeRunSummary:
    source_registry: TableMergeSummary
    jobs: JobMergeSummary
    applications: TableMergeSummary
    application_tracking: TableMergeSummary
    session_history: TableMergeSummary


def merge_sqlite_databases(
    target_path: Path,
    source_path: Path,
    *,
    dry_run: bool = False,
    create_backup: bool = False,
    vacuum: bool = False,
) -> DatabaseMergeResult:
    resolved_target_path = target_path.resolve()
    resolved_source_path = source_path.resolve()
    if not resolved_source_path.exists():
        raise ValueError(f"source database was not found: {resolved_source_path}")
    if not resolved_source_path.is_file():
        raise ValueError(f"source path is not a regular file: {resolved_source_path}")
    if resolved_source_path == resolved_target_path:
        raise ValueError("source and target databases must be different files")

    target_schema_before = assess_database_schema(resolved_target_path)
    source_schema_before = assess_database_schema(resolved_source_path)
    target_created = not resolved_target_path.exists()
    backup_path: Path | None = None

    if dry_run:
        with tempfile.TemporaryDirectory(prefix="jobs-ai-db-merge-") as tmp_dir:
            working_directory = Path(tmp_dir)
            working_target_path = working_directory / "target.db"
            working_source_path = working_directory / "source.db"
            _copy_database_if_present(resolved_target_path, working_target_path)
            shutil.copy2(resolved_source_path, working_source_path)
            _initialize_sqlite_schema(working_target_path)
            _initialize_sqlite_schema(working_source_path)
            run_summary = _merge_working_databases(
                working_target_path,
                working_source_path,
            )
        return DatabaseMergeResult(
            dry_run=True,
            target_path=resolved_target_path,
            source_path=resolved_source_path,
            backup_path=None,
            vacuumed=False,
            target_created=target_created,
            target_schema_before=target_schema_before,
            source_schema_before=source_schema_before,
            source_registry=run_summary.source_registry,
            jobs=run_summary.jobs,
            applications=run_summary.applications,
            application_tracking=run_summary.application_tracking,
            session_history=run_summary.session_history,
        )

    if create_backup and resolved_target_path.exists():
        backup_path = _build_backup_path(resolved_target_path)
        backup_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(resolved_target_path, backup_path)

    _initialize_sqlite_schema(resolved_target_path)
    _initialize_sqlite_schema(resolved_source_path)
    run_summary = _merge_working_databases(
        resolved_target_path,
        resolved_source_path,
    )
    vacuumed = False
    if vacuum:
        _vacuum_database(resolved_target_path)
        vacuumed = True

    return DatabaseMergeResult(
        dry_run=False,
        target_path=resolved_target_path,
        source_path=resolved_source_path,
        backup_path=backup_path,
        vacuumed=vacuumed,
        target_created=target_created,
        target_schema_before=target_schema_before,
        source_schema_before=source_schema_before,
        source_registry=run_summary.source_registry,
        jobs=run_summary.jobs,
        applications=run_summary.applications,
        application_tracking=run_summary.application_tracking,
        session_history=run_summary.session_history,
    )


def assess_database_schema(database_path: Path) -> DatabaseSchemaAssessment:
    if not database_path.exists():
        return DatabaseSchemaAssessment(
            database_exists=False,
            missing_tables=tuple(REQUIRED_TABLES),
            missing_job_columns=(),
        )

    with closing(connect_sqlite_database(database_path)) as connection:
        existing_tables = {
            str(row["name"])
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }
        missing_job_columns: tuple[str, ...] = ()
        if "jobs" in existing_tables:
            available_columns = {
                str(row["name"])
                for row in connection.execute("PRAGMA table_info(jobs)").fetchall()
            }
            missing_job_columns = tuple(
                column_name
                for column_name in _REQUIRED_JOB_COLUMNS
                if column_name not in available_columns
            )
    return DatabaseSchemaAssessment(
        database_exists=True,
        missing_tables=tuple(sorted(set(REQUIRED_TABLES) - existing_tables)),
        missing_job_columns=missing_job_columns,
    )


def _merge_working_databases(target_path: Path, source_path: Path) -> _MergeRunSummary:
    merge_timestamp = _utc_timestamp()
    attached = False
    with closing(connect_sqlite_database(target_path)) as connection:
        connection.execute("ATTACH DATABASE ? AS source_db", (str(source_path),))
        attached = True
        try:
            connection.execute("BEGIN IMMEDIATE")
            source_registry_summary, source_registry_map = _merge_source_registry(
                connection,
                merge_timestamp=merge_timestamp,
            )
            jobs_summary, job_id_map = _merge_jobs(
                connection,
                source_registry_id_map=source_registry_map,
                merge_timestamp=merge_timestamp,
            )
            applications_summary = _merge_applications(connection, job_id_map=job_id_map)
            application_tracking_summary = _merge_application_tracking(
                connection,
                job_id_map=job_id_map,
            )
            session_history_summary = _merge_session_history(connection)
            reconciled_status_count = _reconcile_job_statuses(
                connection,
                job_ids=tuple(dict.fromkeys(job_id_map.values())),
                merge_timestamp=merge_timestamp,
            )
            jobs_summary = JobMergeSummary(
                scanned_count=jobs_summary.scanned_count,
                inserted_count=jobs_summary.inserted_count,
                matched_count=jobs_summary.matched_count,
                updated_existing_count=jobs_summary.updated_existing_count,
                reconciled_status_count=reconciled_status_count,
                rule_counts=jobs_summary.rule_counts,
            )
            _validate_merge_integrity(connection)
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            if attached:
                connection.execute("DETACH DATABASE source_db")

    return _MergeRunSummary(
        source_registry=source_registry_summary,
        jobs=jobs_summary,
        applications=applications_summary,
        application_tracking=application_tracking_summary,
        session_history=session_history_summary,
    )


def _merge_source_registry(
    connection: sqlite3.Connection,
    *,
    merge_timestamp: str,
) -> tuple[TableMergeSummary, dict[int, int]]:
    rows = connection.execute(
        """
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
        FROM source_db.source_registry
        ORDER BY normalized_url, id
        """
    ).fetchall()
    created_count = 0
    updated_count = 0
    unchanged_count = 0
    source_registry_id_map: dict[int, int] = {}

    for row in rows:
        existing_row = connection.execute(
            """
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
            WHERE normalized_url = ?
            LIMIT 1
            """,
            (str(row["normalized_url"]),),
        ).fetchone()

        if existing_row is None:
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
                    _coalesce_text(row["source_url"], str(row["normalized_url"])),
                    str(row["normalized_url"]),
                    _nullable_text(row["portal_type"]),
                    _nullable_text(row["company"]),
                    _nullable_text(row["label"]),
                    _merge_registry_status(None, _nullable_text(row["status"])),
                    _coalesce_text(row["first_seen_at"], merge_timestamp),
                    _nullable_text(row["last_verified_at"]),
                    _nullable_text(row["notes"]),
                    _nullable_text(row["provenance"]),
                    _nullable_text(row["verification_reason_code"]),
                    _nullable_text(row["verification_reason"]),
                    _coalesce_text(row["created_at"], merge_timestamp),
                    _coalesce_text(row["updated_at"], merge_timestamp),
                ),
            )
            target_registry_id = int(cursor.lastrowid)
            source_registry_id_map[int(row["id"])] = target_registry_id
            created_count += 1
            continue

        target_registry_id = int(existing_row["id"])
        source_registry_id_map[int(row["id"])] = target_registry_id
        merged_values = {
            "source_url": _first_present(existing_row["source_url"], row["source_url"], row["normalized_url"]),
            "portal_type": _first_present(existing_row["portal_type"], row["portal_type"]),
            "company": _first_present(existing_row["company"], row["company"]),
            "label": _first_present(existing_row["label"], row["label"]),
            "status": _merge_registry_status(existing_row["status"], row["status"]),
            "first_seen_at": _min_present(existing_row["first_seen_at"], row["first_seen_at"]),
            "last_verified_at": _max_present(existing_row["last_verified_at"], row["last_verified_at"]),
            "notes": _merge_optional_text(existing_row["notes"], row["notes"]),
            "provenance": _merge_optional_text(existing_row["provenance"], row["provenance"]),
            "verification_reason_code": _preferred_verification_value(
                existing_row,
                row,
                field_name="verification_reason_code",
            ),
            "verification_reason": _preferred_verification_value(
                existing_row,
                row,
                field_name="verification_reason",
            ),
            "created_at": _min_present(existing_row["created_at"], row["created_at"]),
        }
        if _registry_rows_match(existing_row, merged_values):
            unchanged_count += 1
            continue

        connection.execute(
            """
            UPDATE source_registry
            SET source_url = ?,
                portal_type = ?,
                company = ?,
                label = ?,
                status = ?,
                first_seen_at = ?,
                last_verified_at = ?,
                notes = ?,
                provenance = ?,
                verification_reason_code = ?,
                verification_reason = ?,
                created_at = ?,
                updated_at = ?
            WHERE id = ?
            """,
            (
                merged_values["source_url"],
                merged_values["portal_type"],
                merged_values["company"],
                merged_values["label"],
                merged_values["status"],
                merged_values["first_seen_at"],
                merged_values["last_verified_at"],
                merged_values["notes"],
                merged_values["provenance"],
                merged_values["verification_reason_code"],
                merged_values["verification_reason"],
                merged_values["created_at"],
                merge_timestamp,
                target_registry_id,
            ),
        )
        updated_count += 1

    return (
        TableMergeSummary(
            scanned_count=len(rows),
            inserted_count=created_count,
            updated_count=updated_count,
            skipped_count=unchanged_count,
        ),
        source_registry_id_map,
    )


def _merge_jobs(
    connection: sqlite3.Connection,
    *,
    source_registry_id_map: dict[int, int],
    merge_timestamp: str,
) -> tuple[JobMergeSummary, dict[int, int]]:
    rows = connection.execute(
        """
        SELECT
            id,
            source,
            source_job_id,
            company,
            title,
            location,
            apply_url,
            portal_type,
            salary_text,
            posted_at,
            found_at,
            ingest_batch_id,
            source_query,
            import_source,
            source_registry_id,
            canonical_apply_url,
            identity_key,
            status,
            applied_at,
            raw_json,
            created_at,
            updated_at
        FROM source_db.jobs
        ORDER BY id
        """
    ).fetchall()
    inserted_count = 0
    matched_count = 0
    updated_existing_count = 0
    match_rule_counter: Counter[str] = Counter()
    job_id_map: dict[int, int] = {}

    for row in rows:
        mapped_source_registry_id = _mapped_registry_id(
            source_registry_id_map,
            row["source_registry_id"],
        )
        job_record = _job_record_from_row(row)
        duplicate_match = find_duplicate_job_match(connection, job_record)

        if duplicate_match is not None:
            target_job_id = duplicate_match.job_id
            job_id_map[int(row["id"])] = target_job_id
            match_rule_counter[duplicate_match.rule] += 1
            matched_count += 1
            existing_row = connection.execute(
                """
                SELECT
                    id,
                    source_job_id,
                    apply_url,
                    portal_type,
                    salary_text,
                    posted_at,
                    found_at,
                    ingest_batch_id,
                    source_query,
                    import_source,
                    source_registry_id,
                    canonical_apply_url,
                    identity_key,
                    status,
                    applied_at,
                    raw_json,
                    created_at
                FROM jobs
                WHERE id = ?
                LIMIT 1
                """,
                (target_job_id,),
            ).fetchone()
            assert existing_row is not None
            updates = _build_job_updates(
                existing_row,
                source_row=row,
                mapped_source_registry_id=mapped_source_registry_id,
            )
            if updates:
                assignments = [f"{field_name} = ?" for field_name in updates]
                assignments.append("updated_at = ?")
                values = [updates[field_name] for field_name in updates]
                values.append(merge_timestamp)
                values.append(target_job_id)
                connection.execute(
                    f"UPDATE jobs SET {', '.join(assignments)} WHERE id = ?",
                    values,
                )
                updated_existing_count += 1
            continue

        cursor = connection.execute(
            """
            INSERT INTO jobs (
                source,
                source_job_id,
                company,
                title,
                location,
                apply_url,
                portal_type,
                salary_text,
                posted_at,
                found_at,
                ingest_batch_id,
                source_query,
                import_source,
                source_registry_id,
                canonical_apply_url,
                identity_key,
                status,
                applied_at,
                raw_json,
                created_at,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(row["source"]),
                _nullable_text(row["source_job_id"]),
                str(row["company"]),
                str(row["title"]),
                _nullable_text(row["location"]),
                _nullable_text(row["apply_url"]),
                _nullable_text(row["portal_type"]),
                _nullable_text(row["salary_text"]),
                _nullable_text(row["posted_at"]),
                _coalesce_text(row["found_at"], merge_timestamp),
                _nullable_text(row["ingest_batch_id"]),
                _nullable_text(row["source_query"]),
                _nullable_text(row["import_source"]),
                mapped_source_registry_id,
                _nullable_text(row["canonical_apply_url"]),
                _nullable_text(row["identity_key"]),
                _normalize_job_status(row["status"]),
                _nullable_text(row["applied_at"]),
                _nullable_text(row["raw_json"]),
                _coalesce_text(row["created_at"], merge_timestamp),
                _coalesce_text(row["updated_at"], merge_timestamp),
            ),
        )
        target_job_id = int(cursor.lastrowid)
        job_id_map[int(row["id"])] = target_job_id
        inserted_count += 1

    return (
        JobMergeSummary(
            scanned_count=len(rows),
            inserted_count=inserted_count,
            matched_count=matched_count,
            updated_existing_count=updated_existing_count,
            reconciled_status_count=0,
            rule_counts=tuple(
                JobMatchRuleCount(rule=rule, count=match_rule_counter[rule])
                for rule in sorted(match_rule_counter)
            ),
        ),
        job_id_map,
    )


def _merge_applications(
    connection: sqlite3.Connection,
    *,
    job_id_map: dict[int, int],
) -> TableMergeSummary:
    existing_keys = {
        _application_key(row["job_id"], row)
        for row in connection.execute(
            """
            SELECT
                job_id,
                state,
                resume_variant,
                notes,
                last_attempted_at,
                applied_at,
                created_at
            FROM applications
            """
        ).fetchall()
    }
    rows = connection.execute(
        """
        SELECT
            job_id,
            state,
            resume_variant,
            notes,
            last_attempted_at,
            applied_at,
            created_at,
            updated_at
        FROM source_db.applications
        ORDER BY created_at, id
        """
    ).fetchall()
    inserted_count = 0
    skipped_count = 0

    for row in rows:
        source_job_id = int(row["job_id"])
        target_job_id = job_id_map.get(source_job_id)
        if target_job_id is None:
            continue
        application_key = _application_key(target_job_id, row)
        if application_key in existing_keys:
            skipped_count += 1
            continue
        connection.execute(
            """
            INSERT INTO applications (
                job_id,
                state,
                resume_variant,
                notes,
                last_attempted_at,
                applied_at,
                created_at,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                target_job_id,
                _coalesce_text(row["state"], "draft"),
                _nullable_text(row["resume_variant"]),
                _nullable_text(row["notes"]),
                _nullable_text(row["last_attempted_at"]),
                _nullable_text(row["applied_at"]),
                _coalesce_text(row["created_at"], _utc_timestamp()),
                _coalesce_text(row["updated_at"], _coalesce_text(row["created_at"], _utc_timestamp())),
            ),
        )
        existing_keys.add(application_key)
        inserted_count += 1

    return TableMergeSummary(
        scanned_count=len(rows),
        inserted_count=inserted_count,
        updated_count=0,
        skipped_count=skipped_count,
    )


def _merge_application_tracking(
    connection: sqlite3.Connection,
    *,
    job_id_map: dict[int, int],
) -> TableMergeSummary:
    existing_keys = {
        _application_tracking_key(row["job_id"], row)
        for row in connection.execute(
            """
            SELECT
                job_id,
                status,
                created_at
            FROM application_tracking
            """
        ).fetchall()
    }
    rows = connection.execute(
        """
        SELECT
            job_id,
            status,
            created_at
        FROM source_db.application_tracking
        ORDER BY created_at, id
        """
    ).fetchall()
    inserted_count = 0
    skipped_count = 0

    for row in rows:
        source_job_id = int(row["job_id"])
        target_job_id = job_id_map.get(source_job_id)
        if target_job_id is None:
            continue
        tracking_key = _application_tracking_key(target_job_id, row)
        if tracking_key in existing_keys:
            skipped_count += 1
            continue
        connection.execute(
            """
            INSERT INTO application_tracking (
                job_id,
                status,
                created_at
            ) VALUES (?, ?, ?)
            """,
            (
                target_job_id,
                _normalize_job_status(row["status"]),
                _coalesce_text(row["created_at"], _utc_timestamp()),
            ),
        )
        existing_keys.add(tracking_key)
        inserted_count += 1

    return TableMergeSummary(
        scanned_count=len(rows),
        inserted_count=inserted_count,
        updated_count=0,
        skipped_count=skipped_count,
    )


def _merge_session_history(connection: sqlite3.Connection) -> TableMergeSummary:
    existing_keys = {
        _session_history_key(row)
        for row in connection.execute(
            """
            SELECT
                manifest_path,
                item_count,
                launchable_count,
                ingest_batch_id,
                source_query,
                created_at
            FROM session_history
            """
        ).fetchall()
    }
    rows = connection.execute(
        """
        SELECT
            manifest_path,
            item_count,
            launchable_count,
            ingest_batch_id,
            source_query,
            created_at
        FROM source_db.session_history
        ORDER BY created_at, id
        """
    ).fetchall()
    inserted_count = 0
    skipped_count = 0

    for row in rows:
        history_key = _session_history_key(row)
        if history_key in existing_keys:
            skipped_count += 1
            continue
        connection.execute(
            """
            INSERT INTO session_history (
                manifest_path,
                item_count,
                launchable_count,
                ingest_batch_id,
                source_query,
                created_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                str(row["manifest_path"]),
                int(row["item_count"]),
                int(row["launchable_count"]),
                _nullable_text(row["ingest_batch_id"]),
                _nullable_text(row["source_query"]),
                _coalesce_text(row["created_at"], _utc_timestamp()),
            ),
        )
        existing_keys.add(history_key)
        inserted_count += 1

    return TableMergeSummary(
        scanned_count=len(rows),
        inserted_count=inserted_count,
        updated_count=0,
        skipped_count=skipped_count,
    )


def _reconcile_job_statuses(
    connection: sqlite3.Connection,
    *,
    job_ids: tuple[int, ...],
    merge_timestamp: str,
) -> int:
    updated_count = 0
    for job_id in job_ids:
        row = connection.execute(
            """
            SELECT
                status,
                (
                    SELECT status
                    FROM application_tracking
                    WHERE job_id = jobs.id
                    ORDER BY created_at DESC, id DESC
                    LIMIT 1
                ) AS latest_tracking_status,
                EXISTS(
                    SELECT 1
                    FROM applications
                    WHERE job_id = jobs.id
                      AND applied_at IS NOT NULL
                    LIMIT 1
                ) AS has_applied_application
            FROM jobs
            WHERE id = ?
            LIMIT 1
            """,
            (job_id,),
        ).fetchone()
        if row is None:
            continue
        current_status = _normalize_job_status(row["status"])
        latest_tracking_status = _normalize_optional_status(row["latest_tracking_status"])
        desired_status = current_status
        if latest_tracking_status is not None:
            desired_status = latest_tracking_status
        elif bool(row["has_applied_application"]) and current_status in {"new", "opened"}:
            desired_status = "applied"
        if desired_status == current_status:
            continue
        connection.execute(
            "UPDATE jobs SET status = ?, updated_at = ? WHERE id = ?",
            (desired_status, merge_timestamp, job_id),
        )
        updated_count += 1
    return updated_count


def _validate_merge_integrity(connection: sqlite3.Connection) -> None:
    foreign_key_violations = connection.execute("PRAGMA foreign_key_check").fetchall()
    if foreign_key_violations:
        raise RuntimeError(
            f"merge produced {len(foreign_key_violations)} foreign key violation(s)"
        )
    orphaned_registry_rows = connection.execute(
        """
        SELECT COUNT(*) AS count
        FROM jobs
        WHERE source_registry_id IS NOT NULL
          AND NOT EXISTS (
              SELECT 1
              FROM source_registry
              WHERE source_registry.id = jobs.source_registry_id
          )
        """
    ).fetchone()
    assert orphaned_registry_rows is not None
    if int(orphaned_registry_rows["count"]) > 0:
        raise RuntimeError(
            "merge produced orphaned jobs.source_registry_id references"
        )


def _build_job_updates(
    existing_row: sqlite3.Row,
    *,
    source_row: sqlite3.Row,
    mapped_source_registry_id: int | None,
) -> dict[str, object]:
    updates: dict[str, object] = {}
    if _nullable_text(existing_row["source_job_id"]) is None and _nullable_text(source_row["source_job_id"]) is not None:
        updates["source_job_id"] = _nullable_text(source_row["source_job_id"])
    if _nullable_text(existing_row["apply_url"]) is None and _nullable_text(source_row["apply_url"]) is not None:
        updates["apply_url"] = _nullable_text(source_row["apply_url"])
    if _nullable_text(existing_row["portal_type"]) is None and _nullable_text(source_row["portal_type"]) is not None:
        updates["portal_type"] = _nullable_text(source_row["portal_type"])
    if _nullable_text(existing_row["salary_text"]) is None and _nullable_text(source_row["salary_text"]) is not None:
        updates["salary_text"] = _nullable_text(source_row["salary_text"])
    if _nullable_text(existing_row["posted_at"]) is None and _nullable_text(source_row["posted_at"]) is not None:
        updates["posted_at"] = _nullable_text(source_row["posted_at"])

    existing_found_at = _nullable_text(existing_row["found_at"])
    source_found_at = _nullable_text(source_row["found_at"])
    if source_found_at is not None and (existing_found_at is None or source_found_at < existing_found_at):
        updates["found_at"] = source_found_at

    if _nullable_text(existing_row["ingest_batch_id"]) is None and _nullable_text(source_row["ingest_batch_id"]) is not None:
        updates["ingest_batch_id"] = _nullable_text(source_row["ingest_batch_id"])
    if _nullable_text(existing_row["source_query"]) is None and _nullable_text(source_row["source_query"]) is not None:
        updates["source_query"] = _nullable_text(source_row["source_query"])
    if _nullable_text(existing_row["import_source"]) is None and _nullable_text(source_row["import_source"]) is not None:
        updates["import_source"] = _nullable_text(source_row["import_source"])

    existing_registry_id = _nullable_int(existing_row["source_registry_id"])
    if existing_registry_id is None and mapped_source_registry_id is not None:
        updates["source_registry_id"] = mapped_source_registry_id

    if _nullable_text(existing_row["canonical_apply_url"]) is None and _nullable_text(source_row["canonical_apply_url"]) is not None:
        updates["canonical_apply_url"] = _nullable_text(source_row["canonical_apply_url"])
    if _nullable_text(existing_row["identity_key"]) is None and _nullable_text(source_row["identity_key"]) is not None:
        updates["identity_key"] = _nullable_text(source_row["identity_key"])
    if _nullable_text(existing_row["applied_at"]) is None and _nullable_text(source_row["applied_at"]) is not None:
        updates["applied_at"] = _nullable_text(source_row["applied_at"])
    if _nullable_text(existing_row["raw_json"]) is None and _nullable_text(source_row["raw_json"]) is not None:
        updates["raw_json"] = _nullable_text(source_row["raw_json"])

    existing_created_at = _nullable_text(existing_row["created_at"])
    source_created_at = _nullable_text(source_row["created_at"])
    if source_created_at is not None and (
        existing_created_at is None or source_created_at < existing_created_at
    ):
        updates["created_at"] = source_created_at

    existing_status = _normalize_job_status(existing_row["status"])
    source_status = _normalize_optional_status(source_row["status"])
    if source_status is not None and existing_status == "new" and source_status != "new":
        updates["status"] = source_status

    return updates


def _registry_rows_match(existing_row: sqlite3.Row, merged_values: dict[str, object]) -> bool:
    return (
        _nullable_text(existing_row["source_url"]) == _nullable_text(merged_values["source_url"])
        and _nullable_text(existing_row["portal_type"]) == _nullable_text(merged_values["portal_type"])
        and _nullable_text(existing_row["company"]) == _nullable_text(merged_values["company"])
        and _nullable_text(existing_row["label"]) == _nullable_text(merged_values["label"])
        and _nullable_text(existing_row["status"]) == _nullable_text(merged_values["status"])
        and _nullable_text(existing_row["first_seen_at"]) == _nullable_text(merged_values["first_seen_at"])
        and _nullable_text(existing_row["last_verified_at"]) == _nullable_text(merged_values["last_verified_at"])
        and _nullable_text(existing_row["notes"]) == _nullable_text(merged_values["notes"])
        and _nullable_text(existing_row["provenance"]) == _nullable_text(merged_values["provenance"])
        and _nullable_text(existing_row["verification_reason_code"]) == _nullable_text(merged_values["verification_reason_code"])
        and _nullable_text(existing_row["verification_reason"]) == _nullable_text(merged_values["verification_reason"])
        and _nullable_text(existing_row["created_at"]) == _nullable_text(merged_values["created_at"])
    )


def _preferred_verification_value(
    existing_row: sqlite3.Row,
    source_row: sqlite3.Row,
    *,
    field_name: str,
) -> str | None:
    source_last_verified_at = _nullable_text(source_row["last_verified_at"])
    existing_last_verified_at = _nullable_text(existing_row["last_verified_at"])
    preferred_row = existing_row
    if source_last_verified_at is not None and (
        existing_last_verified_at is None or source_last_verified_at >= existing_last_verified_at
    ):
        preferred_row = source_row
    return _first_present(
        preferred_row[field_name],
        existing_row[field_name],
        source_row[field_name],
    )


def _merge_registry_status(existing_status: object, source_status: object) -> str:
    existing_value = _normalize_registry_status(existing_status)
    source_value = _normalize_registry_status(source_status)
    if existing_value is None:
        return source_value or "manual_review"
    if source_value is None:
        return existing_value
    if _REGISTRY_STATUS_PRIORITY[source_value] > _REGISTRY_STATUS_PRIORITY[existing_value]:
        return source_value
    return existing_value


def _application_key(job_id: int, row: sqlite3.Row) -> tuple[object, ...]:
    return (
        job_id,
        _coalesce_text(row["state"], "draft"),
        _nullable_text(row["resume_variant"]),
        _nullable_text(row["notes"]),
        _nullable_text(row["last_attempted_at"]),
        _nullable_text(row["applied_at"]),
        _nullable_text(row["created_at"]),
    )


def _application_tracking_key(job_id: int, row: sqlite3.Row) -> tuple[object, ...]:
    return (
        job_id,
        _normalize_job_status(row["status"]),
        _nullable_text(row["created_at"]),
    )


def _session_history_key(row: sqlite3.Row) -> tuple[object, ...]:
    return (
        str(row["manifest_path"]),
        int(row["item_count"]),
        int(row["launchable_count"]),
        _nullable_text(row["ingest_batch_id"]),
        _nullable_text(row["source_query"]),
        _nullable_text(row["created_at"]),
    )


def _job_record_from_row(row: sqlite3.Row) -> dict[str, object]:
    return {
        "source": str(row["source"]),
        "source_job_id": _nullable_text(row["source_job_id"]),
        "company": str(row["company"]),
        "title": str(row["title"]),
        "location": _nullable_text(row["location"]),
        "apply_url": _nullable_text(row["apply_url"]),
        "portal_type": _nullable_text(row["portal_type"]),
    }


def _mapped_registry_id(
    source_registry_id_map: dict[int, int],
    source_registry_id: object,
) -> int | None:
    normalized_id = _nullable_int(source_registry_id)
    if normalized_id is None:
        return None
    return source_registry_id_map.get(normalized_id)


def _build_backup_path(target_path: Path) -> Path:
    return target_path.with_name(
        f"{target_path.name}.bak.{_utc_compact_timestamp()}"
    )


def _vacuum_database(database_path: Path) -> None:
    with closing(connect_sqlite_database(database_path)) as connection:
        connection.execute("VACUUM")


def _initialize_sqlite_schema(database_path: Path) -> None:
    database_path.parent.mkdir(parents=True, exist_ok=True)
    with closing(connect_sqlite_database(database_path)) as connection:
        initialize_schema_connection(connection)
        connection.commit()


def _copy_database_if_present(source_path: Path, destination_path: Path) -> None:
    if source_path.exists():
        shutil.copy2(source_path, destination_path)


def _first_present(*values: object) -> str | None:
    for value in values:
        normalized_value = _nullable_text(value)
        if normalized_value is not None:
            return normalized_value
    return None


def _min_present(*values: object) -> str | None:
    normalized_values = [
        normalized_value
        for value in values
        if (normalized_value := _nullable_text(value)) is not None
    ]
    if not normalized_values:
        return None
    return min(normalized_values)


def _max_present(*values: object) -> str | None:
    normalized_values = [
        normalized_value
        for value in values
        if (normalized_value := _nullable_text(value)) is not None
    ]
    if not normalized_values:
        return None
    return max(normalized_values)


def _merge_optional_text(left: object, right: object) -> str | None:
    normalized_values = [
        value
        for value in (_nullable_text(left), _nullable_text(right))
        if value is not None
    ]
    if not normalized_values:
        return None
    return "\n".join(dict.fromkeys(normalized_values))


def _normalize_registry_status(value: object) -> str | None:
    normalized_value = normalize_optional_metadata(value)
    if normalized_value is None:
        return None
    lowered_value = normalized_value.lower()
    if lowered_value not in _REGISTRY_STATUS_PRIORITY:
        return "manual_review"
    return lowered_value


def _normalize_optional_status(value: object) -> str | None:
    normalized_value = normalize_optional_metadata(value)
    if normalized_value is None:
        return None
    lowered_value = normalized_value.lower()
    if lowered_value not in APPLICATION_STATUSES:
        return None
    return lowered_value


def _normalize_job_status(value: object) -> str:
    return _normalize_optional_status(value) or "new"


def _coalesce_text(value: object, fallback: str) -> str:
    normalized_value = _nullable_text(value)
    if normalized_value is not None:
        return normalized_value
    return fallback


def _nullable_text(value: object) -> str | None:
    return normalize_optional_metadata(value)


def _nullable_int(value: object) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    normalized_value = normalize_optional_metadata(value)
    if normalized_value is None:
        return None
    try:
        return int(normalized_value)
    except ValueError:
        return None


def _utc_timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _utc_compact_timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
