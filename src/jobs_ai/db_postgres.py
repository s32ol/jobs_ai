from __future__ import annotations

from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
import shutil
import sqlite3
import tempfile

from .config import Settings
from .db import REQUIRED_TABLES, find_duplicate_job_match, initialize_schema_connection
from .db_runtime import (
    POSTGRES_SQLITE_RUNTIME_FALLBACK_WARNING,
    backend_name_for_connection,
    connect_database,
    connect_sqlite_database,
    fallback_reason_for_connection,
    fallback_triggered_for_connection,
    resolve_database_runtime,
    table_columns_from_connection,
    table_names_from_connection,
    target_label_for_connection,
)
from .jobs.identity import build_job_identity, normalize_optional_metadata

_REQUIRED_JOB_COLUMNS = (
    "ingest_batch_id",
    "source_query",
    "import_source",
    "source_registry_id",
    "canonical_apply_url",
    "identity_key",
)

_SOURCE_REGISTRY_COLUMNS = (
    "id",
    "source_url",
    "normalized_url",
    "portal_type",
    "company",
    "label",
    "status",
    "first_seen_at",
    "last_verified_at",
    "notes",
    "provenance",
    "verification_reason_code",
    "verification_reason",
    "created_at",
    "updated_at",
)

_JOB_COLUMNS = (
    "id",
    "source",
    "source_job_id",
    "company",
    "title",
    "location",
    "apply_url",
    "portal_type",
    "salary_text",
    "posted_at",
    "found_at",
    "ingest_batch_id",
    "source_query",
    "import_source",
    "source_registry_id",
    "canonical_apply_url",
    "identity_key",
    "status",
    "raw_json",
    "created_at",
    "updated_at",
)

_APPLICATION_COLUMNS = (
    "id",
    "job_id",
    "state",
    "resume_variant",
    "notes",
    "last_attempted_at",
    "applied_at",
    "created_at",
    "updated_at",
)

_APPLICATION_MUTABLE_COLUMNS = (
    "job_id",
    "state",
    "resume_variant",
    "notes",
    "last_attempted_at",
    "applied_at",
    "created_at",
    "updated_at",
)

_APPLICATION_TRACKING_COLUMNS = (
    "id",
    "job_id",
    "status",
    "created_at",
)

_APPLICATION_TRACKING_MUTABLE_COLUMNS = (
    "job_id",
    "status",
    "created_at",
)

_SESSION_HISTORY_COLUMNS = (
    "id",
    "manifest_path",
    "item_count",
    "launchable_count",
    "ingest_batch_id",
    "source_query",
    "created_at",
)


@dataclass(frozen=True, slots=True)
class BackendStatusResult:
    backend: str
    backend_source: str
    fallback_triggered: bool
    fallback_reason: str | None
    warning: str | None
    target_label: str
    sqlite_path: Path
    database_url_configured: bool
    reachable: bool
    missing_tables: tuple[str, ...]
    table_counts: tuple[tuple[str, int], ...]
    message: str


@dataclass(frozen=True, slots=True)
class DatabasePingResult:
    backend: str
    backend_source: str
    fallback_triggered: bool
    fallback_reason: str | None
    warning: str | None
    target_label: str
    ok: bool
    message: str


@dataclass(frozen=True, slots=True)
class TableCopySummary:
    scanned_count: int
    inserted_count: int
    updated_count: int
    matched_count: int
    unchanged_count: int


@dataclass(frozen=True, slots=True)
class PostgresMigrationResult:
    dry_run: bool
    fast_path_used: bool
    source_path: Path
    target_label: str
    source_missing_tables_before: tuple[str, ...]
    source_missing_job_columns_before: tuple[str, ...]
    target_missing_tables_before: tuple[str, ...]
    source_registry: TableCopySummary
    jobs: TableCopySummary
    applications: TableCopySummary
    application_tracking: TableCopySummary
    session_history: TableCopySummary


def build_backend_status(settings: Settings) -> BackendStatusResult:
    runtime = resolve_database_runtime(settings.database_path, settings=settings)
    backend = runtime.backend
    target_label = runtime.target_label
    fallback_triggered, warning, fallback_reason = _resolve_fallback_details(settings)
    try:
        with closing(connect_database(settings.database_path, settings=settings)) as connection:
            backend = backend_name_for_connection(connection)
            target_label = target_label_for_connection(connection, runtime=runtime)
            fallback_triggered, warning, fallback_reason = _resolve_fallback_details(
                settings,
                connection=connection,
            )
            missing_tables = _missing_required_tables_for_connection(connection)
            table_counts = _collect_required_table_counts_for_connection(
                connection,
                missing_tables=missing_tables,
            )
        reachable = True
        if not missing_tables:
            message = "schema ready"
        else:
            message = "connected, schema incomplete"
    except Exception as exc:
        reachable = False
        missing_tables = tuple(REQUIRED_TABLES)
        table_counts = ()
        message = str(exc)

    return BackendStatusResult(
        backend=backend,
        backend_source=settings.database_backend_source,
        fallback_triggered=fallback_triggered,
        fallback_reason=fallback_reason,
        warning=warning,
        target_label=target_label,
        sqlite_path=runtime.sqlite_path,
        database_url_configured=runtime.database_url is not None,
        reachable=reachable,
        missing_tables=missing_tables,
        table_counts=table_counts,
        message=message,
    )


def ping_database_target(settings: Settings) -> DatabasePingResult:
    runtime = resolve_database_runtime(settings.database_path, settings=settings)
    backend = runtime.backend
    target_label = runtime.target_label
    fallback_triggered, warning, fallback_reason = _resolve_fallback_details(settings)
    try:
        with closing(connect_database(settings.database_path, settings=settings)) as connection:
            backend = backend_name_for_connection(connection)
            target_label = target_label_for_connection(connection, runtime=runtime)
            fallback_triggered, warning, fallback_reason = _resolve_fallback_details(
                settings,
                connection=connection,
            )
            if backend == "postgres":
                row = connection.execute("SELECT version() AS version").fetchone()
                message = str(row["version"]).split(",", 1)[0]
            else:
                row = connection.execute("SELECT sqlite_version() AS version").fetchone()
                message = f"SQLite {row['version']}"
        return DatabasePingResult(
            backend=backend,
            backend_source=settings.database_backend_source,
            fallback_triggered=fallback_triggered,
            fallback_reason=fallback_reason,
            warning=warning,
            target_label=target_label,
            ok=True,
            message=message,
        )
    except Exception as exc:
        return DatabasePingResult(
            backend=backend,
            backend_source=settings.database_backend_source,
            fallback_triggered=fallback_triggered,
            fallback_reason=fallback_reason,
            warning=warning,
            target_label=target_label,
            ok=False,
            message=str(exc),
        )


def _resolve_fallback_details(
    settings: Settings,
    *,
    connection=None,
) -> tuple[bool, str | None, str | None]:
    fallback_triggered = settings.database_fallback_triggered
    warning = settings.database_warning
    fallback_reason = warning if fallback_triggered else None
    if connection is not None and fallback_triggered_for_connection(connection):
        fallback_triggered = True
        warning = POSTGRES_SQLITE_RUNTIME_FALLBACK_WARNING
        fallback_reason = fallback_reason_for_connection(connection) or warning
    return fallback_triggered, warning, fallback_reason


def _missing_required_tables_for_connection(connection) -> tuple[str, ...]:
    return tuple(sorted(set(REQUIRED_TABLES) - table_names_from_connection(connection)))


def _collect_required_table_counts_for_connection(
    connection,
    *,
    missing_tables: tuple[str, ...],
) -> tuple[tuple[str, int], ...]:
    if missing_tables:
        return ()
    return tuple(
        (
            table_name,
            int(
                connection.execute(
                    f"SELECT COUNT(*) AS count FROM {table_name}"
                ).fetchone()["count"]
            ),
        )
        for table_name in REQUIRED_TABLES
    )


def migrate_sqlite_to_postgres(
    source_path: Path,
    *,
    database_url: str,
    dry_run: bool = False,
) -> PostgresMigrationResult:
    resolved_source_path = source_path.resolve()
    if not resolved_source_path.exists():
        raise ValueError(f"source SQLite database was not found: {resolved_source_path}")
    if not resolved_source_path.is_file():
        raise ValueError(f"source SQLite path is not a regular file: {resolved_source_path}")
    normalized_database_url = database_url.strip()
    if not normalized_database_url:
        raise ValueError("DATABASE_URL is required for SQLite-to-Postgres migration")

    working_source_path = resolved_source_path
    temp_dir: tempfile.TemporaryDirectory[str] | None = None
    if dry_run:
        temp_dir = tempfile.TemporaryDirectory(prefix="jobs-ai-postgres-migrate-")
        working_source_path = Path(temp_dir.name) / resolved_source_path.name
        shutil.copy2(resolved_source_path, working_source_path)

    try:
        with closing(connect_sqlite_database(working_source_path)) as source_connection:
            source_assessment_before = _assess_sqlite_source_schema(source_connection)
            initialize_schema_connection(source_connection)
            source_connection.commit()

            with closing(
                connect_database(
                    resolved_source_path,
                    backend="postgres",
                    database_url=normalized_database_url,
                )
            ) as target_connection:
                target_missing_tables_before = tuple(
                    sorted(set(REQUIRED_TABLES) - table_names_from_connection(target_connection))
                )
                try:
                    initialize_schema_connection(
                        target_connection,
                        backfill_identity=False,
                        include_secondary_indexes=False,
                    )
                    fast_path_used = is_empty_postgres_target(target_connection)
                    if fast_path_used:
                        (
                            source_registry_summary,
                            jobs_summary,
                            applications_summary,
                            application_tracking_summary,
                            session_history_summary,
                        ) = _bulk_seed_empty_postgres_target(
                            source_connection,
                            target_connection,
                        )
                        initialize_schema_connection(
                            target_connection,
                            backfill_identity=False,
                        )
                    else:
                        initialize_schema_connection(
                            target_connection,
                            backfill_identity=False,
                        )
                        source_registry_summary, source_registry_id_map = _copy_source_registry(
                            source_connection,
                            target_connection,
                        )
                        jobs_summary, job_id_map = _copy_jobs(
                            source_connection,
                            target_connection,
                            source_registry_id_map=source_registry_id_map,
                        )
                        applications_summary = _copy_child_table_with_job_mapping(
                            source_connection,
                            target_connection,
                            table_name="applications",
                            selected_columns=_APPLICATION_COLUMNS,
                            mutable_columns=_APPLICATION_MUTABLE_COLUMNS,
                            job_id_map=job_id_map,
                        )
                        application_tracking_summary = _copy_child_table_with_job_mapping(
                            source_connection,
                            target_connection,
                            table_name="application_tracking",
                            selected_columns=_APPLICATION_TRACKING_COLUMNS,
                            mutable_columns=_APPLICATION_TRACKING_MUTABLE_COLUMNS,
                            job_id_map=job_id_map,
                        )
                        session_history_summary = _copy_simple_table(
                            source_connection,
                            target_connection,
                            table_name="session_history",
                            selected_columns=_SESSION_HISTORY_COLUMNS,
                        )
                    _sync_identity_sequences(target_connection)
                    if dry_run:
                        target_connection.rollback()
                    else:
                        target_connection.commit()
                except Exception:
                    target_connection.rollback()
                    raise

        return PostgresMigrationResult(
            dry_run=dry_run,
            fast_path_used=fast_path_used,
            source_path=resolved_source_path,
            target_label=resolve_database_runtime(
                resolved_source_path,
                backend="postgres",
                database_url=normalized_database_url,
            ).target_label,
            source_missing_tables_before=source_assessment_before[0],
            source_missing_job_columns_before=source_assessment_before[1],
            target_missing_tables_before=target_missing_tables_before,
            source_registry=source_registry_summary,
            jobs=jobs_summary,
            applications=applications_summary,
            application_tracking=application_tracking_summary,
            session_history=session_history_summary,
        )
    finally:
        if temp_dir is not None:
            temp_dir.cleanup()


def _assess_sqlite_source_schema(
    connection: sqlite3.Connection,
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    existing_tables = table_names_from_connection(connection)
    missing_tables = tuple(sorted(set(REQUIRED_TABLES) - existing_tables))
    missing_job_columns: tuple[str, ...] = ()
    if "jobs" in existing_tables:
        available_columns = set(table_columns_from_connection(connection, "jobs"))
        missing_job_columns = tuple(
            column_name
            for column_name in _REQUIRED_JOB_COLUMNS
            if column_name not in available_columns
        )
    return missing_tables, missing_job_columns


def is_empty_postgres_target(target_connection) -> bool:
    existing_tables = table_names_from_connection(target_connection)
    if any(table_name not in existing_tables for table_name in REQUIRED_TABLES):
        return False

    for table_name in REQUIRED_TABLES:
        row = target_connection.execute(
            f"SELECT 1 AS present FROM {table_name} LIMIT 1"
        ).fetchone()
        if row is not None:
            return False
    return True


def _bulk_seed_empty_postgres_target(
    source_connection: sqlite3.Connection,
    target_connection,
) -> tuple[TableCopySummary, TableCopySummary, TableCopySummary, TableCopySummary, TableCopySummary]:
    source_registry_rows = [
        _registry_values_from_row(row)
        for row in source_connection.execute(
            f"SELECT {', '.join(_SOURCE_REGISTRY_COLUMNS)} FROM source_registry ORDER BY id"
        ).fetchall()
    ]
    _bulk_insert_rows(
        target_connection,
        "source_registry",
        _SOURCE_REGISTRY_COLUMNS,
        source_registry_rows,
    )

    source_registry_id_map = {
        int(row["id"]): int(row["id"])
        for row in source_registry_rows
    }
    job_rows = [
        _job_values_from_row(row, source_registry_id_map=source_registry_id_map)
        for row in source_connection.execute(
            f"SELECT {', '.join(_JOB_COLUMNS)} FROM jobs ORDER BY id"
        ).fetchall()
    ]
    _bulk_insert_rows(
        target_connection,
        "jobs",
        _JOB_COLUMNS,
        job_rows,
    )

    application_rows = [
        _values_from_row(row, _APPLICATION_COLUMNS)
        for row in source_connection.execute(
            f"SELECT {', '.join(_APPLICATION_COLUMNS)} FROM applications ORDER BY id"
        ).fetchall()
    ]
    _bulk_insert_rows(
        target_connection,
        "applications",
        _APPLICATION_COLUMNS,
        application_rows,
    )

    application_tracking_rows = [
        _values_from_row(row, _APPLICATION_TRACKING_COLUMNS)
        for row in source_connection.execute(
            f"SELECT {', '.join(_APPLICATION_TRACKING_COLUMNS)} FROM application_tracking ORDER BY id"
        ).fetchall()
    ]
    _bulk_insert_rows(
        target_connection,
        "application_tracking",
        _APPLICATION_TRACKING_COLUMNS,
        application_tracking_rows,
    )

    session_history_rows = [
        _values_from_row(row, _SESSION_HISTORY_COLUMNS)
        for row in source_connection.execute(
            f"SELECT {', '.join(_SESSION_HISTORY_COLUMNS)} FROM session_history ORDER BY id"
        ).fetchall()
    ]
    _bulk_insert_rows(
        target_connection,
        "session_history",
        _SESSION_HISTORY_COLUMNS,
        session_history_rows,
    )

    return (
        _insert_only_table_copy_summary(len(source_registry_rows)),
        _insert_only_table_copy_summary(len(job_rows)),
        _insert_only_table_copy_summary(len(application_rows)),
        _insert_only_table_copy_summary(len(application_tracking_rows)),
        _insert_only_table_copy_summary(len(session_history_rows)),
    )


def _bulk_insert_rows(
    target_connection,
    table_name: str,
    column_names: tuple[str, ...],
    rows: list[dict[str, object]],
    *,
    batch_size: int = 1000,
) -> None:
    if not rows:
        return

    effective_batch_size = batch_size
    if backend_name_for_connection(target_connection) == "sqlite":
        effective_batch_size = min(
            batch_size,
            max(1, 900 // len(column_names)),
        )

    value_group = f"({', '.join('?' for _ in column_names)})"
    for start_index in range(0, len(rows), effective_batch_size):
        batch = rows[start_index : start_index + effective_batch_size]
        target_connection.execute(
            f"""
            INSERT INTO {table_name} ({', '.join(column_names)})
            VALUES {', '.join(value_group for _ in batch)}
            """,
            tuple(
                row[column_name]
                for row in batch
                for column_name in column_names
            ),
        )


def _insert_only_table_copy_summary(row_count: int) -> TableCopySummary:
    return TableCopySummary(
        scanned_count=row_count,
        inserted_count=row_count,
        updated_count=0,
        matched_count=0,
        unchanged_count=0,
    )


def _copy_source_registry(
    source_connection: sqlite3.Connection,
    target_connection,
) -> tuple[TableCopySummary, dict[int, int]]:
    rows = source_connection.execute(
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
        ORDER BY id
        """
    ).fetchall()

    inserted_count = 0
    updated_count = 0
    matched_count = 0
    unchanged_count = 0
    source_registry_id_map: dict[int, int] = {}
    for row in rows:
        values = _registry_values_from_row(row)
        existing_by_url = target_connection.execute(
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
            (values["normalized_url"],),
        ).fetchone()
        existing_by_id = target_connection.execute(
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
            WHERE id = ?
            LIMIT 1
            """,
            (values["id"],),
        ).fetchone()

        target_id: int
        target_row = existing_by_url if existing_by_url is not None else existing_by_id
        if target_row is None:
            target_connection.execute(
                """
                INSERT INTO source_registry (
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
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                tuple(values[column_name] for column_name in values),
            )
            inserted_count += 1
            target_id = int(values["id"])
        else:
            target_id = int(target_row["id"])
            if target_id != int(values["id"]):
                matched_count += 1
            if _row_matches_values(target_row, values, ignore_columns=("id",)):
                unchanged_count += 1
            else:
                target_connection.execute(
                    """
                    UPDATE source_registry
                    SET source_url = ?,
                        normalized_url = ?,
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
                        values["source_url"],
                        values["normalized_url"],
                        values["portal_type"],
                        values["company"],
                        values["label"],
                        values["status"],
                        values["first_seen_at"],
                        values["last_verified_at"],
                        values["notes"],
                        values["provenance"],
                        values["verification_reason_code"],
                        values["verification_reason"],
                        values["created_at"],
                        values["updated_at"],
                        target_id,
                    ),
                )
                updated_count += 1
        source_registry_id_map[int(values["id"])] = target_id

    return (
        TableCopySummary(
            scanned_count=len(rows),
            inserted_count=inserted_count,
            updated_count=updated_count,
            matched_count=matched_count,
            unchanged_count=unchanged_count,
        ),
        source_registry_id_map,
    )


def _copy_jobs(
    source_connection: sqlite3.Connection,
    target_connection,
    *,
    source_registry_id_map: dict[int, int],
) -> tuple[TableCopySummary, dict[int, int]]:
    rows = source_connection.execute(
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
            raw_json,
            created_at,
            updated_at
        FROM jobs
        ORDER BY id
        """
    ).fetchall()

    inserted_count = 0
    updated_count = 0
    matched_count = 0
    unchanged_count = 0
    job_id_map: dict[int, int] = {}
    for row in rows:
        values = _job_values_from_row(row, source_registry_id_map=source_registry_id_map)
        existing_by_id = target_connection.execute(
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
                raw_json,
                created_at,
                updated_at
            FROM jobs
            WHERE id = ?
            LIMIT 1
            """,
            (values["id"],),
        ).fetchone()
        duplicate_match = None
        target_id: int
        target_row = existing_by_id
        if target_row is None:
            duplicate_match = find_duplicate_job_match(
                target_connection,
                {
                    "source": values["source"],
                    "source_job_id": values["source_job_id"],
                    "company": values["company"],
                    "title": values["title"],
                    "location": values["location"],
                    "apply_url": values["apply_url"],
                    "portal_type": values["portal_type"],
                    "canonical_apply_url": values["canonical_apply_url"],
                    "identity_key": values["identity_key"],
                },
            )
            if duplicate_match is not None:
                target_row = target_connection.execute(
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
                        raw_json,
                        created_at,
                        updated_at
                    FROM jobs
                    WHERE id = ?
                    LIMIT 1
                    """,
                    (duplicate_match.job_id,),
                ).fetchone()

        if target_row is None:
            target_connection.execute(
                """
                INSERT INTO jobs (
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
                    raw_json,
                    created_at,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                tuple(values[column_name] for column_name in values),
            )
            inserted_count += 1
            target_id = int(values["id"])
        else:
            target_id = int(target_row["id"])
            if duplicate_match is not None and target_id != int(values["id"]):
                matched_count += 1
            if _row_matches_values(target_row, values, ignore_columns=("id",)):
                unchanged_count += 1
            else:
                target_connection.execute(
                    """
                    UPDATE jobs
                    SET source = ?,
                        source_job_id = ?,
                        company = ?,
                        title = ?,
                        location = ?,
                        apply_url = ?,
                        portal_type = ?,
                        salary_text = ?,
                        posted_at = ?,
                        found_at = ?,
                        ingest_batch_id = ?,
                        source_query = ?,
                        import_source = ?,
                        source_registry_id = ?,
                        canonical_apply_url = ?,
                        identity_key = ?,
                        status = ?,
                        raw_json = ?,
                        created_at = ?,
                        updated_at = ?
                    WHERE id = ?
                    """,
                    (
                        values["source"],
                        values["source_job_id"],
                        values["company"],
                        values["title"],
                        values["location"],
                        values["apply_url"],
                        values["portal_type"],
                        values["salary_text"],
                        values["posted_at"],
                        values["found_at"],
                        values["ingest_batch_id"],
                        values["source_query"],
                        values["import_source"],
                        values["source_registry_id"],
                        values["canonical_apply_url"],
                        values["identity_key"],
                        values["status"],
                        values["raw_json"],
                        values["created_at"],
                        values["updated_at"],
                        target_id,
                    ),
                )
                updated_count += 1
        job_id_map[int(values["id"])] = target_id

    return (
        TableCopySummary(
            scanned_count=len(rows),
            inserted_count=inserted_count,
            updated_count=updated_count,
            matched_count=matched_count,
            unchanged_count=unchanged_count,
        ),
        job_id_map,
    )


def _copy_child_table_with_job_mapping(
    source_connection: sqlite3.Connection,
    target_connection,
    *,
    table_name: str,
    selected_columns: tuple[str, ...],
    mutable_columns: tuple[str, ...],
    job_id_map: dict[int, int],
) -> TableCopySummary:
    rows = source_connection.execute(
        f"SELECT {', '.join(selected_columns)} FROM {table_name} ORDER BY id"
    ).fetchall()

    inserted_count = 0
    updated_count = 0
    matched_count = 0
    unchanged_count = 0
    for row in rows:
        values = {
            column_name: _normalize_value(row[column_name])
            for column_name in selected_columns
        }
        mapped_job_id = job_id_map.get(int(values["job_id"]))
        if mapped_job_id is None:
            raise RuntimeError(
                f"{table_name} row {values['id']} references missing migrated job id {values['job_id']}"
            )
        if mapped_job_id != int(values["job_id"]):
            matched_count += 1
        values["job_id"] = mapped_job_id

        existing_row = target_connection.execute(
            f"SELECT {', '.join(selected_columns)} FROM {table_name} WHERE id = ? LIMIT 1",
            (values["id"],),
        ).fetchone()
        if existing_row is None:
            target_connection.execute(
                f"""
                INSERT INTO {table_name} ({', '.join(selected_columns)})
                VALUES ({', '.join('?' for _ in selected_columns)})
                """,
                tuple(values[column_name] for column_name in selected_columns),
            )
            inserted_count += 1
            continue

        if _row_matches_values(existing_row, values, ignore_columns=()):
            unchanged_count += 1
            continue

        assignments = ", ".join(f"{column_name} = ?" for column_name in mutable_columns)
        target_connection.execute(
            f"UPDATE {table_name} SET {assignments} WHERE id = ?",
            tuple(values[column_name] for column_name in mutable_columns) + (values["id"],),
        )
        updated_count += 1

    return TableCopySummary(
        scanned_count=len(rows),
        inserted_count=inserted_count,
        updated_count=updated_count,
        matched_count=matched_count,
        unchanged_count=unchanged_count,
    )


def _copy_simple_table(
    source_connection: sqlite3.Connection,
    target_connection,
    *,
    table_name: str,
    selected_columns: tuple[str, ...],
) -> TableCopySummary:
    rows = source_connection.execute(
        f"SELECT {', '.join(selected_columns)} FROM {table_name} ORDER BY id"
    ).fetchall()

    inserted_count = 0
    updated_count = 0
    unchanged_count = 0
    for row in rows:
        values = {
            column_name: _normalize_value(row[column_name])
            for column_name in selected_columns
        }
        existing_row = target_connection.execute(
            f"SELECT {', '.join(selected_columns)} FROM {table_name} WHERE id = ? LIMIT 1",
            (values["id"],),
        ).fetchone()
        if existing_row is None:
            target_connection.execute(
                f"""
                INSERT INTO {table_name} ({', '.join(selected_columns)})
                VALUES ({', '.join('?' for _ in selected_columns)})
                """,
                tuple(values[column_name] for column_name in selected_columns),
            )
            inserted_count += 1
            continue

        if _row_matches_values(existing_row, values, ignore_columns=()):
            unchanged_count += 1
            continue

        mutable_columns = tuple(column_name for column_name in selected_columns if column_name != "id")
        assignments = ", ".join(f"{column_name} = ?" for column_name in mutable_columns)
        target_connection.execute(
            f"UPDATE {table_name} SET {assignments} WHERE id = ?",
            tuple(values[column_name] for column_name in mutable_columns) + (values["id"],),
        )
        updated_count += 1

    return TableCopySummary(
        scanned_count=len(rows),
        inserted_count=inserted_count,
        updated_count=updated_count,
        matched_count=0,
        unchanged_count=unchanged_count,
    )


def _sync_identity_sequences(target_connection) -> None:
    if backend_name_for_connection(target_connection) != "postgres":
        return

    for table_name in REQUIRED_TABLES:
        max_id_row = target_connection.execute(
            f"SELECT COALESCE(MAX(id), 0) AS max_id FROM {table_name}"
        ).fetchone()
        max_id = int(max_id_row["max_id"] or 0)
        set_value = max(max_id, 1)
        is_called = "true" if max_id > 0 else "false"
        target_connection.execute(
            f"""
            SELECT setval(
                pg_get_serial_sequence('{table_name}', 'id'),
                ?,
                {is_called}
            )
            """,
            (set_value,),
        ).fetchone()


def _registry_values_from_row(row: sqlite3.Row) -> dict[str, object]:
    return {
        "id": int(row["id"]),
        "source_url": _normalize_value(row["source_url"]),
        "normalized_url": _normalize_value(row["normalized_url"]),
        "portal_type": _normalize_value(row["portal_type"]),
        "company": _normalize_value(row["company"]),
        "label": _normalize_value(row["label"]),
        "status": _normalize_value(row["status"]),
        "first_seen_at": _normalize_value(row["first_seen_at"]),
        "last_verified_at": _normalize_value(row["last_verified_at"]),
        "notes": _normalize_value(row["notes"]),
        "provenance": _normalize_value(row["provenance"]),
        "verification_reason_code": _normalize_value(row["verification_reason_code"]),
        "verification_reason": _normalize_value(row["verification_reason"]),
        "created_at": _normalize_value(row["created_at"]),
        "updated_at": _normalize_value(row["updated_at"]),
    }


def _job_values_from_row(
    row: sqlite3.Row,
    *,
    source_registry_id_map: dict[int, int],
) -> dict[str, object]:
    canonical_apply_url = normalize_optional_metadata(row["canonical_apply_url"])
    identity_key = normalize_optional_metadata(row["identity_key"])
    if canonical_apply_url is None or identity_key is None:
        identity = build_job_identity(row)
        canonical_apply_url = canonical_apply_url or identity.canonical_apply_url
        identity_key = identity_key or identity.identity_key
    source_registry_id = _nullable_int(row["source_registry_id"])
    return {
        "id": int(row["id"]),
        "source": _normalize_value(row["source"]),
        "source_job_id": _normalize_value(row["source_job_id"]),
        "company": _normalize_value(row["company"]),
        "title": _normalize_value(row["title"]),
        "location": _normalize_value(row["location"]),
        "apply_url": _normalize_value(row["apply_url"]),
        "portal_type": _normalize_value(row["portal_type"]),
        "salary_text": _normalize_value(row["salary_text"]),
        "posted_at": _normalize_value(row["posted_at"]),
        "found_at": _normalize_value(row["found_at"]),
        "ingest_batch_id": _normalize_value(row["ingest_batch_id"]),
        "source_query": _normalize_value(row["source_query"]),
        "import_source": _normalize_value(row["import_source"]),
        "source_registry_id": (
            source_registry_id_map.get(source_registry_id)
            if source_registry_id is not None
            else None
        ),
        "canonical_apply_url": canonical_apply_url,
        "identity_key": identity_key,
        "status": _normalize_value(row["status"]) or "new",
        "raw_json": _normalize_value(row["raw_json"]),
        "created_at": _normalize_value(row["created_at"]),
        "updated_at": _normalize_value(row["updated_at"]),
    }


def _values_from_row(
    row,
    selected_columns: tuple[str, ...],
) -> dict[str, object]:
    return {
        column_name: _normalize_value(row[column_name])
        for column_name in selected_columns
    }


def _row_matches_values(
    existing_row,
    values: dict[str, object],
    *,
    ignore_columns: tuple[str, ...],
) -> bool:
    ignored = set(ignore_columns)
    return all(
        _normalize_value(existing_row[column_name]) == _normalize_value(values[column_name])
        for column_name in values
        if column_name not in ignored
    )


def _normalize_value(value: object) -> object:
    if value is None:
        return None
    if isinstance(value, bool):
        return bool(value)
    if isinstance(value, int):
        return int(value)
    normalized_text = normalize_optional_metadata(value)
    if normalized_text is not None:
        return normalized_text
    return str(value)


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
