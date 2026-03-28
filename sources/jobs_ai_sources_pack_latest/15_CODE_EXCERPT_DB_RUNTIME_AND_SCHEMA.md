# Code Excerpt: DB Runtime and Schema

Exact excerpts from the current repo for backend selection, runtime fallback, canonical schema, dedupe, and backend inspection helpers.

## Database settings, env precedence, and config-time fallback
Source: `src/jobs_ai/config.py` lines 31-176

```python
SUPPORTED_DB_BACKENDS = ("sqlite", "postgres")
DEFAULT_DB_BACKEND = "postgres"
POSTGRES_SQLITE_FALLBACK_WARNING = "Postgres config missing, falling back to SQLite"


def discover_project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def load_repo_env_file(project_root: Path | None = None) -> dict[str, str]:
    env_path = (discover_project_root() if project_root is None else project_root) / ".env"
    if not env_path.exists():
        return {}

    values: dict[str, str] = {}
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export "):].strip()
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        normalized_key = key.strip()
        if not normalized_key:
            continue
        normalized_value = value.strip()
        if (
            len(normalized_value) >= 2
            and normalized_value[0] == normalized_value[-1]
            and normalized_value[0] in {"'", '"'}
        ):
            normalized_value = normalized_value[1:-1]
        values[normalized_key] = normalized_value
    return values


def _build_database_url_from_pg_env(source: Mapping[str, str]) -> str | None:
    host = source.get("PGHOST", "").strip()
    database = source.get("PGDATABASE", "").strip()
    if not host or not database:
        return None

    user = source.get("PGUSER", "").strip()
    password = source.get("PGPASSWORD", "").strip()
    port = source.get("PGPORT", "").strip() or "5432"
    sslmode = source.get("PGSSLMODE", "").strip() or "require"
    credentials = user
    if password:
        credentials = f"{credentials}:{password}" if credentials else f":{password}"
    if credentials:
        credentials = f"{credentials}@"
    return f"postgresql://{credentials}{host}:{port}/{database}?sslmode={sslmode}"


def _is_valid_postgres_database_url(database_url: str) -> bool:
    parsed = urlparse(database_url)
    return (
        parsed.scheme in {"postgres", "postgresql"}
        and bool(parsed.hostname)
        and bool(parsed.path)
        and parsed.path != "/"
    )


class Settings(BaseModel):
    model_config = ConfigDict(frozen=True)

    environment: str = Field(default="dev")
    profile: str = Field(default="default")
    database_backend: str = Field(default=DEFAULT_DB_BACKEND)
    database_backend_source: str = Field(default="default")
    database_fallback_triggered: bool = Field(default=False)
    database_warning: str | None = Field(default=None)
    sqlite_path: Path = Field(default=Path("data") / "jobs_ai.db")
    database_url: str | None = Field(default=None)

    @property
    def database_path(self) -> Path:
        return self.sqlite_path


def load_settings(env: Mapping[str, str] | None = None) -> Settings:
    if env is None:
        env_file = load_repo_env_file()
        source = {
            **env_file,
            **os.environ,
        }
        sqlite_path_value = (
            os.environ.get("JOBS_AI_SQLITE_PATH")
            or os.environ.get("JOBS_AI_DB_PATH")
            or env_file.get("JOBS_AI_SQLITE_PATH")
            or env_file.get("JOBS_AI_DB_PATH")
            or "data/jobs_ai.db"
        )
    else:
        source = dict(env)
        sqlite_path_value = (
            source.get("JOBS_AI_SQLITE_PATH")
            or source.get("JOBS_AI_DB_PATH")
            or "data/jobs_ai.db"
        )
    raw_database_backend = source.get("JOBS_AI_DB_BACKEND")
    if raw_database_backend is None or not raw_database_backend.strip():
        database_backend = DEFAULT_DB_BACKEND
        database_backend_source = "default"
    else:
        database_backend = raw_database_backend.strip().lower()
        database_backend_source = "env"
    if database_backend not in SUPPORTED_DB_BACKENDS:
        supported = ", ".join(SUPPORTED_DB_BACKENDS)
        raise ValueError(
            f"unsupported JOBS_AI_DB_BACKEND '{database_backend}'; expected one of: {supported}"
        )
    database_url = source.get("DATABASE_URL")
    if database_url is None:
        database_url = _build_database_url_from_pg_env(source)
    normalized_database_url = (
        database_url.strip() if database_url is not None and database_url.strip() else None
    )
    fallback_triggered = False
    warning: str | None = None
    sqlite_path = Path(sqlite_path_value)
    postgres_config_missing = normalized_database_url is None
    postgres_config_invalid = (
        normalized_database_url is not None
        and not _is_valid_postgres_database_url(normalized_database_url)
    )
    if database_backend == "postgres" and (postgres_config_missing or postgres_config_invalid):
        fallback_triggered = True
        warning = POSTGRES_SQLITE_FALLBACK_WARNING
        database_backend = "sqlite"
        normalized_database_url = None

    return Settings(
        environment=source.get("JOBS_AI_ENV", "dev"),
        profile=source.get("JOBS_AI_PROFILE", "default"),
        database_backend=database_backend,
        database_backend_source=database_backend_source,
        database_fallback_triggered=fallback_triggered,
        database_warning=warning,
        sqlite_path=sqlite_path,
        database_url=normalized_database_url,
    )
```

## Database runtime resolution and connect_database fallback logic
Source: `src/jobs_ai/db_runtime.py` lines 149-237

```python
def resolve_database_runtime(
    database_path: Path | None = None,
    *,
    settings: Settings | None = None,
    backend: str | None = None,
    database_url: str | None = None,
    sqlite_path: Path | None = None,
) -> DatabaseRuntime:
    resolved_settings = load_settings() if settings is None else settings
    resolved_sqlite_path = (
        resolved_settings.sqlite_path
        if sqlite_path is None
        else sqlite_path
    )
    if database_path is not None:
        resolved_sqlite_path = database_path
    resolved_backend = (
        resolved_settings.database_backend
        if backend is None
        else backend.strip().lower()
    )
    resolved_database_url = (
        resolved_settings.database_url
        if database_url is None
        else database_url.strip() or None
    )
    return DatabaseRuntime(
        backend=resolved_backend,
        sqlite_path=resolved_sqlite_path,
        database_url=resolved_database_url,
    )


def connect_sqlite_database(database_path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(database_path)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    return connection


def connect_database(
    database_path: Path,
    *,
    settings: Settings | None = None,
    backend: str | None = None,
    database_url: str | None = None,
) -> SQLiteConnectionAdapter | PostgresConnectionAdapter:
    runtime = resolve_database_runtime(
        database_path,
        settings=settings,
        backend=backend,
        database_url=database_url,
    )
    if runtime.backend == "sqlite":
        return _connect_sqlite_runtime(runtime)

    try:
        connection = _connect_postgres_runtime(runtime)
    except _fallback_exception_types() as exc:
        fallback_reason = _sanitize_database_error_text(
            str(exc),
            runtime.database_url,
        )
        LOGGER.warning(
            "%s: %s",
            POSTGRES_SQLITE_RUNTIME_FALLBACK_WARNING,
            fallback_reason,
        )
        try:
            return _connect_sqlite_runtime(
                runtime,
                fallback_triggered=True,
                fallback_reason=fallback_reason,
            )
        except Exception as sqlite_exc:
            sqlite_reason = _sanitize_database_error_text(
                str(sqlite_exc),
                runtime.database_url,
            )
            raise RuntimeError(
                f"{POSTGRES_SQLITE_RUNTIME_FALLBACK_WARNING}: {fallback_reason}; "
                f"SQLite fallback failed: {sqlite_reason}"
            ) from sqlite_exc
    return PostgresConnectionAdapter(
        connection,
        target_label=runtime.target_label,
    )
```

## Postgres and SQLite backend helpers
Source: `src/jobs_ai/db_runtime.py` lines 429-498

```python
def _connect_postgres_runtime(runtime: DatabaseRuntime):
    if runtime.database_url is None:
        raise RuntimeError(
            "JOBS_AI_DB_BACKEND=postgres requires DATABASE_URL or PGHOST/PGDATABASE-style settings"
        )
    if psycopg is None:
        raise RuntimeError(
            "Postgres support requires psycopg. Install project dependencies before using Neon/Postgres mode."
        )
    return psycopg.connect(
        runtime.database_url,
        row_factory=dict_row,
        autocommit=False,
    )


def _connect_sqlite_runtime(
    runtime: DatabaseRuntime,
    *,
    fallback_triggered: bool = False,
    fallback_reason: str | None = None,
) -> SQLiteConnectionAdapter:
    runtime.sqlite_path.parent.mkdir(parents=True, exist_ok=True)
    connection = connect_sqlite_database(runtime.sqlite_path)
    return SQLiteConnectionAdapter(
        connection,
        target_label=str(runtime.sqlite_path),
        fallback_triggered=fallback_triggered,
        fallback_reason=fallback_reason,
    )


def _fallback_exception_types() -> tuple[type[Exception], ...]:
    fallback_exception_types: tuple[type[Exception], ...] = (
        OSError,
        RuntimeError,
        ValueError,
    )
    if psycopg is None:
        return fallback_exception_types
    return fallback_exception_types + (psycopg.Error,)


def _sanitize_database_error_text(message: str, database_url: str | None) -> str:
    sanitized = message.strip() or "connection failed"
    if database_url is not None:
        parsed = urlparse(database_url)
        if parsed.password:
            sanitized = sanitized.replace(parsed.password, "****")
        sanitized = sanitized.replace(database_url, mask_database_url(database_url))
    return _POSTGRES_URL_PATTERN.sub(
        lambda match: mask_database_url(match.group(0)),
        sanitized,
    )


def _resolve_postgres_lastrowid(connection, *, is_insert: bool) -> int | None:
    if not is_insert:
        return None
    cursor = connection.cursor(row_factory=dict_row)
    try:
        cursor.execute("SELECT LASTVAL() AS id")
        row = cursor.fetchone()
        if row is None:
            return None
        return int(row["id"])
    except Exception:
        return None
    finally:
        cursor.close()
```

## Core schema and indexes
Source: `src/jobs_ai/db.py` lines 21-122

```python
REQUIRED_TABLES = (
    "jobs",
    "applications",
    "application_tracking",
    "session_history",
    "source_registry",
)

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS source_registry (
    id INTEGER PRIMARY KEY,
    source_url TEXT NOT NULL,
    normalized_url TEXT NOT NULL,
    portal_type TEXT,
    company TEXT,
    label TEXT,
    status TEXT NOT NULL DEFAULT 'manual_review',
    first_seen_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_verified_at TEXT,
    notes TEXT,
    provenance TEXT,
    verification_reason_code TEXT,
    verification_reason TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS jobs (
    id INTEGER PRIMARY KEY,
    source TEXT NOT NULL,
    source_job_id TEXT,
    company TEXT NOT NULL,
    title TEXT NOT NULL,
    location TEXT,
    apply_url TEXT,
    portal_type TEXT,
    salary_text TEXT,
    posted_at TEXT,
    found_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    ingest_batch_id TEXT,
    source_query TEXT,
    import_source TEXT,
    source_registry_id INTEGER,
    canonical_apply_url TEXT,
    identity_key TEXT,
    status TEXT NOT NULL DEFAULT 'new',
    raw_json TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (source_registry_id) REFERENCES source_registry(id) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS applications (
    id INTEGER PRIMARY KEY,
    job_id INTEGER NOT NULL,
    state TEXT NOT NULL DEFAULT 'draft',
    resume_variant TEXT,
    notes TEXT,
    last_attempted_at TEXT,
    applied_at TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (job_id) REFERENCES jobs(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS application_tracking (
    id INTEGER PRIMARY KEY,
    job_id INTEGER NOT NULL,
    status TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (job_id) REFERENCES jobs(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS session_history (
    id INTEGER PRIMARY KEY,
    manifest_path TEXT NOT NULL,
    item_count INTEGER NOT NULL,
    launchable_count INTEGER NOT NULL,
    ingest_batch_id TEXT,
    source_query TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_jobs_apply_url ON jobs(apply_url);
CREATE INDEX IF NOT EXISTS idx_jobs_source_company_title_location
ON jobs(source, company, title, location);
CREATE INDEX IF NOT EXISTS idx_applications_job_id ON applications(job_id);
CREATE INDEX IF NOT EXISTS idx_application_tracking_job_id ON application_tracking(job_id);
CREATE INDEX IF NOT EXISTS idx_session_history_created_at ON session_history(created_at);
CREATE INDEX IF NOT EXISTS idx_session_history_ingest_batch_id ON session_history(ingest_batch_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_source_registry_normalized_url ON source_registry(normalized_url);
CREATE INDEX IF NOT EXISTS idx_source_registry_status ON source_registry(status);
CREATE INDEX IF NOT EXISTS idx_source_registry_last_verified_at ON source_registry(last_verified_at);
"""

POST_SCHEMA_SQL = """
CREATE INDEX IF NOT EXISTS idx_jobs_ingest_batch_id ON jobs(ingest_batch_id);
CREATE INDEX IF NOT EXISTS idx_jobs_canonical_apply_url ON jobs(canonical_apply_url);
CREATE INDEX IF NOT EXISTS idx_jobs_identity_key ON jobs(identity_key);
CREATE INDEX IF NOT EXISTS idx_jobs_source_registry_id ON jobs(source_registry_id);
"""
```

## Schema init, insert_job, duplicate detection, and session history writes
Source: `src/jobs_ai/db.py` lines 387-544

```python
def initialize_schema(database_path: Path, *, backfill_identity: bool = True) -> None:
    runtime = resolve_database_runtime(database_path)
    if runtime.backend == "sqlite" and not runtime.sqlite_path.exists():
        database_path.parent.mkdir(parents=True, exist_ok=True)
    with closing(connect_database(database_path)) as connection:
        _initialize_schema_connection(
            connection,
            backfill_identity=backfill_identity,
            include_secondary_indexes=True,
        )
        connection.commit()


def existing_tables(database_path: Path) -> set[str]:
    if not database_exists(database_path):
        return set()

    with closing(connect_database(database_path)) as connection:
        return table_names_from_connection(connection)


def missing_required_tables(database_path: Path) -> list[str]:
    return sorted(set(REQUIRED_TABLES) - existing_tables(database_path))


def schema_exists(database_path: Path) -> bool:
    return database_exists(database_path) and not missing_required_tables(database_path)


def insert_job(connection, job_record: Mapping[str, object]) -> int:
    identity = build_job_identity(job_record)
    cursor = connection.execute(
        JOB_INSERT_SQL,
        (
            job_record["source"],
            job_record.get("source_job_id"),
            job_record["company"],
            job_record["title"],
            job_record["location"],
            job_record["apply_url"],
            job_record.get("portal_type"),
            job_record.get("salary_text"),
            job_record.get("posted_at"),
            job_record.get("found_at"),
            normalize_optional_metadata(job_record.get("ingest_batch_id")),
            normalize_optional_metadata(job_record.get("source_query")),
            normalize_optional_metadata(job_record.get("import_source")),
            _nullable_int(job_record.get("source_registry_id")),
            identity.canonical_apply_url,
            identity.identity_key,
            job_record["raw_json"],
        ),
    )
    return int(cursor.lastrowid)


def find_duplicate_job_match(
    connection,
    job_record: Mapping[str, object],
) -> DuplicateJobMatch | None:
    apply_url = job_record.get("apply_url")
    if apply_url is not None:
        row = connection.execute(
            EXACT_APPLY_URL_MATCH_SQL,
            (apply_url,),
        ).fetchone()
        if row is not None:
            return DuplicateJobMatch(
                job_id=int(row["id"]),
                rule="exact apply_url match",
                matched_value=apply_url,
            )

    identity = build_job_identity(job_record)
    if identity.canonical_apply_url is not None:
        row = connection.execute(
            CANONICAL_APPLY_URL_MATCH_SQL,
            (identity.canonical_apply_url,),
        ).fetchone()
        if row is not None:
            return DuplicateJobMatch(
                job_id=int(row["id"]),
                rule="canonical apply_url match",
                matched_value=identity.canonical_apply_url,
            )

    if not _should_use_identity_key_match(job_record):
        return None

    row = connection.execute(
        IDENTITY_KEY_MATCH_SQL,
        (identity.identity_key,),
    ).fetchone()
    if row is None:
        return None
    return DuplicateJobMatch(
        job_id=int(row["id"]),
        rule="identity key match",
        matched_value=_describe_identity_match(job_record),
    )


def find_duplicate_job_id(
    connection,
    job_record: Mapping[str, object],
) -> int | None:
    match = find_duplicate_job_match(connection, job_record)
    if match is None:
        return None
    return match.job_id


def get_ingest_batch_summary(
    database_path: Path,
    *,
    batch_id: str,
) -> IngestBatchSummary | None:
    with closing(connect_database(database_path)) as connection:
        row = connection.execute(
            GET_INGEST_BATCH_SUMMARY_SQL,
            (batch_id,),
        ).fetchone()
    if row is None:
        return None
    return IngestBatchSummary(
        batch_id=str(row["ingest_batch_id"]),
        source_query=_nullable_text(row["source_query"]),
        import_source=_nullable_text(row["import_source"]),
        job_count=int(row["job_count"]),
    )


def record_session_history(
    database_path: Path,
    *,
    manifest_path: Path,
    item_count: int,
    launchable_count: int,
    batch_id: str | None,
    source_query: str | None,
    created_at: str | None = None,
) -> int:
    with closing(connect_database(database_path)) as connection:
        cursor = connection.execute(
            SESSION_HISTORY_INSERT_SQL,
            (
                str(manifest_path),
                item_count,
                launchable_count,
                normalize_optional_metadata(batch_id),
                normalize_optional_metadata(source_query),
                normalize_optional_metadata(created_at),
            ),
        )
        connection.commit()
    return int(cursor.lastrowid)
```

## Job identity and canonical apply URL logic
Source: `src/jobs_ai/jobs/identity.py` lines 1-133

```python
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
import re
from urllib.parse import urlparse, urlunparse

from ..portal_support import build_portal_support

_BATCH_ID_RE = re.compile(r"[^A-Za-z0-9._-]+")
_KEY_TEXT_RE = re.compile(r"[\s,]+")


@dataclass(frozen=True, slots=True)
class JobIdentity:
    canonical_apply_url: str | None
    identity_key: str


def build_job_identity(job_record: Mapping[str, object]) -> JobIdentity:
    portal_type = _normalized_portal_type(_field_value(job_record, "portal_type"))
    apply_url = _normalized_text(_field_value(job_record, "apply_url"))
    canonical_apply_url = canonicalize_apply_url(
        apply_url,
        portal_type=portal_type,
    )
    apply_host = _apply_host(canonical_apply_url or apply_url)
    source_job_id = _normalized_key_text(_field_value(job_record, "source_job_id"))
    company = _normalized_key_text(_field_value(job_record, "company")) or "<missing-company>"
    title = _normalized_key_text(_field_value(job_record, "title")) or "<missing-title>"
    location = _normalized_key_text(_field_value(job_record, "location")) or "<missing-location>"
    source = _normalized_key_text(_field_value(job_record, "source")) or "<missing-source>"
    portal_or_host = portal_type or apply_host

    if source_job_id is not None:
        anchor = portal_or_host or source
        identity_key = f"{anchor}|job_id|{source_job_id}"
    elif portal_or_host is not None:
        identity_key = f"{portal_or_host}|{company}|{title}|{location}"
    else:
        identity_key = f"{source}|{company}|{title}|{location}"

    return JobIdentity(
        canonical_apply_url=canonical_apply_url,
        identity_key=identity_key,
    )


def canonicalize_apply_url(
    apply_url: str | None,
    *,
    portal_type: str | None = None,
) -> str | None:
    normalized_apply_url = _normalized_text(apply_url)
    if normalized_apply_url is None:
        return None

    portal_support = build_portal_support(
        normalized_apply_url,
        portal_type=portal_type,
    )
    if portal_support is not None:
        if portal_support.company_apply_url is not None:
            return portal_support.company_apply_url
        return portal_support.normalized_apply_url

    parsed_url = urlparse(normalized_apply_url)
    if not parsed_url.scheme or not parsed_url.netloc:
        return normalized_apply_url

    normalized_path = parsed_url.path or ""
    if normalized_path != "/" and normalized_path.endswith("/"):
        normalized_path = normalized_path.rstrip("/")

    return urlunparse(
        parsed_url._replace(
            scheme=parsed_url.scheme.lower(),
            netloc=parsed_url.netloc.lower(),
            path=normalized_path,
            fragment="",
        )
    )


def normalize_batch_id(value: str | None) -> str | None:
    normalized_value = _normalized_text(value)
    if normalized_value is None:
        return None
    slug = _BATCH_ID_RE.sub("-", normalized_value).strip("-.")
    if not slug:
        raise ValueError("batch id must contain at least one letter or number")
    return slug


def normalize_optional_metadata(value: object) -> str | None:
    return _normalized_text(value)


def _normalized_portal_type(value: object) -> str | None:
    normalized_value = _normalized_text(value)
    if normalized_value is None:
        return None
    return normalized_value.lower()


def _normalized_text(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    text = value.strip()
    return text or None


def _normalized_key_text(value: object) -> str | None:
    normalized_value = _normalized_text(value)
    if normalized_value is None:
        return None
    return _KEY_TEXT_RE.sub(" ", normalized_value.casefold()).strip()


def _apply_host(apply_url: str | None) -> str | None:
    if apply_url is None:
        return None
    parsed_url = urlparse(apply_url)
    if not parsed_url.netloc:
        return None
    return parsed_url.netloc.lower()


def _field_value(job_record: Mapping[str, object], field_name: str) -> object:
    try:
        return job_record[field_name]
    except KeyError:
        return None
```

## Backend status and ping helpers
Source: `src/jobs_ai/db_postgres.py` lines 177-263

```python
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
```
