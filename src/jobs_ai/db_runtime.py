from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
import sqlite3
from urllib.parse import urlparse, urlunparse

from .config import Settings, load_settings

try:
    import psycopg
    from psycopg.rows import dict_row
except ImportError:  # pragma: no cover - exercised in environments without psycopg installed.
    psycopg = None
    dict_row = None


@dataclass(frozen=True, slots=True)
class DatabaseRuntime:
    backend: str
    sqlite_path: Path
    database_url: str | None

    @property
    def backend_label(self) -> str:
        if self.backend == "postgres":
            return "postgres"
        return "sqlite"

    @property
    def target_label(self) -> str:
        if self.backend == "postgres":
            if self.database_url is None:
                return "postgres (DATABASE_URL missing)"
            return mask_database_url(self.database_url)
        return str(self.sqlite_path)


class PostgresCursorAdapter:
    def __init__(self, connection, cursor, *, is_insert: bool) -> None:
        self._connection = connection
        self._cursor = cursor
        self._is_insert = is_insert
        self._lastrowid: int | None | object = _UNSET

    @property
    def lastrowid(self) -> int | None:
        if self._lastrowid is _UNSET:
            self._lastrowid = _resolve_postgres_lastrowid(
                self._connection,
                is_insert=self._is_insert,
            )
        return None if self._lastrowid is _UNSET else self._lastrowid

    def fetchone(self):
        return self._cursor.fetchone()

    def fetchall(self):
        return self._cursor.fetchall()


class PostgresConnectionAdapter:
    def __init__(self, connection) -> None:
        self._connection = connection
        self.backend_name = "postgres"

    def execute(self, query: str, params: Sequence[object] | None = None):
        cursor = self._connection.cursor()
        cursor.execute(
            normalize_postgres_query(query),
            tuple(() if params is None else params),
        )
        return PostgresCursorAdapter(
            self._connection,
            cursor,
            is_insert=query.lstrip().upper().startswith("INSERT "),
        )

    def executescript(self, script: str) -> None:
        for statement in split_sql_statements(script):
            self.execute(statement)

    def commit(self) -> None:
        self._connection.commit()

    def rollback(self) -> None:
        self._connection.rollback()

    def close(self) -> None:
        self._connection.close()


_UNSET = object()


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
) -> sqlite3.Connection | PostgresConnectionAdapter:
    runtime = resolve_database_runtime(
        database_path,
        settings=settings,
        backend=backend,
        database_url=database_url,
    )
    if runtime.backend == "sqlite":
        return connect_sqlite_database(runtime.sqlite_path)

    if runtime.database_url is None:
        raise RuntimeError(
            "JOBS_AI_DB_BACKEND=postgres requires DATABASE_URL or PGHOST/PGDATABASE-style settings"
        )
    if psycopg is None:
        raise RuntimeError(
            "Postgres support requires psycopg. Install project dependencies before using Neon/Postgres mode."
        )
    connection = psycopg.connect(
        runtime.database_url,
        row_factory=dict_row,
        autocommit=False,
    )
    return PostgresConnectionAdapter(connection)


def database_exists(
    database_path: Path,
    *,
    settings: Settings | None = None,
    backend: str | None = None,
    database_url: str | None = None,
) -> bool:
    runtime = resolve_database_runtime(
        database_path,
        settings=settings,
        backend=backend,
        database_url=database_url,
    )
    if runtime.backend == "sqlite":
        return runtime.sqlite_path.exists()

    connection = connect_database(
        database_path,
        settings=settings,
        backend=backend,
        database_url=database_url,
    )
    try:
        connection.execute("SELECT 1").fetchone()
        return True
    finally:
        connection.close()


def backend_name_for_connection(connection) -> str:
    if getattr(connection, "backend_name", "") == "postgres":
        return "postgres"
    return "sqlite"


def table_names_from_connection(connection) -> set[str]:
    backend = backend_name_for_connection(connection)
    if backend == "sqlite":
        rows = connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        ).fetchall()
        return {str(row["name"]) for row in rows}

    rows = connection.execute(
        """
        SELECT table_name AS name
        FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_type = 'BASE TABLE'
        """
    ).fetchall()
    return {str(row["name"]) for row in rows}


def table_columns_from_connection(connection, table_name: str) -> tuple[str, ...]:
    backend = backend_name_for_connection(connection)
    if backend == "sqlite":
        rows = connection.execute(f"PRAGMA table_info({table_name})").fetchall()
        return tuple(str(row["name"]) for row in rows)

    rows = connection.execute(
        """
        SELECT column_name AS name
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = ?
        ORDER BY ordinal_position
        """,
        (table_name,),
    ).fetchall()
    return tuple(str(row["name"]) for row in rows)


def mask_database_url(database_url: str) -> str:
    parsed = urlparse(database_url)
    if not parsed.scheme or not parsed.netloc:
        return "postgres (configured)"
    username = parsed.username or ""
    host = parsed.hostname or ""
    port = f":{parsed.port}" if parsed.port is not None else ""
    path = parsed.path or ""
    masked_netloc = host
    if username:
        masked_netloc = f"{username}@{host}"
    return urlunparse(
        parsed._replace(
            netloc=f"{masked_netloc}{port}",
            query="",
            fragment="",
        )
    )


def normalize_postgres_query(query: str) -> str:
    normalized = query.strip()
    if normalized.upper() == "BEGIN IMMEDIATE":
        normalized = "BEGIN"
    return replace_qmark_placeholders(normalized)


def replace_qmark_placeholders(query: str) -> str:
    parts: list[str] = []
    in_single_quote = False
    in_double_quote = False
    index = 0
    while index < len(query):
        char = query[index]
        if char == "'" and not in_double_quote:
            parts.append(char)
            if in_single_quote and index + 1 < len(query) and query[index + 1] == "'":
                parts.append("'")
                index += 2
                continue
            in_single_quote = not in_single_quote
            index += 1
            continue
        if char == '"' and not in_single_quote:
            parts.append(char)
            if in_double_quote and index + 1 < len(query) and query[index + 1] == '"':
                parts.append('"')
                index += 2
                continue
            in_double_quote = not in_double_quote
            index += 1
            continue
        if char == "?" and not in_single_quote and not in_double_quote:
            parts.append("%s")
        else:
            parts.append(char)
        index += 1
    return "".join(parts)


def split_sql_statements(script: str) -> tuple[str, ...]:
    statements: list[str] = []
    current: list[str] = []
    in_single_quote = False
    in_double_quote = False
    index = 0
    while index < len(script):
        char = script[index]
        current.append(char)
        if char == "'" and not in_double_quote:
            if in_single_quote and index + 1 < len(script) and script[index + 1] == "'":
                current.append("'")
                index += 2
                continue
            in_single_quote = not in_single_quote
        elif char == '"' and not in_single_quote:
            if in_double_quote and index + 1 < len(script) and script[index + 1] == '"':
                current.append('"')
                index += 2
                continue
            in_double_quote = not in_double_quote
        elif char == ";" and not in_single_quote and not in_double_quote:
            statement = "".join(current).strip()
            if statement:
                statements.append(statement)
            current = []
        index += 1
    trailing_statement = "".join(current).strip()
    if trailing_statement:
        statements.append(trailing_statement)
    return tuple(statements)


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
