from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
import os
from urllib.parse import urlparse

from pydantic import BaseModel, ConfigDict, Field

TARGET_ROLES = (
    "Data Engineer",
    "Analytics Engineer",
    "Telemetry / Observability Engineer",
    "Platform Data Engineer",
    "BigQuery / GCP-oriented roles",
)

SEARCH_PRIORITY = (
    "staffing agencies / recruiter-driven contract roles",
    "contract platforms",
    "vendor / consulting ecosystems",
    "direct company portals",
)

GEOGRAPHY_PRIORITY = (
    "Remote",
    "Sacramento / Folsom",
    "San Jose / Bay Area",
)

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
