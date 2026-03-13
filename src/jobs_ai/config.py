from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
import os

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


class Settings(BaseModel):
    model_config = ConfigDict(frozen=True)

    environment: str = Field(default="dev")
    profile: str = Field(default="default")
    database_path: Path = Field(default=Path("data") / "jobs_ai.db")


def load_settings(env: Mapping[str, str] | None = None) -> Settings:
    source = os.environ if env is None else env
    return Settings(
        environment=source.get("JOBS_AI_ENV", "dev"),
        profile=source.get("JOBS_AI_PROFILE", "default"),
        database_path=Path(source.get("JOBS_AI_DB_PATH", "data/jobs_ai.db")),
    )
