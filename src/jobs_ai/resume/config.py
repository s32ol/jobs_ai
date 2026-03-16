from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
import json
import os
import re


@dataclass(frozen=True, slots=True)
class ResumeVariant:
    key: str
    label: str
    summary: str


@dataclass(frozen=True, slots=True)
class ProfileSnippet:
    key: str
    label: str
    text: str


@dataclass(frozen=True, slots=True)
class ResolvedResumeVariant:
    key: str
    label: str
    summary: str
    resolved_path: Path | None
    fallback_reason: str | None


RESUME_MAP_PATH_ENV_VAR = "JOBS_AI_RESUME_MAP_PATH"
DEFAULT_RESUME_MAP_FILENAME = ".jobs_ai_resume_paths.json"
DEFAULT_RESUME_SEARCH_DIRNAME = "resumes"
DEFAULT_RESUME_SUFFIXES = (".pdf", ".doc", ".docx", ".txt")
_ENV_KEY_RE = re.compile(r"[^A-Za-z0-9]+")


RESUME_VARIANTS: dict[str, ResumeVariant] = {
    "general-data": ResumeVariant(
        key="general-data",
        label="General Data Resume",
        summary="Broad data-platform baseline for mixed technical roles.",
    ),
    "data-engineering": ResumeVariant(
        key="data-engineering",
        label="Data Engineering Resume",
        summary="Python, warehouse, and pipeline-heavy resume focus.",
    ),
    "analytics-engineering": ResumeVariant(
        key="analytics-engineering",
        label="Analytics Engineering Resume",
        summary="SQL, semantic layer, and BI-oriented resume focus.",
    ),
    "telemetry-observability": ResumeVariant(
        key="telemetry-observability",
        label="Telemetry / Observability Resume",
        summary="Signals, instrumentation, and platform reliability resume focus.",
    ),
}


PROFILE_SNIPPETS: dict[str, ProfileSnippet] = {
    "general-data-platform": ProfileSnippet(
        key="general-data-platform",
        label="General Data Platform",
        text="Hands-on data platform work spanning ingestion, transformation, and production support.",
    ),
    "pipeline-delivery": ProfileSnippet(
        key="pipeline-delivery",
        label="Pipeline Delivery",
        text="Python-first pipeline delivery across SQL warehouses, BigQuery/GCP, and production data systems.",
    ),
    "analytics-modeling": ProfileSnippet(
        key="analytics-modeling",
        label="Analytics Modeling",
        text="Analytics engineering work centered on SQL modeling, semantic layers, and Looker-facing delivery.",
    ),
    "observability-signals": ProfileSnippet(
        key="observability-signals",
        label="Observability Signals",
        text="Telemetry and observability work across logs, metrics, traces, and instrumentation pipelines.",
    ),
}


def get_resume_variant(key: str) -> ResumeVariant:
    return RESUME_VARIANTS[key]


def get_profile_snippet(key: str) -> ProfileSnippet:
    return PROFILE_SNIPPETS[key]


def resolve_resume_variant(
    key: str,
    *,
    project_root: Path,
    env: Mapping[str, str] | None = None,
) -> ResolvedResumeVariant:
    variant = get_resume_variant(key)
    resolved_project_root = project_root.resolve()
    source = os.environ if env is None else env

    env_var_name = resume_variant_path_env_var(key)
    configured_path = source.get(env_var_name)
    if configured_path is not None and configured_path.strip():
        resolved_path = _resolve_candidate_path(configured_path, base_dir=resolved_project_root)
        if resolved_path.is_file():
            return ResolvedResumeVariant(
                key=variant.key,
                label=variant.label,
                summary=variant.summary,
                resolved_path=resolved_path,
                fallback_reason=None,
            )
        return ResolvedResumeVariant(
            key=variant.key,
            label=variant.label,
            summary=variant.summary,
            resolved_path=None,
            fallback_reason=f"{env_var_name} points to a missing file: {resolved_path}",
        )

    mapping_path = _discover_resume_mapping_path(resolved_project_root, source)
    if mapping_path is not None:
        mapping_value, mapping_error = _lookup_mapping_value(mapping_path, key)
        if mapping_error is not None:
            return ResolvedResumeVariant(
                key=variant.key,
                label=variant.label,
                summary=variant.summary,
                resolved_path=None,
                fallback_reason=mapping_error,
            )
        if mapping_value is not None:
            resolved_path = _resolve_candidate_path(mapping_value, base_dir=mapping_path.parent)
            if resolved_path.is_file():
                return ResolvedResumeVariant(
                    key=variant.key,
                    label=variant.label,
                    summary=variant.summary,
                    resolved_path=resolved_path,
                    fallback_reason=None,
                )
            return ResolvedResumeVariant(
                key=variant.key,
                label=variant.label,
                summary=variant.summary,
                resolved_path=None,
                fallback_reason=f"{mapping_path} maps {key} to a missing file: {resolved_path}",
            )

    default_path = _discover_default_resume_path(resolved_project_root, key)
    if default_path is not None:
        return ResolvedResumeVariant(
            key=variant.key,
            label=variant.label,
            summary=variant.summary,
            resolved_path=default_path,
            fallback_reason=None,
        )

    return ResolvedResumeVariant(
        key=variant.key,
        label=variant.label,
        summary=variant.summary,
        resolved_path=None,
        fallback_reason=(
            f"resume file not resolved; set {env_var_name}, add {key} to "
            f"{resolved_project_root / DEFAULT_RESUME_MAP_FILENAME}, or place "
            f"{key}<suffix> in {resolved_project_root / DEFAULT_RESUME_SEARCH_DIRNAME}"
        ),
    )


def resume_variant_path_env_var(key: str) -> str:
    normalized_key = _ENV_KEY_RE.sub("_", key.strip()).strip("_").upper()
    return f"JOBS_AI_RESUME_{normalized_key}_PATH"


def _discover_resume_mapping_path(
    project_root: Path,
    env: Mapping[str, str],
) -> Path | None:
    configured_path = env.get(RESUME_MAP_PATH_ENV_VAR)
    if configured_path is not None and configured_path.strip():
        return _resolve_candidate_path(configured_path, base_dir=project_root)

    default_path = project_root / DEFAULT_RESUME_MAP_FILENAME
    if default_path.is_file():
        return default_path
    return None


def _lookup_mapping_value(mapping_path: Path, key: str) -> tuple[str | None, str | None]:
    try:
        payload = json.loads(mapping_path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None, f"resume map was not found: {mapping_path}"
    except OSError as exc:
        return None, f"resume map could not be read: {mapping_path} ({exc})"
    except json.JSONDecodeError as exc:
        return None, f"resume map is not valid JSON: {mapping_path} ({exc.msg})"

    if not isinstance(payload, dict):
        return None, f"resume map must be a JSON object: {mapping_path}"

    value = payload.get(key)
    if value is None:
        return None, None
    if not isinstance(value, str) or not value.strip():
        return None, f"resume map entry for {key} must be a non-empty string: {mapping_path}"
    return value.strip(), None


def _discover_default_resume_path(project_root: Path, key: str) -> Path | None:
    resumes_dir = project_root / DEFAULT_RESUME_SEARCH_DIRNAME
    if not resumes_dir.is_dir():
        return None

    for suffix in DEFAULT_RESUME_SUFFIXES:
        candidate = resumes_dir / f"{key}{suffix}"
        if candidate.is_file():
            return candidate.resolve()
    return None


def _resolve_candidate_path(value: str, *, base_dir: Path) -> Path:
    candidate = Path(value.strip()).expanduser()
    if not candidate.is_absolute():
        candidate = (base_dir / candidate).resolve()
    return candidate
