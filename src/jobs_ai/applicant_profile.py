from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
import json
import os
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from .resume.config import ResolvedResumeVariant, resolve_resume_variant

APPLICANT_PROFILE_PATH_ENV_VAR = "JOBS_AI_APPLICANT_PROFILE_PATH"
DEFAULT_APPLICANT_PROFILE_FILENAME = ".jobs_ai_applicant_profile.json"


class ApplicantProfile(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", populate_by_name=True)

    full_name: str
    email: str
    phone: str
    location: str | None = None
    first_name: str | None = None
    last_name: str | None = None
    linkedin_url: str | None = Field(default=None, alias="linkedin")
    github_url: str | None = Field(default=None, alias="github")
    portfolio_url: str | None = Field(default=None, alias="portfolio")
    work_authorization: str | None = None
    authorized_to_work_in_us: bool | None = None
    requires_sponsorship: bool | None = None
    short_text: str | None = None
    use_recommended_profile_snippet: bool = False
    canned_answers: dict[str, str] = Field(default_factory=dict)
    resume_paths: dict[str, str] = Field(default_factory=dict)

    @property
    def resolved_first_name(self) -> str | None:
        if self.first_name is not None:
            return self.first_name.strip() or None
        parts = self.full_name.strip().split()
        if not parts:
            return None
        return parts[0]

    @property
    def resolved_last_name(self) -> str | None:
        if self.last_name is not None:
            return self.last_name.strip() or None
        parts = self.full_name.strip().split()
        if len(parts) < 2:
            return None
        return parts[-1]


@dataclass(frozen=True, slots=True)
class LoadedApplicantProfile:
    profile: ApplicantProfile
    profile_path: Path


def load_applicant_profile(
    profile_path: Path | None,
    *,
    project_root: Path,
    env: Mapping[str, str] | None = None,
) -> LoadedApplicantProfile:
    source = os.environ if env is None else env
    resolved_path = _resolve_profile_path(
        profile_path,
        project_root=project_root,
        env=source,
    )
    try:
        payload = json.loads(resolved_path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise ValueError(f"applicant profile was not found: {resolved_path}") from exc
    except OSError as exc:
        raise ValueError(f"applicant profile could not be read: {resolved_path} ({exc})") from exc
    except json.JSONDecodeError as exc:
        raise ValueError(f"applicant profile is not valid JSON: {resolved_path} ({exc.msg})") from exc

    try:
        profile = ApplicantProfile.model_validate(payload)
    except ValidationError as exc:
        raise ValueError(f"applicant profile is invalid: {exc}") from exc

    return LoadedApplicantProfile(
        profile=profile,
        profile_path=resolved_path,
    )


def resolve_applicant_resume_variant(
    loaded_profile: LoadedApplicantProfile,
    *,
    resume_variant_key: str,
    project_root: Path,
    env: Mapping[str, str] | None = None,
) -> ResolvedResumeVariant:
    configured_path = loaded_profile.profile.resume_paths.get(resume_variant_key)
    if configured_path is not None and configured_path.strip():
        resolved_path = _resolve_candidate_path(
            configured_path,
            base_dir=loaded_profile.profile_path.parent,
        )
        fallback_reason = None
        if not resolved_path.is_file():
            fallback_reason = (
                f"{loaded_profile.profile_path} maps {resume_variant_key} to a missing file: "
                f"{resolved_path}"
            )
            return ResolvedResumeVariant(
                key=resume_variant_key,
                label=resume_variant_key,
                summary=f"Resume override for {resume_variant_key}.",
                resolved_path=None,
                fallback_reason=fallback_reason,
            )
        resolved = resolve_resume_variant(resume_variant_key, project_root=project_root, env=env)
        return ResolvedResumeVariant(
            key=resolved.key,
            label=resolved.label,
            summary=resolved.summary,
            resolved_path=resolved_path,
            fallback_reason=None,
        )

    return resolve_resume_variant(resume_variant_key, project_root=project_root, env=env)


def _resolve_profile_path(
    profile_path: Path | None,
    *,
    project_root: Path,
    env: Mapping[str, str],
) -> Path:
    if profile_path is not None:
        return _resolve_candidate_path(str(profile_path), base_dir=project_root)

    configured_path = env.get(APPLICANT_PROFILE_PATH_ENV_VAR)
    if configured_path is not None and configured_path.strip():
        return _resolve_candidate_path(configured_path, base_dir=project_root)

    return project_root / DEFAULT_APPLICANT_PROFILE_FILENAME


def _resolve_candidate_path(value: str, *, base_dir: Path) -> Path:
    candidate = Path(value.strip()).expanduser()
    if not candidate.is_absolute():
        candidate = (base_dir / candidate).resolve()
    return candidate
