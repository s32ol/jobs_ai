# Code Excerpt: Resume and Application Assist

Exact excerpts from the current repo for resume selection, applicant profiles, application assist, browser prefill, portal rules, and JSON application logging.

## Resume variant resolution
Source: `src/jobs_ai/resume/config.py` lines 1-235

```python
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
```

## Resume and profile snippet recommendations
Source: `src/jobs_ai/resume/recommendations.py` lines 1-133

```python
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from ..config import TARGET_ROLES
from ..jobs.queue import RankedQueuedJob, select_ranked_apply_queue
from ..jobs.scoring import ScoredJob
from .config import get_profile_snippet, get_resume_variant

TELEMETRY_KEYWORDS = ("telemetry", "observability")
ANALYTICS_KEYWORDS = ("analytics engineer", "analytics engineering")
DATA_ENGINEERING_KEYWORDS = ("platform data engineer", "data engineer")


@dataclass(frozen=True, slots=True)
class QueueRecommendation:
    rank: int
    job_id: int
    company: str
    title: str
    location: str | None
    apply_url: str | None
    portal_type: str | None
    source: str
    score: int
    resume_variant_key: str
    resume_variant_label: str
    snippet_key: str
    snippet_label: str
    snippet_text: str
    explanation: str


@dataclass(frozen=True, slots=True)
class _RecommendationDecision:
    resume_variant_key: str
    snippet_key: str
    explanation_prefix: str


def select_queue_recommendations(
    database_path: Path,
    *,
    limit: int | None = None,
    ingest_batch_id: str | None = None,
    query_text: str | None = None,
) -> tuple[QueueRecommendation, ...]:
    queued_jobs = select_ranked_apply_queue(
        database_path,
        limit=limit,
        ingest_batch_id=ingest_batch_id,
        query_text=query_text,
    )
    return tuple(recommend_queued_job(job) for job in queued_jobs)


def recommend_queued_job(queued_job: RankedQueuedJob) -> QueueRecommendation:
    decision = _decide_recommendation(queued_job.scored_job)
    resume_variant = get_resume_variant(decision.resume_variant_key)
    snippet = get_profile_snippet(decision.snippet_key)
    return QueueRecommendation(
        rank=queued_job.rank,
        job_id=queued_job.scored_job.job_id,
        company=queued_job.scored_job.company,
        title=queued_job.scored_job.title,
        location=queued_job.scored_job.location,
        apply_url=queued_job.scored_job.apply_url,
        portal_type=queued_job.scored_job.portal_type,
        source=queued_job.scored_job.source,
        score=queued_job.scored_job.total_score,
        resume_variant_key=resume_variant.key,
        resume_variant_label=resume_variant.label,
        snippet_key=snippet.key,
        snippet_label=snippet.label,
        snippet_text=snippet.text,
        explanation=_build_explanation(decision.explanation_prefix, queued_job),
    )


def _decide_recommendation(job: ScoredJob) -> _RecommendationDecision:
    normalized_title = job.title.strip().lower()
    matched_stack_keywords = set(job.matched_stack_keywords)

    if (
        job.matched_target_role == TARGET_ROLES[2]
        or _contains_keyword(normalized_title, TELEMETRY_KEYWORDS)
        or "telemetry/observability" in matched_stack_keywords
    ):
        return _RecommendationDecision(
            resume_variant_key="telemetry-observability",
            snippet_key="observability-signals",
            explanation_prefix="matched telemetry / observability signals from title or stack",
        )

    if (
        job.matched_target_role == TARGET_ROLES[1]
        or _contains_keyword(normalized_title, ANALYTICS_KEYWORDS)
        or "Looker" in matched_stack_keywords
    ):
        return _RecommendationDecision(
            resume_variant_key="analytics-engineering",
            snippet_key="analytics-modeling",
            explanation_prefix="matched analytics engineering signals from title or stack",
        )

    if (
        job.matched_target_role in {TARGET_ROLES[0], TARGET_ROLES[3], TARGET_ROLES[4]}
        or _contains_keyword(normalized_title, DATA_ENGINEERING_KEYWORDS)
        or bool({"Python", "BigQuery", "GCP"} & matched_stack_keywords)
    ):
        snippet_key = "pipeline-delivery" if {"Python", "BigQuery", "GCP"} & matched_stack_keywords else "general-data-platform"
        return _RecommendationDecision(
            resume_variant_key="data-engineering",
            snippet_key=snippet_key,
            explanation_prefix="matched data engineering signals from title or stack",
        )

    return _RecommendationDecision(
        resume_variant_key="general-data",
        snippet_key="general-data-platform",
        explanation_prefix="no strong specialization signal matched, using the general data profile",
    )


def _build_explanation(explanation_prefix: str, queued_job: RankedQueuedJob) -> str:
    if queued_job.reason_summary == "no strong score signals yet":
        return explanation_prefix
    return f"{explanation_prefix}; queue signals: {queued_job.reason_summary}"


def _contains_keyword(text: str, keywords: tuple[str, ...]) -> bool:
    return any(keyword in text for keyword in keywords)
```

## Applicant profile loading and resume overrides
Source: `src/jobs_ai/applicant_profile.py` lines 1-152

```python
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
```

## Read-only application assist view
Source: `src/jobs_ai/application_assist.py` lines 1-84

```python
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from .launch_plan import LaunchPlan
from .session_manifest import ManifestSelection


@dataclass(frozen=True, slots=True)
class ApplicationAssistEntry:
    launch_order: int
    job_id: int | None
    company: str | None
    title: str | None
    apply_url: str
    portal_type: str | None
    recommended_resume_variant: ManifestSelection
    recommended_profile_snippet: ManifestSelection


@dataclass(frozen=True, slots=True)
class ApplicationAssist:
    manifest_path: Path
    created_at: str
    total_items: int
    assist_items: tuple[ApplicationAssistEntry, ...]


def build_application_assist(plan: LaunchPlan) -> ApplicationAssist:
    assist_items = []
    for item in plan.items:
        if not item.launchable or item.launch_order is None:
            continue
        if item.recommended_resume_variant is None:
            raise ValueError(
                f"launchable plan item {item.launch_order} is missing recommended_resume_variant"
            )
        if item.recommended_profile_snippet is None:
            raise ValueError(
                f"launchable plan item {item.launch_order} is missing recommended_profile_snippet"
            )
        if item.apply_url is None:
            raise ValueError(f"launchable plan item {item.launch_order} is missing apply_url")

        assist_items.append(
            ApplicationAssistEntry(
                launch_order=item.launch_order,
                job_id=item.job_id,
                company=item.company,
                title=item.title,
                apply_url=item.apply_url,
                portal_type=item.portal_type,
                recommended_resume_variant=item.recommended_resume_variant,
                recommended_profile_snippet=item.recommended_profile_snippet,
            )
        )

    return ApplicationAssist(
        manifest_path=plan.manifest_path,
        created_at=plan.created_at,
        total_items=plan.total_items,
        assist_items=tuple(assist_items),
    )


def select_application_assist_entry(
    assist: ApplicationAssist,
    *,
    launch_order: int | None,
) -> ApplicationAssistEntry:
    if not assist.assist_items:
        raise ValueError("manifest contains no launchable application assists")
    if launch_order is None:
        if len(assist.assist_items) == 1:
            return assist.assist_items[0]
        raise ValueError(
            "provide --launch-order when the manifest contains more than one launchable application"
        )

    for entry in assist.assist_items:
        if entry.launch_order == launch_order:
            return entry
    raise ValueError(f"launch order {launch_order} was not found in the manifest")
```

## Prefill orchestration
Source: `src/jobs_ai/application_prefill.py` lines 76-196

```python
def run_application_prefill(
    manifest_path: Path,
    *,
    project_root: Path,
    applicant_profile_path: Path | None,
    launch_order: int | None,
    browser_backend: PrefillBrowserBackend,
    env: Mapping[str, str] | None = None,
) -> ApplicationPrefillResult:
    manifest = load_session_manifest(manifest_path)
    assist = build_application_assist(build_launch_plan(manifest))
    entry = select_application_assist_entry(assist, launch_order=launch_order)
    loaded_profile = load_applicant_profile(
        applicant_profile_path,
        project_root=project_root,
        env=env,
    )

    portal_support = build_portal_support(entry.apply_url, portal_type=entry.portal_type)
    opened_url = (
        portal_support.company_apply_url
        if portal_support is not None and portal_support.company_apply_url is not None
        else portal_support.normalized_apply_url
        if portal_support is not None
        else entry.apply_url
    )
    portal_type = (
        portal_support.portal_type
        if portal_support is not None
        else entry.portal_type
    )
    portal_adapter = select_portal_prefill_adapter(portal_type)

    browser_backend.open_url(opened_url)
    initial_snapshot = browser_backend.snapshot()
    filled_fields: list[PrefillAction] = []
    skipped_fields: list[PrefillSkippedField] = []
    notes: list[str] = []

    resolved_resume = resolve_applicant_resume_variant(
        loaded_profile,
        resume_variant_key=entry.recommended_resume_variant.key,
        project_root=project_root,
        env=env,
    )

    if portal_adapter is None:
        notes.append("No supported portal prefill adapter matched this application page.")
    elif portal_adapter.support_level != "supported":
        notes.append(
            f"{portal_adapter.portal_label} stays manual-review only in Phase 2; no fields were auto-filled."
        )
    else:
        fill_result = _fill_supported_portal_fields(
            entry,
            loaded_profile=loaded_profile,
            portal_adapter=portal_adapter,
            browser_backend=browser_backend,
            resolved_resume_path=resolved_resume.resolved_path,
            snapshot=initial_snapshot,
        )
        filled_fields.extend(fill_result.filled_fields)
        skipped_fields.extend(fill_result.skipped_fields)
        notes.extend(fill_result.notes)

    if resolved_resume.fallback_reason is not None:
        skipped_fields.append(
            PrefillSkippedField(
                field_key="resume",
                field_label="Resume",
                reason=resolved_resume.fallback_reason,
            )
        )

    final_snapshot = browser_backend.snapshot()
    unresolved_required_fields = tuple(
        field_display_name(field)
        for field in final_snapshot.fields
        if field.required and field.visible and not (field.current_value or "").strip()
    )

    support_level = portal_adapter.support_level if portal_adapter is not None else "unsupported"
    portal_label = portal_adapter.portal_label if portal_adapter is not None else "Unsupported / Unknown"

    status = _result_status(
        support_level=support_level,
        filled_count=len(filled_fields),
        unresolved_required_fields=unresolved_required_fields,
    )
    if unresolved_required_fields:
        notes.append(
            f"{len(unresolved_required_fields)} required field(s) remain unresolved for manual review."
        )

    return ApplicationPrefillResult(
        manifest_path=manifest_path,
        applicant_profile_path=loaded_profile.profile_path,
        launch_order=entry.launch_order,
        company=entry.company,
        title=entry.title,
        original_apply_url=entry.apply_url,
        opened_url=final_snapshot.url or opened_url,
        page_title=final_snapshot.title,
        portal_type=portal_type,
        portal_label=portal_label,
        support_level=support_level,
        browser_backend=browser_backend.backend_name,
        recommended_resume_variant=entry.recommended_resume_variant,
        recommended_profile_snippet=entry.recommended_profile_snippet,
        resolved_resume_path=resolved_resume.resolved_path,
        filled_fields=tuple(filled_fields),
        skipped_fields=tuple(skipped_fields),
        unresolved_required_fields=unresolved_required_fields,
        submit_controls=final_snapshot.submit_controls,
        stopped_before_submit=True,
        status=status,
        notes=tuple(notes),
    )


@dataclass(frozen=True, slots=True)
```

## Supported field filling logic
Source: `src/jobs_ai/application_prefill.py` lines 203-411

```python
def _fill_supported_portal_fields(
    entry: ApplicationAssistEntry,
    *,
    loaded_profile: LoadedApplicantProfile,
    portal_adapter: PortalPrefillAdapter,
    browser_backend: PrefillBrowserBackend,
    resolved_resume_path: Path | None,
    snapshot,
) -> _FillResult:
    used_selectors: set[str] = set()
    filled_fields: list[PrefillAction] = []
    skipped_fields: list[PrefillSkippedField] = []
    notes: list[str] = []
    canned_answers = normalized_canned_answers(loaded_profile.profile.canned_answers)

    for spec in portal_adapter.safe_fields:
        if spec.field_key == "resume":
            result = _handle_resume_upload(
                spec,
                browser_backend=browser_backend,
                resolved_resume_path=resolved_resume_path,
                snapshot=snapshot,
                used_selectors=used_selectors,
            )
        else:
            result = _handle_profile_field(
                spec,
                profile=loaded_profile.profile,
                browser_backend=browser_backend,
                snapshot=snapshot,
                used_selectors=used_selectors,
            )
        if isinstance(result, PrefillAction):
            filled_fields.append(result)
        elif isinstance(result, PrefillSkippedField):
            skipped_fields.append(result)

    short_text_value = _select_short_text(
        loaded_profile.profile,
        recommended_profile_snippet=entry.recommended_profile_snippet,
    )
    if short_text_value is not None:
        field, reason = find_unique_field(
            snapshot,
            aliases=portal_adapter.short_text_aliases,
            control_types=("textarea", "text"),
            used_selectors=used_selectors,
        )
        if field is None:
            skipped_fields.append(
                PrefillSkippedField(
                    field_key="short_text",
                    field_label="Short text / cover letter",
                    reason=reason or "field not found",
                )
            )
        else:
            browser_backend.fill_text(field.selector, short_text_value)
            used_selectors.add(field.selector)
            filled_fields.append(
                PrefillAction(
                    field_key="short_text",
                    field_label=field_display_name(field),
                    selector=field.selector,
                    action_type="fill_text",
                    value=short_text_value,
                )
            )

    for field in snapshot.fields:
        if field.selector in used_selectors:
            continue
        matched_answer = _matched_canned_answer(field, canned_answers)
        if matched_answer is None:
            continue
        result = _fill_field_with_answer(
            field_key="canned_answer",
            field_label=field_display_name(field),
            answer=matched_answer,
            field=field,
            browser_backend=browser_backend,
        )
        if isinstance(result, PrefillAction):
            used_selectors.add(field.selector)
            filled_fields.append(result)
        elif isinstance(result, PrefillSkippedField):
            skipped_fields.append(result)

    if portal_adapter.portal_type == "ashby":
        notes.append("Ashby support is limited to single-page visible fields in Phase 2.")

    return _FillResult(
        filled_fields=tuple(filled_fields),
        skipped_fields=tuple(skipped_fields),
        notes=tuple(notes),
    )


def _handle_profile_field(
    spec: PortalFieldSpec,
    *,
    profile: ApplicantProfile,
    browser_backend: PrefillBrowserBackend,
    snapshot,
    used_selectors: set[str],
) -> PrefillAction | PrefillSkippedField | None:
    value = _profile_value_for_field(profile, spec.field_key)
    if value is None:
        return None
    field, reason = find_unique_field(
        snapshot,
        aliases=spec.aliases,
        control_types=spec.control_types,
        used_selectors=used_selectors,
    )
    if field is None:
        return PrefillSkippedField(
            field_key=spec.field_key,
            field_label=_field_label_from_spec(spec),
            reason=reason or "field not found",
        )
    result = _fill_field_with_answer(
        field_key=spec.field_key,
        field_label=field_display_name(field),
        answer=value,
        field=field,
        browser_backend=browser_backend,
    )
    if isinstance(result, PrefillAction):
        used_selectors.add(field.selector)
    return result


def _handle_resume_upload(
    spec: PortalFieldSpec,
    *,
    browser_backend: PrefillBrowserBackend,
    resolved_resume_path: Path | None,
    snapshot,
    used_selectors: set[str],
) -> PrefillAction | PrefillSkippedField | None:
    if resolved_resume_path is None:
        return None
    field, reason = find_unique_field(
        snapshot,
        aliases=spec.aliases,
        control_types=spec.control_types,
        include_hidden=True,
        used_selectors=used_selectors,
    )
    if field is None:
        return PrefillSkippedField(
            field_key="resume",
            field_label="Resume",
            reason=reason or "file input not found",
        )
    browser_backend.upload_file(field.selector, resolved_resume_path)
    used_selectors.add(field.selector)
    return PrefillAction(
        field_key="resume",
        field_label=field_display_name(field),
        selector=field.selector,
        action_type="upload_file",
        value=str(resolved_resume_path),
    )


def _fill_field_with_answer(
    *,
    field_key: str,
    field_label: str,
    answer: str | bool,
    field,
    browser_backend: PrefillBrowserBackend,
) -> PrefillAction | PrefillSkippedField:
    if field.control_type == "select":
        option_value = _option_value_for_field_answer(field_key, field, answer)
        if option_value is None:
            return PrefillSkippedField(
                field_key=field_key,
                field_label=field_label,
                reason="no matching select option for configured answer",
            )
        browser_backend.select_option(field.selector, option_value)
        return PrefillAction(
            field_key=field_key,
            field_label=field_label,
            selector=field.selector,
            action_type="select_option",
            value=option_value,
        )

    text_value = _text_value_for_answer(answer)
    if text_value is None:
        return PrefillSkippedField(
            field_key=field_key,
            field_label=field_label,
            reason="configured answer was blank",
        )
    browser_backend.fill_text(field.selector, text_value)
    return PrefillAction(
        field_key=field_key,
        field_label=field_label,
        selector=field.selector,
        action_type="fill_text",
        value=text_value,
    )
```

## Portal prefill adapter definitions
Source: `src/jobs_ai/prefill_portals.py` lines 1-203

```python
from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
import re

from .prefill_browser import BrowserFieldSnapshot, BrowserPageSnapshot

_NORMALIZE_RE = re.compile(r"[^a-z0-9]+")


@dataclass(frozen=True, slots=True)
class PortalFieldSpec:
    field_key: str
    aliases: tuple[str, ...]
    control_types: tuple[str, ...] | None = None


@dataclass(frozen=True, slots=True)
class PortalPrefillAdapter:
    portal_type: str
    portal_label: str
    support_level: str
    safe_fields: tuple[PortalFieldSpec, ...]
    short_text_aliases: tuple[str, ...]


COMMON_SAFE_FIELDS = (
    PortalFieldSpec("full_name", ("full name", "name")),
    PortalFieldSpec("first_name", ("first name", "first")),
    PortalFieldSpec("last_name", ("last name", "last")),
    PortalFieldSpec("email", ("email", "email address"), ("email", "text")),
    PortalFieldSpec("phone", ("phone", "phone number", "mobile phone"), ("tel", "text")),
    PortalFieldSpec("location", ("location", "current location", "city")),
    PortalFieldSpec("linkedin_url", ("linkedin", "linkedin profile"), ("url", "text")),
    PortalFieldSpec("github_url", ("github", "github profile"), ("url", "text")),
    PortalFieldSpec("portfolio_url", ("portfolio", "portfolio url", "website", "personal website"), ("url", "text")),
    PortalFieldSpec(
        "authorized_to_work_in_us",
        (
            "are you legally authorized to work in the united states",
            "are you authorized to work in the united states",
            "authorized to work in the united states",
        ),
        ("select",),
    ),
    PortalFieldSpec(
        "requires_sponsorship",
        (
            "will you now or in the future require sponsorship",
            "do you require sponsorship",
            "require visa sponsorship",
        ),
        ("select",),
    ),
    PortalFieldSpec(
        "work_authorization",
        ("work authorization", "authorization to work"),
        ("select",),
    ),
    PortalFieldSpec("resume", ("resume", "resume cv", "cv", "upload resume"), ("file",)),
)

GREENHOUSE_ADAPTER = PortalPrefillAdapter(
    portal_type="greenhouse",
    portal_label="Greenhouse",
    support_level="supported",
    safe_fields=COMMON_SAFE_FIELDS,
    short_text_aliases=("cover letter", "additional information", "why do you want to work here"),
)

LEVER_ADAPTER = PortalPrefillAdapter(
    portal_type="lever",
    portal_label="Lever",
    support_level="supported",
    safe_fields=COMMON_SAFE_FIELDS,
    short_text_aliases=("additional information", "cover letter", "why lever"),
)

ASHBY_ADAPTER = PortalPrefillAdapter(
    portal_type="ashby",
    portal_label="Ashby",
    support_level="supported",
    safe_fields=COMMON_SAFE_FIELDS,
    short_text_aliases=("cover letter", "why are you interested", "summary", "additional information"),
)

WORKDAY_ADAPTER = PortalPrefillAdapter(
    portal_type="workday",
    portal_label="Workday",
    support_level="limited_manual_support",
    safe_fields=(
        PortalFieldSpec("first_name", ("first name",)),
        PortalFieldSpec("last_name", ("last name",)),
        PortalFieldSpec("email", ("email", "email address"), ("email", "text")),
        PortalFieldSpec("phone", ("phone", "phone number"), ("tel", "text")),
        PortalFieldSpec("location", ("location", "current location")),
    ),
    short_text_aliases=(),
)

PORTAL_PREFILL_ADAPTERS: dict[str, PortalPrefillAdapter] = {
    adapter.portal_type: adapter
    for adapter in (GREENHOUSE_ADAPTER, LEVER_ADAPTER, ASHBY_ADAPTER, WORKDAY_ADAPTER)
}


def select_portal_prefill_adapter(portal_type: str | None) -> PortalPrefillAdapter | None:
    if portal_type is None:
        return None
    return PORTAL_PREFILL_ADAPTERS.get(portal_type)


def field_lookup_keys(field: BrowserFieldSnapshot) -> tuple[str, ...]:
    values = [
        _normalize_lookup_value(field.label),
        _normalize_lookup_value(field.name),
        _normalize_lookup_value(field.placeholder),
    ]
    return tuple(
        value
        for value in dict.fromkeys(values)
        if value is not None
    )


def find_unique_field(
    snapshot: BrowserPageSnapshot,
    *,
    aliases: Sequence[str],
    control_types: Sequence[str] | None = None,
    include_hidden: bool = False,
    used_selectors: set[str] | None = None,
) -> tuple[BrowserFieldSnapshot | None, str | None]:
    normalized_aliases = {
        normalized
        for normalized in (_normalize_lookup_value(alias) for alias in aliases)
        if normalized is not None
    }
    candidates = []
    for field in snapshot.fields:
        if used_selectors is not None and field.selector in used_selectors:
            continue
        if not include_hidden and not field.visible:
            continue
        if control_types is not None and field.control_type not in control_types:
            continue
        if normalized_aliases.intersection(field_lookup_keys(field)):
            candidates.append(field)
    if not candidates:
        return None, "field not found"
    if len(candidates) > 1:
        return None, "multiple matching fields"
    return candidates[0], None


def normalized_canned_answers(answers: Mapping[str, str]) -> dict[str, str]:
    return {
        normalized_key: value
        for key, value in answers.items()
        for normalized_key in (_normalize_lookup_value(key),)
        if normalized_key is not None and value.strip()
    }


def option_value_for_answer(
    field: BrowserFieldSnapshot,
    answer: str,
) -> str | None:
    normalized_answer = _normalize_lookup_value(answer)
    if normalized_answer is None:
        return None
    for option in field.options:
        option_values = {
            _normalize_lookup_value(option.label),
            _normalize_lookup_value(option.value),
        }
        if normalized_answer in option_values:
            return option.value
        if any(
            option_value is not None
            and (
                normalized_answer in option_value
                or option_value in normalized_answer
            )
            for option_value in option_values
        ):
            return option.value
    return None


def field_display_name(field: BrowserFieldSnapshot) -> str:
    for candidate in (field.label, field.name, field.placeholder):
        if candidate is not None and candidate.strip():
            return candidate.strip()
    return field.selector


def _normalize_lookup_value(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = _NORMALIZE_RE.sub(" ", value.strip().lower()).strip()
    return normalized or None
```

## Playwright prefill backend and backend selection
Source: `src/jobs_ai/prefill_browser.py` lines 124-316

```python
class PlaywrightPrefillBrowserBackend:
    backend_name = "playwright"

    def __init__(
        self,
        *,
        headless: bool = False,
        profile_config: LocalPlaywrightProfileConfig | None = None,
        sync_playwright_factory=None,
    ) -> None:
        if sync_playwright_factory is None:
            try:
                from playwright.sync_api import sync_playwright
            except ImportError as exc:
                raise ValueError(
                    "Playwright is not installed. Install it in the project environment to use "
                    "browser-fill mode."
                ) from exc
            sync_playwright_factory = sync_playwright

        self._sync_playwright = sync_playwright_factory
        self._playwright_context = None
        self._browser = None
        self._browser_context = None
        self._page = None
        self._headless = headless
        self._profile_config = profile_config

    def open_url(self, url: str) -> None:
        if self._page is None:
            try:
                self._playwright_context = self._sync_playwright().start()
                if self._profile_config is None:
                    self._browser = self._playwright_context.chromium.launch(headless=self._headless)
                    self._page = self._browser.new_page()
                else:
                    self._browser_context = self._launch_persistent_context_with_fallback()
                    pages = tuple(self._browser_context.pages)
                    self._page = pages[0] if pages else self._browser_context.new_page()
            except Exception as exc:
                self.close()
                if self._profile_config is not None:
                    if self._profile_config.profile_directory is None:
                        profile_hint = (
                            "Close other Chrome windows using that user data dir, or set "
                            f"{BROWSER_PROFILE_DIRECTORY_ENV_VAR} to an existing profile inside "
                            f"{self._profile_config.user_data_dir}. "
                        )
                    else:
                        profile_hint = (
                            "Close other Chrome windows using "
                            f"{self._profile_config.profile_directory!r}, or point "
                            f"{BROWSER_PROFILE_DIRECTORY_ENV_VAR} at a separate profile. "
                        )
                    raise ValueError(
                        "Playwright could not launch the configured local Chrome profile for "
                        f"application-assist --prefill. {profile_hint}"
                        f"Underlying error: {exc}"
                    ) from exc
                raise ValueError(f"Playwright could not launch the browser backend: {exc}") from exc
        assert self._page is not None
        self._page.goto(url, wait_until="domcontentloaded")
        self._settle_page_after_navigation()

    def _settle_page_after_navigation(self) -> None:
        if self._page is None:
            return
        try:
            self._page.wait_for_load_state("networkidle", timeout=1000)
        except Exception:
            pass
        # Modern hosted application pages often hydrate after domcontentloaded.
        # Give the client app one more beat so controlled inputs are stable
        # before snapshotting and filling.
        self._page.wait_for_timeout(250)

    def _launch_persistent_context_with_fallback(self):
        assert self._playwright_context is not None
        assert self._profile_config is not None
        launch_error: Exception | None = None
        for profile_config in _persistent_launch_attempts(self._profile_config):
            try:
                launch_kwargs = self._persistent_launch_kwargs(profile_config)
                return self._playwright_context.chromium.launch_persistent_context(
                    str(_persistent_launch_user_data_dir(profile_config)),
                    **launch_kwargs,
                )
            except Exception as exc:
                launch_error = exc
        assert launch_error is not None
        raise launch_error

    def _persistent_launch_kwargs(
        self,
        profile_config: LocalPlaywrightProfileConfig,
    ) -> dict[str, object]:
        kwargs: dict[str, object] = {
            "headless": self._headless,
        }
        if profile_config.channel is not None:
            kwargs["channel"] = profile_config.channel
        if profile_config.launch_args:
            kwargs["args"] = list(profile_config.launch_args)
        if _persistent_uses_native_window(profile_config):
            kwargs["no_viewport"] = True
        return kwargs

    def snapshot(self) -> BrowserPageSnapshot:
        if self._page is None:
            raise ValueError("no browser page is open")
        result = self._page.evaluate(_PLAYWRIGHT_SNAPSHOT_SCRIPT)
        fields = tuple(
            BrowserFieldSnapshot(
                selector=str(field["selector"]),
                control_type=str(field["control_type"]),
                label=_optional_string(field.get("label")),
                name=_optional_string(field.get("name")),
                placeholder=_optional_string(field.get("placeholder")),
                required=bool(field.get("required")),
                visible=bool(field.get("visible")),
                current_value=_optional_string(field.get("current_value")),
                options=tuple(
                    BrowserFieldOption(
                        label=str(option.get("label") or ""),
                        value=str(option.get("value") or ""),
                    )
                    for option in field.get("options", [])
                    if isinstance(option, dict)
                ),
            )
            for field in result.get("fields", [])
            if isinstance(field, dict)
        )
        submit_controls = tuple(
            label
            for label in (
                _optional_string(value)
                for value in result.get("submit_controls", [])
            )
            if label is not None
        )
        return BrowserPageSnapshot(
            url=str(result.get("url") or self._page.url),
            title=_optional_string(result.get("title")) or self._page.title(),
            fields=fields,
            submit_controls=submit_controls,
        )

    def fill_text(self, selector: str, value: str) -> None:
        if self._page is None:
            raise ValueError("no browser page is open")
        self._page.locator(selector).fill(value)

    def select_option(self, selector: str, value: str) -> None:
        if self._page is None:
            raise ValueError("no browser page is open")
        locator = self._page.locator(selector)
        try:
            locator.select_option(label=value)
        except Exception:
            locator.select_option(value=value)

    def upload_file(self, selector: str, file_path: Path) -> None:
        if self._page is None:
            raise ValueError("no browser page is open")
        self._page.locator(selector).set_input_files(str(file_path))

    def close(self) -> None:
        if self._browser_context is not None:
            self._browser_context.close()
            self._browser_context = None
        if self._browser is not None:
            self._browser.close()
            self._browser = None
        if self._playwright_context is not None:
            self._playwright_context.stop()
            self._playwright_context = None
        self._page = None


def create_prefill_browser_backend(
    backend_name: str,
    *,
    env: Mapping[str, str] | None = None,
) -> PrefillBrowserBackend:
    if backend_name == "playwright":
        return PlaywrightPrefillBrowserBackend(
            profile_config=resolve_local_playwright_profile_config(env),
        )
    supported = ", ".join(SUPPORTED_PREFILL_BROWSER_BACKENDS)
    raise ValueError(f"unsupported prefill browser backend: {backend_name}; expected one of: {supported}")
```

## Application log writer
Source: `src/jobs_ai/application_log.py` lines 50-244

```python
def write_application_log(
    project_root: Path,
    *,
    company: str | None,
    role: str | None,
    portal: str | None,
    apply_url: str | None,
    status: str,
    notes: str | None = None,
    manifest_path: Path | None = None,
    launch_order: int | None = None,
    created_at: datetime | None = None,
) -> ApplicationLogResult:
    if manifest_path is None and launch_order is not None:
        raise ValueError("--launch-order requires --manifest")

    manifest_item = None
    if manifest_path is not None:
        manifest_item = _select_manifest_launch_item(
            manifest_path,
            launch_order=launch_order,
        )

    resolved_company = _require_text(
        company if company is not None else _manifest_text(manifest_item, "company"),
        label="company",
    )
    resolved_role = _require_text(
        role if role is not None else _manifest_text(manifest_item, "title"),
        label="role",
    )
    resolved_apply_url = _require_text(
        apply_url if apply_url is not None else _manifest_text(manifest_item, "apply_url"),
        label="apply_url",
    )
    resolved_portal = _resolve_portal(
        portal if portal is not None else _manifest_text(manifest_item, "portal_type"),
        apply_url=resolved_apply_url,
    )
    resolved_status = normalize_application_log_status(status)
    normalized_notes = _normalize_optional_text(notes)

    local_timestamp = _resolve_local_datetime(created_at)
    record = ApplicationLogRecord(
        company=resolved_company,
        role=resolved_role,
        portal=resolved_portal,
        apply_url=resolved_apply_url,
        status=resolved_status,
        method=APPLICATION_LOG_METHOD,
        notes=normalized_notes,
        timestamp=local_timestamp.isoformat(timespec="seconds"),
    )
    log_dir = project_root / "data" / "applications"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / _build_log_filename(
        local_timestamp=local_timestamp,
        company=record.company,
        portal=record.portal,
    )
    _write_log_payload(log_path, record)

    return ApplicationLogResult(
        log_path=log_path,
        record=record,
        manifest_path=manifest_path,
        launch_order=manifest_item.launch_order if manifest_item is not None else None,
    )


def _select_manifest_launch_item(
    manifest_path: Path,
    *,
    launch_order: int | None,
) -> LaunchPlanItem:
    manifest = load_session_manifest(manifest_path)
    plan = build_launch_plan(manifest)
    launchable_items = tuple(
        item
        for item in plan.items
        if item.launchable and item.launch_order is not None
    )
    if not launchable_items:
        raise ValueError("manifest contains no launchable application items")
    if launch_order is None:
        if len(launchable_items) == 1:
            return launchable_items[0]
        raise ValueError(
            "provide --launch-order when the manifest contains more than one launchable application"
        )

    for item in launchable_items:
        if item.launch_order == launch_order:
            return item
    raise ValueError(f"launch order {launch_order} was not found in the manifest")


def _manifest_text(item: LaunchPlanItem | None, field_name: str) -> str | None:
    if item is None:
        return None
    value = getattr(item, field_name)
    return value if isinstance(value, str) else None


def _resolve_portal(value: str | None, *, apply_url: str) -> str:
    normalized_value = _normalize_optional_text(value)
    if normalized_value is None:
        detected_portal = detect_portal_type(apply_url)
        if detected_portal is None:
            raise ValueError("portal is required when it cannot be inferred from apply_url")
        return detected_portal
    return normalized_value.lower()


def _require_text(value: str | None, *, label: str) -> str:
    normalized_value = _normalize_optional_text(value)
    if normalized_value is None:
        raise ValueError(f"{label} is required")
    return normalized_value


def _normalize_optional_text(value: str | None) -> str | None:
    if value is None:
        return None
    normalized_value = value.strip()
    return normalized_value or None


def _resolve_local_datetime(created_at: datetime | None) -> datetime:
    if created_at is None:
        return _current_local_datetime()
    if created_at.tzinfo is None:
        return created_at.astimezone()
    return created_at.astimezone()


def _current_local_datetime() -> datetime:
    return datetime.now().astimezone()


def _build_log_filename(
    *,
    local_timestamp: datetime,
    company: str,
    portal: str,
) -> str:
    date_text = local_timestamp.date().isoformat()
    company_slug = _slugify_filename_part(company)
    portal_slug = _slugify_filename_part(portal)
    return f"{date_text}-{company_slug}-{portal_slug}.json"


def _slugify_filename_part(value: str) -> str:
    ascii_value = (
        unicodedata.normalize("NFKD", value)
        .encode("ascii", "ignore")
        .decode("ascii")
        .lower()
    )
    slug = _FILENAME_TOKEN_RE.sub("-", ascii_value).strip("-.")
    slug = _HYPHEN_RE.sub("-", slug)
    return slug or "unknown"


def _write_log_payload(log_path: Path, record: ApplicationLogRecord) -> None:
    payload = asdict(record)
    if log_path.exists():
        existing_payload = _load_existing_payload(log_path)
        if not _same_logged_application(existing_payload, payload):
            raise ValueError(
                "log file already exists for a different application: "
                f"{log_path}"
            )
    log_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def _load_existing_payload(log_path: Path) -> dict[str, object]:
    try:
        with log_path.open("r", encoding="utf-8") as input_file:
            payload = json.load(input_file)
    except json.JSONDecodeError as exc:
        raise ValueError(f"existing log file is not valid JSON: {log_path}") from exc
    except OSError as exc:
        raise ValueError(f"existing log file could not be read: {exc}") from exc
    if not isinstance(payload, dict):
        raise ValueError(f"existing log file must contain a JSON object: {log_path}")
    return payload


def _same_logged_application(
    existing_payload: dict[str, object],
    requested_payload: dict[str, object],
) -> bool:
    identity_fields = ("company", "role", "portal", "apply_url")
    return all(existing_payload.get(field_name) == requested_payload[field_name] for field_name in identity_fields)
```
