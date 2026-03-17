from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
import re

from ..launch_preview import LaunchPreview
from ..portal_support import detect_portal_type
from ..resume.recommendations import recommend_queued_job
from .queue import RankedQueuedJob, select_ranked_apply_queue

DEFAULT_FAST_APPLY_LIMIT = 20
SUPPORTED_FAST_APPLY_FAMILIES = ("data", "backend", "software")
_EASY_APPLY_PORTAL_TYPES = frozenset({"greenhouse", "lever", "ashby"})
_NORMALIZE_RE = re.compile(r"[^a-z0-9]+")
_FAMILY_TERMS = {
    "data": (
        "analytics engineer",
        "bigquery",
        "data engineer",
        "data infrastructure",
        "data pipeline",
        "data platform",
        "dbt",
        "etl",
        "platform data",
        "warehouse",
    ),
    "backend": (
        "api",
        "back end",
        "backend",
        "distributed systems",
        "platform engineer",
        "python engineer",
        "services",
    ),
    "software": (
        "full stack",
        "software developer",
        "software engineer",
    ),
}
_SOFTWARE_ANCHOR_TERMS = (
    "api",
    "backend",
    "cloud",
    "data",
    "infrastructure",
    "platform",
    "python",
)
_TARGET_ROLE_FAMILY_MAP = {
    "Analytics Engineer": "data",
    "BigQuery / GCP-oriented roles": "data",
    "Data Engineer": "data",
    "Platform Data Engineer": "data",
}


@dataclass(frozen=True, slots=True)
class FastApplySelection:
    preview: LaunchPreview
    matched_families: tuple[str, ...]
    requested_family_hit: bool
    easy_apply_supported: bool
    fit_reason: str
    queue_reason_summary: str


@dataclass(frozen=True, slots=True)
class _FastApplyCandidate:
    queued_job: RankedQueuedJob
    matched_families: tuple[str, ...]
    requested_family_hit: bool
    easy_apply_supported: bool
    fit_reason: str


def parse_fast_apply_families(
    value: str | Sequence[str] | None,
) -> tuple[str, ...]:
    if value is None:
        return ()

    raw_values: list[str] = []
    if isinstance(value, str):
        raw_values.extend(value.split(","))
    else:
        for item in value:
            raw_values.extend(item.split(","))

    normalized_values: list[str] = []
    invalid_values: list[str] = []
    for raw_value in raw_values:
        normalized_value = raw_value.strip().lower()
        if not normalized_value:
            continue
        if normalized_value not in SUPPORTED_FAST_APPLY_FAMILIES:
            invalid_values.append(normalized_value)
            continue
        if normalized_value not in normalized_values:
            normalized_values.append(normalized_value)

    if invalid_values:
        supported = ", ".join(SUPPORTED_FAST_APPLY_FAMILIES)
        invalid = ", ".join(invalid_values)
        raise ValueError(
            f"invalid fast-apply families: {invalid}; expected any of: {supported}"
        )
    return tuple(normalized_values)


def select_fast_apply_selections(
    database_path: Path,
    *,
    limit: int | None = None,
    ingest_batch_id: str | None = None,
    query_text: str | None = None,
    families: str | Sequence[str] | None = None,
    remote_only: bool = False,
    easy_apply_first: bool = False,
) -> tuple[FastApplySelection, ...]:
    requested_families = parse_fast_apply_families(families)
    ranked_jobs = select_ranked_apply_queue(
        database_path,
        limit=None,
        ingest_batch_id=ingest_batch_id,
        query_text=query_text,
    )

    candidates = [
        candidate
        for queued_job in ranked_jobs
        for candidate in (_candidate_from_ranked_job(queued_job, requested_families=requested_families),)
        if candidate is not None
        if not remote_only or _is_remote_job(candidate.queued_job.scored_job.location)
    ]
    candidates.sort(
        key=lambda candidate: (
            -int(candidate.requested_family_hit) if requested_families else 0,
            -int(candidate.easy_apply_supported) if easy_apply_first else 0,
            -candidate.queued_job.scored_job.total_score,
            candidate.queued_job.rank,
        )
    )
    if limit is not None:
        candidates = candidates[:limit]

    selections: list[FastApplySelection] = []
    for index, candidate in enumerate(candidates, start=1):
        reranked_job = RankedQueuedJob(
            rank=index,
            scored_job=candidate.queued_job.scored_job,
            reason_summary=candidate.queued_job.reason_summary,
        )
        recommendation = recommend_queued_job(reranked_job)
        preview = LaunchPreview(
            rank=recommendation.rank,
            job_id=recommendation.job_id,
            company=recommendation.company,
            title=recommendation.title,
            location=recommendation.location,
            apply_url=recommendation.apply_url,
            portal_type=recommendation.portal_type,
            source=recommendation.source,
            score=recommendation.score,
            resume_variant_key=recommendation.resume_variant_key,
            resume_variant_label=recommendation.resume_variant_label,
            snippet_key=recommendation.snippet_key,
            snippet_label=recommendation.snippet_label,
            snippet_text=recommendation.snippet_text,
            explanation=recommendation.explanation,
        )
        selections.append(
            FastApplySelection(
                preview=preview,
                matched_families=candidate.matched_families,
                requested_family_hit=candidate.requested_family_hit,
                easy_apply_supported=candidate.easy_apply_supported,
                fit_reason=candidate.fit_reason,
                queue_reason_summary=candidate.queued_job.reason_summary,
            )
        )
    return tuple(selections)


def _candidate_from_ranked_job(
    queued_job: RankedQueuedJob,
    *,
    requested_families: Sequence[str],
) -> _FastApplyCandidate | None:
    scored_job = queued_job.scored_job
    if _normalize_text(scored_job.apply_url) is None:
        return None

    matched_families = _matched_families(scored_job)
    if not matched_families and scored_job.matched_target_role is None:
        return None

    requested_family_hit = bool(
        requested_families
        and set(matched_families).intersection(requested_families)
    )
    return _FastApplyCandidate(
        queued_job=queued_job,
        matched_families=matched_families,
        requested_family_hit=requested_family_hit,
        easy_apply_supported=_is_easy_apply_supported(
            scored_job.apply_url,
            portal_type=scored_job.portal_type,
        ),
        fit_reason=_build_fit_reason(
            scored_job.matched_target_role,
            matched_families,
        ),
    )


def _matched_families(scored_job) -> tuple[str, ...]:
    normalized_search_text = _normalize_search_text(
        scored_job.title,
        scored_job.company,
        scored_job.location,
        scored_job.source,
        scored_job.portal_type,
        scored_job.raw_json,
    )
    matched_families: list[str] = []

    mapped_target_family = _TARGET_ROLE_FAMILY_MAP.get(scored_job.matched_target_role)
    if mapped_target_family is not None:
        matched_families.append(mapped_target_family)

    for family in SUPPORTED_FAST_APPLY_FAMILIES:
        if _family_matches(normalized_search_text, family):
            matched_families.append(family)

    return tuple(dict.fromkeys(matched_families))


def _family_matches(normalized_search_text: str, family: str) -> bool:
    if family == "software":
        return _software_family_matches(normalized_search_text)
    return _contains_any_term(normalized_search_text, _FAMILY_TERMS[family])


def _software_family_matches(normalized_search_text: str) -> bool:
    if not _contains_any_term(normalized_search_text, _FAMILY_TERMS["software"]):
        return False
    return _contains_any_term(normalized_search_text, _SOFTWARE_ANCHOR_TERMS)


def _is_remote_job(location: str | None) -> bool:
    normalized_location = _normalize_search_text(location)
    return _contains_any_term(normalized_location, ("remote",))


def _is_easy_apply_supported(
    apply_url: str | None,
    *,
    portal_type: str | None,
) -> bool:
    detected_portal_type = detect_portal_type(apply_url, portal_type=portal_type)
    return detected_portal_type in _EASY_APPLY_PORTAL_TYPES


def _build_fit_reason(
    matched_target_role: str | None,
    matched_families: Sequence[str],
) -> str:
    parts: list[str] = []
    if matched_families:
        parts.append(f"matched families: {', '.join(matched_families)}")
    if matched_target_role is not None:
        parts.append(f"existing role signal: {matched_target_role}")
    return "; ".join(parts) if parts else "matched fast-apply heuristics"


def _contains_any_term(normalized_text: str, terms: Sequence[str]) -> bool:
    for term in terms:
        normalized_term = _normalize_match_text(term)
        if normalized_term and f" {normalized_term} " in normalized_text:
            return True
    return False


def _normalize_search_text(*parts: str | None) -> str:
    joined_text = " ".join(
        normalized_part
        for part in parts
        for normalized_part in (_normalize_text(part),)
        if normalized_part is not None
    )
    return f" {_normalize_match_text(joined_text)} "


def _normalize_match_text(text: str) -> str:
    return _NORMALIZE_RE.sub(" ", text.lower()).strip()


def _normalize_text(value: str | None) -> str | None:
    if value is None:
        return None
    normalized_value = value.strip()
    return normalized_value or None
