from __future__ import annotations

from collections.abc import Mapping, Sequence
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import re
from urllib.parse import urlparse

from ..config import GEOGRAPHY_PRIORITY, SEARCH_PRIORITY, TARGET_ROLES
from ..db import connect_database

SCOREABLE_JOBS_SQL = """
SELECT
    id,
    source,
    company,
    title,
    location,
    apply_url,
    portal_type,
    posted_at,
    found_at,
    raw_json
FROM jobs
ORDER BY id
"""

STACK_MATCH_POINTS = 5

ROLE_RULES = (
    (TARGET_ROLES[3], 40, (re.compile(r"\bplatform data engineer\b"),)),
    (
        TARGET_ROLES[2],
        40,
        (
            re.compile(r"\btelemetry engineer\b"),
            re.compile(r"\bobservability engineer\b"),
            re.compile(r"\btelemetry\b.*\bengineer\b"),
            re.compile(r"\bobservability\b.*\bengineer\b"),
        ),
    ),
    (TARGET_ROLES[1], 40, (re.compile(r"\banalytics engineer\b"),)),
    (TARGET_ROLES[0], 35, (re.compile(r"\bdata engineer\b"),)),
    (
        TARGET_ROLES[4],
        25,
        (
            re.compile(r"\bbigquery\b"),
            re.compile(r"\bgcp\b"),
            re.compile(r"\bgoogle cloud(?: platform)?\b"),
        ),
    ),
)

STACK_RULES = (
    ("Python", (re.compile(r"\bpython\b"),)),
    ("BigQuery", (re.compile(r"\bbigquery\b"),)),
    ("Looker", (re.compile(r"\blooker\b"),)),
    ("GCP", (re.compile(r"\bgcp\b"), re.compile(r"\bgoogle cloud(?: platform)?\b"))),
    ("telemetry/observability", (re.compile(r"\btelemetry\b"), re.compile(r"\bobservability\b"))),
)

SOURCE_RULES = (
    (
        SEARCH_PRIORITY[0],
        20,
        (
            (re.compile(r"\bstaff(?:ing)?\b"), "staffing"),
            (re.compile(r"\brecruit(?:er|ing)?\b"), "recruiter"),
            (re.compile(r"\bagency\b"), "agency"),
        ),
    ),
    (
        SEARCH_PRIORITY[1],
        15,
        (
            (re.compile(r"\bdice\b"), "dice"),
            (re.compile(r"\bupwork\b"), "upwork"),
            (re.compile(r"\btoptal\b"), "toptal"),
            (re.compile(r"\bgun\.io\b"), "gun.io"),
            (re.compile(r"\bcontra\b"), "contra"),
        ),
    ),
    (
        SEARCH_PRIORITY[2],
        10,
        (
            (re.compile(r"\bvendor\b"), "vendor"),
            (re.compile(r"\bconsult(?:ing|ant|ancy)?\b"), "consulting"),
        ),
    ),
)

DIRECT_PORTAL_PATTERNS = (
    (re.compile(r"\bgreenhouse\b"), "greenhouse"),
    (re.compile(r"\bworkday\b"), "workday"),
    (re.compile(r"\blever\b"), "lever"),
    (re.compile(r"\bashby\b"), "ashby"),
    (re.compile(r"\bicims\b"), "icims"),
    (re.compile(r"\bsmartrecruiters\b"), "smartrecruiters"),
)

DIRECT_SOURCE_PATTERNS = (
    (re.compile(r"\bmanual\b"), "manual"),
    (re.compile(r"\breferral\b"), "referral"),
    (re.compile(r"\bdirect\b"), "direct"),
    (re.compile(r"\bcompany\b"), "company"),
    (re.compile(r"\bcareer(?:s| site| portal)?\b"), "career"),
)


@dataclass(frozen=True, slots=True)
class ScoredJob:
    job_id: int
    source: str
    company: str
    title: str
    location: str | None
    apply_url: str | None
    portal_type: str | None
    posted_at: str | None
    found_at: str | None
    raw_json: str
    total_score: int
    role_score: int
    role_reason: str
    matched_target_role: str | None
    stack_score: int
    stack_reason: str
    matched_stack_keywords: tuple[str, ...]
    geography_score: int
    geography_reason: str
    geography_bucket: str | None
    source_score: int
    source_reason: str
    source_category: str
    actionability_score: int
    actionability_reason: str


def score_jobs_from_database(database_path: Path) -> tuple[ScoredJob, ...]:
    with closing(connect_database(database_path)) as connection:
        rows = connection.execute(SCOREABLE_JOBS_SQL).fetchall()
    return rank_jobs(rows)


def rank_jobs(job_records: Sequence[Mapping[str, object]]) -> tuple[ScoredJob, ...]:
    scored_jobs = [score_job(job_record) for job_record in job_records]
    return tuple(
        sorted(
            scored_jobs,
            key=lambda job: (
                -job.total_score,
                -job.role_score,
                -job.stack_score,
                -job.geography_score,
                -job.source_score,
                -_freshness_sort_key(job),
                job.job_id,
            ),
        )
    )


def score_job(job_record: Mapping[str, object]) -> ScoredJob:
    title = _text_value(_field_value(job_record, "title")) or ""
    source = _text_value(_field_value(job_record, "source")) or ""
    company = _text_value(_field_value(job_record, "company")) or ""
    location = _text_value(_field_value(job_record, "location"))
    apply_url = _text_value(_field_value(job_record, "apply_url"))
    portal_type = _text_value(_field_value(job_record, "portal_type"))
    posted_at = _text_value(_field_value(job_record, "posted_at"))
    found_at = _text_value(_field_value(job_record, "found_at"))
    raw_json = _text_value(_field_value(job_record, "raw_json")) or ""

    role_score, matched_target_role, role_reason = _score_role(title)
    stack_score, matched_stack_keywords, stack_reason = _score_stack(title, company, raw_json)
    geography_score, geography_bucket, geography_reason = _score_geography(location)
    source_score, source_category, source_reason = _score_source(source, portal_type, apply_url)
    actionability_score, actionability_reason = _score_actionability(apply_url)

    return ScoredJob(
        job_id=_int_value(_field_value(job_record, "id")),
        source=source,
        company=company,
        title=title,
        location=location,
        apply_url=apply_url,
        portal_type=portal_type,
        posted_at=posted_at,
        found_at=found_at,
        raw_json=raw_json,
        total_score=role_score + stack_score + geography_score + source_score + actionability_score,
        role_score=role_score,
        role_reason=role_reason,
        matched_target_role=matched_target_role,
        stack_score=stack_score,
        stack_reason=stack_reason,
        matched_stack_keywords=matched_stack_keywords,
        geography_score=geography_score,
        geography_reason=geography_reason,
        geography_bucket=geography_bucket,
        source_score=source_score,
        source_reason=source_reason,
        source_category=source_category,
        actionability_score=actionability_score,
        actionability_reason=actionability_reason,
    )


def _score_role(title: str) -> tuple[int, str | None, str]:
    searchable_text = _normalize_text(title)
    for target_role, score, patterns in ROLE_RULES:
        if _matches_any_pattern(searchable_text, patterns):
            return score, target_role, f'title matched target role "{target_role}"'
    return 0, None, "no target role keyword matched in title"


def _score_stack(title: str, company: str, raw_json: str) -> tuple[int, tuple[str, ...], str]:
    searchable_text = _normalize_text(title, company, raw_json)
    matched_labels = tuple(
        label
        for label, patterns in STACK_RULES
        if _matches_any_pattern(searchable_text, patterns)
    )
    score = len(matched_labels) * STACK_MATCH_POINTS
    if matched_labels:
        return score, matched_labels, f"matched stack keywords: {', '.join(matched_labels)}"
    return 0, (), "no tracked stack keywords matched"


def _score_geography(location: str | None) -> tuple[int, str | None, str]:
    searchable_text = _normalize_text(location)
    if not searchable_text:
        return 0, None, "location missing"
    if re.search(r"\bremote\b", searchable_text):
        return 18, GEOGRAPHY_PRIORITY[0], f'location matched geography priority "{GEOGRAPHY_PRIORITY[0]}"'
    if re.search(r"\bhybrid\b", searchable_text):
        if re.search(r"\b(sacramento|folsom)\b", searchable_text):
            return 9, GEOGRAPHY_PRIORITY[1], f'hybrid location matched geography priority "{GEOGRAPHY_PRIORITY[1]}"'
        if re.search(r"\b(san jose|bay area)\b", searchable_text):
            return 5, GEOGRAPHY_PRIORITY[2], f'hybrid location matched geography priority "{GEOGRAPHY_PRIORITY[2]}"'
        return 3, "Hybrid", 'location matched fallback geography priority "Hybrid"'
    if re.search(r"\b(sacramento|folsom)\b", searchable_text):
        return 12, GEOGRAPHY_PRIORITY[1], f'location matched geography priority "{GEOGRAPHY_PRIORITY[1]}"'
    if re.search(r"\b(san jose|bay area)\b", searchable_text):
        return 6, GEOGRAPHY_PRIORITY[2], f'location matched geography priority "{GEOGRAPHY_PRIORITY[2]}"'
    return 0, None, f'location "{location}" did not match a priority geography'


def _score_source(
    source: str,
    portal_type: str | None,
    apply_url: str | None,
) -> tuple[int, str, str]:
    apply_host = _apply_url_host(apply_url)
    searchable_text = _normalize_text(source, portal_type, apply_host)
    for category, score, patterns in SOURCE_RULES:
        matched_keyword = _first_matching_keyword(searchable_text, patterns)
        if matched_keyword is not None:
            return score, category, f'{category} via keyword "{matched_keyword}"'

    if portal_type:
        return (
            5,
            SEARCH_PRIORITY[3],
            f'{SEARCH_PRIORITY[3]} via portal_type "{portal_type}"',
        )

    matched_keyword = _first_matching_keyword(searchable_text, DIRECT_PORTAL_PATTERNS)
    if matched_keyword is not None:
        return (
            5,
            SEARCH_PRIORITY[3],
            f'{SEARCH_PRIORITY[3]} via portal host keyword "{matched_keyword}"',
        )

    matched_keyword = _first_matching_keyword(searchable_text, DIRECT_SOURCE_PATTERNS)
    if matched_keyword is not None:
        return (
            5,
            SEARCH_PRIORITY[3],
            f'{SEARCH_PRIORITY[3]} via source keyword "{matched_keyword}"',
        )

    return 0, "unclassified", "no source priority rule matched"


def _score_actionability(apply_url: str | None) -> tuple[int, str]:
    if apply_url:
        return 0, "apply_url present"
    return -8, "apply_url missing; deprioritized because the listing is not launchable yet"


def _freshness_sort_key(job: ScoredJob) -> int:
    return _timestamp_sort_value(job.posted_at) or _timestamp_sort_value(job.found_at)


def _timestamp_sort_value(value: str | None) -> int:
    normalized_value = _normalize_timestamp_text(value)
    if normalized_value is None:
        return 0
    try:
        parsed = datetime.fromisoformat(normalized_value.replace("Z", "+00:00"))
    except ValueError:
        return 0
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    else:
        parsed = parsed.astimezone(timezone.utc)
    return int(parsed.timestamp())


def _normalize_timestamp_text(value: str | None) -> str | None:
    if value is None:
        return None
    normalized_value = value.strip()
    if not normalized_value:
        return None
    if len(normalized_value) == 10 and normalized_value.count("-") == 2:
        return f"{normalized_value}T00:00:00+00:00"
    if " " in normalized_value and "T" not in normalized_value:
        return normalized_value.replace(" ", "T", 1)
    return normalized_value


def _apply_url_host(apply_url: str | None) -> str:
    if not apply_url:
        return ""
    return urlparse(apply_url).netloc


def _normalize_text(*parts: str | None) -> str:
    return " ".join(part.strip().lower() for part in parts if part)


def _matches_any_pattern(text: str, patterns: Sequence[re.Pattern[str]]) -> bool:
    return any(pattern.search(text) for pattern in patterns)


def _first_matching_keyword(
    text: str,
    patterns: Sequence[tuple[re.Pattern[str], str]],
) -> str | None:
    for pattern, label in patterns:
        if pattern.search(text):
            return label
    return None


def _text_value(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    return value


def _int_value(value: object) -> int:
    if isinstance(value, int):
        return value
    return int(value or 0)


def _field_value(job_record: Mapping[str, object], field_name: str) -> object:
    try:
        return job_record[field_name]
    except KeyError:
        return None
