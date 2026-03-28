from __future__ import annotations

from collections.abc import Mapping
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
import re
from urllib.parse import urlparse

from .db import connect_database
from .db_runtime import table_names_from_connection
from .jobs.identity import canonicalize_apply_url, normalize_optional_metadata
from .jobs.scoring import score_job
from .launch_dry_run import OPEN_URL_ACTION
from .launch_executor import (
    BROWSER_STUB_EXECUTOR_MODE,
    LaunchExecutionReport,
    select_launch_executor,
)
from .resume.recommendations import QueueRecommendation, recommend_job_record

_JOB_REFERENCE_SELECT_SQL = """
SELECT
    id,
    source,
    company,
    title,
    location,
    apply_url,
    portal_type,
    canonical_apply_url,
    identity_key,
    status,
    raw_json,
    created_at
FROM jobs
"""
_TITLE_TOKEN_RE = re.compile(r"[A-Za-z0-9]+")
_APPLIED_STATUS_BUCKET = frozenset(
    {
        "applied",
        "recruiter_screen",
        "assessment",
        "interview",
        "offer",
        "rejected",
    }
)


@dataclass(frozen=True, slots=True)
class JobReferenceRow:
    job_id: int
    source: str
    company: str
    title: str
    location: str | None
    apply_url: str | None
    portal_type: str | None
    canonical_apply_url: str | None
    identity_key: str | None
    status: str
    raw_json: str
    created_at: str | None

    def to_job_record(self) -> dict[str, object]:
        return {
            "id": self.job_id,
            "source": self.source,
            "company": self.company,
            "title": self.title,
            "location": self.location,
            "apply_url": self.apply_url,
            "portal_type": self.portal_type,
            "raw_json": self.raw_json,
        }


@dataclass(frozen=True, slots=True)
class JobReferenceResolution:
    reference_text: str
    reference_kind: str
    normalized_apply_url: str | None
    canonical_lookup_url: str | None
    selected_row: JobReferenceRow
    preferred_row: JobReferenceRow
    matched_rows: tuple[JobReferenceRow, ...]

    @property
    def sibling_rows(self) -> tuple[JobReferenceRow, ...]:
        return tuple(
            row for row in self.matched_rows if row.job_id != self.selected_row.job_id
        )


@dataclass(frozen=True, slots=True)
class JobReferenceInspectResult:
    resolution: JobReferenceResolution
    actionable: bool
    launchable: bool
    warnings: tuple[str, ...]
    skip_reasons: tuple[str, ...]
    recommendation: QueueRecommendation


@dataclass(frozen=True, slots=True)
class JobReferenceOpenResult:
    resolution: JobReferenceResolution
    execution_report: LaunchExecutionReport


@dataclass(frozen=True, slots=True)
class _DirectOpenStep:
    launch_order: int
    action_label: str
    company: str | None
    title: str | None
    apply_url: str


def resolve_job_reference(
    database_path: Path,
    reference: str | int,
) -> JobReferenceResolution:
    if isinstance(reference, int):
        return _resolve_job_id_reference(database_path, reference, reference_text=str(reference))

    normalized_reference = normalize_optional_metadata(reference)
    if normalized_reference is None:
        raise ValueError("reference must not be blank")

    if normalized_reference.isdigit() and not _looks_like_url(normalized_reference):
        return _resolve_job_id_reference(
            database_path,
            int(normalized_reference),
            reference_text=normalized_reference,
        )

    if _looks_like_url(normalized_reference):
        return _resolve_apply_url_reference(database_path, normalized_reference)

    raise ValueError("reference must be a numeric job_id or an absolute apply_url")


def inspect_job_reference(
    database_path: Path,
    reference: str | int,
) -> JobReferenceInspectResult:
    resolution = resolve_job_reference(database_path, reference)
    selected_row = resolution.selected_row
    actionable = selected_row.status == "new"
    launchable = actionable and selected_row.apply_url is not None

    warnings: list[str] = []
    if selected_row.apply_url is None:
        warnings.append("apply_url missing")

    skip_reasons: list[str] = []
    if not actionable:
        skip_reasons.append(
            f"status {selected_row.status} is excluded from the normal queue/session flow"
        )
    if selected_row.apply_url is None:
        skip_reasons.append("apply_url missing")

    return JobReferenceInspectResult(
        resolution=resolution,
        actionable=actionable,
        launchable=launchable,
        warnings=tuple(warnings),
        skip_reasons=tuple(skip_reasons),
        recommendation=recommend_job_record(selected_row.to_job_record()),
    )


def open_job_reference(
    database_path: Path,
    reference: str | int,
    *,
    executor_mode: str = BROWSER_STUB_EXECUTOR_MODE,
) -> JobReferenceOpenResult:
    resolution = resolve_job_reference(database_path, reference)
    selected_row = resolution.selected_row
    if selected_row.apply_url is None:
        raise ValueError(f"job id {selected_row.job_id} is missing apply_url")

    execution_report = select_launch_executor(executor_mode).execute_step(
        _DirectOpenStep(
            launch_order=1,
            action_label=OPEN_URL_ACTION,
            company=selected_row.company,
            title=selected_row.title,
            apply_url=selected_row.apply_url,
        )
    )
    return JobReferenceOpenResult(
        resolution=resolution,
        execution_report=execution_report,
    )


def _resolve_job_id_reference(
    database_path: Path,
    job_id: int,
    *,
    reference_text: str,
) -> JobReferenceResolution:
    if job_id < 1:
        raise ValueError("job_id must be at least 1")

    with closing(connect_database(database_path)) as connection:
        _require_jobs_table(connection)
        selected_row = _fetch_job_row(connection, job_id=job_id)
        if selected_row is None:
            raise ValueError(f"job id {job_id} was not found")

        matched_rows = _fetch_opportunity_group(connection, selected_row)
        preferred_row = _select_preferred_row(matched_rows)
        return JobReferenceResolution(
            reference_text=reference_text,
            reference_kind="job_id",
            normalized_apply_url=selected_row.apply_url,
            canonical_lookup_url=selected_row.canonical_apply_url,
            selected_row=selected_row,
            preferred_row=preferred_row,
            matched_rows=_sort_rows_by_preference(matched_rows),
        )


def _resolve_apply_url_reference(
    database_path: Path,
    apply_url: str,
) -> JobReferenceResolution:
    normalized_apply_url = normalize_optional_metadata(apply_url)
    if normalized_apply_url is None:
        raise ValueError("apply_url must not be blank")

    canonical_lookup_url = canonicalize_apply_url(normalized_apply_url)

    with closing(connect_database(database_path)) as connection:
        _require_jobs_table(connection)
        matched_rows = _dedupe_rows(
            _fetch_rows_by_apply_url(connection, normalized_apply_url),
            _fetch_rows_by_canonical_apply_url(connection, canonical_lookup_url),
        )
        if not matched_rows:
            raise ValueError(f"no job matched apply_url {normalized_apply_url}")

        preferred_row = _select_preferred_row(matched_rows)
        return JobReferenceResolution(
            reference_text=normalized_apply_url,
            reference_kind="apply_url",
            normalized_apply_url=normalized_apply_url,
            canonical_lookup_url=canonical_lookup_url,
            selected_row=preferred_row,
            preferred_row=preferred_row,
            matched_rows=_sort_rows_by_preference(matched_rows),
        )


def _fetch_job_row(connection, *, job_id: int) -> JobReferenceRow | None:
    row = connection.execute(
        f"{_JOB_REFERENCE_SELECT_SQL}\nWHERE id = ?\nLIMIT 1",
        (job_id,),
    ).fetchone()
    if row is None:
        return None
    return _job_reference_row_from_db_row(row)


def _fetch_rows_by_apply_url(connection, apply_url: str) -> tuple[JobReferenceRow, ...]:
    rows = connection.execute(
        f"{_JOB_REFERENCE_SELECT_SQL}\nWHERE apply_url = ?\nORDER BY id",
        (apply_url,),
    ).fetchall()
    return tuple(_job_reference_row_from_db_row(row) for row in rows)


def _fetch_rows_by_canonical_apply_url(
    connection,
    canonical_apply_url: str | None,
) -> tuple[JobReferenceRow, ...]:
    if canonical_apply_url is None:
        return ()
    rows = connection.execute(
        f"{_JOB_REFERENCE_SELECT_SQL}\nWHERE canonical_apply_url = ?\nORDER BY id",
        (canonical_apply_url,),
    ).fetchall()
    return tuple(_job_reference_row_from_db_row(row) for row in rows)


def _fetch_opportunity_group(
    connection,
    selected_row: JobReferenceRow,
) -> tuple[JobReferenceRow, ...]:
    group_rows = _fetch_rows_by_canonical_apply_url(connection, selected_row.canonical_apply_url)
    if group_rows:
        return group_rows
    return (selected_row,)


def _job_reference_row_from_db_row(row) -> JobReferenceRow:
    apply_url = _nullable_text(row["apply_url"])
    portal_type = _nullable_text(row["portal_type"])
    canonical_apply_url = _nullable_text(row["canonical_apply_url"]) or canonicalize_apply_url(
        apply_url,
        portal_type=portal_type,
    )
    return JobReferenceRow(
        job_id=int(row["id"]),
        source=str(row["source"]),
        company=str(row["company"]),
        title=str(row["title"]),
        location=_nullable_text(row["location"]),
        apply_url=apply_url,
        portal_type=portal_type,
        canonical_apply_url=canonical_apply_url,
        identity_key=_nullable_text(row["identity_key"]),
        status=str(row["status"]),
        raw_json=str(row["raw_json"] or ""),
        created_at=_nullable_text(row["created_at"]),
    )


def _dedupe_rows(*row_groups: tuple[JobReferenceRow, ...]) -> tuple[JobReferenceRow, ...]:
    deduped: dict[int, JobReferenceRow] = {}
    for row_group in row_groups:
        for row in row_group:
            deduped[row.job_id] = row
    return tuple(deduped[job_id] for job_id in sorted(deduped))


def _sort_rows_by_preference(rows: tuple[JobReferenceRow, ...]) -> tuple[JobReferenceRow, ...]:
    return tuple(sorted(rows, key=_preferred_row_sort_key))


def _select_preferred_row(rows: tuple[JobReferenceRow, ...]) -> JobReferenceRow:
    return min(rows, key=_preferred_row_sort_key)


def _preferred_row_sort_key(row: JobReferenceRow) -> tuple[int, int, int, int, int]:
    title_token_count, title_length = _title_quality_sort_key(row.title)
    return (
        _status_priority(row.status),
        -title_token_count,
        -title_length,
        -score_job(row.to_job_record()).total_score,
        row.job_id,
    )


def _title_quality_sort_key(value: str) -> tuple[int, int]:
    tokens = _TITLE_TOKEN_RE.findall(value)
    return len(tokens), len(value)


def _status_priority(status: str) -> int:
    normalized_status = status.strip().lower()
    if normalized_status in _APPLIED_STATUS_BUCKET:
        return 0
    if normalized_status == "opened":
        return 1
    return 2


def _require_jobs_table(connection) -> None:
    if "jobs" not in table_names_from_connection(connection):
        raise ValueError("jobs table is not available in the active database backend")


def _looks_like_url(value: str) -> bool:
    parsed_url = urlparse(value)
    return bool(parsed_url.scheme and parsed_url.netloc)


def _nullable_text(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    return str(value)
