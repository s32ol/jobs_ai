from __future__ import annotations

from collections.abc import Sequence
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, timezone
from html import unescape
from html.parser import HTMLParser
import json
import threading
import time
from pathlib import Path
from typing import Literal
from urllib.parse import urljoin

from ..collect.fetch import FetchError, FetchRequest, FetchResponse, Fetcher, fetch_text
from ..db import connect_database, find_duplicate_job_match, initialize_schema, insert_job
from ..jobs.importer import describe_duplicate_match, normalize_import_record
from ..jobs.identity import normalize_batch_id
from ..portal_support import build_portal_support, extract_portal_board_root_url
from ..source_seed.infer import parse_company_inputs, primary_domain_label
from ..source_seed.models import CompanySeedInput
from ..workspace import WorkspacePaths
from .registry import (
    get_registry_source_by_normalized_url_from_connection,
    normalize_registry_source_url,
)

DEFAULT_JOBPOSTING_TIMEOUT_SECONDS = 10.0
DEFAULT_JOBPOSTING_MAX_REQUESTS_PER_SECOND = 2.0
_JOBPOSTING_SOURCE = "jobposting_json_ld"

JobPostingExtractionOutcome = Literal["success", "no_match", "failed"]


@dataclass(frozen=True, slots=True)
class JobPostingExtractionTargetResult:
    raw_input: str
    resolved_url: str | None
    final_url: str | None
    json_ld_block_count: int
    jobposting_count: int
    inserted_count: int
    duplicate_count: int
    invalid_count: int
    registry_link_count: int
    outcome: JobPostingExtractionOutcome
    reason_code: str
    reason: str
    duplicate_messages: tuple[str, ...] = ()
    invalid_messages: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class JobPostingExtractionResult:
    batch_id: str
    target_results: tuple[JobPostingExtractionTargetResult, ...]

    @property
    def input_count(self) -> int:
        return len(self.target_results)

    @property
    def matched_page_count(self) -> int:
        return sum(1 for result in self.target_results if result.outcome == "success")

    @property
    def no_match_count(self) -> int:
        return sum(1 for result in self.target_results if result.outcome == "no_match")

    @property
    def failed_count(self) -> int:
        return sum(1 for result in self.target_results if result.outcome == "failed")

    @property
    def json_ld_block_count(self) -> int:
        return sum(result.json_ld_block_count for result in self.target_results)

    @property
    def jobposting_count(self) -> int:
        return sum(result.jobposting_count for result in self.target_results)

    @property
    def inserted_count(self) -> int:
        return sum(result.inserted_count for result in self.target_results)

    @property
    def duplicate_count(self) -> int:
        return sum(result.duplicate_count for result in self.target_results)

    @property
    def invalid_count(self) -> int:
        return sum(result.invalid_count for result in self.target_results)

    @property
    def registry_link_count(self) -> int:
        return sum(result.registry_link_count for result in self.target_results)


class _RateLimiter:
    def __init__(self, max_requests_per_second: float) -> None:
        self._interval = 1.0 / max_requests_per_second if max_requests_per_second > 0 else 0.0
        self._next_allowed_at = 0.0
        self._lock = threading.Lock()

    def acquire(self) -> None:
        if self._interval <= 0:
            return

        while True:
            with self._lock:
                now = time.monotonic()
                wait_seconds = self._next_allowed_at - now
                if wait_seconds <= 0:
                    self._next_allowed_at = max(self._next_allowed_at, now) + self._interval
                    return
            time.sleep(wait_seconds)


class _JsonLdScriptExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=False)
        self.blocks: list[str] = []
        self._capture_depth = 0
        self._current_parts: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() != "script":
            return
        attr_map = {
            key.lower(): (value or "")
            for key, value in attrs
        }
        script_type = attr_map.get("type", "").lower()
        if "application/ld+json" not in script_type:
            return
        self._capture_depth += 1
        self._current_parts = []

    def handle_data(self, data: str) -> None:
        if self._capture_depth:
            self._current_parts.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() != "script" or not self._capture_depth:
            return
        self.blocks.append("".join(self._current_parts))
        self._capture_depth = 0
        self._current_parts = []


def extract_jobposting_sources(
    paths: WorkspacePaths,
    *,
    targets: Sequence[str],
    from_file: Path | None,
    timeout_seconds: float = DEFAULT_JOBPOSTING_TIMEOUT_SECONDS,
    max_requests_per_second: float = DEFAULT_JOBPOSTING_MAX_REQUESTS_PER_SECOND,
    batch_id: str | None = None,
    created_at: datetime | None = None,
    fetcher: Fetcher | None = None,
) -> JobPostingExtractionResult:
    raw_values = _load_target_values(targets, from_file)
    company_inputs = parse_company_inputs(raw_values)
    if not company_inputs:
        raise ValueError("at least one domain or careers page is required via arguments or --from-file")
    if timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be greater than 0")
    if max_requests_per_second <= 0:
        raise ValueError("max_requests_per_second must be greater than 0")

    initialize_schema(paths.database_path)
    created_at_dt = _normalize_created_at(created_at)
    created_at_text = _format_created_at(created_at_dt)
    normalized_batch_id = _resolve_batch_id(batch_id, created_at=created_at_dt)
    rate_limiter = _RateLimiter(max_requests_per_second)
    effective_fetcher = fetch_text if fetcher is None else fetcher
    target_results: list[JobPostingExtractionTargetResult] = []

    with closing(connect_database(paths.database_path)) as connection:
        for company_input in company_inputs:
            target_results.append(
                _extract_target_jobs(
                    connection,
                    company_input=company_input,
                    batch_id=normalized_batch_id,
                    created_at_text=created_at_text,
                    timeout_seconds=timeout_seconds,
                    rate_limiter=rate_limiter,
                    fetcher=effective_fetcher,
                )
            )
        connection.commit()

    return JobPostingExtractionResult(
        batch_id=normalized_batch_id,
        target_results=tuple(target_results),
    )


def _extract_target_jobs(
    connection,
    *,
    company_input: CompanySeedInput,
    batch_id: str,
    created_at_text: str,
    timeout_seconds: float,
    rate_limiter: _RateLimiter,
    fetcher: Fetcher,
) -> JobPostingExtractionTargetResult:
    resolved_url = _resolve_target_url(company_input)
    if resolved_url is None:
        return JobPostingExtractionTargetResult(
            raw_input=company_input.raw_value,
            resolved_url=None,
            final_url=None,
            json_ld_block_count=0,
            jobposting_count=0,
            inserted_count=0,
            duplicate_count=0,
            invalid_count=0,
            registry_link_count=0,
            outcome="failed",
            reason_code="missing_domain_or_url",
            reason="extract-jobposting needs a domain, homepage URL, or direct careers page URL",
        )

    rate_limiter.acquire()
    try:
        response = fetcher(
            FetchRequest(
                url=resolved_url,
                timeout_seconds=timeout_seconds,
                headers={"Accept": "text/html, application/xhtml+xml"},
            )
        )
    except FetchError as exc:
        return JobPostingExtractionTargetResult(
            raw_input=company_input.raw_value,
            resolved_url=resolved_url,
            final_url=None,
            json_ld_block_count=0,
            jobposting_count=0,
            inserted_count=0,
            duplicate_count=0,
            invalid_count=0,
            registry_link_count=0,
            outcome="failed",
            reason_code="fetch_failed",
            reason=str(exc),
        )

    if not _looks_like_html(response):
        return JobPostingExtractionTargetResult(
            raw_input=company_input.raw_value,
            resolved_url=resolved_url,
            final_url=response.final_url or response.url,
            json_ld_block_count=0,
            jobposting_count=0,
            inserted_count=0,
            duplicate_count=0,
            invalid_count=0,
            registry_link_count=0,
            outcome="failed",
            reason_code="non_html_content",
            reason="the fetched page was not HTML",
        )

    final_url = response.final_url or response.url
    json_ld_blocks = _extract_json_ld_blocks(response.text)
    if not json_ld_blocks:
        return JobPostingExtractionTargetResult(
            raw_input=company_input.raw_value,
            resolved_url=resolved_url,
            final_url=final_url,
            json_ld_block_count=0,
            jobposting_count=0,
            inserted_count=0,
            duplicate_count=0,
            invalid_count=0,
            registry_link_count=0,
            outcome="no_match",
            reason_code="no_jobposting_json_ld",
            reason="no JSON-LD script blocks were found on the fetched page",
        )

    jobposting_objects: list[dict[str, object]] = []
    for block in json_ld_blocks:
        payload = _load_json_ld_payload(block)
        if payload is None:
            continue
        jobposting_objects.extend(_collect_jobposting_objects(payload))

    if not jobposting_objects:
        return JobPostingExtractionTargetResult(
            raw_input=company_input.raw_value,
            resolved_url=resolved_url,
            final_url=final_url,
            json_ld_block_count=len(json_ld_blocks),
            jobposting_count=0,
            inserted_count=0,
            duplicate_count=0,
            invalid_count=0,
            registry_link_count=0,
            outcome="no_match",
            reason_code="no_jobposting_objects",
            reason="JSON-LD was present, but no schema.org JobPosting objects were found",
        )

    duplicate_messages: list[str] = []
    invalid_messages: list[str] = []
    inserted_count = 0
    duplicate_count = 0
    invalid_count = 0
    registry_link_count = 0

    for posting in jobposting_objects:
        record = _build_import_record(
            posting,
            company_input=company_input,
            source_page_url=final_url,
        )
        normalized_record, error = normalize_import_record(record)
        if error is not None:
            invalid_count += 1
            invalid_messages.append(error)
            continue

        duplicate_match = find_duplicate_job_match(connection, normalized_record)
        if duplicate_match is not None:
            duplicate_count += 1
            duplicate_messages.append(describe_duplicate_match(duplicate_match))
            continue

        registry_source_id = _registry_source_id_for_apply_url(
            connection,
            apply_url=normalized_record.get("apply_url"),
            portal_type=normalized_record.get("portal_type"),
        )
        insert_job(
            connection,
            {
                **normalized_record,
                "found_at": created_at_text,
                "ingest_batch_id": batch_id,
                "import_source": final_url,
                "source_registry_id": registry_source_id,
            },
        )
        inserted_count += 1
        if registry_source_id is not None:
            registry_link_count += 1

    outcome: JobPostingExtractionOutcome
    reason_code: str
    reason: str
    if inserted_count or duplicate_count:
        outcome = "success"
        if inserted_count:
            reason_code = "jobpostings_imported"
            reason = f"processed {len(jobposting_objects)} JobPosting object(s)"
        else:
            reason_code = "jobpostings_already_imported"
            reason = "all extracted jobs were already present in the database"
    elif invalid_count:
        outcome = "failed"
        reason_code = "invalid_jobpostings"
        reason = "JobPosting objects were found, but none could be normalized into importable jobs"
    else:
        outcome = "no_match"
        reason_code = "no_importable_jobpostings"
        reason = "no importable JobPosting entries were found on the page"

    return JobPostingExtractionTargetResult(
        raw_input=company_input.raw_value,
        resolved_url=resolved_url,
        final_url=final_url,
        json_ld_block_count=len(json_ld_blocks),
        jobposting_count=len(jobposting_objects),
        inserted_count=inserted_count,
        duplicate_count=duplicate_count,
        invalid_count=invalid_count,
        registry_link_count=registry_link_count,
        outcome=outcome,
        reason_code=reason_code,
        reason=reason,
        duplicate_messages=tuple(duplicate_messages),
        invalid_messages=tuple(invalid_messages),
    )


def _load_target_values(targets: Sequence[str], from_file: Path | None) -> tuple[str, ...]:
    values = [value for value in targets]
    if from_file is not None:
        values.extend(from_file.read_text(encoding="utf-8").splitlines())
    return tuple(values)


def _resolve_target_url(company_input: CompanySeedInput) -> str | None:
    if company_input.career_page_url is not None:
        return company_input.career_page_url
    if company_input.domain is None:
        return None
    return f"https://{company_input.domain}"


def _looks_like_html(response: FetchResponse) -> bool:
    content_type = (response.content_type or "").lower()
    if not content_type:
        return response.text.lstrip().startswith("<")
    return "html" in content_type or "xhtml" in content_type


def _extract_json_ld_blocks(html_text: str) -> tuple[str, ...]:
    parser = _JsonLdScriptExtractor()
    parser.feed(html_text)
    return tuple(block.strip() for block in parser.blocks if block.strip())


def _load_json_ld_payload(block_text: str) -> object | None:
    stripped = unescape(block_text).strip()
    if not stripped:
        return None
    if stripped.startswith("<!--") and stripped.endswith("-->"):
        stripped = stripped.removeprefix("<!--").removesuffix("-->").strip()
    if stripped.startswith("<![CDATA[") and stripped.endswith("]]>"):
        stripped = stripped.removeprefix("<![CDATA[").removesuffix("]]>").strip()
    try:
        return json.loads(stripped)
    except json.JSONDecodeError:
        return None


def _collect_jobposting_objects(payload: object) -> list[dict[str, object]]:
    postings: list[dict[str, object]] = []

    def walk(value: object) -> None:
        if isinstance(value, dict):
            if _is_jobposting_type(value.get("@type")):
                postings.append(value)
            for child in value.values():
                walk(child)
            return
        if isinstance(value, list):
            for item in value:
                walk(item)

    walk(payload)
    return postings


def _is_jobposting_type(value: object) -> bool:
    if isinstance(value, str):
        normalized = value.strip().lower()
        return normalized == "jobposting" or normalized.endswith("/jobposting")
    if isinstance(value, list):
        return any(_is_jobposting_type(item) for item in value)
    return False


def _build_import_record(
    posting: dict[str, object],
    *,
    company_input: CompanySeedInput,
    source_page_url: str,
) -> dict[str, object]:
    title = _extract_text(posting.get("title"))
    company = _organization_name(posting.get("hiringOrganization")) or _fallback_company_name(company_input)
    jobposting_url = _absolute_url(_extract_text(posting.get("url")), base_url=source_page_url)
    apply_url = _absolute_url(_extract_text(posting.get("applyUrl")), base_url=source_page_url) or jobposting_url
    portal_support = build_portal_support(apply_url)
    job_location = _extract_location_texts(posting.get("jobLocation"))
    applicant_location_requirements = _extract_location_texts(posting.get("applicantLocationRequirements"))
    job_location_type = _extract_text(posting.get("jobLocationType"))
    location = _resolve_location(
        job_location=job_location,
        applicant_location_requirements=applicant_location_requirements,
        job_location_type=job_location_type,
    )
    employment_type = _extract_employment_type(posting.get("employmentType"))

    return {
        "source": _JOBPOSTING_SOURCE,
        "company": company,
        "title": title,
        "location": location,
        "apply_url": apply_url,
        "portal_type": portal_support.portal_type if portal_support is not None else None,
        "posted_at": _extract_text(posting.get("datePosted")),
        "employment_type": employment_type,
        "applicant_location_requirements": list(applicant_location_requirements),
        "job_location": list(job_location),
        "job_location_type": job_location_type,
        "jobposting_url": jobposting_url,
        "source_page_url": source_page_url,
    }


def _organization_name(value: object) -> str | None:
    if isinstance(value, str):
        return _normalize_text(value)
    if isinstance(value, list):
        for item in value:
            name = _organization_name(item)
            if name is not None:
                return name
        return None
    if not isinstance(value, dict):
        return None
    return (
        _extract_text(value.get("name"))
        or _extract_text(value.get("legalName"))
        or _extract_text(value.get("alternateName"))
    )


def _fallback_company_name(company_input: CompanySeedInput) -> str | None:
    if company_input.company is not None:
        return company_input.company
    label = primary_domain_label(company_input.domain)
    if label is None:
        return None
    return " ".join(part.capitalize() for part in label.split("-"))


def _extract_employment_type(value: object) -> str | None:
    if isinstance(value, str):
        return _normalize_text(value)
    if isinstance(value, list):
        values = [
            normalized
            for item in value
            if (normalized := _extract_text(item)) is not None
        ]
        if not values:
            return None
        return ", ".join(dict.fromkeys(values))
    return None


def _resolve_location(
    *,
    job_location: tuple[str, ...],
    applicant_location_requirements: tuple[str, ...],
    job_location_type: str | None,
) -> str | None:
    is_remote = _is_remote_job_location_type(job_location_type)
    if job_location:
        return "; ".join(job_location)
    if is_remote and applicant_location_requirements:
        return f"Remote ({'; '.join(applicant_location_requirements)})"
    if applicant_location_requirements:
        return "; ".join(applicant_location_requirements)
    if is_remote:
        return "Remote"
    return None


def _is_remote_job_location_type(value: str | None) -> bool:
    if value is None:
        return False
    normalized = value.strip().lower()
    return "telecommute" in normalized or "remote" in normalized


def _extract_location_texts(value: object) -> tuple[str, ...]:
    collected: list[str] = []

    def append(text: str | None) -> None:
        normalized = _normalize_text(text)
        if normalized is None or normalized in collected:
            return
        collected.append(normalized)

    def walk(item: object) -> None:
        if isinstance(item, str):
            append(item)
            return
        if isinstance(item, list):
            for value_item in item:
                walk(value_item)
            return
        if not isinstance(item, dict):
            return

        direct_name = (
            _extract_text(item.get("name"))
            or _extract_text(item.get("value"))
        )
        if direct_name is not None:
            append(direct_name)

        address = item.get("address")
        if address is not None:
            for address_text in _extract_address_texts(address):
                append(address_text)
            return

        for address_text in _extract_address_texts(item):
            append(address_text)

    walk(value)
    return tuple(collected)


def _extract_address_texts(value: object) -> tuple[str, ...]:
    if isinstance(value, list):
        results: list[str] = []
        for item in value:
            results.extend(_extract_address_texts(item))
        return tuple(dict.fromkeys(results))
    if isinstance(value, str):
        normalized = _normalize_text(value)
        return () if normalized is None else (normalized,)
    if not isinstance(value, dict):
        return ()

    locality = _extract_text(value.get("addressLocality"))
    region = _extract_text(value.get("addressRegion"))
    country = _extract_text(value.get("addressCountry"))
    if country is None and isinstance(value.get("addressCountry"), dict):
        country = (
            _extract_text(value["addressCountry"].get("name"))
            or _extract_text(value["addressCountry"].get("value"))
        )
    street = _extract_text(value.get("streetAddress"))
    postal_code = _extract_text(value.get("postalCode"))

    parts = [
        part
        for part in (locality, region, country)
        if part is not None
    ]
    if parts:
        return (", ".join(parts),)

    fallback_parts = [
        part
        for part in (street, postal_code)
        if part is not None
    ]
    if fallback_parts:
        return (", ".join(fallback_parts),)
    return ()


def _extract_text(value: object) -> str | None:
    if isinstance(value, str):
        return _normalize_text(value)
    if isinstance(value, list):
        for item in value:
            text = _extract_text(item)
            if text is not None:
                return text
        return None
    return None


def _absolute_url(value: str | None, *, base_url: str) -> str | None:
    normalized = _normalize_text(value)
    if normalized is None:
        return None
    if normalized.startswith(("mailto:", "tel:", "javascript:", "#")):
        return None
    return urljoin(base_url, normalized)


def _registry_source_id_for_apply_url(
    connection,
    *,
    apply_url: str | None,
    portal_type: str | None,
) -> int | None:
    if apply_url is None:
        return None
    normalized_portal_type = _normalize_text(portal_type)
    if normalized_portal_type not in {"greenhouse", "lever", "ashby"}:
        return None

    board_root_url = extract_portal_board_root_url(
        apply_url,
        portal_type=normalized_portal_type,
    )
    if board_root_url is None:
        return None
    normalized_url, _ = normalize_registry_source_url(
        board_root_url,
        portal_type=normalized_portal_type,
    )
    entry = get_registry_source_by_normalized_url_from_connection(
        connection,
        normalized_url=normalized_url,
    )
    if entry is None:
        return None
    return entry.source_id


def _resolve_batch_id(batch_id: str | None, *, created_at: datetime) -> str:
    normalized_batch_id = normalize_batch_id(batch_id)
    if normalized_batch_id is not None:
        return normalized_batch_id
    return f"jobposting-{created_at.strftime('%Y%m%dT%H%M%S%fZ')}"


def _normalize_created_at(created_at: datetime | None) -> datetime:
    if created_at is None:
        return datetime.now(timezone.utc)
    if created_at.tzinfo is None:
        return created_at.replace(tzinfo=timezone.utc)
    return created_at.astimezone(timezone.utc)


def _format_created_at(created_at: datetime) -> str:
    return created_at.isoformat(timespec="seconds").replace("+00:00", "Z")


def _normalize_text(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = " ".join(value.strip().split())
    return normalized or None
