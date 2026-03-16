from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
import re
from urllib.parse import urlparse, urlunparse

from ..portal_support import build_portal_support

_BATCH_ID_RE = re.compile(r"[^A-Za-z0-9._-]+")
_KEY_TEXT_RE = re.compile(r"[\s,]+")


@dataclass(frozen=True, slots=True)
class JobIdentity:
    canonical_apply_url: str | None
    identity_key: str


def build_job_identity(job_record: Mapping[str, object]) -> JobIdentity:
    portal_type = _normalized_portal_type(_field_value(job_record, "portal_type"))
    apply_url = _normalized_text(_field_value(job_record, "apply_url"))
    canonical_apply_url = canonicalize_apply_url(
        apply_url,
        portal_type=portal_type,
    )
    apply_host = _apply_host(canonical_apply_url or apply_url)
    source_job_id = _normalized_key_text(_field_value(job_record, "source_job_id"))
    company = _normalized_key_text(_field_value(job_record, "company")) or "<missing-company>"
    title = _normalized_key_text(_field_value(job_record, "title")) or "<missing-title>"
    location = _normalized_key_text(_field_value(job_record, "location")) or "<missing-location>"
    source = _normalized_key_text(_field_value(job_record, "source")) or "<missing-source>"
    portal_or_host = portal_type or apply_host

    if source_job_id is not None:
        anchor = portal_or_host or source
        identity_key = f"{anchor}|job_id|{source_job_id}"
    elif portal_or_host is not None:
        identity_key = f"{portal_or_host}|{company}|{title}|{location}"
    else:
        identity_key = f"{source}|{company}|{title}|{location}"

    return JobIdentity(
        canonical_apply_url=canonical_apply_url,
        identity_key=identity_key,
    )


def canonicalize_apply_url(
    apply_url: str | None,
    *,
    portal_type: str | None = None,
) -> str | None:
    normalized_apply_url = _normalized_text(apply_url)
    if normalized_apply_url is None:
        return None

    portal_support = build_portal_support(
        normalized_apply_url,
        portal_type=portal_type,
    )
    if portal_support is not None:
        if portal_support.company_apply_url is not None:
            return portal_support.company_apply_url
        return portal_support.normalized_apply_url

    parsed_url = urlparse(normalized_apply_url)
    if not parsed_url.scheme or not parsed_url.netloc:
        return normalized_apply_url

    normalized_path = parsed_url.path or ""
    if normalized_path != "/" and normalized_path.endswith("/"):
        normalized_path = normalized_path.rstrip("/")

    return urlunparse(
        parsed_url._replace(
            scheme=parsed_url.scheme.lower(),
            netloc=parsed_url.netloc.lower(),
            path=normalized_path,
            fragment="",
        )
    )


def normalize_batch_id(value: str | None) -> str | None:
    normalized_value = _normalized_text(value)
    if normalized_value is None:
        return None
    slug = _BATCH_ID_RE.sub("-", normalized_value).strip("-.")
    if not slug:
        raise ValueError("batch id must contain at least one letter or number")
    return slug


def normalize_optional_metadata(value: object) -> str | None:
    return _normalized_text(value)


def _normalized_portal_type(value: object) -> str | None:
    normalized_value = _normalized_text(value)
    if normalized_value is None:
        return None
    return normalized_value.lower()


def _normalized_text(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    text = value.strip()
    return text or None


def _normalized_key_text(value: object) -> str | None:
    normalized_value = _normalized_text(value)
    if normalized_value is None:
        return None
    return _KEY_TEXT_RE.sub(" ", normalized_value.casefold()).strip()


def _apply_host(apply_url: str | None) -> str | None:
    if apply_url is None:
        return None
    parsed_url = urlparse(apply_url)
    if not parsed_url.netloc:
        return None
    return parsed_url.netloc.lower()


def _field_value(job_record: Mapping[str, object], field_name: str) -> object:
    try:
        return job_record[field_name]
    except KeyError:
        return None
