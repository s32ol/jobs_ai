from __future__ import annotations

from dataclasses import dataclass
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

SUPPORTED_PORTAL_TYPES = ("greenhouse", "lever", "ashby", "workday")

_PORTAL_LABELS = {
    "greenhouse": "Greenhouse",
    "lever": "Lever",
    "ashby": "Ashby",
    "workday": "Workday",
}

_PORTAL_HINTS = {
    "greenhouse": (
        "Prefer company-scoped Greenhouse links that end in /jobs/<job_id> when available.",
        "Keep resume upload and basic profile answers ready on the hosted application page.",
    ),
    "lever": (
        "Lever links are usually already company-scoped, so removing tracking-only query params is safe.",
        "Expect resume upload plus work authorization, location, or LinkedIn profile prompts.",
    ),
    "ashby": (
        "Ashby links may hide the job id in a jobId query param that can be promoted into a direct company job link.",
        "Expect a hosted multi-step application flow after the job page opens.",
    ),
    "workday": (
        "Keep the tenant hostname and requisition id intact because Workday links are company-tenant specific.",
        "Expect resume parsing, account prompts, or repeated profile questions even without automation.",
    ),
}

_TRACKING_QUERY_KEYS = {
    "greenhouse": frozenset({"gh_src"}),
    "lever": frozenset({"lever-source", "lever-via"}),
    "ashby": frozenset({"ashby_source"}),
    "workday": frozenset({"source", "sourceid", "source_id"}),
}


@dataclass(frozen=True, slots=True)
class PortalSupport:
    portal_type: str
    portal_label: str
    original_apply_url: str
    normalized_apply_url: str
    company_apply_url: str | None
    hints: tuple[str, ...]


def detect_portal_type(
    apply_url: str | None,
    *,
    portal_type: str | None = None,
) -> str | None:
    explicit_portal_type = _normalize_portal_type(portal_type)
    if explicit_portal_type is not None:
        return explicit_portal_type

    parsed_url = _parse_apply_url(apply_url)
    if parsed_url is None:
        return None

    if _is_greenhouse_apply_url(parsed_url):
        return "greenhouse"
    if _is_lever_apply_url(parsed_url):
        return "lever"
    if _is_ashby_apply_url(parsed_url):
        return "ashby"
    netloc = parsed_url.netloc.lower()
    path = parsed_url.path.lower()
    if "myworkdayjobs.com" in netloc:
        return "workday"
    if "workday" in netloc and ("/job/" in path or "/recruiting/" in path):
        return "workday"
    return None


def build_portal_support(
    apply_url: str | None,
    *,
    portal_type: str | None = None,
) -> PortalSupport | None:
    normalized_text = _normalize_apply_url_text(apply_url)
    if normalized_text is None:
        return None

    detected_portal_type = detect_portal_type(
        normalized_text,
        portal_type=portal_type,
    )
    if detected_portal_type is None:
        return None

    parsed_url = _parse_apply_url(normalized_text)
    normalized_apply_url = normalized_text
    company_apply_url = None
    if parsed_url is not None:
        normalized_apply_url = _normalize_portal_apply_url(detected_portal_type, parsed_url)
        company_apply_url = _extract_company_apply_url(
            detected_portal_type,
            urlparse(normalized_apply_url),
        )

    return PortalSupport(
        portal_type=detected_portal_type,
        portal_label=_PORTAL_LABELS[detected_portal_type],
        original_apply_url=normalized_text,
        normalized_apply_url=normalized_apply_url,
        company_apply_url=company_apply_url,
        hints=_PORTAL_HINTS[detected_portal_type],
    )


def _normalize_portal_type(value: str | None) -> str | None:
    if value is None:
        return None
    normalized_value = value.strip().lower()
    if normalized_value in SUPPORTED_PORTAL_TYPES:
        return normalized_value
    return None


def _normalize_apply_url_text(value: str | None) -> str | None:
    if value is None:
        return None
    normalized_value = value.strip()
    return normalized_value or None


def _parse_apply_url(value: str | None):
    normalized_value = _normalize_apply_url_text(value)
    if normalized_value is None:
        return None

    parsed_url = urlparse(normalized_value)
    if not parsed_url.scheme or not parsed_url.netloc:
        return None
    return parsed_url


def _normalize_portal_apply_url(portal_type: str, parsed_url) -> str:
    query_pairs = parse_qsl(parsed_url.query, keep_blank_values=True)
    normalized_pairs = [
        (key, value)
        for key, value in query_pairs
        if not _should_drop_query_param(portal_type, key, parsed_url.path)
    ]
    return urlunparse(
        parsed_url._replace(
            query=urlencode(normalized_pairs, doseq=True),
            fragment="",
        )
    )


def _should_drop_query_param(portal_type: str, key: str, path: str) -> bool:
    normalized_key = key.lower()
    if normalized_key.startswith("utm_"):
        return True
    if normalized_key in _TRACKING_QUERY_KEYS[portal_type]:
        return True
    if portal_type == "greenhouse" and normalized_key == "gh_jid" and _greenhouse_job_path(path):
        return True
    if portal_type == "ashby" and normalized_key == "jobid" and _ashby_job_path(path):
        return True
    return False


def _extract_company_apply_url(portal_type: str, parsed_url) -> str | None:
    if portal_type == "greenhouse":
        return _extract_greenhouse_company_apply_url(parsed_url)
    if portal_type == "ashby":
        return _extract_ashby_company_apply_url(parsed_url)
    return None


def _extract_greenhouse_company_apply_url(parsed_url) -> str | None:
    segments = _path_segments(parsed_url.path)
    if not segments or _greenhouse_job_path(parsed_url.path):
        return None

    gh_job_id = _first_query_value(parsed_url.query, "gh_jid")
    if gh_job_id is None:
        return None

    return urlunparse(
        parsed_url._replace(
            path=f"/{segments[0]}/jobs/{gh_job_id}",
            query="",
            fragment="",
        )
    )


def _extract_ashby_company_apply_url(parsed_url) -> str | None:
    segments = _path_segments(parsed_url.path)
    if not segments or _ashby_job_path(parsed_url.path):
        return None

    job_id = _first_query_value(parsed_url.query, "jobId")
    if job_id is None:
        job_id = _first_query_value(parsed_url.query, "jobid")
    if job_id is None:
        return None

    return urlunparse(
        parsed_url._replace(
            path=f"/{segments[0]}/{job_id}",
            query="",
            fragment="",
        )
    )


def _first_query_value(query: str, key: str) -> str | None:
    normalized_key = key.lower()
    for query_key, query_value in parse_qsl(query, keep_blank_values=True):
        if query_key.lower() != normalized_key:
            continue
        if query_value:
            return query_value
    return None


def _path_segments(path: str) -> tuple[str, ...]:
    return tuple(segment for segment in path.split("/") if segment)


def _is_greenhouse_apply_url(parsed_url) -> bool:
    netloc = parsed_url.netloc.lower()
    if _has_query_key(parsed_url.query, "gh_jid"):
        return True
    if netloc not in {"boards.greenhouse.io", "job-boards.greenhouse.io"}:
        return False
    return len(_path_segments(parsed_url.path)) >= 1


def _is_lever_apply_url(parsed_url) -> bool:
    netloc = parsed_url.netloc.lower()
    if not (netloc.startswith("jobs.") and netloc.endswith("lever.co")):
        return False
    if _has_query_key(parsed_url.query, "lever-source") or _has_query_key(parsed_url.query, "lever-via"):
        return True
    return len(_path_segments(parsed_url.path)) >= 1


def _is_ashby_apply_url(parsed_url) -> bool:
    netloc = parsed_url.netloc.lower()
    if not (netloc.startswith("jobs.") and netloc.endswith("ashbyhq.com")):
        return False
    if _has_query_key(parsed_url.query, "jobId") or _has_query_key(parsed_url.query, "jobid"):
        return True
    return len(_path_segments(parsed_url.path)) >= 1


def _has_query_key(query: str, key: str) -> bool:
    normalized_key = key.lower()
    return any(query_key.lower() == normalized_key for query_key, _ in parse_qsl(query, keep_blank_values=True))


def _greenhouse_job_path(path: str) -> bool:
    segments = _path_segments(path.lower())
    return len(segments) >= 2 and segments[1] == "jobs"


def _ashby_job_path(path: str) -> bool:
    return len(_path_segments(path)) >= 2
