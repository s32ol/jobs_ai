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
    if _is_workday_apply_url(parsed_url):
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
        parsed_url = urlparse(normalized_apply_url)

    return PortalSupport(
        portal_type=detected_portal_type,
        portal_label=_PORTAL_LABELS[detected_portal_type],
        original_apply_url=normalized_text,
        normalized_apply_url=normalized_apply_url,
        company_apply_url=company_apply_url,
        hints=_build_portal_hints(
            detected_portal_type,
            parsed_url,
        ),
    )


def extract_portal_board_root_url(
    apply_url: str | None,
    *,
    portal_type: str | None = None,
) -> str | None:
    portal_support = build_portal_support(apply_url, portal_type=portal_type)
    if portal_support is None or portal_support.portal_type == "workday":
        return None

    parsed_url = _parse_apply_url(portal_support.normalized_apply_url)
    if parsed_url is None:
        return None

    segments = _path_segments(parsed_url.path)
    if not segments:
        return None

    if portal_support.portal_type == "greenhouse":
        if parsed_url.netloc.lower() not in {"boards.greenhouse.io", "job-boards.greenhouse.io"}:
            return None
        board_path = f"/{segments[0]}"
    elif portal_support.portal_type == "lever":
        if parsed_url.netloc.lower() != "jobs.lever.co":
            return None
        board_path = f"/{segments[0]}"
    elif portal_support.portal_type == "ashby":
        if parsed_url.netloc.lower() != "jobs.ashbyhq.com":
            return None
        board_path = f"/{segments[0]}"
    else:
        return None

    return urlunparse(
        parsed_url._replace(
            path=board_path,
            query="",
            fragment="",
        )
    )


def normalize_workday_url(apply_url: str | None) -> str | None:
    parsed_url = _parse_apply_url(apply_url)
    if parsed_url is None or not _is_workday_apply_url(parsed_url):
        return None

    query_pairs = parse_qsl(parsed_url.query, keep_blank_values=True)
    normalized_pairs = [
        (key, value)
        for key, value in query_pairs
        if not _should_drop_query_param("workday", key, parsed_url.path)
    ]
    return urlunparse(
        parsed_url._replace(
            path=_normalize_workday_path(parsed_url.path),
            query=urlencode(normalized_pairs, doseq=True),
            fragment="",
        )
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
    normalized_url = urlunparse(
        parsed_url._replace(
            query=urlencode(normalized_pairs, doseq=True),
            fragment="",
        )
    )
    if portal_type == "workday":
        return normalize_workday_url(normalized_url) or normalized_url
    return normalized_url


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


def _is_workday_apply_url(parsed_url) -> bool:
    hostname = (parsed_url.hostname or "").lower()
    if not _is_workday_hostname(hostname):
        return False
    return _workday_path_has_job_marker(parsed_url.path)


def _is_workday_hostname(hostname: str) -> bool:
    return (
        hostname == "myworkdayjobs.com"
        or hostname.endswith(".myworkdayjobs.com")
        or hostname == "myworkdaysite.com"
        or hostname.endswith(".myworkdaysite.com")
        or hostname == "workday.com"
        or hostname.endswith(".workday.com")
    )


def _workday_path_has_job_marker(path: str) -> bool:
    normalized_path = path.lower()
    return "/job/" in normalized_path or normalized_path.endswith("/job") or "/recruiting/" in normalized_path


def _has_query_key(query: str, key: str) -> bool:
    normalized_key = key.lower()
    return any(query_key.lower() == normalized_key for query_key, _ in parse_qsl(query, keep_blank_values=True))


def _greenhouse_job_path(path: str) -> bool:
    segments = _path_segments(path.lower())
    return len(segments) >= 2 and segments[1] == "jobs"


def _ashby_job_path(path: str) -> bool:
    return len(_path_segments(path)) >= 2


def _normalize_workday_path(path: str) -> str:
    segments = list(_path_segments(path))
    if segments and segments[-1].lower() == "apply":
        segments.pop()
    if not segments:
        return "/"
    return f"/{'/'.join(segments)}"


def _build_portal_hints(portal_type: str, parsed_url) -> tuple[str, ...]:
    hints = list(_PORTAL_HINTS[portal_type])
    if portal_type != "workday" or parsed_url is None:
        return tuple(hints)

    tenant_hint = _extract_workday_tenant_hint(parsed_url)
    if tenant_hint is not None:
        hints.append(f"Workday tenant hint: {tenant_hint}.")

    site_hint = _extract_workday_site_hint(parsed_url)
    if site_hint is not None:
        hints.append(f"Workday site hint: {site_hint}.")

    requisition_hint = _extract_workday_requisition_hint(parsed_url)
    if requisition_hint is not None:
        hints.append(f"Workday requisition hint: {requisition_hint}.")

    return tuple(hints)


def _extract_workday_tenant_hint(parsed_url) -> str | None:
    hostname = (parsed_url.hostname or "").lower()
    if hostname.endswith(".myworkdayjobs.com") or hostname.endswith(".myworkdaysite.com"):
        tenant = hostname.split(".", 1)[0]
        return tenant or None
    segments = _workday_non_locale_segments(parsed_url.path)
    if "recruiting" in segments:
        recruiting_index = segments.index("recruiting")
        if recruiting_index + 1 < len(segments):
            return segments[recruiting_index + 1]
    return None


def _extract_workday_site_hint(parsed_url) -> str | None:
    segments = _workday_non_locale_segments(parsed_url.path)
    if not segments:
        return None
    if "job" in segments:
        job_index = segments.index("job")
        if job_index > 0:
            return segments[job_index - 1]
    if "recruiting" in segments:
        recruiting_index = segments.index("recruiting")
        if recruiting_index + 2 < len(segments):
            return segments[recruiting_index + 2]
    return None


def _extract_workday_requisition_hint(parsed_url) -> str | None:
    segments = _workday_non_locale_segments(parsed_url.path)
    if not segments:
        return None
    final_segment = segments[-1]
    if "_" in final_segment:
        requisition = final_segment.rsplit("_", 1)[-1]
        return requisition or None
    if final_segment.lower() != "job":
        return final_segment or None
    return None


def _workday_non_locale_segments(path: str) -> list[str]:
    segments = list(_path_segments(path))
    if segments and len(segments[0]) == 5 and segments[0][2] == "-":
        segments = segments[1:]
    return segments
