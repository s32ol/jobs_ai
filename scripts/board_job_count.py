from __future__ import annotations

from collections import Counter
from collections.abc import Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from functools import lru_cache
from html import unescape
from pathlib import Path
from urllib.parse import parse_qs, urljoin, urlparse
import argparse
import re
import sys

import requests

SRC_PATH = Path(__file__).resolve().parents[1] / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

from jobs_ai.collect.adapters import DEFAULT_ADAPTERS
from jobs_ai.collect.fetch import FetchError, FetchRequest, FetchResponse
from jobs_ai.collect.models import SourceInput
from jobs_ai.portal_support import build_portal_support

BOARDS_PATH = Path(__file__).resolve().parents[1] / "boards.txt"
MAX_WORKERS = 10
TIMEOUT_SECONDS = 10
TOP_MATCHING_COMPANIES_LIMIT = 10
WORKDAY_PAGE_SIZE = 20
PORTAL_ORDER = {
    "Greenhouse": 0,
    "Lever": 1,
    "Ashby": 2,
    "Workday": 3,
    "Unknown": 4,
}
TARGET_TITLES = [
    "data engineer",
    "analytics engineer",
    "data platform",
    "data infrastructure",
    "data pipeline",
]
STACK_KEYWORDS = [
    "python",
    "sql",
    "bigquery",
    "gcp",
    "google cloud",
    "analytics",
    "telemetry",
]
_LOCALE_RE = re.compile(r"^[a-z]{2}-[a-z]{2}$", re.IGNORECASE)
_MATCH_TEXT_RE = re.compile(r"[^a-z0-9]+")
_TAG_RE = re.compile(r"<[^>]+>")
_WHITESPACE_RE = re.compile(r"\s+")


@dataclass(frozen=True, slots=True)
class BoardCountResult:
    count: int
    company: str
    resume_match_count: int = 0


@dataclass(frozen=True, slots=True)
class WorkdayPosting:
    title: str
    job_url: str | None
    summary_text: str


def main(argv: Sequence[str] | None = None) -> None:
    args = _parse_args(argv)
    board_urls = _load_board_urls(args.boards_path)
    portal_totals = {
        "Greenhouse": 0,
        "Lever": 0,
        "Ashby": 0,
        "Workday": 0,
    }
    counted_results: list[dict[str, object]] = []
    matching_companies: Counter[str] = Counter()
    failed_count = 0
    resume_match_total = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [
            executor.submit(
                _count_board,
                board_url,
                estimate_resume_matches=args.estimate_resume_matches,
            )
            for board_url in board_urls
        ]
        for future in as_completed(futures):
            result = future.result()
            if result["success"]:
                counted_results.append(result)
                portal_totals[result["portal"]] += result["count"]
                if args.estimate_resume_matches:
                    resume_match_count = int(result["resume_match_count"])
                    resume_match_total += resume_match_count
                    if resume_match_count > 0:
                        matching_companies[str(result["company"])] += resume_match_count
            else:
                failed_count += 1

    counted_results.sort(
        key=lambda item: (
            PORTAL_ORDER.get(item["portal"], PORTAL_ORDER["Unknown"]),
            -int(item["count"]),
            str(item["url"]),
        )
    )
    grand_total = sum(portal_totals.values())

    print(f"Boards configured: {len(board_urls)}")
    print(f"Boards counted: {len(counted_results)}")
    print(f"Boards failed: {failed_count}")
    print()
    print("Per-board counts")
    for result in counted_results:
        print(f'{result["portal"]} | {result["url"]} | {result["count"]}')
    print()
    print("Portal totals")
    print(f'Greenhouse: {portal_totals["Greenhouse"]}')
    print(f'Lever: {portal_totals["Lever"]}')
    print(f'Ashby: {portal_totals["Ashby"]}')
    print(f'Workday: {portal_totals["Workday"]}')
    print()
    print(f"Grand total: {grand_total}")

    if args.estimate_resume_matches:
        print()
        print("Resume match estimate")
        print(f"Estimated matching jobs: {resume_match_total}")
        print(f"Match rate: {_format_match_rate(resume_match_total, grand_total)}")
        print("Top matching companies")
        top_companies = _top_matching_companies(matching_companies)
        if not top_companies:
            print("None")
        else:
            for company, count in top_companies:
                print(f"{company}: {count}")


def _parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Count jobs across ATS boards from boards.txt.",
    )
    parser.add_argument(
        "--boards-path",
        type=Path,
        default=BOARDS_PATH,
        help=f"Path to the board URL list (default: {BOARDS_PATH}).",
    )
    parser.add_argument(
        "--estimate-resume-matches",
        "--resume-match-estimate",
        dest="estimate_resume_matches",
        action="store_true",
        help="Estimate jobs matching the built-in resume profile.",
    )
    return parser.parse_args(argv)


def _load_board_urls(input_path: Path) -> list[str]:
    if not input_path.exists():
        raise SystemExit(f"missing boards file: {input_path}")

    board_urls: list[str] = []
    for line in input_path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if stripped:
            board_urls.append(stripped)
    if not board_urls:
        raise SystemExit(f"no board URLs found in {input_path}")
    return board_urls


def _count_board(
    board_url: str,
    *,
    estimate_resume_matches: bool,
) -> dict[str, object]:
    portal = _detect_portal(board_url)
    try:
        if portal == "Greenhouse":
            metrics = _count_greenhouse(
                board_url,
                estimate_resume_matches=estimate_resume_matches,
            )
        elif portal == "Lever":
            metrics = _count_lever(
                board_url,
                estimate_resume_matches=estimate_resume_matches,
            )
        elif portal == "Ashby":
            metrics = _count_ashby(
                board_url,
                estimate_resume_matches=estimate_resume_matches,
            )
        elif portal == "Workday":
            metrics = _count_workday(
                board_url,
                estimate_resume_matches=estimate_resume_matches,
            )
        else:
            raise ValueError("unsupported portal URL")
    except Exception as exc:
        return {
            "success": False,
            "portal": portal,
            "url": board_url,
            "count": 0,
            "company": "",
            "resume_match_count": 0,
            "error": str(exc),
        }

    return {
        "success": True,
        "portal": portal,
        "url": board_url,
        "count": metrics.count,
        "company": metrics.company,
        "resume_match_count": metrics.resume_match_count,
        "error": "",
    }


def _detect_portal(board_url: str) -> str:
    host = urlparse(board_url).netloc.lower()
    if _is_greenhouse_host(host):
        return "Greenhouse"
    if host == "jobs.lever.co":
        return "Lever"
    if host == "jobs.ashbyhq.com":
        return "Ashby"
    if "myworkdayjobs.com" in host or "myworkdaysite.com" in host:
        return "Workday"
    return "Unknown"


def _count_greenhouse(
    board_url: str,
    *,
    estimate_resume_matches: bool,
) -> BoardCountResult:
    company_slug = _greenhouse_company_slug(board_url)
    if not company_slug:
        raise ValueError("unable to determine Greenhouse company slug")

    response = requests.get(
        f"https://boards-api.greenhouse.io/v1/boards/{company_slug}/jobs",
        timeout=TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    payload = response.json()
    jobs = payload.get("jobs")
    if not isinstance(jobs, list):
        raise ValueError("Greenhouse response did not include a jobs list")

    company = _company_name_from_slug(company_slug)
    resume_match_count = 0
    if estimate_resume_matches:
        company, resume_match_count = _safe_supported_resume_match_estimate(
            board_url,
            portal_type="greenhouse",
            fallback_company=company,
        )
    return BoardCountResult(count=len(jobs), company=company, resume_match_count=resume_match_count)


def _count_lever(
    board_url: str,
    *,
    estimate_resume_matches: bool,
) -> BoardCountResult:
    path_segments = [segment for segment in urlparse(board_url).path.split("/") if segment]
    if not path_segments:
        raise ValueError("unable to determine Lever company slug")

    response = requests.get(
        f"https://api.lever.co/v0/postings/{path_segments[0]}?mode=json",
        timeout=TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, list):
        raise ValueError("Lever response did not return a postings list")

    company = _company_name_from_slug(path_segments[0])
    resume_match_count = 0
    if estimate_resume_matches:
        company, resume_match_count = _safe_supported_resume_match_estimate(
            board_url,
            portal_type="lever",
            fallback_company=company,
        )
    return BoardCountResult(count=len(payload), company=company, resume_match_count=resume_match_count)


def _count_ashby(
    board_url: str,
    *,
    estimate_resume_matches: bool,
) -> BoardCountResult:
    path_segments = [segment for segment in urlparse(board_url).path.split("/") if segment]
    if not path_segments:
        raise ValueError("unable to determine Ashby company slug")

    query = (
        "query ApiJobBoardWithTeams($organizationHostedJobsPageName: String!) { "
        "jobBoard: jobBoardWithTeams("
        "organizationHostedJobsPageName: $organizationHostedJobsPageName"
        ") { jobPostings { id } } }"
    )
    payload = {
        "operationName": "ApiJobBoardWithTeams",
        "variables": {"organizationHostedJobsPageName": path_segments[0]},
        "query": query,
    }
    endpoints = (
        "https://jobs.ashbyhq.com/api/non-user-graphql",
        "https://jobs.ashbyhq.com/api/non-user-graphql?op=ApiJobBoardWithTeams",
    )
    last_error: Exception | None = None
    job_count = None
    for endpoint in endpoints:
        try:
            response = requests.post(endpoint, json=payload, timeout=TIMEOUT_SECONDS)
            response.raise_for_status()
            data = response.json()
            job_postings = _find_list_by_key(data, "jobPostings")
            if job_postings is None:
                raise ValueError("Ashby response did not include jobPostings")
            job_count = len(job_postings)
            break
        except Exception as exc:
            last_error = exc

    if job_count is None:
        raise ValueError(f"unable to count Ashby jobs: {last_error}")

    company = _company_name_from_slug(path_segments[0])
    resume_match_count = 0
    if estimate_resume_matches:
        company, resume_match_count = _safe_supported_resume_match_estimate(
            board_url,
            portal_type="ashby",
            fallback_company=company,
        )
    return BoardCountResult(count=job_count, company=company, resume_match_count=resume_match_count)


def _count_workday(
    board_url: str,
    *,
    estimate_resume_matches: bool,
) -> BoardCountResult:
    parsed = urlparse(board_url)
    host = parsed.netloc
    tenant = _workday_tenant(parsed)
    site = _workday_site(parsed)
    if not tenant:
        raise ValueError("unable to determine Workday tenant")

    candidate_urls: list[str] = []
    if site:
        candidate_urls.append(f"{parsed.scheme}://{host}/wday/cxs/{tenant}/{site}/jobs")
    candidate_urls.append(f"{parsed.scheme}://{host}/wday/cxs/{tenant}/jobs")

    payload = {
        "appliedFacets": {},
        "limit": WORKDAY_PAGE_SIZE,
        "offset": 0,
        "searchText": "",
    }
    last_error: Exception | None = None
    count = None
    for endpoint in _dedupe_preserving_order(candidate_urls):
        try:
            response = requests.post(endpoint, json=payload, timeout=TIMEOUT_SECONDS)
            response.raise_for_status()
            data = response.json()
            count = _extract_workday_count(data)
            if count is None:
                raise ValueError("Workday response did not include a recognizable job count")
            break
        except Exception as exc:
            last_error = exc

    if count is None:
        raise ValueError(f"unable to count Workday jobs: {last_error}")

    company = _workday_company_name(board_url)
    resume_match_count = 0
    if estimate_resume_matches:
        company, resume_match_count = _safe_workday_resume_match_estimate(
            board_url,
            fallback_company=company,
        )
    return BoardCountResult(count=count, company=company, resume_match_count=resume_match_count)


def _safe_supported_resume_match_estimate(
    board_url: str,
    *,
    portal_type: str,
    fallback_company: str,
) -> tuple[str, int]:
    try:
        return _estimate_supported_resume_matches(
            board_url,
            portal_type=portal_type,
            fallback_company=fallback_company,
        )
    except Exception:
        return fallback_company, 0


def _estimate_supported_resume_matches(
    board_url: str,
    *,
    portal_type: str,
    fallback_company: str,
) -> tuple[str, int]:
    portal_support = build_portal_support(board_url, portal_type=portal_type)
    if portal_support is None:
        raise ValueError(f"unable to normalize {portal_type} board URL")

    adapter = DEFAULT_ADAPTERS.get(portal_type)
    if adapter is None:
        raise ValueError(f"no adapter registered for {portal_type}")

    source = SourceInput(
        index=1,
        source_url=board_url,
        normalized_url=portal_support.normalized_apply_url,
        portal_type=portal_type,
        portal_support=portal_support,
    )
    result = adapter.collect(
        source,
        timeout_seconds=TIMEOUT_SECONDS,
        fetcher=_requests_fetcher,
    )
    company = next(
        (lead.company for lead in result.collected_leads if lead.company),
        fallback_company,
    )

    resume_match_count = 0
    for lead in result.collected_leads:
        if not _contains_any_term(lead.title, TARGET_TITLES):
            continue
        if not lead.apply_url:
            continue
        description_text = _fetch_job_page_text(lead.apply_url)
        if description_text is None:
            continue
        if _contains_any_term(description_text, STACK_KEYWORDS):
            resume_match_count += 1

    return company, resume_match_count


def _safe_workday_resume_match_estimate(
    board_url: str,
    *,
    fallback_company: str,
) -> tuple[str, int]:
    try:
        return fallback_company, _estimate_workday_resume_matches(board_url)
    except Exception:
        return fallback_company, 0


def _estimate_workday_resume_matches(board_url: str) -> int:
    postings = _fetch_workday_postings(board_url)
    resume_match_count = 0
    for posting in postings:
        if not _contains_any_term(posting.title, TARGET_TITLES):
            continue
        description_text = posting.summary_text
        if posting.job_url is not None:
            fetched_text = _fetch_job_page_text(posting.job_url)
            if fetched_text is not None:
                description_text = fetched_text
        if _contains_any_term(description_text, STACK_KEYWORDS):
            resume_match_count += 1
    return resume_match_count


def _fetch_workday_postings(board_url: str) -> tuple[WorkdayPosting, ...]:
    parsed = urlparse(board_url)
    host = parsed.netloc
    tenant = _workday_tenant(parsed)
    site = _workday_site(parsed)
    if not tenant:
        raise ValueError("unable to determine Workday tenant")

    candidate_urls: list[str] = []
    if site:
        candidate_urls.append(f"{parsed.scheme}://{host}/wday/cxs/{tenant}/{site}/jobs")
    candidate_urls.append(f"{parsed.scheme}://{host}/wday/cxs/{tenant}/jobs")

    last_error: Exception | None = None
    for endpoint in _dedupe_preserving_order(candidate_urls):
        try:
            return _fetch_workday_postings_from_endpoint(board_url, endpoint)
        except Exception as exc:
            last_error = exc

    raise ValueError(f"unable to fetch Workday postings: {last_error}")


def _fetch_workday_postings_from_endpoint(
    board_url: str,
    endpoint: str,
) -> tuple[WorkdayPosting, ...]:
    postings: list[WorkdayPosting] = []
    seen_keys: set[tuple[str, str | None]] = set()
    offset = 0
    total_count: int | None = None

    while True:
        payload = {
            "appliedFacets": {},
            "limit": WORKDAY_PAGE_SIZE,
            "offset": offset,
            "searchText": "",
        }
        response = requests.post(endpoint, json=payload, timeout=TIMEOUT_SECONDS)
        response.raise_for_status()
        data = response.json()
        if total_count is None:
            total_count = _extract_workday_count(data)

        jobs = _extract_workday_jobs(data)
        if jobs is None:
            raise ValueError("Workday response did not include a recognizable jobs list")
        if not jobs:
            break

        new_items = 0
        for job in jobs:
            posting = _workday_posting_from_payload(board_url, job)
            if posting is None:
                continue
            key = (posting.title, posting.job_url)
            if key in seen_keys:
                continue
            seen_keys.add(key)
            postings.append(posting)
            new_items += 1

        if total_count is not None and len(postings) >= total_count:
            break
        if len(jobs) < WORKDAY_PAGE_SIZE or new_items == 0:
            break
        offset += WORKDAY_PAGE_SIZE

    return tuple(postings)


def _workday_posting_from_payload(
    board_url: str,
    payload: object,
) -> WorkdayPosting | None:
    title = _find_first_text_by_keys(
        payload,
        {
            "title",
            "name",
            "jobtitle",
            "jobpostingtitle",
            "postedtitle",
        },
    )
    if title is None:
        return None

    summary_text = " ".join(
        _dedupe_preserving_order(
            [
                *_collect_text_by_keys(
                    payload,
                    {
                        "description",
                        "descriptiontext",
                        "shortdescription",
                        "jobdescription",
                        "summary",
                        "bulletfields",
                        "details",
                    },
                ),
                *_flatten_text_values(payload),
            ]
        )
    )
    return WorkdayPosting(
        title=title,
        job_url=_workday_job_url(board_url, payload),
        summary_text=summary_text,
    )


def _workday_job_url(board_url: str, payload: object) -> str | None:
    candidate = _find_first_text_by_keys(
        payload,
        {
            "externalpath",
            "externalurl",
            "joburl",
            "hostedurl",
            "applyurl",
            "url",
        },
    )
    if candidate is None:
        return None
    if candidate.startswith(("https://", "http://")):
        return candidate

    normalized_candidate = candidate.lstrip("/")
    if not normalized_candidate:
        return None
    if not normalized_candidate.startswith("job/"):
        normalized_candidate = f"job/{normalized_candidate}"

    parsed = urlparse(board_url)
    base_path = parsed.path.rstrip("/")
    if "/job/" in base_path:
        base_path = base_path.split("/job/", 1)[0]
    elif base_path.endswith("/job"):
        base_path = base_path[: -len("/job")]
    base_url = f"{parsed.scheme}://{parsed.netloc}{base_path}/"
    return urljoin(base_url, normalized_candidate)


def _extract_workday_jobs(payload: object) -> list[object] | None:
    for key_name in ("jobPostings", "jobs", "searchResults"):
        jobs = _find_list_by_key(payload, key_name)
        if jobs is None:
            continue
        if any(_find_first_text_by_keys(item, {"title", "name", "jobtitle", "postedtitle"}) for item in jobs):
            return jobs
    return None


def _requests_fetcher(request: FetchRequest) -> FetchResponse:
    headers = {
        "Accept": "text/html,application/json",
        "User-Agent": "jobs_ai/1.0.0",
    }
    headers.update(dict(request.headers))

    try:
        response = requests.get(
            request.url,
            headers=headers,
            timeout=request.timeout_seconds,
        )
        response.raise_for_status()
    except requests.exceptions.Timeout as exc:
        raise FetchError(f"timed out after {request.timeout_seconds:.1f}s while fetching {request.url}") from exc
    except requests.exceptions.HTTPError as exc:
        status_code = exc.response.status_code if exc.response is not None else "unknown"
        raise FetchError(f"HTTP {status_code} while fetching {request.url}") from exc
    except requests.exceptions.RequestException as exc:
        raise FetchError(f"unable to fetch {request.url}: {exc}") from exc

    return FetchResponse(
        url=request.url,
        final_url=str(response.url),
        status_code=response.status_code,
        content_type=response.headers.get("Content-Type"),
        text=response.text,
    )


@lru_cache(maxsize=4096)
def _fetch_job_page_text(job_url: str) -> str | None:
    try:
        response = _requests_fetcher(
            FetchRequest(
                url=job_url,
                timeout_seconds=TIMEOUT_SECONDS,
                headers={"Accept": "text/html"},
            )
        )
    except FetchError:
        return None
    return _searchable_text(response.text)


def _greenhouse_company_slug(board_url: str) -> str | None:
    parsed = urlparse(board_url)
    path_segments = [segment for segment in parsed.path.split("/") if segment]
    if len(path_segments) >= 2 and path_segments[0] == "embed" and path_segments[1] == "job_board":
        return parse_qs(parsed.query).get("for", [None])[0]
    if path_segments:
        return path_segments[0]
    return None


def _is_greenhouse_host(host: str) -> bool:
    if host == "boards.greenhouse.io":
        return True
    if host.endswith(".greenhouse.io"):
        first_label = host.split(".", 1)[0]
        return first_label in {"boards", "job-boards"}
    return False


def _workday_tenant(parsed_url) -> str | None:
    host = parsed_url.hostname or ""
    segments = _workday_non_locale_segments(parsed_url.path)
    if "recruiting" in segments:
        recruiting_index = segments.index("recruiting")
        if recruiting_index + 1 < len(segments):
            return segments[recruiting_index + 1]
    if host.endswith(".myworkdayjobs.com") or host.endswith(".myworkdaysite.com"):
        return host.split(".", 1)[0] or None
    return None


def _workday_site(parsed_url) -> str | None:
    segments = _workday_non_locale_segments(parsed_url.path)
    if not segments:
        return None
    if "recruiting" in segments:
        recruiting_index = segments.index("recruiting")
        if recruiting_index + 2 < len(segments):
            return segments[recruiting_index + 2]
        return None
    return segments[0]


def _workday_company_name(board_url: str) -> str:
    parsed = urlparse(board_url)
    return _company_name_from_slug(_workday_site(parsed) or _workday_tenant(parsed) or parsed.netloc)


def _workday_non_locale_segments(path: str) -> list[str]:
    segments = [segment for segment in path.split("/") if segment]
    if segments and _LOCALE_RE.match(segments[0]):
        segments = segments[1:]
    return segments


def _extract_workday_count(payload: object) -> int | None:
    count = _find_int_by_key(
        payload,
        {
            "total",
            "totalcount",
            "totalelements",
            "jobpostingcount",
            "totaljobs",
            "count",
        },
    )
    if count is not None:
        return count

    job_postings = _find_list_by_key(payload, "jobPostings")
    if job_postings is not None:
        return len(job_postings)

    jobs = _find_list_by_key(payload, "jobs")
    if jobs is not None:
        return len(jobs)

    return None


def _find_int_by_key(payload: object, keys: set[str]) -> int | None:
    if isinstance(payload, dict):
        for key, value in payload.items():
            if key.lower() in keys and isinstance(value, int):
                return value
        for value in payload.values():
            found = _find_int_by_key(value, keys)
            if found is not None:
                return found
    elif isinstance(payload, list):
        for item in payload:
            found = _find_int_by_key(item, keys)
            if found is not None:
                return found
    return None


def _find_list_by_key(payload: object, key_name: str) -> list[object] | None:
    if isinstance(payload, dict):
        for key, value in payload.items():
            if key.lower() == key_name.lower() and isinstance(value, list):
                return value
        for value in payload.values():
            found = _find_list_by_key(value, key_name)
            if found is not None:
                return found
    elif isinstance(payload, list):
        for item in payload:
            found = _find_list_by_key(item, key_name)
            if found is not None:
                return found
    return None


def _find_first_text_by_keys(payload: object, keys: set[str]) -> str | None:
    if isinstance(payload, dict):
        for key, value in payload.items():
            if key.lower() in keys:
                text = _value_to_text(value)
                if text is not None:
                    return text
        for value in payload.values():
            found = _find_first_text_by_keys(value, keys)
            if found is not None:
                return found
    elif isinstance(payload, list):
        for item in payload:
            found = _find_first_text_by_keys(item, keys)
            if found is not None:
                return found
    return None


def _collect_text_by_keys(payload: object, keys: set[str]) -> list[str]:
    collected: list[str] = []
    if isinstance(payload, dict):
        for key, value in payload.items():
            if key.lower() in keys:
                collected.extend(_value_to_text_list(value))
            collected.extend(_collect_text_by_keys(value, keys))
    elif isinstance(payload, list):
        for item in payload:
            collected.extend(_collect_text_by_keys(item, keys))
    return collected


def _flatten_text_values(payload: object) -> list[str]:
    collected: list[str] = []
    if isinstance(payload, dict):
        for value in payload.values():
            collected.extend(_flatten_text_values(value))
    elif isinstance(payload, list):
        for item in payload:
            collected.extend(_flatten_text_values(item))
    else:
        text = _value_to_text(payload)
        if text is not None and len(text) >= 3:
            collected.append(text)
    return collected[:50]


def _value_to_text(value: object) -> str | None:
    if isinstance(value, str):
        normalized = _normalize_whitespace(unescape(value))
        return normalized or None
    if isinstance(value, (int, float)):
        return str(value)
    return None


def _value_to_text_list(value: object) -> list[str]:
    if isinstance(value, list):
        items: list[str] = []
        for item in value:
            items.extend(_value_to_text_list(item))
        return items
    text = _value_to_text(value)
    return [text] if text is not None else []


def _searchable_text(text: str) -> str:
    return _normalize_whitespace(_TAG_RE.sub(" ", unescape(text)))


def _contains_any_term(text: str, terms: Sequence[str]) -> bool:
    normalized_text = f" {_normalize_match_text(text)} "
    for term in terms:
        normalized_term = _normalize_match_text(term)
        if normalized_term and f" {normalized_term} " in normalized_text:
            return True
    return False


def _normalize_match_text(text: str) -> str:
    return _MATCH_TEXT_RE.sub(" ", unescape(text).lower()).strip()


def _normalize_whitespace(text: str) -> str:
    return _WHITESPACE_RE.sub(" ", text).strip()


def _company_name_from_slug(slug: str) -> str:
    normalized = re.sub(r"[-_]+", " ", slug).strip()
    if not normalized:
        return slug
    return " ".join(part.capitalize() for part in normalized.split())


def _format_match_rate(match_count: int, total_count: int) -> str:
    if total_count <= 0:
        return "0.0%"
    return f"{(match_count / total_count) * 100:.1f}%"


def _top_matching_companies(
    matching_companies: Counter[str],
) -> list[tuple[str, int]]:
    return sorted(
        matching_companies.items(),
        key=lambda item: (-item[1], item[0].lower()),
    )[:TOP_MATCHING_COMPANIES_LIMIT]


def _dedupe_preserving_order(values: Sequence[str]) -> list[str]:
    seen: set[str] = set()
    deduped: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        deduped.append(value)
    return deduped


if __name__ == "__main__":
    main()
