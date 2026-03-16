from __future__ import annotations

from collections import OrderedDict
from html.parser import HTMLParser
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse

from ..collect.fetch import FetchRequest, FetchResponse, Fetcher
from .models import SearchExecutionResult, SearchHit, SearchPlan

SEARCH_ENDPOINT = "https://html.duckduckgo.com/html/"
SEARCH_SITE_FILTERS: tuple[tuple[str, str], ...] = (
    ("greenhouse", "boards.greenhouse.io"),
    ("greenhouse", "job-boards.greenhouse.io"),
    ("lever", "jobs.lever.co"),
    ("ashby", "jobs.ashbyhq.com"),
    ("workday", "myworkdayjobs.com"),
    ("workday", "workday.com"),
)


class _AnchorExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self._href: str | None = None
        self._text_parts: list[str] = []
        self.links: list[tuple[str, str]] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() != "a":
            return
        attr_map = {key.lower(): value for key, value in attrs}
        href = attr_map.get("href")
        if href is None:
            return
        self._href = href
        self._text_parts = []

    def handle_data(self, data: str) -> None:
        if self._href is None:
            return
        self._text_parts.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() != "a" or self._href is None:
            return
        title = " ".join(part.strip() for part in self._text_parts if part.strip()).strip()
        self.links.append((self._href, title))
        self._href = None
        self._text_parts = []


def build_search_plans(query: str) -> tuple[SearchPlan, ...]:
    normalized_query = query.strip()
    if not normalized_query:
        return ()

    plans: list[SearchPlan] = []
    for portal_type, site_filter in SEARCH_SITE_FILTERS:
        search_text = f"{normalized_query} site:{site_filter}"
        search_url = f"{SEARCH_ENDPOINT}?{urlencode({'q': search_text})}"
        plans.append(
            SearchPlan(
                portal_type=portal_type,
                site_filter=site_filter,
                search_text=search_text,
                search_url=search_url,
            )
        )
    return tuple(plans)


def execute_search_plan(
    plan: SearchPlan,
    *,
    timeout_seconds: float,
    fetcher: Fetcher,
) -> tuple[SearchExecutionResult, tuple[SearchHit, ...]]:
    response = fetcher(
        FetchRequest(
            url=plan.search_url,
            timeout_seconds=timeout_seconds,
            headers={"Accept": "text/html"},
        )
    )
    hits = extract_search_hits(
        response,
        search_text=plan.search_text,
        search_url=plan.search_url,
    )
    return SearchExecutionResult(plan=plan, hit_count=len(hits)), hits


def extract_search_hits(
    response: FetchResponse,
    *,
    search_text: str,
    search_url: str,
) -> tuple[SearchHit, ...]:
    parser = _AnchorExtractor()
    parser.feed(response.text)

    search_host = urlparse(search_url).netloc.lower()
    hits_by_target: OrderedDict[str, SearchHit] = OrderedDict()
    for href, title in parser.links:
        target_url = decode_search_target_url(href, search_url=search_url)
        if target_url is None:
            continue
        target_host = urlparse(target_url).netloc.lower()
        if target_host == search_host or target_host.endswith(".duckduckgo.com") or target_host == "duckduckgo.com":
            continue
        hits_by_target.setdefault(
            target_url,
            SearchHit(
                search_text=search_text,
                search_url=search_url,
                target_url=target_url,
                title=title or None,
            ),
        )
    return tuple(hits_by_target.values())


def decode_search_target_url(value: str, *, search_url: str) -> str | None:
    absolute_url = urljoin(search_url, value)
    parsed = urlparse(absolute_url)
    if parsed.scheme.lower() not in {"http", "https"} or not parsed.netloc:
        return None

    query_map = dict(parse_qsl(parsed.query, keep_blank_values=True))
    redirect_target = query_map.get("uddg") or query_map.get("rut")
    if redirect_target:
        decoded = urlparse(redirect_target)
        if decoded.scheme.lower() in {"http", "https"} and decoded.netloc:
            return decoded._replace(fragment="").geturl()

    return parsed._replace(fragment="").geturl()
