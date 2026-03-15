from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

DEFAULT_FETCH_USER_AGENT = "jobs_ai/0.1.0"


@dataclass(frozen=True, slots=True)
class FetchRequest:
    url: str
    timeout_seconds: float
    headers: Mapping[str, str] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class FetchResponse:
    url: str
    final_url: str
    status_code: int
    content_type: str | None
    text: str


class FetchError(RuntimeError):
    """Raised when a collection fetch fails."""


Fetcher = Callable[[FetchRequest], FetchResponse]


def fetch_text(request: FetchRequest) -> FetchResponse:
    headers = {
        "Accept": "text/html,application/json",
        "User-Agent": DEFAULT_FETCH_USER_AGENT,
    }
    headers.update(dict(request.headers))
    url_request = Request(request.url, headers=headers)

    try:
        with urlopen(url_request, timeout=request.timeout_seconds) as response:
            charset = response.headers.get_content_charset() or "utf-8"
            content_type = response.headers.get("Content-Type")
            payload = response.read()
            text = payload.decode(charset, errors="replace")
            return FetchResponse(
                url=request.url,
                final_url=response.geturl(),
                status_code=response.getcode() or 200,
                content_type=content_type,
                text=text,
            )
    except HTTPError as exc:
        raise FetchError(f"HTTP {exc.code} while fetching {request.url}") from exc
    except URLError as exc:
        raise FetchError(f"unable to fetch {request.url}: {exc.reason}") from exc
    except TimeoutError as exc:
        raise FetchError(f"timed out after {request.timeout_seconds:.1f}s while fetching {request.url}") from exc
