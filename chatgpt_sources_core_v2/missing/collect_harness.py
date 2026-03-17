from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from urllib.parse import urlparse

from ..portal_support import build_portal_support
from .adapters import DEFAULT_ADAPTERS, GENERIC_ADAPTER, select_adapter
from .adapters.base import build_skipped_result
from .fetch import Fetcher, fetch_text
from .models import CollectRun, CollectRunReport, ManualReviewItem, OutcomeEvidence, SourceInput, SourceResult


def run_collection(
    source_values: Sequence[str],
    *,
    timeout_seconds: float,
    label: str | None = None,
    report_only: bool = False,
    created_at: datetime | None = None,
    adapter_registry: Mapping[str, object] | None = None,
    generic_adapter: object | None = None,
    fetcher: Fetcher = fetch_text,
) -> CollectRun:
    created_at_dt = _normalize_created_at(created_at)
    source_results: list[SourceResult] = []
    collected_leads = []
    manual_review_items: list[ManualReviewItem] = []
    seen_urls: set[str] = set()

    for index, raw_value in enumerate(source_values, start=1):
        source, validation_problem = _prepare_source_input(index, raw_value)
        if validation_problem is not None:
            reason_code, reason = validation_problem
            source_results.append(
                build_skipped_result(
                    source,
                    adapter_key="harness",
                    reason_code=reason_code,
                    reason=reason,
                )
            )
            continue

        assert source.normalized_url is not None
        if source.normalized_url in seen_urls:
            source_results.append(
                build_skipped_result(
                    source,
                    adapter_key="harness",
                    reason_code="duplicate_normalized_source",
                    reason=f"duplicate source skipped after normalization: {source.normalized_url}",
                )
            )
            continue
        seen_urls.add(source.normalized_url)

        adapter = select_adapter(
            source,
            registry=DEFAULT_ADAPTERS if adapter_registry is None else adapter_registry,
            generic_adapter=GENERIC_ADAPTER if generic_adapter is None else generic_adapter,
        )
        result = _collect_source_result(
            adapter,
            source,
            timeout_seconds=timeout_seconds,
            fetcher=fetcher,
        )
        source_results.append(result)
        collected_leads.extend(result.collected_leads)
        if result.manual_review_item is not None:
            manual_review_items.append(result.manual_review_item)

    report = CollectRunReport(
        created_at=_format_created_at(created_at_dt),
        finished_at=None,
        run_id=None,
        label=label,
        timeout_seconds=timeout_seconds,
        report_only=report_only,
        input_sources=tuple(source_values),
        input_source_count=len(source_values),
        collected_count=len(collected_leads),
        manual_review_count=len(manual_review_items),
        skipped_count=sum(1 for result in source_results if result.outcome == "skipped"),
        source_results=tuple(source_results),
    )
    return CollectRun(
        report=report,
        collected_leads=tuple(collected_leads),
        manual_review_items=tuple(manual_review_items),
    )


def _prepare_source_input(index: int, raw_value: str) -> tuple[SourceInput, tuple[str, str] | None]:
    source_url = raw_value.strip()
    if not source_url:
        source = SourceInput(
            index=index,
            source_url=raw_value,
            normalized_url=None,
            portal_type=None,
            portal_support=None,
        )
        return source, ("blank_source_url", "blank source URL")

    parsed_url = urlparse(source_url)
    if parsed_url.scheme.lower() not in {"http", "https"}:
        source = SourceInput(
            index=index,
            source_url=source_url,
            normalized_url=None,
            portal_type=None,
            portal_support=None,
        )
        return source, ("unsupported_url_scheme", f"unsupported URL scheme: {parsed_url.scheme or '<missing>'}")
    if not parsed_url.netloc:
        source = SourceInput(
            index=index,
            source_url=source_url,
            normalized_url=None,
            portal_type=None,
            portal_support=None,
        )
        return source, ("missing_network_host", "source URL is missing a network host")

    portal_support = build_portal_support(source_url)
    normalized_url = (
        portal_support.company_apply_url
        if portal_support is not None and portal_support.company_apply_url is not None
        else portal_support.normalized_apply_url
        if portal_support is not None
        else parsed_url._replace(fragment="").geturl()
    )
    source = SourceInput(
        index=index,
        source_url=source_url,
        normalized_url=normalized_url,
        portal_type=portal_support.portal_type if portal_support is not None else None,
        portal_support=portal_support,
    )
    return source, None


def _collect_source_result(
    adapter: object,
    source: SourceInput,
    *,
    timeout_seconds: float,
    fetcher: Fetcher,
) -> SourceResult:
    adapter_key = getattr(adapter, "adapter_key", "unknown")
    try:
        return adapter.collect(
            source,
            timeout_seconds=timeout_seconds,
            fetcher=fetcher,
        )
    except Exception as exc:
        reason = f"{adapter_key} adapter failed unexpectedly: {exc.__class__.__name__}: {exc}"
        return build_skipped_result(
            source,
            adapter_key=adapter_key,
            reason_code="adapter_failed",
            reason=reason,
            evidence=OutcomeEvidence(error=reason),
        )


def _normalize_created_at(created_at: datetime | None) -> datetime:
    if created_at is None:
        return datetime.now(timezone.utc)
    if created_at.tzinfo is None:
        return created_at.replace(tzinfo=timezone.utc)
    return created_at.astimezone(timezone.utc)


def _format_created_at(created_at: datetime) -> str:
    return created_at.isoformat(timespec="seconds").replace("+00:00", "Z")
