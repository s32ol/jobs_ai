from __future__ import annotations

from ..fetch import FetchError, Fetcher
from ..models import OutcomeEvidence, SourceInput, SourceResult
from .base import (
    build_manual_review_result,
    build_response_evidence,
    build_skipped_result,
    detect_generic_page_patterns,
    fetch_source,
    inspect_response_for_skip,
)


class GenericAdapter:
    adapter_key = "generic"

    def collect(
        self,
        source: SourceInput,
        *,
        timeout_seconds: float,
        fetcher: Fetcher,
    ) -> SourceResult:
        try:
            response = fetch_source(source, timeout_seconds=timeout_seconds, fetcher=fetcher)
        except FetchError as exc:
            return build_skipped_result(
                source,
                adapter_key=self.adapter_key,
                reason_code="fetch_failed",
                reason=str(exc),
                evidence=OutcomeEvidence(error=str(exc)),
            )

        skip_result = inspect_response_for_skip(
            source,
            adapter_key=self.adapter_key,
            response=response,
        )
        if skip_result is not None:
            return skip_result

        detected_patterns = detect_generic_page_patterns(response.text)
        reason_code, reason, suggested_next_action = _manual_review_reason(source, detected_patterns)
        return build_manual_review_result(
            source,
            adapter_key=self.adapter_key,
            reason_code=reason_code,
            reason=reason,
            suggested_next_action=suggested_next_action,
            evidence=build_response_evidence(response, detected_patterns=detected_patterns),
        )


def _manual_review_reason(
    source: SourceInput,
    detected_patterns: tuple[str, ...],
) -> tuple[str, str, str]:
    if source.portal_type == "workday":
        requisition_hint = _workday_requisition_hint(source)
        reason = (
            "Accessible Workday HTML was fetched, but automatic collection is intentionally unsupported; manual review required."
        )
        if requisition_hint is not None:
            reason = (
                "Accessible Workday HTML was fetched for "
                f"{requisition_hint}, but automatic collection is intentionally unsupported; manual review required."
            )
        return (
            "workday_manual_review",
            reason,
            "Open the Workday page manually and capture company, title, location, and apply URL into leads.import.json.",
        )

    if source.portal_type is not None:
        portal_label = source.portal_type.capitalize()
        return (
            "unsupported_accessible_html",
            f"Accessible {portal_label} HTML was fetched, but no automatic collector is available for this source; manual review required.",
            "Review the page manually and copy company, title, location, and apply URL into leads.import.json.",
        )

    if detected_patterns:
        return (
            "unsupported_accessible_html",
            "Accessible HTML was fetched and job-page signals were detected, but the source is not supported automatically; manual review required.",
            "Review the page manually and copy company, title, location, and apply URL into leads.import.json.",
        )

    return (
        "unsupported_accessible_html",
        "Accessible HTML was fetched, but no supported automatic collector matched this source; manual review required.",
        "Confirm the page is a real job posting, then copy company, title, location, and apply URL into leads.import.json manually.",
    )


def _workday_requisition_hint(source: SourceInput) -> str | None:
    if source.portal_support is None:
        return None
    for hint in source.portal_support.hints:
        prefix = "Workday requisition hint: "
        if not hint.startswith(prefix):
            continue
        return hint.removeprefix(prefix).rstrip(".")
    return None
