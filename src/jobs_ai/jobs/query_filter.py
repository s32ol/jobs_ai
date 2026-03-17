from __future__ import annotations

from collections.abc import Mapping
import re

_TOKEN_RE = re.compile(r"[a-z0-9]+")
_STOPWORDS = frozenset(
    {
        "a",
        "an",
        "and",
        "for",
        "in",
        "of",
        "on",
        "or",
        "the",
        "to",
        "with",
    }
)


def normalize_query_terms(query_text: str | None) -> tuple[str, ...]:
    if query_text is None:
        return ()
    return tuple(
        term
        for term in _TOKEN_RE.findall(query_text.lower())
        if term not in _STOPWORDS
    )


def job_matches_query(job_record: Mapping[str, object], query_text: str | None) -> bool:
    query_terms = normalize_query_terms(query_text)
    if not query_terms:
        return True
    searchable_text = build_job_search_text(job_record)
    return all(term in searchable_text for term in query_terms)


def build_job_search_text(job_record: Mapping[str, object]) -> str:
    parts = (
        _text_value(_field_value(job_record, "company")),
        _text_value(_field_value(job_record, "title")),
        _text_value(_field_value(job_record, "location")),
        _text_value(_field_value(job_record, "source")),
        _text_value(_field_value(job_record, "portal_type")),
        _text_value(_field_value(job_record, "apply_url")),
        _text_value(_field_value(job_record, "raw_json")),
    )
    return " ".join(part for part in parts if part is not None).lower()


def _text_value(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    normalized_value = value.strip()
    return normalized_value or None


def _field_value(job_record: Mapping[str, object], field_name: str) -> object:
    try:
        return job_record[field_name]
    except KeyError:
        return None
