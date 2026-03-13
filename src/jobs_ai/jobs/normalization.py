from __future__ import annotations

from collections.abc import Iterable, Mapping
import re

_COLLAPSE_WHITESPACE_FIELDS = frozenset(
    {
        "source",
        "company",
        "title",
        "location",
        "portal_type",
        "salary_text",
    }
)
_REPEATED_WHITESPACE_RE = re.compile(r"\s+")


def normalize_job_import_fields(
    record: Mapping[str, object],
    fields: Iterable[str],
) -> dict[str, str | None]:
    return {
        field: normalize_job_import_value(field, record.get(field))
        for field in fields
    }


def normalize_job_import_value(field: str, value: object) -> str | None:
    if not isinstance(value, str):
        return None

    text = value.strip()
    if not text:
        return None

    if field in _COLLAPSE_WHITESPACE_FIELDS:
        text = _REPEATED_WHITESPACE_RE.sub(" ", text)

    if field == "portal_type":
        text = text.lower()

    return text
