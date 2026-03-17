from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
import re

from .prefill_browser import BrowserFieldSnapshot, BrowserPageSnapshot

_NORMALIZE_RE = re.compile(r"[^a-z0-9]+")


@dataclass(frozen=True, slots=True)
class PortalFieldSpec:
    field_key: str
    aliases: tuple[str, ...]
    control_types: tuple[str, ...] | None = None


@dataclass(frozen=True, slots=True)
class PortalPrefillAdapter:
    portal_type: str
    portal_label: str
    support_level: str
    safe_fields: tuple[PortalFieldSpec, ...]
    short_text_aliases: tuple[str, ...]


COMMON_SAFE_FIELDS = (
    PortalFieldSpec("full_name", ("full name", "name")),
    PortalFieldSpec("first_name", ("first name", "first")),
    PortalFieldSpec("last_name", ("last name", "last")),
    PortalFieldSpec("email", ("email", "email address"), ("email", "text")),
    PortalFieldSpec("phone", ("phone", "phone number", "mobile phone"), ("tel", "text")),
    PortalFieldSpec("location", ("location", "current location", "city")),
    PortalFieldSpec("linkedin_url", ("linkedin", "linkedin profile"), ("url", "text")),
    PortalFieldSpec("github_url", ("github", "github profile"), ("url", "text")),
    PortalFieldSpec("portfolio_url", ("portfolio", "portfolio url", "website", "personal website"), ("url", "text")),
    PortalFieldSpec(
        "authorized_to_work_in_us",
        (
            "are you legally authorized to work in the united states",
            "are you authorized to work in the united states",
            "authorized to work in the united states",
        ),
        ("select",),
    ),
    PortalFieldSpec(
        "requires_sponsorship",
        (
            "will you now or in the future require sponsorship",
            "do you require sponsorship",
            "require visa sponsorship",
        ),
        ("select",),
    ),
    PortalFieldSpec(
        "work_authorization",
        ("work authorization", "authorization to work"),
        ("select",),
    ),
    PortalFieldSpec("resume", ("resume", "resume cv", "cv", "upload resume"), ("file",)),
)

GREENHOUSE_ADAPTER = PortalPrefillAdapter(
    portal_type="greenhouse",
    portal_label="Greenhouse",
    support_level="supported",
    safe_fields=COMMON_SAFE_FIELDS,
    short_text_aliases=("cover letter", "additional information", "why do you want to work here"),
)

LEVER_ADAPTER = PortalPrefillAdapter(
    portal_type="lever",
    portal_label="Lever",
    support_level="supported",
    safe_fields=COMMON_SAFE_FIELDS,
    short_text_aliases=("additional information", "cover letter", "why lever"),
)

ASHBY_ADAPTER = PortalPrefillAdapter(
    portal_type="ashby",
    portal_label="Ashby",
    support_level="supported",
    safe_fields=COMMON_SAFE_FIELDS,
    short_text_aliases=("cover letter", "why are you interested", "summary", "additional information"),
)

WORKDAY_ADAPTER = PortalPrefillAdapter(
    portal_type="workday",
    portal_label="Workday",
    support_level="limited_manual_support",
    safe_fields=(
        PortalFieldSpec("first_name", ("first name",)),
        PortalFieldSpec("last_name", ("last name",)),
        PortalFieldSpec("email", ("email", "email address"), ("email", "text")),
        PortalFieldSpec("phone", ("phone", "phone number"), ("tel", "text")),
        PortalFieldSpec("location", ("location", "current location")),
    ),
    short_text_aliases=(),
)

PORTAL_PREFILL_ADAPTERS: dict[str, PortalPrefillAdapter] = {
    adapter.portal_type: adapter
    for adapter in (GREENHOUSE_ADAPTER, LEVER_ADAPTER, ASHBY_ADAPTER, WORKDAY_ADAPTER)
}


def select_portal_prefill_adapter(portal_type: str | None) -> PortalPrefillAdapter | None:
    if portal_type is None:
        return None
    return PORTAL_PREFILL_ADAPTERS.get(portal_type)


def field_lookup_keys(field: BrowserFieldSnapshot) -> tuple[str, ...]:
    values = [
        _normalize_lookup_value(field.label),
        _normalize_lookup_value(field.name),
        _normalize_lookup_value(field.placeholder),
    ]
    return tuple(
        value
        for value in dict.fromkeys(values)
        if value is not None
    )


def find_unique_field(
    snapshot: BrowserPageSnapshot,
    *,
    aliases: Sequence[str],
    control_types: Sequence[str] | None = None,
    include_hidden: bool = False,
    used_selectors: set[str] | None = None,
) -> tuple[BrowserFieldSnapshot | None, str | None]:
    normalized_aliases = {
        normalized
        for normalized in (_normalize_lookup_value(alias) for alias in aliases)
        if normalized is not None
    }
    candidates = []
    for field in snapshot.fields:
        if used_selectors is not None and field.selector in used_selectors:
            continue
        if not include_hidden and not field.visible:
            continue
        if control_types is not None and field.control_type not in control_types:
            continue
        if normalized_aliases.intersection(field_lookup_keys(field)):
            candidates.append(field)
    if not candidates:
        return None, "field not found"
    if len(candidates) > 1:
        return None, "multiple matching fields"
    return candidates[0], None


def normalized_canned_answers(answers: Mapping[str, str]) -> dict[str, str]:
    return {
        normalized_key: value
        for key, value in answers.items()
        for normalized_key in (_normalize_lookup_value(key),)
        if normalized_key is not None and value.strip()
    }


def option_value_for_answer(
    field: BrowserFieldSnapshot,
    answer: str,
) -> str | None:
    normalized_answer = _normalize_lookup_value(answer)
    if normalized_answer is None:
        return None
    for option in field.options:
        option_values = {
            _normalize_lookup_value(option.label),
            _normalize_lookup_value(option.value),
        }
        if normalized_answer in option_values:
            return option.value
        if any(
            option_value is not None
            and (
                normalized_answer in option_value
                or option_value in normalized_answer
            )
            for option_value in option_values
        ):
            return option.value
    return None


def field_display_name(field: BrowserFieldSnapshot) -> str:
    for candidate in (field.label, field.name, field.placeholder):
        if candidate is not None and candidate.strip():
            return candidate.strip()
    return field.selector


def _normalize_lookup_value(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = _NORMALIZE_RE.sub(" ", value.strip().lower()).strip()
    return normalized or None
