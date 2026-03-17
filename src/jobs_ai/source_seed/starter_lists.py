from __future__ import annotations

from dataclasses import dataclass
from importlib.resources import files

from ..portal_support import build_portal_support


@dataclass(frozen=True, slots=True)
class StarterListSpec:
    file_name: str
    legacy: bool = False


@dataclass(frozen=True, slots=True)
class StarterListEntry:
    raw_value: str
    provider_type: str | None
    company_hint: str | None


_STARTER_LIST_SPECS = {
    "ats-tech": StarterListSpec("ats_tech.txt"),
    "ats-data-ai": StarterListSpec("ats_data_ai.txt"),
    "ats-general-remote": StarterListSpec("ats_general_remote.txt"),
    "ats-startups": StarterListSpec("ats_startups.txt"),
    "major-tech": StarterListSpec("major_tech.txt", legacy=True),
    "fortune-500": StarterListSpec("fortune500_style.txt", legacy=True),
    "staffing-large-employers": StarterListSpec("staffing_large_employers.txt", legacy=True),
}


def available_starter_lists() -> tuple[str, ...]:
    return tuple(_STARTER_LIST_SPECS.keys())


def recommended_starter_lists() -> tuple[str, ...]:
    return tuple(
        name
        for name, spec in _STARTER_LIST_SPECS.items()
        if not spec.legacy
    )


def legacy_starter_lists() -> tuple[str, ...]:
    return tuple(
        name
        for name, spec in _STARTER_LIST_SPECS.items()
        if spec.legacy
    )


def starter_lists_help_text() -> str:
    recommended = ", ".join(recommended_starter_lists())
    legacy = ", ".join(legacy_starter_lists())
    return (
        f"Recommended ATS-native starters: {recommended}. "
        f"Legacy low-confidence starters: {legacy}. Use all to include every starter."
    )


def resolve_starter_lists(requested_lists: tuple[str, ...]) -> tuple[str, ...]:
    normalized = tuple(
        dict.fromkeys(
            _normalize_starter_list_name(value)
            for value in requested_lists
        )
    )
    if not normalized:
        return ()
    if "all" in normalized:
        return available_starter_lists()
    unknown = [value for value in normalized if value not in _STARTER_LIST_SPECS]
    if unknown:
        allowed = ", ".join((*available_starter_lists(), "all"))
        unknown_text = ", ".join(sorted(unknown))
        raise ValueError(f"unknown starter list(s): {unknown_text}. Allowed values: {allowed}")
    return normalized


def load_starter_list_entries(name: str) -> tuple[str, ...]:
    return _load_starter_list_lines(name)


def load_starter_list_items(name: str) -> tuple[StarterListEntry, ...]:
    items: list[StarterListEntry] = []
    for line in _load_starter_list_lines(name):
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        company_hint, _, source_value = stripped.partition("|")
        normalized_source_value = source_value.strip() if source_value else stripped
        portal_support = build_portal_support(normalized_source_value)
        items.append(
            StarterListEntry(
                raw_value=stripped,
                provider_type=None if portal_support is None else portal_support.portal_type,
                company_hint=company_hint.strip() or None,
            )
        )
    return tuple(items)


def _load_starter_list_lines(name: str) -> tuple[str, ...]:
    normalized_name = _normalize_starter_list_name(name)
    if normalized_name not in _STARTER_LIST_SPECS:
        allowed = ", ".join(available_starter_lists())
        raise ValueError(f"unknown starter list {name!r}. Allowed values: {allowed}")

    spec = _STARTER_LIST_SPECS[normalized_name]
    resource = files("jobs_ai.source_seed").joinpath(
        "data",
        spec.file_name,
    )
    return tuple(resource.read_text(encoding="utf-8").splitlines())


def _normalize_starter_list_name(value: str) -> str:
    return value.strip().lower()
