from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from pathlib import Path

from ..collect.fetch import Fetcher
from ..portal_support import build_portal_support
from ..workspace import WorkspacePaths
from .detect_sites import detect_registry_sources_from_loaded_inputs
from .discover_ats import discover_registry_ats_sources
from .intake import LoadedDiscoveryInput, load_discovery_inputs
from .models import SourceRegistryExpandResult
from .seeding import seed_registry_loaded_inputs
from ..source_seed.starter_lists import resolve_starter_lists


def expand_registry_sources(
    paths: WorkspacePaths,
    *,
    companies: Sequence[str],
    from_file: Path | None,
    starter_lists: Sequence[str],
    detect_sites: bool,
    discover_ats: bool,
    structured_clues: bool,
    discover_ats_limit: int,
    discover_ats_providers: Sequence[str],
    timeout_seconds: float,
    created_at: datetime | None = None,
    fetcher: Fetcher | None = None,
) -> SourceRegistryExpandResult:
    resolved_starter_lists = resolve_starter_lists(tuple(starter_lists))
    loaded_inputs = load_discovery_inputs(
        command_label="sources expand-registry",
        companies=companies,
        from_file=from_file,
        starter_lists=resolved_starter_lists,
    )
    if not loaded_inputs and not discover_ats:
        raise ValueError(
            "provide discovery inputs via arguments, --from-file, or --starter, or enable --discover-ats"
        )

    seed_inputs, detect_inputs = _split_loaded_inputs_for_expand(
        loaded_inputs,
        detect_sites=detect_sites,
    )

    seed_result = None
    if seed_inputs:
        seed_result = seed_registry_loaded_inputs(
            paths,
            loaded_inputs=seed_inputs,
            starter_lists=resolved_starter_lists,
            timeout_seconds=timeout_seconds,
            created_at=created_at,
            fetcher=fetcher,
        )

    detect_sites_result = None
    if detect_sites and detect_inputs:
        detect_sites_result = detect_registry_sources_from_loaded_inputs(
            paths,
            loaded_inputs=detect_inputs,
            starter_lists=resolved_starter_lists,
            timeout_seconds=timeout_seconds,
            use_structured_clues=structured_clues,
            created_at=created_at,
            fetcher=fetcher,
        )

    discover_ats_result = None
    if discover_ats:
        discover_ats_result = discover_registry_ats_sources(
            paths,
            limit=discover_ats_limit,
            providers=discover_ats_providers,
            timeout_seconds=timeout_seconds,
            created_at=created_at,
            fetcher=fetcher,
        )

    if seed_result is None and detect_sites_result is None and discover_ats_result is None:
        raise ValueError(
            "no discovery lane ran; provide direct ATS inputs/starter lists, enable --detect-sites, or enable --discover-ats"
        )

    return SourceRegistryExpandResult(
        seed_result=seed_result,
        detect_sites_result=detect_sites_result,
        discover_ats_result=discover_ats_result,
        structured_clues_enabled=structured_clues,
    )


def _split_loaded_inputs_for_expand(
    loaded_inputs: Sequence[LoadedDiscoveryInput],
    *,
    detect_sites: bool,
) -> tuple[tuple[LoadedDiscoveryInput, ...], tuple[LoadedDiscoveryInput, ...]]:
    seed_inputs: list[LoadedDiscoveryInput] = []
    detect_inputs: list[LoadedDiscoveryInput] = []

    for loaded_input in loaded_inputs:
        if _is_direct_portal_input(loaded_input):
            seed_inputs.append(loaded_input)
            continue
        if detect_sites:
            detect_inputs.append(loaded_input)
            continue
        seed_inputs.append(loaded_input)

    return tuple(seed_inputs), tuple(detect_inputs)


def _is_direct_portal_input(loaded_input: LoadedDiscoveryInput) -> bool:
    career_page_url = loaded_input.company_input.career_page_url
    if career_page_url is None:
        return False
    return build_portal_support(career_page_url) is not None
