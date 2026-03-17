from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

from ..source_seed.infer import parse_company_input_line
from ..source_seed.models import CompanySeedInput
from ..source_seed.starter_lists import load_starter_list_entries


@dataclass(frozen=True, slots=True)
class LoadedDiscoveryInput:
    company_input: CompanySeedInput
    provenance: str


def load_discovery_inputs(
    *,
    command_label: str,
    companies: Sequence[str],
    from_file: Path | None,
    starter_lists: Sequence[str],
) -> tuple[LoadedDiscoveryInput, ...]:
    loaded_inputs: list[LoadedDiscoveryInput] = []
    next_index = 1

    for raw_value in companies:
        next_index = _append_loaded_input(
            loaded_inputs,
            raw_value=raw_value,
            provenance=f'{command_label} argument "{raw_value.strip()}"',
            index=next_index,
        )

    if from_file is not None:
        for raw_value in from_file.read_text(encoding="utf-8").splitlines():
            next_index = _append_loaded_input(
                loaded_inputs,
                raw_value=raw_value,
                provenance=(
                    f'{command_label} file "{from_file}" input "{raw_value.strip()}"'
                ),
                index=next_index,
            )

    for starter_list in starter_lists:
        for raw_value in load_starter_list_entries(starter_list):
            next_index = _append_loaded_input(
                loaded_inputs,
                raw_value=raw_value,
                provenance=(
                    f'{command_label} starter "{starter_list}" input "{raw_value.strip()}"'
                ),
                index=next_index,
            )

    return tuple(loaded_inputs)


def _append_loaded_input(
    loaded_inputs: list[LoadedDiscoveryInput],
    *,
    raw_value: str,
    provenance: str,
    index: int,
) -> int:
    company_input = parse_company_input_line(index, raw_value)
    if company_input is None:
        return index
    loaded_inputs.append(
        LoadedDiscoveryInput(
            company_input=company_input,
            provenance=provenance,
        )
    )
    return index + 1
