from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from .launch_dry_run import OPEN_URL_ACTION
from .launch_executor import (
    BROWSER_STUB_EXECUTOR_MODE,
    LaunchExecutionReport,
    select_launch_executor,
)
from .session_manifest import ManifestItem, load_session_manifest


@dataclass(frozen=True, slots=True)
class SessionOpenResult:
    manifest_path: Path
    manifest_item_count: int
    selected_item: ManifestItem
    execution_report: LaunchExecutionReport


@dataclass(frozen=True, slots=True)
class _ManifestOpenStep:
    launch_order: int
    action_label: str
    company: str | None
    title: str | None
    apply_url: str


def open_manifest_item(
    manifest_path: Path,
    *,
    index: int,
) -> SessionOpenResult:
    manifest = load_session_manifest(manifest_path)
    resolved_index = _require_manifest_index(index)
    if resolved_index > manifest.item_count:
        raise ValueError(
            f"manifest index {resolved_index} exceeds manifest size {manifest.item_count}"
        )

    selected_item = manifest.items[resolved_index - 1]
    if selected_item.apply_url is None:
        raise ValueError(f"manifest index {resolved_index} is missing apply_url")

    execution_report = select_launch_executor(BROWSER_STUB_EXECUTOR_MODE).execute_step(
        _ManifestOpenStep(
            launch_order=selected_item.index,
            action_label=OPEN_URL_ACTION,
            company=selected_item.company,
            title=selected_item.title,
            apply_url=selected_item.apply_url,
        )
    )
    return SessionOpenResult(
        manifest_path=manifest.manifest_path,
        manifest_item_count=manifest.item_count,
        selected_item=selected_item,
        execution_report=execution_report,
    )


def _require_manifest_index(value: int) -> int:
    resolved_value = int(value)
    if resolved_value < 1:
        raise ValueError("manifest index must be at least 1")
    return resolved_value
