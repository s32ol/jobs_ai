from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from .session_manifest import ManifestSelection, SessionManifest, SessionSelectionScope


@dataclass(frozen=True, slots=True)
class LaunchPlanItem:
    manifest_index: int
    launch_order: int | None
    job_id: int | None
    company: str | None
    title: str | None
    apply_url: str | None
    portal_type: str | None
    recommended_resume_variant: ManifestSelection | None
    recommended_profile_snippet: ManifestSelection | None
    launchable: bool
    skip_reasons: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class LaunchPlan:
    manifest_path: Path
    created_at: str
    label: str | None
    selection_scope: SessionSelectionScope | None
    total_items: int
    launchable_items: int
    skipped_items: int
    items: tuple[LaunchPlanItem, ...]


def build_launch_plan(manifest: SessionManifest) -> LaunchPlan:
    planned_items = []
    launch_order = 0

    for item in manifest.items:
        launchable = not item.warnings
        planned_order = None
        if launchable:
            launch_order += 1
            planned_order = launch_order

        planned_items.append(
            LaunchPlanItem(
                manifest_index=item.index,
                launch_order=planned_order,
                job_id=item.job_id,
                company=item.company,
                title=item.title,
                apply_url=item.apply_url,
                portal_type=item.portal_type,
                recommended_resume_variant=item.recommended_resume_variant,
                recommended_profile_snippet=item.recommended_profile_snippet,
                launchable=launchable,
                skip_reasons=item.warnings,
            )
        )

    return LaunchPlan(
        manifest_path=manifest.manifest_path,
        created_at=manifest.created_at,
        label=manifest.label,
        selection_scope=manifest.selection_scope,
        total_items=len(planned_items),
        launchable_items=launch_order,
        skipped_items=len(planned_items) - launch_order,
        items=tuple(planned_items),
    )
