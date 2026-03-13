from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from .launch_plan import LaunchPlan
from .session_manifest import ManifestSelection

OPEN_URL_ACTION = "OPEN_URL"


@dataclass(frozen=True, slots=True)
class LaunchDryRunStep:
    launch_order: int
    action_label: str
    company: str | None
    title: str | None
    apply_url: str
    recommended_resume_variant: ManifestSelection
    recommended_profile_snippet: ManifestSelection


@dataclass(frozen=True, slots=True)
class LaunchDryRun:
    manifest_path: Path
    created_at: str
    total_items: int
    launchable_items: int
    skipped_items: int
    steps: tuple[LaunchDryRunStep, ...]


def build_launch_dry_run(plan: LaunchPlan) -> LaunchDryRun:
    steps = []
    for item in plan.items:
        if not item.launchable or item.launch_order is None:
            continue

        if item.apply_url is None:
            raise ValueError(f"launchable plan item {item.launch_order} is missing apply_url")
        if item.recommended_resume_variant is None:
            raise ValueError(
                f"launchable plan item {item.launch_order} is missing recommended_resume_variant"
            )
        if item.recommended_profile_snippet is None:
            raise ValueError(
                f"launchable plan item {item.launch_order} is missing recommended_profile_snippet"
            )

        steps.append(
            LaunchDryRunStep(
                launch_order=item.launch_order,
                action_label=OPEN_URL_ACTION,
                company=item.company,
                title=item.title,
                apply_url=item.apply_url,
                recommended_resume_variant=item.recommended_resume_variant,
                recommended_profile_snippet=item.recommended_profile_snippet,
            )
        )

    return LaunchDryRun(
        manifest_path=plan.manifest_path,
        created_at=plan.created_at,
        total_items=plan.total_items,
        launchable_items=plan.launchable_items,
        skipped_items=plan.skipped_items,
        steps=tuple(steps),
    )
