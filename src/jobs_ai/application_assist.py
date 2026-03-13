from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from .launch_plan import LaunchPlan
from .session_manifest import ManifestSelection


@dataclass(frozen=True, slots=True)
class ApplicationAssistEntry:
    launch_order: int
    company: str | None
    title: str | None
    apply_url: str
    recommended_resume_variant: ManifestSelection
    recommended_profile_snippet: ManifestSelection


@dataclass(frozen=True, slots=True)
class ApplicationAssist:
    manifest_path: Path
    created_at: str
    total_items: int
    assist_items: tuple[ApplicationAssistEntry, ...]


def build_application_assist(plan: LaunchPlan) -> ApplicationAssist:
    assist_items = []
    for item in plan.items:
        if not item.launchable or item.launch_order is None:
            continue
        if item.recommended_resume_variant is None:
            raise ValueError(
                f"launchable plan item {item.launch_order} is missing recommended_resume_variant"
            )
        if item.recommended_profile_snippet is None:
            raise ValueError(
                f"launchable plan item {item.launch_order} is missing recommended_profile_snippet"
            )
        if item.apply_url is None:
            raise ValueError(f"launchable plan item {item.launch_order} is missing apply_url")

        assist_items.append(
            ApplicationAssistEntry(
                launch_order=item.launch_order,
                company=item.company,
                title=item.title,
                apply_url=item.apply_url,
                recommended_resume_variant=item.recommended_resume_variant,
                recommended_profile_snippet=item.recommended_profile_snippet,
            )
        )

    return ApplicationAssist(
        manifest_path=plan.manifest_path,
        created_at=plan.created_at,
        total_items=plan.total_items,
        assist_items=tuple(assist_items),
    )
