from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from .launch_plan import LaunchPlan
from .session_manifest import ManifestSelection


@dataclass(frozen=True, slots=True)
class ApplicationAssistEntry:
    launch_order: int
    job_id: int | None
    company: str | None
    title: str | None
    apply_url: str
    portal_type: str | None
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
                job_id=item.job_id,
                company=item.company,
                title=item.title,
                apply_url=item.apply_url,
                portal_type=item.portal_type,
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


def select_application_assist_entry(
    assist: ApplicationAssist,
    *,
    launch_order: int | None,
) -> ApplicationAssistEntry:
    if not assist.assist_items:
        raise ValueError("manifest contains no launchable application assists")
    if launch_order is None:
        if len(assist.assist_items) == 1:
            return assist.assist_items[0]
        raise ValueError(
            "provide --launch-order when the manifest contains more than one launchable application"
        )

    for entry in assist.assist_items:
        if entry.launch_order == launch_order:
            return entry
    raise ValueError(f"launch order {launch_order} was not found in the manifest")
