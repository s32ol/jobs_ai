# Code Excerpt: Launch and Executor

Exact excerpts from the current repo for preview selection, launchability rules, dry-run generation, and executor modes.

## Launch preview objects
Source: `src/jobs_ai/launch_preview.py` lines 1-61

```python
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from .resume.recommendations import QueueRecommendation, select_queue_recommendations


@dataclass(frozen=True, slots=True)
class LaunchPreview:
    rank: int
    job_id: int
    company: str
    title: str
    location: str | None
    apply_url: str | None
    portal_type: str | None
    source: str
    score: int
    resume_variant_key: str
    resume_variant_label: str
    snippet_key: str
    snippet_label: str
    snippet_text: str
    explanation: str


def select_launch_preview(
    database_path: Path,
    *,
    limit: int | None = None,
    ingest_batch_id: str | None = None,
    query_text: str | None = None,
) -> tuple[LaunchPreview, ...]:
    recommendations = select_queue_recommendations(
        database_path,
        limit=limit,
        ingest_batch_id=ingest_batch_id,
        query_text=query_text,
    )
    return tuple(_preview_from_recommendation(recommendation) for recommendation in recommendations)


def _preview_from_recommendation(recommendation: QueueRecommendation) -> LaunchPreview:
    return LaunchPreview(
        rank=recommendation.rank,
        job_id=recommendation.job_id,
        company=recommendation.company,
        title=recommendation.title,
        location=recommendation.location,
        apply_url=recommendation.apply_url,
        portal_type=recommendation.portal_type,
        source=recommendation.source,
        score=recommendation.score,
        resume_variant_key=recommendation.resume_variant_key,
        resume_variant_label=recommendation.resume_variant_label,
        snippet_key=recommendation.snippet_key,
        snippet_label=recommendation.snippet_label,
        snippet_text=recommendation.snippet_text,
        explanation=recommendation.explanation,
    )
```

## Launch plan generation
Source: `src/jobs_ai/launch_plan.py` lines 1-72

```python
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
```

## Launch dry-run steps
Source: `src/jobs_ai/launch_dry_run.py` lines 1-69

```python
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
```

## Launch executor modes
Source: `src/jobs_ai/launch_executor.py` lines 1-157

```python
from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol, runtime_checkable
import webbrowser

if TYPE_CHECKING:
    from .launch_dry_run import LaunchDryRun, LaunchDryRunStep

NO_OP_EXECUTOR_MODE = "noop"
BROWSER_STUB_EXECUTOR_MODE = "browser_stub"
REMOTE_PRINT_EXECUTOR_MODE = "remote_print"
SUPPORTED_EXECUTOR_MODES = (
    NO_OP_EXECUTOR_MODE,
    BROWSER_STUB_EXECUTOR_MODE,
    REMOTE_PRINT_EXECUTOR_MODE,
)
NO_OP_EXECUTION_STATUS = "noop"
OPENED_EXECUTION_STATUS = "opened"
PRINTED_EXECUTION_STATUS = "printed"
SKIPPED_MISSING_URL_EXECUTION_STATUS = "skipped_missing_url"


@dataclass(frozen=True, slots=True)
class LaunchExecutionReport:
    executor_mode: str
    launch_order: int
    action_label: str
    company: str | None
    title: str | None
    apply_url: str
    status: str


@runtime_checkable
class LaunchStepExecutor(Protocol):
    def execute_step(self, step: LaunchDryRunStep) -> LaunchExecutionReport:
        """Execute a single launch step."""


class NoOpLaunchExecutor:
    def execute_step(self, step: LaunchDryRunStep) -> LaunchExecutionReport:
        return _build_execution_report(
            step,
            executor_mode=NO_OP_EXECUTOR_MODE,
            status=NO_OP_EXECUTION_STATUS,
        )


class BrowserLaunchExecutor:
    def __init__(self) -> None:
        self.reported_actions: list[LaunchExecutionReport] = []

    def execute_step(self, step: LaunchDryRunStep) -> LaunchExecutionReport:
        normalized_url = _normalize_launch_url(step.apply_url)
        if normalized_url is None:
            report = _build_execution_report(
                step,
                executor_mode=BROWSER_STUB_EXECUTOR_MODE,
                status=SKIPPED_MISSING_URL_EXECUTION_STATUS,
            )
            self.reported_actions.append(report)
            return report

        webbrowser.open(normalized_url, new=2)
        report = _build_execution_report(
            step,
            executor_mode=BROWSER_STUB_EXECUTOR_MODE,
            status=OPENED_EXECUTION_STATUS,
            apply_url=normalized_url,
        )
        self.reported_actions.append(report)
        return report


class RemotePrintLaunchExecutor:
    def __init__(self) -> None:
        self.reported_actions: list[LaunchExecutionReport] = []

    def execute_step(self, step: LaunchDryRunStep) -> LaunchExecutionReport:
        normalized_url = _normalize_launch_url(step.apply_url)
        if normalized_url is None:
            report = _build_execution_report(
                step,
                executor_mode=REMOTE_PRINT_EXECUTOR_MODE,
                status=SKIPPED_MISSING_URL_EXECUTION_STATUS,
            )
            self.reported_actions.append(report)
            return report

        report = _build_execution_report(
            step,
            executor_mode=REMOTE_PRINT_EXECUTOR_MODE,
            status=PRINTED_EXECUTION_STATUS,
            apply_url=normalized_url,
        )
        self.reported_actions.append(report)
        return report


def select_launch_executor(mode: str = NO_OP_EXECUTOR_MODE) -> LaunchStepExecutor:
    if mode == NO_OP_EXECUTOR_MODE:
        return NoOpLaunchExecutor()
    if mode == BROWSER_STUB_EXECUTOR_MODE:
        return BrowserLaunchExecutor()
    if mode == REMOTE_PRINT_EXECUTOR_MODE:
        return RemotePrintLaunchExecutor()
    supported = ", ".join(SUPPORTED_EXECUTOR_MODES)
    raise ValueError(f"unsupported launch executor mode: {mode}; expected one of: {supported}")


def collect_launch_execution_reports(
    dry_run: LaunchDryRun,
    executor: LaunchStepExecutor,
) -> tuple[LaunchExecutionReport, ...]:
    return collect_launch_execution_reports_for_steps(dry_run.steps, executor)


def collect_launch_execution_reports_for_steps(
    steps: Sequence[LaunchDryRunStep],
    executor: LaunchStepExecutor,
) -> tuple[LaunchExecutionReport, ...]:
    return tuple(executor.execute_step(step) for step in steps)


def execute_launch_dry_run(
    dry_run: LaunchDryRun,
    executor: LaunchStepExecutor,
) -> LaunchDryRun:
    collect_launch_execution_reports(dry_run, executor)
    return dry_run


def _build_execution_report(
    step: LaunchDryRunStep,
    *,
    executor_mode: str,
    status: str,
    apply_url: str | None = None,
) -> LaunchExecutionReport:
    return LaunchExecutionReport(
        executor_mode=executor_mode,
        launch_order=step.launch_order,
        action_label=step.action_label,
        company=step.company,
        title=step.title,
        apply_url=step.apply_url if apply_url is None else apply_url,
        status=status,
    )


def _normalize_launch_url(value: str | None) -> str | None:
    if value is None:
        return None
    normalized_value = value.strip()
    return normalized_value or None
```
