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
