from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path

from .application_tracking import ApplicationStatusDetail, ApplicationStatusSnapshot
from .application_assist import ApplicationAssist
from .config import GEOGRAPHY_PRIORITY, SEARCH_PRIORITY, TARGET_ROLES, Settings
from .db import REQUIRED_TABLES
from .launch_dry_run import LaunchDryRunStep
from .launch_executor import LaunchExecutionReport
from .jobs.importer import JobImportResult
from .launch_plan import LaunchPlan
from .jobs.queue import QueuedJob
from .launch_preview import LaunchPreview
from .jobs.scoring import ScoredJob
from .portal_support import SUPPORTED_PORTAL_TYPES, PortalSupport, build_portal_support
from .resume.recommendations import QueueRecommendation
from .session_export import SessionExportResult
from .session_manifest import ManifestSelection, SessionManifest
from .workspace import WorkspacePaths

CLI_EXAMPLE_PREFIX = "python -m jobs_ai"


def _render_bullets(title: str, values: Sequence[str | Path]) -> list[str]:
    lines = [title]
    lines.extend(f"- {value}" for value in values)
    return lines


def _append_guidance(lines: list[str], title: str, values: Sequence[str]) -> None:
    if not values:
        return
    lines.append("")
    lines.append(title)
    lines.extend(f"- {value}" for value in values)


def _cli_example(command: str) -> str:
    command_text = command.strip()
    if not command_text:
        return CLI_EXAMPLE_PREFIX
    return f"{CLI_EXAMPLE_PREFIX} {command_text}"


def _command_with_optional_limit(command: str, limit: int | None) -> str:
    if limit is None:
        return _cli_example(command)
    return _cli_example(f"{command} --limit {limit}")


def render_status_report(settings: Settings, paths: WorkspacePaths) -> str:
    return "\n".join(
        [
            "jobs_ai control tower",
            f"environment: {settings.environment}",
            f"profile: {settings.profile}",
            f"project root: {paths.project_root}",
            f"database path: {paths.database_path}",
            "",
            "current focus: milestone 11 operational polish",
            "",
            *_render_bullets(
                "workspace paths:",
                (
                    f"raw leads inbox: {paths.raw_dir}",
                    f"normalized outputs: {paths.processed_dir}",
                    f"exports: {paths.exports_dir}",
                    f"session logs: {paths.sessions_dir}",
                    f"runtime logs: {paths.logs_dir}",
                ),
            ),
            "",
            *_render_bullets("target roles:", TARGET_ROLES),
            "",
            *_render_bullets("search priority:", SEARCH_PRIORITY),
            "",
            *_render_bullets("geography priority:", GEOGRAPHY_PRIORITY),
            "",
            f"tip: run {_cli_example('--help')} for the recommended sprint workflow.",
        ]
    )


def render_init_report(paths: WorkspacePaths, created_paths: Sequence[Path]) -> str:
    lines = [
        "jobs_ai workspace bootstrap",
        f"database path: {paths.database_path}",
    ]
    if created_paths:
        lines.append("created directories:")
        lines.extend(f"- {path}" for path in created_paths)
    else:
        lines.append("workspace already initialized")
    _append_guidance(
        lines,
        "next:",
        (
            _cli_example("doctor"),
            _cli_example("db init"),
        ),
    )
    return "\n".join(lines)


def render_doctor_report(paths: WorkspacePaths, missing_paths: Sequence[Path]) -> str:
    lines = [
        "jobs_ai workspace doctor",
        f"database path: {paths.database_path}",
    ]
    if missing_paths:
        lines.append("missing directories:")
        lines.extend(f"- {path}" for path in missing_paths)
        _append_guidance(lines, "fix:", (_cli_example("init"),))
    else:
        lines.append("workspace looks ready for database setup")
        _append_guidance(lines, "next:", (_cli_example("db status"),))
    return "\n".join(lines)


def render_db_init_report(paths: WorkspacePaths, created_paths: Sequence[Path]) -> str:
    lines = [
        "jobs_ai database init",
        f"database path: {paths.database_path}",
        "schema: ready",
    ]
    if created_paths:
        lines.append("created directories:")
        lines.extend(f"- {path}" for path in created_paths)
    else:
        lines.append("workspace directories already existed")
    lines.append("required tables:")
    lines.extend(f"- {table_name}" for table_name in REQUIRED_TABLES)
    _append_guidance(
        lines,
        "next:",
        (
            _cli_example("import data/raw/sample_job_leads.json"),
            _cli_example("score"),
        ),
    )
    return "\n".join(lines)


def render_db_status_report(paths: WorkspacePaths, missing_tables: Sequence[str]) -> str:
    lines = [
        "jobs_ai database status",
        f"database path: {paths.database_path}",
        f"database file: {'present' if paths.database_path.exists() else 'missing'}",
    ]
    if missing_tables:
        lines.append("schema: missing")
        lines.append("missing required tables:")
        lines.extend(f"- {table_name}" for table_name in missing_tables)
        _append_guidance(lines, "fix:", (_cli_example("db init"),))
    else:
        lines.append("schema: ready")
        lines.append("required tables present:")
        lines.extend(f"- {table_name}" for table_name in REQUIRED_TABLES)
        _append_guidance(lines, "next:", (_cli_example("import data/raw/sample_job_leads.json"),))
    return "\n".join(lines)


def render_import_report(
    paths: WorkspacePaths,
    input_path: Path,
    result: JobImportResult,
) -> str:
    lines = [
        "jobs_ai import",
        f"database path: {paths.database_path}",
        f"input file: {input_path}",
        f"inserted: {result.inserted_count}",
        f"skipped: {result.skipped_count}",
    ]
    if result.skipped:
        lines.append("skipped records:")
        lines.extend(f"- {message}" for message in result.skipped)
    if result.errors:
        lines.append("status: completed with errors" if result.inserted_count else "status: failed")
        lines.append("errors:")
        lines.extend(f"- {error}" for error in result.errors)
        if result.inserted_count:
            lines.append("note: valid records were still committed")
            _append_guidance(lines, "next:", (_cli_example("score"),))
        else:
            _append_guidance(
                lines,
                "next:",
                (f"Fix {input_path.name} and rerun {_cli_example(f'import {input_path}')}",),
            )
    else:
        lines.append("status: success")
        if result.inserted_count:
            _append_guidance(lines, "next:", (_cli_example("score"),))
        elif result.skipped:
            lines.append("note: database contents were unchanged")
            _append_guidance(
                lines,
                "next:",
                (f"Import a fresh lead batch with {_cli_example(f'import {input_path}')}",),
            )
    return "\n".join(lines)


def render_score_report(paths: WorkspacePaths, scored_jobs: Sequence[ScoredJob]) -> str:
    lines = [
        "jobs_ai score",
        f"database path: {paths.database_path}",
        f"jobs scored: {len(scored_jobs)}",
    ]
    if not scored_jobs:
        lines.append("status: no jobs to score")
        _append_guidance(lines, "next:", (_cli_example("import data/raw/sample_job_leads.json"),))
        return "\n".join(lines)

    lines.append("status: success")
    for index, job in enumerate(scored_jobs, start=1):
        location = job.location or "location missing"
        lines.extend(
            [
                "",
                f"{index}. score {job.total_score} | {job.company} | {job.title} | {location}",
                f"   job id: {job.job_id}",
                f"   role: {job.role_reason} (+{job.role_score})",
                f"   stack: {job.stack_reason} (+{job.stack_score})",
                f"   geography: {job.geography_reason} (+{job.geography_score})",
                f"   source: {job.source_reason} (+{job.source_score})",
            ]
        )
        if job.apply_url:
            lines.append(f"   apply_url: {job.apply_url}")
    _append_guidance(lines, "next:", (_cli_example("queue"),))
    return "\n".join(lines)


def render_queue_report(
    paths: WorkspacePaths,
    queued_jobs: Sequence[QueuedJob],
    *,
    limit: int | None = None,
) -> str:
    lines = [
        "jobs_ai queue",
        f"database path: {paths.database_path}",
        f"working set size: {len(queued_jobs)}",
        f"limit: {limit if limit is not None else 'none'}",
    ]
    if not queued_jobs:
        lines.append("status: no new jobs in queue")
        lines.append("tip: queue only shows jobs where status = 'new'")
        _append_guidance(lines, "next:", (_cli_example("import data/raw/sample_job_leads.json"),))
        return "\n".join(lines)

    lines.append("status: success")
    for job in queued_jobs:
        location = job.location or "location missing"
        lines.extend(
            [
                "",
                f"{job.rank}. score {job.score} | {job.company} | {job.title} | {location} | {job.source}",
                f"   reason: {job.reason_summary}",
            ]
        )
    _append_guidance(lines, "next:", (_command_with_optional_limit("recommend", limit),))
    return "\n".join(lines)


def render_recommendation_report(
    paths: WorkspacePaths,
    recommendations: Sequence[QueueRecommendation],
    *,
    limit: int | None = None,
) -> str:
    lines = [
        "jobs_ai recommend",
        f"database path: {paths.database_path}",
        f"recommendations: {len(recommendations)}",
        f"limit: {limit if limit is not None else 'none'}",
    ]
    if not recommendations:
        lines.append("status: no new jobs in queue")
        lines.append("tip: recommendations follow the same ranked queue used by jobs_ai queue")
        _append_guidance(lines, "next:", (_cli_example("queue"),))
        return "\n".join(lines)

    lines.append("status: success")
    for recommendation in recommendations:
        location = recommendation.location or "location missing"
        lines.extend(
            [
                "",
                (
                    f"{recommendation.rank}. score {recommendation.score} | "
                    f"{recommendation.company} | {recommendation.title} | {location} | {recommendation.source}"
                ),
                (
                    f"   resume variant: {recommendation.resume_variant_key} "
                    f"({recommendation.resume_variant_label})"
                ),
                (
                    f"   profile snippet: {recommendation.snippet_key} "
                    f"({recommendation.snippet_label})"
                ),
                f"   snippet text: {recommendation.snippet_text}",
                f"   explanation: {recommendation.explanation}",
            ]
        )
    _append_guidance(lines, "next:", (_command_with_optional_limit("launch-preview", limit),))
    return "\n".join(lines)


def render_launch_preview_report(
    paths: WorkspacePaths,
    previews: Sequence[LaunchPreview],
    *,
    limit: int | None = None,
    show_portal_hints: bool = False,
) -> str:
    lines = [
        "jobs_ai launch-preview",
        f"database path: {paths.database_path}",
        f"preview size: {len(previews)}",
        f"limit: {limit if limit is not None else 'none'}",
    ]
    if not previews:
        lines.append("status: no new jobs in queue")
        _append_guidance(lines, "next:", (_cli_example("queue"),))
        return "\n".join(lines)

    lines.append("status: success")
    for preview in previews:
        location = preview.location or "location missing"
        apply_url = preview.apply_url or "apply_url missing"
        lines.extend(
            [
                "",
                f"{preview.rank}. score {preview.score} | {preview.company} | {preview.title} | {location}",
                f"   apply_url: {apply_url}",
                (
                    f"   recommended resume variant: {preview.resume_variant_key} "
                    f"({preview.resume_variant_label})"
                ),
                (
                    f"   recommended profile snippet: {preview.snippet_key} "
                    f"({preview.snippet_label})"
                ),
                f"   snippet text: {preview.snippet_text}",
                f"   explanation: {preview.explanation}",
            ]
        )
        if show_portal_hints:
            lines.extend(_render_portal_support_lines(build_portal_support(preview.apply_url)))
    if not show_portal_hints:
        lines.append("tip: rerun with --portal-hints when links look portal-hosted")
    _append_guidance(lines, "next:", (_command_with_optional_limit("export-session", limit),))
    return "\n".join(lines)


def render_export_session_report(
    paths: WorkspacePaths,
    result: SessionExportResult,
) -> str:
    lines = [
        "jobs_ai export-session",
        f"database path: {paths.database_path}",
        f"export path: {result.export_path}",
        f"created_at: {result.created_at}",
        f"item count: {result.item_count}",
        f"limit: {result.limit if result.limit is not None else 'none'}",
        "status: success",
    ]
    _append_guidance(lines, "next:", (_cli_example(f"preflight {result.export_path}"),))
    return "\n".join(lines)


def render_preflight_report(manifest: SessionManifest) -> str:
    lines = [
        "jobs_ai preflight",
        f"manifest path: {manifest.manifest_path}",
        f"created_at: {manifest.created_at}",
        f"item count: {manifest.item_count}",
        f"warnings: {manifest.warning_count}",
        f"status: {'success with warnings' if manifest.warning_count else 'success'}",
    ]
    if not manifest.items:
        lines.append("preview: empty manifest")
        _append_guidance(lines, "next:", (_cli_example("export-session"),))
        return "\n".join(lines)

    for item in manifest.items:
        rank = item.rank if item.rank is not None else item.index
        lines.extend(
            [
                "",
                f"{rank}. {item.company or 'company missing'} | {item.title or 'title missing'}",
                f"   apply_url: {item.apply_url or 'apply_url missing'}",
                (
                    "   recommended resume variant: "
                    f"{_format_manifest_selection(item.recommended_resume_variant, fallback='resume variant missing')}"
                ),
                (
                    "   recommended profile snippet: "
                    f"{_format_manifest_selection(item.recommended_profile_snippet, fallback='profile snippet missing')}"
                ),
            ]
        )
        if item.warnings:
            lines.append(f"   warnings: {'; '.join(item.warnings)}")
    if manifest.warning_count:
        lines.append("tip: warnings are safe, but incomplete items will be skipped downstream")
    _append_guidance(lines, "next:", (_cli_example(f"launch-plan {manifest.manifest_path}"),))
    return "\n".join(lines)


def render_preflight_error_report(manifest_path: Path, error: str) -> str:
    lines = [
        "jobs_ai preflight",
        f"manifest path: {manifest_path}",
        "status: failed",
        f"error: {error}",
    ]
    _append_guidance(lines, "next:", (_cli_example("export-session"),))
    return "\n".join(lines)


def render_launch_plan_report(plan: LaunchPlan) -> str:
    lines = [
        "jobs_ai launch-plan",
        f"manifest path: {plan.manifest_path}",
        f"created_at: {plan.created_at}",
        f"total items: {plan.total_items}",
        f"launchable items: {plan.launchable_items}",
        f"skipped items: {plan.skipped_items}",
        f"status: {'success with skipped items' if plan.skipped_items else 'success'}",
    ]
    if not plan.items:
        lines.append("plan: empty manifest")
        _append_guidance(lines, "next:", (_cli_example("export-session"),))
        return "\n".join(lines)

    for item in plan.items:
        lines.extend(
            [
                "",
                (
                    f"{item.manifest_index}. launch order "
                    f"{item.launch_order if item.launch_order is not None else 'skipped'} | "
                    f"{item.company or 'company missing'} | {item.title or 'title missing'}"
                ),
                f"   apply_url: {item.apply_url or 'apply_url missing'}",
                (
                    "   recommended resume variant: "
                    f"{_format_manifest_selection(item.recommended_resume_variant, fallback='resume variant missing')}"
                ),
                (
                    "   recommended profile snippet: "
                    f"{_format_manifest_selection(item.recommended_profile_snippet, fallback='profile snippet missing')}"
                ),
            ]
        )
        if item.skip_reasons:
            lines.append(f"   status: skipped ({'; '.join(item.skip_reasons)})")
    if plan.skipped_items:
        lines.append("tip: skipped items are left untouched and will not be launched")
    if plan.launchable_items:
        _append_guidance(
            lines,
            "next:",
            (
                _cli_example(f"application-assist {plan.manifest_path}"),
                _cli_example(f"launch-dry-run --confirm --executor browser_stub {plan.manifest_path}"),
            ),
        )
    else:
        _append_guidance(lines, "next:", (_cli_example(f"preflight {plan.manifest_path}"),))
    return "\n".join(lines)


def render_launch_plan_error_report(manifest_path: Path, error: str) -> str:
    lines = [
        "jobs_ai launch-plan",
        f"manifest path: {manifest_path}",
        "status: failed",
        f"error: {error}",
    ]
    _append_guidance(lines, "next:", (_cli_example(f"preflight {manifest_path}"),))
    return "\n".join(lines)


def render_launch_dry_run_report(reports: Sequence[LaunchExecutionReport]) -> str:
    ordered_reports = sorted(reports, key=_launch_dry_run_sort_key)
    if not ordered_reports:
        return "No launchable actions. Run python -m jobs_ai launch-plan <manifest_path> to inspect skipped items."

    return "\n\n".join(
        "\n".join(
            [
                (
                    f"[{report.launch_order}] "
                    f"{_format_launch_dry_run_text(report.company, fallback='<missing company>')} | "
                    f"{_format_launch_dry_run_text(report.title, fallback='<missing role title>')}"
                ),
                f"URL: {_format_launch_dry_run_url(report.apply_url)}",
                f"Executor: {report.executor_mode}",
                f"Action: {report.action_label}",
                f"Result: {_format_launch_execution_status(report.status)}",
            ]
        )
        for report in ordered_reports
    )


def render_launch_execution_summary(steps: Sequence[LaunchDryRunStep]) -> str:
    application_label = "application" if len(steps) == 1 else "applications"
    lines = [f"Launching {len(steps)} {application_label}:"]
    lines.extend(
        (
            f"[{step.launch_order}] "
            f"{_format_launch_dry_run_text(step.company, fallback='<missing company>')} | "
            f"{_format_launch_dry_run_text(step.title, fallback='<missing role title>')}"
        )
        for step in steps
    )
    return "\n".join(lines)


def render_application_assist_report(
    assist: ApplicationAssist,
    *,
    show_portal_hints: bool = False,
) -> str:
    if not assist.assist_items:
        lines = [
            "jobs_ai application-assist",
            f"manifest path: {assist.manifest_path}",
            "launchable items: 0",
            "status: no launchable application assists",
        ]
        _append_guidance(lines, "next:", (_cli_example(f"launch-plan {assist.manifest_path}"),))
        return "\n".join(lines)

    lines = [
        "jobs_ai application-assist",
        f"manifest path: {assist.manifest_path}",
        f"launchable items: {len(assist.assist_items)}",
        "status: success",
    ]
    if not show_portal_hints:
        lines.append("tip: rerun with --portal-hints for portal-specific guidance")

    body = "\n\n".join(
        "\n".join(
            [
                (
                    f"[{entry.launch_order}] "
                    f"{_format_launch_dry_run_text(entry.company, fallback='<missing company>')} | "
                    f"{_format_launch_dry_run_text(entry.title, fallback='<missing role title>')}"
                ),
                f"URL: {_format_launch_dry_run_url(entry.apply_url)}",
                (
                    "Resume: "
                    f"{_format_manifest_selection(entry.recommended_resume_variant, fallback='<missing>')}"
                ),
                (
                    "Snippet: "
                    f"{_format_manifest_selection(entry.recommended_profile_snippet, fallback='<missing>')}"
                ),
                (
                    "Text: "
                    f"{entry.recommended_profile_snippet.text or '<missing>'}"
                ),
                *(
                    _render_portal_support_lines(build_portal_support(entry.apply_url))
                    if show_portal_hints
                    else []
                ),
            ]
        )
        for entry in assist.assist_items
    )
    lines.extend(["", body])
    _append_guidance(
        lines,
        "next:",
        (_cli_example(f"launch-dry-run --confirm --executor browser_stub {assist.manifest_path}"),),
    )
    return "\n".join(lines)


def render_application_assist_error_report(manifest_path: Path, error: str) -> str:
    lines = [
        "jobs_ai application-assist",
        f"manifest path: {manifest_path}",
        "status: failed",
        f"error: {error}",
    ]
    _append_guidance(lines, "next:", (_cli_example(f"preflight {manifest_path}"),))
    return "\n".join(lines)


def render_portal_hint_report(apply_url: str, portal_support: PortalSupport | None) -> str:
    lines = [
        "jobs_ai portal-hint",
        f"input apply_url: {apply_url}",
    ]
    if portal_support is None:
        lines.append("status: no supported portal helper available")
        lines.append(f"supported portals: {', '.join(SUPPORTED_PORTAL_TYPES)}")
        lines.append("tip: use --portal-type only when you already know the hosting portal")
        return "\n".join(lines)

    lines.extend(
        [
            f"portal type: {portal_support.portal_label}",
            f"normalized apply_url: {portal_support.normalized_apply_url}",
        ]
    )
    if portal_support.company_apply_url is not None:
        lines.append(f"company apply_url: {portal_support.company_apply_url}")
    lines.append("hints:")
    lines.extend(f"- {hint}" for hint in portal_support.hints)
    lines.append("status: supported")
    return "\n".join(lines)


def render_application_tracking_mark_report(
    paths: WorkspacePaths,
    snapshot: ApplicationStatusSnapshot,
) -> str:
    return "\n".join(
        [
            "jobs_ai track mark",
            f"database path: {paths.database_path}",
            f"job id: {snapshot.job_id}",
            f"job: {snapshot.company} | {snapshot.title} | {snapshot.location or 'location missing'}",
            f"recorded status: {snapshot.current_status}",
            f"timestamp: {snapshot.latest_timestamp or 'none'}",
            "status: success",
        ]
    )


def render_application_tracking_list_report(
    paths: WorkspacePaths,
    snapshots: Sequence[ApplicationStatusSnapshot],
    *,
    status_filter: str | None = None,
) -> str:
    lines = [
        "jobs_ai track list",
        f"database path: {paths.database_path}",
        f"jobs listed: {len(snapshots)}",
        f"filter status: {status_filter if status_filter is not None else 'none'}",
    ]
    if not snapshots:
        lines.append("status: no jobs found")
        return "\n".join(lines)

    lines.append("status: success")
    for index, snapshot in enumerate(snapshots, start=1):
        lines.extend(
            [
                "",
                (
                    f"{index}. [job {snapshot.job_id}] {snapshot.company} | "
                    f"{snapshot.title} | {snapshot.location or 'location missing'}"
                ),
                f"   current status: {snapshot.current_status}",
                f"   latest timestamp: {snapshot.latest_timestamp or 'none'}",
            ]
        )
    return "\n".join(lines)


def render_application_tracking_status_report(
    paths: WorkspacePaths,
    detail: ApplicationStatusDetail,
) -> str:
    lines = [
        "jobs_ai track status",
        f"database path: {paths.database_path}",
        f"job id: {detail.snapshot.job_id}",
        (
            f"job: {detail.snapshot.company} | {detail.snapshot.title} | "
            f"{detail.snapshot.location or 'location missing'}"
        ),
        f"current status: {detail.snapshot.current_status}",
        f"latest timestamp: {detail.snapshot.latest_timestamp or 'none'}",
        f"tracking entries: {len(detail.history)}",
    ]
    if not detail.history:
        lines.append("history: no tracked updates yet")
        return "\n".join(lines)

    lines.append("history:")
    lines.extend(f"- {entry.timestamp} | {entry.status}" for entry in detail.history)
    return "\n".join(lines)


def render_application_tracking_error_report(
    command_name: str,
    paths: WorkspacePaths,
    error: str,
) -> str:
    return "\n".join(
        [
            f"jobs_ai track {command_name}",
            f"database path: {paths.database_path}",
            "status: failed",
            f"error: {error}",
        ]
    )


def render_launch_dry_run_error_report(manifest_path: Path, error: str) -> str:
    lines = [
        "jobs_ai launch-dry-run",
        f"manifest path: {manifest_path}",
        "status: failed",
        f"error: {error}",
    ]
    _append_guidance(lines, "next:", (_cli_example(f"preflight {manifest_path}"),))
    return "\n".join(lines)


def _format_launch_execution_status(value: str) -> str:
    if value == "opened":
        return "opened in browser"
    if value == "noop":
        return "dry run only"
    if value == "skipped_missing_url":
        return "skipped (missing URL)"
    return value.replace("_", " ")


def _format_manifest_selection(selection: ManifestSelection | None, *, fallback: str) -> str:
    if selection is None:
        return fallback
    if selection.key and selection.label:
        return f"{selection.key} ({selection.label})"
    if selection.key:
        return selection.key
    if selection.label:
        return selection.label
    return fallback


def _launch_dry_run_sort_key(report: LaunchExecutionReport) -> tuple[int, str, str, str, str, str]:
    return (
        report.launch_order,
        _format_launch_dry_run_text(report.company, fallback="<missing company>"),
        _format_launch_dry_run_text(report.title, fallback="<missing role title>"),
        _format_launch_dry_run_url(report.apply_url),
        report.executor_mode,
        report.action_label,
    )


def _render_portal_support_lines(portal_support: PortalSupport | None) -> list[str]:
    if portal_support is None:
        return []

    lines = [f"   portal: {portal_support.portal_label}"]
    if portal_support.company_apply_url is not None:
        lines.append(f"   company apply_url: {portal_support.company_apply_url}")
    elif portal_support.normalized_apply_url != portal_support.original_apply_url:
        lines.append(f"   normalized apply_url: {portal_support.normalized_apply_url}")
    lines.append(f"   portal hints: {'; '.join(portal_support.hints)}")
    return lines


def _format_launch_dry_run_text(value: str | None, *, fallback: str) -> str:
    if value is None:
        return fallback
    text = value.strip()
    return text or fallback


def _format_launch_dry_run_url(value: str | None) -> str:
    if value is None:
        return "<missing>"
    text = value.strip()
    return text or "<missing>"
