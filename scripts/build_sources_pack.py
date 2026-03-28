from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
import shutil
import sys
import textwrap


PROJECT_ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = PROJECT_ROOT / "sources" / "jobs_ai_sources_pack_latest"
EXPECTED_FILE_COUNT = 21

PACK_FILE_ORDER = (
    "00_README_START_HERE.md",
    "01_REPO_MAP.md",
    "02_CLI_COMMANDS_AND_FLOW.md",
    "03_ARCHITECTURE_OVERVIEW.md",
    "04_DATA_MODEL_AND_DB.md",
    "05_JOB_PIPELINE.md",
    "06_DISCOVERY_AND_IMPORT.md",
    "07_SCORING_QUEUE_AND_SESSION_FLOW.md",
    "08_LAUNCH_EXECUTION_AND_SAFETY.md",
    "09_RESUME_AND_APPLICATION_ASSIST.md",
    "10_CONFIG_ENV_AND_PATHS.md",
    "11_KNOWN_LIMITATIONS_AND_GAPS.md",
    "12_OPERATOR_QUICKSTART.md",
    "13_FILE_INDEX.json",
    "14_CODE_EXCERPT_CLI_ENTRYPOINTS.md",
    "15_CODE_EXCERPT_DB_RUNTIME_AND_SCHEMA.md",
    "16_CODE_EXCERPT_DISCOVERY_COLLECTION_IMPORT.md",
    "17_CODE_EXCERPT_SESSION_MANIFEST_AND_HISTORY.md",
    "18_CODE_EXCERPT_LAUNCH_AND_EXECUTOR.md",
    "19_CODE_EXCERPT_RESUME_AND_APPLICATION_ASSIST.md",
    "99_UPLOAD_RECOMMENDATION.md",
)

PRIORITY_ORDER = (
    "00_README_START_HERE.md",
    "02_CLI_COMMANDS_AND_FLOW.md",
    "03_ARCHITECTURE_OVERVIEW.md",
    "04_DATA_MODEL_AND_DB.md",
    "05_JOB_PIPELINE.md",
    "06_DISCOVERY_AND_IMPORT.md",
    "07_SCORING_QUEUE_AND_SESSION_FLOW.md",
    "08_LAUNCH_EXECUTION_AND_SAFETY.md",
    "09_RESUME_AND_APPLICATION_ASSIST.md",
    "10_CONFIG_ENV_AND_PATHS.md",
    "12_OPERATOR_QUICKSTART.md",
    "01_REPO_MAP.md",
    "11_KNOWN_LIMITATIONS_AND_GAPS.md",
    "14_CODE_EXCERPT_CLI_ENTRYPOINTS.md",
    "15_CODE_EXCERPT_DB_RUNTIME_AND_SCHEMA.md",
    "16_CODE_EXCERPT_DISCOVERY_COLLECTION_IMPORT.md",
    "17_CODE_EXCERPT_SESSION_MANIFEST_AND_HISTORY.md",
    "18_CODE_EXCERPT_LAUNCH_AND_EXECUTOR.md",
    "19_CODE_EXCERPT_RESUME_AND_APPLICATION_ASSIST.md",
    "13_FILE_INDEX.json",
    "99_UPLOAD_RECOMMENDATION.md",
)

FIRST_FIVE_UPLOAD = (
    "00_README_START_HERE.md",
    "02_CLI_COMMANDS_AND_FLOW.md",
    "03_ARCHITECTURE_OVERVIEW.md",
    "04_DATA_MODEL_AND_DB.md",
    "08_LAUNCH_EXECUTION_AND_SAFETY.md",
)
FIRST_TEN_UPLOAD = PRIORITY_ORDER[:10]


class BuildError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class ExcerptSpec:
    title: str
    path: str
    start_anchor: str
    end_anchor: str | None = None
    language: str = "python"
    start_occurrence: int = 1
    end_occurrence: int = 1


@dataclass(frozen=True, slots=True)
class FileMeta:
    name: str
    purpose: str
    source_paths: tuple[str, ...]
    kind: str


def _unique_paths(paths) -> list[str]:
    return list(dict.fromkeys(paths))


CLI_EXCERPTS = (
    ExcerptSpec(
        title="Typer app setup and command groups",
        path="src/jobs_ai/cli.py",
        start_anchor="app = typer.Typer(",
        end_anchor="def _load_runtime():",
    ),
    ExcerptSpec(
        title="Primary operator entrypoint: run_command",
        path="src/jobs_ai/cli.py",
        start_anchor='@app.command("run")',
        end_anchor='@app.command()',
    ),
    ExcerptSpec(
        title="Session freeze command: session_start",
        path="src/jobs_ai/cli.py",
        start_anchor='@session_app.command("start")',
        end_anchor='@app.command("stats")',
    ),
    ExcerptSpec(
        title="Registry-first collection command: sources_collect",
        path="src/jobs_ai/cli.py",
        start_anchor='@sources_app.command("collect")',
        end_anchor='@maintenance_app.command("backfill")',
    ),
    ExcerptSpec(
        title="Application assist command",
        path="src/jobs_ai/cli.py",
        start_anchor='@app.command("application-assist")',
        end_anchor='@app.command("application-log")',
    ),
    ExcerptSpec(
        title="Database backend inspection commands",
        path="src/jobs_ai/cli.py",
        start_anchor='@db_app.command("backend-status")',
        end_anchor='@db_app.command("migrate-to-postgres")',
    ),
    ExcerptSpec(
        title="CLI run()/main entrypoint wiring",
        path="src/jobs_ai/cli.py",
        start_anchor="def run(argv: Sequence[str] | None = None) -> int:",
    ),
    ExcerptSpec(
        title="Module entrypoint",
        path="src/jobs_ai/__main__.py",
        start_anchor="from __future__ import annotations",
    ),
)

DB_EXCERPTS = (
    ExcerptSpec(
        title="Database settings, env precedence, and config-time fallback",
        path="src/jobs_ai/config.py",
        start_anchor='SUPPORTED_DB_BACKENDS = ("sqlite", "postgres")',
    ),
    ExcerptSpec(
        title="Database runtime resolution and connect_database fallback logic",
        path="src/jobs_ai/db_runtime.py",
        start_anchor="def resolve_database_runtime(",
        end_anchor="def database_exists(",
    ),
    ExcerptSpec(
        title="Postgres and SQLite backend helpers",
        path="src/jobs_ai/db_runtime.py",
        start_anchor="def _connect_postgres_runtime(runtime: DatabaseRuntime):",
    ),
    ExcerptSpec(
        title="Core schema and indexes",
        path="src/jobs_ai/db.py",
        start_anchor="REQUIRED_TABLES = (",
        end_anchor="POSTGRES_BASE_SCHEMA_STATEMENTS = (",
    ),
    ExcerptSpec(
        title="Schema init, insert_job, duplicate detection, and session history writes",
        path="src/jobs_ai/db.py",
        start_anchor="def initialize_schema(database_path: Path, *, backfill_identity: bool = True) -> None:",
        end_anchor="def list_recent_session_history(",
    ),
    ExcerptSpec(
        title="Job identity and canonical apply URL logic",
        path="src/jobs_ai/jobs/identity.py",
        start_anchor="from __future__ import annotations",
    ),
    ExcerptSpec(
        title="Backend status and ping helpers",
        path="src/jobs_ai/db_postgres.py",
        start_anchor="def build_backend_status(settings: Settings) -> BackendStatusResult:",
        end_anchor="def _resolve_fallback_details(",
    ),
)

DISCOVERY_EXCERPTS = (
    ExcerptSpec(
        title="Top-level run workflow orchestration",
        path="src/jobs_ai/run_workflow.py",
        start_anchor="def run_operator_workflow(",
        end_anchor="def _normalize_created_at(",
    ),
    ExcerptSpec(
        title="Discover command orchestration and follow-on collect/import",
        path="src/jobs_ai/discover/cli.py",
        start_anchor="def run_discover_command(",
        end_anchor="def _resolve_output_dir(",
    ),
    ExcerptSpec(
        title="Collector harness and source normalization",
        path="src/jobs_ai/collect/harness.py",
        start_anchor="def run_collection(",
        end_anchor="def _normalize_created_at(",
    ),
    ExcerptSpec(
        title="Registry-first collect/import workflow",
        path="src/jobs_ai/sources/workflow.py",
        start_anchor="def collect_registry_sources(",
    ),
    ExcerptSpec(
        title="JSON import entrypoint and record normalization",
        path="src/jobs_ai/jobs/importer.py",
        start_anchor="@dataclass(frozen=True, slots=True)",
    ),
    ExcerptSpec(
        title="Import field normalization helpers",
        path="src/jobs_ai/jobs/normalization.py",
        start_anchor="from __future__ import annotations",
    ),
)

SESSION_EXCERPTS = (
    ExcerptSpec(
        title="Manifest export writer",
        path="src/jobs_ai/session_export.py",
        start_anchor="def export_launch_previews_session(",
        end_anchor="def _normalize_created_at(",
    ),
    ExcerptSpec(
        title="Session manifest schema and validation",
        path="src/jobs_ai/session_manifest.py",
        start_anchor="from __future__ import annotations",
    ),
    ExcerptSpec(
        title="Session inspect and reopen helpers",
        path="src/jobs_ai/session_history.py",
        start_anchor="from __future__ import annotations",
    ),
    ExcerptSpec(
        title="Open one manifest item directly",
        path="src/jobs_ai/session_open.py",
        start_anchor="from __future__ import annotations",
    ),
    ExcerptSpec(
        title="Session mark and manifest target resolution",
        path="src/jobs_ai/session_mark.py",
        start_anchor="from __future__ import annotations",
    ),
)

LAUNCH_EXCERPTS = (
    ExcerptSpec(
        title="Launch preview objects",
        path="src/jobs_ai/launch_preview.py",
        start_anchor="from __future__ import annotations",
    ),
    ExcerptSpec(
        title="Launch plan generation",
        path="src/jobs_ai/launch_plan.py",
        start_anchor="from __future__ import annotations",
    ),
    ExcerptSpec(
        title="Launch dry-run steps",
        path="src/jobs_ai/launch_dry_run.py",
        start_anchor="from __future__ import annotations",
    ),
    ExcerptSpec(
        title="Launch executor modes",
        path="src/jobs_ai/launch_executor.py",
        start_anchor="from __future__ import annotations",
    ),
)

RESUME_EXCERPTS = (
    ExcerptSpec(
        title="Resume variant resolution",
        path="src/jobs_ai/resume/config.py",
        start_anchor="from __future__ import annotations",
    ),
    ExcerptSpec(
        title="Resume and profile snippet recommendations",
        path="src/jobs_ai/resume/recommendations.py",
        start_anchor="from __future__ import annotations",
    ),
    ExcerptSpec(
        title="Applicant profile loading and resume overrides",
        path="src/jobs_ai/applicant_profile.py",
        start_anchor="from __future__ import annotations",
    ),
    ExcerptSpec(
        title="Read-only application assist view",
        path="src/jobs_ai/application_assist.py",
        start_anchor="from __future__ import annotations",
    ),
    ExcerptSpec(
        title="Prefill orchestration",
        path="src/jobs_ai/application_prefill.py",
        start_anchor="def run_application_prefill(",
        end_anchor="class _FillResult:",
    ),
    ExcerptSpec(
        title="Supported field filling logic",
        path="src/jobs_ai/application_prefill.py",
        start_anchor="def _fill_supported_portal_fields(",
        end_anchor="def _option_value_for_field_answer(",
    ),
    ExcerptSpec(
        title="Portal prefill adapter definitions",
        path="src/jobs_ai/prefill_portals.py",
        start_anchor="from __future__ import annotations",
    ),
    ExcerptSpec(
        title="Playwright prefill backend and backend selection",
        path="src/jobs_ai/prefill_browser.py",
        start_anchor="class PlaywrightPrefillBrowserBackend:",
        end_anchor="def _field_by_selector(",
    ),
    ExcerptSpec(
        title="Application log writer",
        path="src/jobs_ai/application_log.py",
        start_anchor="def write_application_log(",
    ),
)


FILE_METADATA = (
    FileMeta(
        name="00_README_START_HERE.md",
        purpose="Start here. Explains what this pack is, the current happy path, and the most important correction about current Postgres-or-SQLite backend support.",
        source_paths=(
            "README.md",
            "docs/architecture.md",
            "scripts/README.md",
            "src/jobs_ai/cli.py",
            "src/jobs_ai/config.py",
            "src/jobs_ai/db_runtime.py",
        ),
        kind="summary",
    ),
    FileMeta(
        name="01_REPO_MAP.md",
        purpose="Compact map of the current repo layout and which modules matter most for operator workflow, discovery, DB/runtime, launch/session, and application assist.",
        source_paths=(
            "README.md",
            "docs/architecture.md",
            "src/jobs_ai/cli.py",
            "src/jobs_ai/run_workflow.py",
            "src/jobs_ai/session_start.py",
        ),
        kind="summary",
    ),
    FileMeta(
        name="02_CLI_COMMANDS_AND_FLOW.md",
        purpose="Explains canonical commands, command groups, and how the CLI delegates into workflow, session, DB, and application-assist modules.",
        source_paths=(
            "README.md",
            "scripts/README.md",
            "src/jobs_ai/cli.py",
            "src/jobs_ai/run_workflow.py",
            "src/jobs_ai/session_start.py",
            "src/jobs_ai/application_prefill.py",
        ),
        kind="summary",
    ),
    FileMeta(
        name="03_ARCHITECTURE_OVERVIEW.md",
        purpose="High-signal description of how jobs_ai works now, including both intake modes and the human-in-the-loop boundary.",
        source_paths=(
            "README.md",
            "docs/architecture.md",
            "src/jobs_ai/run_workflow.py",
            "src/jobs_ai/session_start.py",
            "src/jobs_ai/launch_executor.py",
        ),
        kind="summary",
    ),
    FileMeta(
        name="04_DATA_MODEL_AND_DB.md",
        purpose="Backend selection, runtime fallback behavior, core schema, dedupe rules, and the current migration/merge model.",
        source_paths=(
            "README.md",
            ".env.example",
            "docs/architecture.md",
            "src/jobs_ai/config.py",
            "src/jobs_ai/db_runtime.py",
            "src/jobs_ai/db.py",
            "src/jobs_ai/db_postgres.py",
            "src/jobs_ai/db_merge.py",
            "src/jobs_ai/jobs/identity.py",
            "src/jobs_ai/jobs/normalization.py",
        ),
        kind="summary",
    ),
    FileMeta(
        name="05_JOB_PIPELINE.md",
        purpose="Stage-by-stage view of the daily operator pipeline, with the main inputs, outputs, and side effects.",
        source_paths=(
            "README.md",
            "src/jobs_ai/run_workflow.py",
            "src/jobs_ai/discover/cli.py",
            "src/jobs_ai/collect/harness.py",
            "src/jobs_ai/jobs/importer.py",
            "src/jobs_ai/session_start.py",
        ),
        kind="summary",
    ),
    FileMeta(
        name="06_DISCOVERY_AND_IMPORT.md",
        purpose="Detailed map of ATS discovery, collection, import normalization, dedupe, and registry-first collection.",
        source_paths=(
            "README.md",
            "docs/architecture.md",
            "src/jobs_ai/discover/cli.py",
            "src/jobs_ai/discover/harness.py",
            "src/jobs_ai/collect/harness.py",
            "src/jobs_ai/sources/workflow.py",
            "src/jobs_ai/jobs/importer.py",
            "src/jobs_ai/jobs/identity.py",
            "src/jobs_ai/jobs/normalization.py",
            "src/jobs_ai/portal_support.py",
        ),
        kind="summary",
    ),
    FileMeta(
        name="07_SCORING_QUEUE_AND_SESSION_FLOW.md",
        purpose="How jobs are ranked, recommended, selected into a session, exported, and tracked in session history.",
        source_paths=(
            "src/jobs_ai/jobs/scoring.py",
            "src/jobs_ai/jobs/queue.py",
            "src/jobs_ai/resume/recommendations.py",
            "src/jobs_ai/launch_preview.py",
            "src/jobs_ai/session_start.py",
            "src/jobs_ai/session_export.py",
            "src/jobs_ai/session_manifest.py",
            "src/jobs_ai/launch_plan.py",
            "src/jobs_ai/db.py",
        ),
        kind="summary",
    ),
    FileMeta(
        name="08_LAUNCH_EXECUTION_AND_SAFETY.md",
        purpose="Clarifies launch-preview vs launch-plan vs dry-run vs open/reopen and documents what is read-only versus side-effectful.",
        source_paths=(
            "README.md",
            "src/jobs_ai/launch_preview.py",
            "src/jobs_ai/launch_plan.py",
            "src/jobs_ai/launch_dry_run.py",
            "src/jobs_ai/launch_executor.py",
            "src/jobs_ai/session_start.py",
            "src/jobs_ai/session_history.py",
            "src/jobs_ai/session_open.py",
            "src/jobs_ai/session_mark.py",
            "src/jobs_ai/application_tracking.py",
            "src/jobs_ai/application_prefill.py",
        ),
        kind="summary",
    ),
    FileMeta(
        name="09_RESUME_AND_APPLICATION_ASSIST.md",
        purpose="Explains resume resolution, applicant profiles, application-assist, browser prefill, safe fields, and application logging.",
        source_paths=(
            "README.md",
            "docs/applicant_profile.example.json",
            "src/jobs_ai/resume/config.py",
            "src/jobs_ai/resume/recommendations.py",
            "src/jobs_ai/applicant_profile.py",
            "src/jobs_ai/application_assist.py",
            "src/jobs_ai/application_prefill.py",
            "src/jobs_ai/prefill_portals.py",
            "src/jobs_ai/prefill_browser.py",
            "src/jobs_ai/autofill/profile_config.py",
            "src/jobs_ai/application_log.py",
        ),
        kind="summary",
    ),
    FileMeta(
        name="10_CONFIG_ENV_AND_PATHS.md",
        purpose="Current env vars, workspace path layout, and the practical config combinations that matter for the current repo.",
        source_paths=(
            ".env.example",
            "README.md",
            "src/jobs_ai/config.py",
            "src/jobs_ai/workspace.py",
            "src/jobs_ai/resume/config.py",
            "src/jobs_ai/applicant_profile.py",
            "src/jobs_ai/autofill/profile_config.py",
        ),
        kind="summary",
    ),
    FileMeta(
        name="11_KNOWN_LIMITATIONS_AND_GAPS.md",
        purpose="Lists the current rough edges, doc drift, stale old pack, Workday/manual gaps, and executor/prefill edge cases.",
        source_paths=(
            "README.md",
            "docs/architecture.md",
            "scripts/build_chatgpt_sources.py",
            "src/jobs_ai/config.py",
            "src/jobs_ai/session_open.py",
            "src/jobs_ai/session_mark.py",
            "src/jobs_ai/launch_executor.py",
            "src/jobs_ai/application_prefill.py",
            "src/jobs_ai/prefill_portals.py",
        ),
        kind="summary",
    ),
    FileMeta(
        name="12_OPERATOR_QUICKSTART.md",
        purpose="Exact command recipes for installation, daily workflows, session review, application assist, tracking, and stats.",
        source_paths=(
            "README.md",
            "scripts/README.md",
            "pyproject.toml",
            "src/jobs_ai/cli.py",
            "src/jobs_ai/config.py",
        ),
        kind="summary",
    ),
    FileMeta(
        name="13_FILE_INDEX.json",
        purpose="Machine-readable manifest of the generated pack, including each file purpose, source paths, kind, and upload priority rank.",
        source_paths=("scripts/build_sources_pack.py",),
        kind="summary",
    ),
    FileMeta(
        name="14_CODE_EXCERPT_CLI_ENTRYPOINTS.md",
        purpose="Exact code excerpts for the canonical CLI entrypoints and operator-facing commands.",
        source_paths=tuple(_unique_paths(spec.path for spec in CLI_EXCERPTS)),
        kind="excerpt",
    ),
    FileMeta(
        name="15_CODE_EXCERPT_DB_RUNTIME_AND_SCHEMA.md",
        purpose="Exact code excerpts for backend selection, runtime fallback, schema definition, dedupe, and backend status helpers.",
        source_paths=tuple(_unique_paths(spec.path for spec in DB_EXCERPTS)),
        kind="excerpt",
    ),
    FileMeta(
        name="16_CODE_EXCERPT_DISCOVERY_COLLECTION_IMPORT.md",
        purpose="Exact code excerpts for workflow orchestration, discovery, collection, registry-first collect, and import normalization.",
        source_paths=tuple(_unique_paths(spec.path for spec in DISCOVERY_EXCERPTS)),
        kind="excerpt",
    ),
    FileMeta(
        name="17_CODE_EXCERPT_SESSION_MANIFEST_AND_HISTORY.md",
        purpose="Exact code excerpts for manifest export, manifest validation, session inspection/reopen, open, and mark behavior.",
        source_paths=tuple(_unique_paths(spec.path for spec in SESSION_EXCERPTS)),
        kind="excerpt",
    ),
    FileMeta(
        name="18_CODE_EXCERPT_LAUNCH_AND_EXECUTOR.md",
        purpose="Exact code excerpts for launch preview, launch plan, dry-run, and executor modes.",
        source_paths=tuple(_unique_paths(spec.path for spec in LAUNCH_EXCERPTS)),
        kind="excerpt",
    ),
    FileMeta(
        name="19_CODE_EXCERPT_RESUME_AND_APPLICATION_ASSIST.md",
        purpose="Exact code excerpts for resume selection, applicant profiles, assist generation, browser prefill, portal rules, and application logging.",
        source_paths=tuple(_unique_paths(spec.path for spec in RESUME_EXCERPTS)),
        kind="excerpt",
    ),
    FileMeta(
        name="99_UPLOAD_RECOMMENDATION.md",
        purpose="Recommended upload order for 5, 10, or all files, plus which files are optional after the core summaries.",
        source_paths=(
            "README.md",
            "docs/architecture.md",
            "src/jobs_ai/cli.py",
            "src/jobs_ai/run_workflow.py",
            "src/jobs_ai/session_start.py",
            "scripts/build_sources_pack.py",
        ),
        kind="summary",
    ),
)


def repo_path(relative_path: str) -> Path:
    path = PROJECT_ROOT / relative_path
    if not path.exists():
        raise BuildError(f"required repo path was not found: {relative_path}")
    return path


def read_text(relative_path: str) -> str:
    return repo_path(relative_path).read_text(encoding="utf-8")


def format_md(text: str) -> str:
    normalized = textwrap.dedent(text).strip()
    return normalized + "\n"


def format_json(payload: object) -> str:
    return json.dumps(payload, indent=2, ensure_ascii=True) + "\n"


def find_anchor(
    lines: list[str],
    anchor: str,
    *,
    start_index: int = 0,
    occurrence: int = 1,
) -> int:
    if occurrence < 1:
        raise BuildError(f"invalid occurrence {occurrence} for anchor {anchor!r}")
    matches = [
        index
        for index, line in enumerate(lines[start_index:], start=start_index)
        if anchor in line
    ]
    if len(matches) < occurrence:
        raise BuildError(
            f"anchor {anchor!r} was not found {occurrence} time(s) in source"
        )
    return matches[occurrence - 1]


def extract_excerpt(spec: ExcerptSpec) -> tuple[int, int, str]:
    path = repo_path(spec.path)
    lines = path.read_text(encoding="utf-8").splitlines()
    start_index = find_anchor(
        lines,
        spec.start_anchor,
        start_index=0,
        occurrence=spec.start_occurrence,
    )
    if spec.end_anchor is None:
        end_index = len(lines)
    else:
        end_index = find_anchor(
            lines,
            spec.end_anchor,
            start_index=start_index + 1,
            occurrence=spec.end_occurrence,
        )
    excerpt_lines = lines[start_index:end_index]
    if not excerpt_lines:
        raise BuildError(f"excerpt {spec.title!r} resolved to an empty block")
    code = "\n".join(excerpt_lines).rstrip() + "\n"
    return start_index + 1, end_index, code


def render_excerpt_document(
    title: str,
    intro: str,
    specs: tuple[ExcerptSpec, ...],
) -> str:
    parts = [f"# {title}", "", intro.strip(), ""]
    for spec in specs:
        start_line, end_line, code = extract_excerpt(spec)
        parts.append(f"## {spec.title}")
        parts.append(
            f"Source: `{spec.path}` lines {start_line}-{end_line}"
        )
        parts.append("")
        parts.append(f"```{spec.language}")
        parts.append(code.rstrip())
        parts.append("```")
        parts.append("")
    return "\n".join(parts).rstrip() + "\n"


def render_readme_start() -> str:
    return format_md(
        """
        # ChatGPT Sources Pack for `jobs_ai`

        This pack is a compact, upload-friendly snapshot of how `jobs_ai` works now. It is intentionally summary-heavy and excludes the raw repo, runtime artifacts, secrets, `.env`, `data/`, `sessions/`, `logs/`, and the older `chatgpt_sources_core_v2/` bundle.

        ## What this pack is optimized for
        - Understanding the current operator workflow quickly
        - Seeing the real CLI entrypoints and delegation path
        - Understanding current DB/runtime behavior, including Postgres and SQLite fallback
        - Understanding how session manifests, launch behavior, resume recommendations, and application assist fit together

        ## Current happy path
        - Daily default: `jobs-ai run "python backend engineer remote" --limit 25`
        - Registry-first daily variant: `jobs-ai run "python backend engineer remote" --use-registry --limit 25`
        - Modular variant: `jobs-ai discover "python backend engineer remote" --collect --import`, then `jobs-ai session start --limit 25`
        - Manual follow-through: `jobs-ai open`, `jobs-ai application-assist --prefill`, `jobs-ai session mark`, `jobs-ai track list`, `jobs-ai stats`

        ## Important correction
        `docs/architecture.md` still describes the project as "SQLite-backed". Current code in `src/jobs_ai/config.py`, `src/jobs_ai/db_runtime.py`, and `src/jobs_ai/db_postgres.py` supports Postgres or SQLite, selects Postgres by default, and falls back to SQLite when config or runtime availability requires it.

        ## Read these files first
        1. `02_CLI_COMMANDS_AND_FLOW.md`
        2. `03_ARCHITECTURE_OVERVIEW.md`
        3. `04_DATA_MODEL_AND_DB.md`
        4. `08_LAUNCH_EXECUTION_AND_SAFETY.md`
        5. `09_RESUME_AND_APPLICATION_ASSIST.md`

        ## Then use these as the navigation spine
        - `05_JOB_PIPELINE.md`
        - `06_DISCOVERY_AND_IMPORT.md`
        - `07_SCORING_QUEUE_AND_SESSION_FLOW.md`
        - `10_CONFIG_ENV_AND_PATHS.md`
        - `12_OPERATOR_QUICKSTART.md`

        ## What the code excerpt files are for
        - `14_...` shows the canonical CLI entrypoints
        - `15_...` shows backend selection, runtime fallback, and schema logic
        - `16_...` shows discover, collect, registry-first intake, and import logic
        - `17_...` shows manifest export, validation, inspect/reopen, open, and mark logic
        - `18_...` shows launch-plan, dry-run, and executor behavior
        - `19_...` shows resume selection, applicant profiles, application assist, and browser prefill

        ## Pack design choices
        - Summary docs first, exact code excerpts second
        - No raw repo dump
        - No copied secrets or operator-local artifacts
        - Explicitly current-repo focused, not historical
        """
    )


def render_repo_map() -> str:
    return format_md(
        """
        # Repo Map

        `jobs_ai` is a `src/`-layout Python project. The packaged code lives under `src/jobs_ai/`. The old `chatgpt_sources_core_v2/` directory is an older export bundle, not the active package.

        ## Primary entrypoints
        - `pyproject.toml`: registers `jobs-ai = jobs_ai.cli:run`
        - `src/jobs_ai/cli.py`: Typer app, command groups, and command wiring
        - `src/jobs_ai/__main__.py`: `python -m jobs_ai` entrypoint
        - `src/jobs_ai/main.py`: rendering/report layer, not the startup file

        ## Workflow orchestration
        - `src/jobs_ai/run_workflow.py`: end-to-end `run` orchestration
        - `src/jobs_ai/session_start.py`: freeze a session manifest, derive launch info, and optionally execute launch steps

        ## Discovery and collection
        - `src/jobs_ai/discover/cli.py`: discover command orchestration
        - `src/jobs_ai/discover/harness.py`: search planning, hit classification, and candidate verification
        - `src/jobs_ai/collect/cli.py`: collect command wrapper
        - `src/jobs_ai/collect/harness.py`: source normalization, adapter selection, and collected/manual-review outputs
        - `src/jobs_ai/collect/adapters/*.py`: native ATS collectors for Greenhouse, Lever, and Ashby

        ## Source registry and source seeding
        - `src/jobs_ai/sources/workflow.py`: collect from active registry sources
        - `src/jobs_ai/sources/registry.py`: registry storage, verification, status changes
        - `src/jobs_ai/source_seed/*.py`: company/domain-driven source inference and starter-list workflows

        ## Database and data model
        - `src/jobs_ai/config.py`: env loading and backend selection
        - `src/jobs_ai/db_runtime.py`: runtime connection and Postgres-to-SQLite fallback
        - `src/jobs_ai/db.py`: canonical schema, init/backfill, inserts, dedupe, session history
        - `src/jobs_ai/db_postgres.py`: backend status, ping, and SQLite-to-Postgres migration
        - `src/jobs_ai/db_merge.py`: SQLite-to-SQLite merge

        ## Jobs pipeline
        - `src/jobs_ai/jobs/importer.py`: import collected leads into the DB
        - `src/jobs_ai/jobs/identity.py`: canonical apply URLs and identity keys
        - `src/jobs_ai/jobs/normalization.py`: import-field cleanup
        - `src/jobs_ai/jobs/scoring.py`: rule-based scoring
        - `src/jobs_ai/jobs/queue.py`: queue selection from `jobs.status = 'new'`
        - `src/jobs_ai/jobs/fast_apply.py`: shortlist flow from an already-populated DB

        ## Session and launch
        - `src/jobs_ai/session_export.py`: manifest JSON writer
        - `src/jobs_ai/session_manifest.py`: manifest schema and validation
        - `src/jobs_ai/session_history.py`: recent/inspect/reopen behavior
        - `src/jobs_ai/session_open.py`: open one manifest item
        - `src/jobs_ai/session_mark.py`: mark jobs from manifest selections
        - `src/jobs_ai/launch_preview.py`: recommendation-backed launchable preview objects
        - `src/jobs_ai/launch_plan.py`: convert manifest warnings into launchability
        - `src/jobs_ai/launch_dry_run.py`: dry-run steps
        - `src/jobs_ai/launch_executor.py`: `noop`, `browser_stub`, and `remote_print`

        ## Resume and application assist
        - `src/jobs_ai/resume/config.py`: resume variant definitions and file resolution
        - `src/jobs_ai/resume/recommendations.py`: map ranked jobs to resume variants and profile snippets
        - `src/jobs_ai/applicant_profile.py`: applicant profile JSON loading
        - `src/jobs_ai/application_assist.py`: read-only assist model from a launch plan
        - `src/jobs_ai/application_prefill.py`: review-first prefill flow
        - `src/jobs_ai/prefill_portals.py`: safe field rules by portal
        - `src/jobs_ai/prefill_browser.py`: Playwright backend
        - `src/jobs_ai/application_log.py`: JSON application log writer
        - `src/jobs_ai/application_tracking.py`: DB-based status transitions and history

        ## Workspace, docs, tests, scripts
        - `src/jobs_ai/workspace.py`: canonical repo-local path layout
        - `README.md`: current operator-facing docs
        - `docs/architecture.md`: compact architecture summary, slightly stale on DB wording
        - `docs/applicant_profile.example.json`: applicant-profile example
        - `tests/`: behavior-level verification
        - `scripts/README.md`: helper-script note that the main CLI is the canonical operator interface
        """
    )


def render_cli_commands_and_flow() -> str:
    return format_md(
        """
        # CLI Commands and Flow

        `jobs-ai` is the canonical operator entrypoint. `python -m jobs_ai` is the alternate module entrypoint. `src/jobs_ai/main.py` mostly renders reports; it is not the startup file.

        ## Canonical commands
        - Daily default: `jobs-ai run "python backend engineer remote" --limit 25`
        - Daily registry-first: `jobs-ai run "python backend engineer remote" --use-registry --limit 25`
        - Modular intake: `jobs-ai discover "python backend engineer remote" --collect --import`
        - Modular session freeze: `jobs-ai session start --limit 25`
        - Reopen or inspect a prior batch: `jobs-ai session recent`, `jobs-ai session inspect 1`, `jobs-ai session reopen 1`
        - Launch one manifest item directly: `jobs-ai open data/exports/<manifest>.json 2`
        - Review-first browser handoff: `jobs-ai application-assist data/exports/<manifest>.json --prefill --launch-order 1`
        - Track outcomes: `jobs-ai session mark applied --manifest data/exports/<manifest>.json --all`
        - Inspect DB backend: `jobs-ai db backend-status`, `jobs-ai db ping`, `jobs-ai db status`

        ## Command groups that matter most
        - Top-level commands: `run`, `fast-apply`, `discover`, `collect`, `import`, `launch-preview`, `launch-plan`, `launch-dry-run`, `application-assist`, `application-log`, `stats`
        - `session` group: `start`, `recent`, `inspect`, `reopen`, `mark`
        - `track` group: `mark`, `list`, `status`
        - `db` group: `init`, `status`, `backend-status`, `ping`, `migrate-to-postgres`, `merge`
        - `sources` group: registry management and registry-first collect/import workflows

        ## Delegation flow from the CLI
        - `src/jobs_ai/cli.py` loads settings and workspace paths, then delegates to focused modules.
        - `run_command` calls `src/jobs_ai/run_workflow.py`.
        - `run_workflow.py` has two intake branches:
          - discover-first: `discover -> collect -> import -> session start`
          - registry-first: `sources/workflow.collect_registry_sources -> session start`
        - `session start` delegates to `src/jobs_ai/session_start.py`.
        - `session_start.py` selects previews, exports the manifest, reloads it, builds a launch plan, builds a dry run, records session history, and optionally executes launch steps.
        - `application-assist` uses `src/jobs_ai/application_assist.py` for read-only guidance and `src/jobs_ai/application_prefill.py` for browser prefill.

        ## Practical command routing map
        - `jobs-ai run` -> `src/jobs_ai/run_workflow.py`
        - `jobs-ai discover` -> `src/jobs_ai/discover/cli.py`
        - `jobs-ai collect` -> `src/jobs_ai/collect/cli.py`
        - `jobs-ai sources collect` -> `src/jobs_ai/sources/workflow.py`
        - `jobs-ai session start` -> `src/jobs_ai/session_start.py`
        - `jobs-ai session reopen` -> `src/jobs_ai/session_history.py`
        - `jobs-ai open` -> `src/jobs_ai/session_open.py`
        - `jobs-ai session mark` and `jobs-ai track mark` -> `src/jobs_ai/session_mark.py` and `src/jobs_ai/application_tracking.py`
        - `jobs-ai application-assist --prefill` -> `src/jobs_ai/application_prefill.py`

        ## Minimal examples
        ```bash
        jobs-ai run "python backend engineer remote" --limit 25
        jobs-ai run "python backend engineer remote" --use-registry --limit 25
        jobs-ai discover "python backend engineer remote" --collect --import
        jobs-ai session start --limit 20 --open --executor remote_print
        jobs-ai application-assist data/exports/<manifest>.json --prefill --launch-order 1
        jobs-ai session mark applied --manifest data/exports/<manifest>.json --all
        ```
        """
    )


def render_architecture_overview() -> str:
    return format_md(
        """
        # Architecture Overview

        `jobs_ai` is a local, deterministic job-application preparation toolkit. It discovers likely ATS sources, collects structured job data, imports normalized jobs into SQLite or Postgres, ranks the `new` queue, freezes deterministic session manifests, and helps the operator open or prefill applications without turning into a blind auto-submit bot.

        ## The two current intake modes
        - Discover-first intake:
          - `src/jobs_ai/run_workflow.py`
          - `src/jobs_ai/discover/cli.py`
          - `src/jobs_ai/collect/harness.py`
          - `src/jobs_ai/jobs/importer.py`
        - Registry-first intake:
          - `src/jobs_ai/sources/workflow.py`
          - `src/jobs_ai/sources/registry.py`
          - `src/jobs_ai/session_start.py`

        ## End-to-end architecture
        ```text
        query or registry
          -> discovery or registry collect
          -> ATS collection
          -> import + dedupe
          -> score + queue + resume recommendation
          -> session manifest export
          -> launch plan / dry run / optional URL open
          -> manual review or browser prefill
          -> manual submit
          -> session mark / track / stats
        ```

        ## Major runtime layers
        - Intake: `discover`, `collect`, `sources`
        - Persistence: `config.py`, `db_runtime.py`, `db.py`, `db_postgres.py`, `db_merge.py`
        - Selection: `jobs/scoring.py`, `jobs/queue.py`, `resume/recommendations.py`
        - Session/launch: `session_export.py`, `session_manifest.py`, `session_start.py`, `launch_plan.py`, `launch_dry_run.py`, `launch_executor.py`
        - Operator assist: `application_assist.py`, `application_prefill.py`, `application_log.py`, `application_tracking.py`

        ## Human-in-the-loop boundary
        - Automated:
          - source discovery
          - ATS collection
          - import normalization and dedupe
          - ranking and recommendation
          - manifest creation
          - safe URL opening
          - safe field prefilling
        - Manual by design:
          - final application judgment
          - final submit click
          - edge-case answers
          - downstream status updates

        ## ATS and portal support as of the current repo
        - Native discovery/collection: Greenhouse, Lever, Ashby
        - Detected but more manual: Workday
        - Portal-aware prefill support:
          - supported: Greenhouse, Lever, Ashby
          - limited manual support: Workday

        ## Important repo-state note
        `docs/architecture.md` is still useful for the broad picture, but its opening “SQLite-backed” wording is stale relative to the current code. The code path now supports both Postgres and SQLite.
        """
    )


def render_data_model_and_db() -> str:
    return format_md(
        """
        # Data Model and DB

        The current backend story is defined by `src/jobs_ai/config.py`, `src/jobs_ai/db_runtime.py`, `src/jobs_ai/db.py`, and `src/jobs_ai/db_postgres.py`.

        ## Backend selection
        - `load_settings()` merges the repo `.env` file and process environment.
        - Preferred SQLite path var: `JOBS_AI_SQLITE_PATH`
        - Backward-compatible SQLite path var: `JOBS_AI_DB_PATH`
        - Postgres URL source:
          - direct `DATABASE_URL`, or
          - `PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER`, `PGPASSWORD`, `PGSSLMODE`
        - Default backend value in code: `postgres`

        ## Fallback behavior
        - Config-time fallback:
          - if Postgres is selected but the URL is missing or invalid, settings fall back to SQLite and record a warning
        - Runtime fallback:
          - if Postgres is selected but the live connection fails, `connect_database()` falls back to SQLite and marks the connection as a fallback connection
        - Important nuance:
          - passing a SQLite path into many functions does not force SQLite if the resolved backend is still Postgres from env/settings

        ## Core tables
        - `jobs`
          - core backlog table
          - stores `source`, `source_job_id`, `company`, `title`, `location`, `apply_url`, `portal_type`, `status`, `raw_json`
          - also stores ingest and dedupe metadata: `ingest_batch_id`, `source_query`, `import_source`, `source_registry_id`, `canonical_apply_url`, `identity_key`
        - `applications`
          - per-job application state, notes, resume variant, and timestamps
        - `application_tracking`
          - append-only status history such as `opened`, `applied`, `interview`, `offer`, `rejected`
        - `session_history`
          - records exported manifest path, item counts, launchable counts, batch id, and source query
        - `source_registry`
          - durable ATS source registry keyed by `normalized_url`

        ## Dedupe rules
        Duplicate matching is centralized in `src/jobs_ai/db.py` and `src/jobs_ai/jobs/identity.py`.

        Match order:
        1. exact `apply_url`
        2. `canonical_apply_url`
        3. `identity_key`

        Identity behavior:
        - if `source_job_id` exists, prefer `portal-or-host | job_id | source_job_id`
        - otherwise use `portal-or-host-or-source | company | title | location`
        - canonical apply URLs use `src/jobs_ai/portal_support.py` to strip tracking params and promote company-scoped Greenhouse/Ashby job URLs when possible

        ## Schema and backfill behavior
        - `initialize_schema()` creates missing tables and indexes
        - it also backfills newer `jobs` columns such as `canonical_apply_url` and `identity_key` for older DBs
        - SQLite uses `sqlite3` with foreign keys enabled
        - Postgres uses `psycopg` with a thin adapter so the rest of the code can keep using DB-agnostic calls

        ## Migration and merge
        - `src/jobs_ai/db_postgres.py`
          - `build_backend_status()`
          - `ping_database_target()`
          - `migrate_sqlite_to_postgres()`
        - `src/jobs_ai/db_merge.py`
          - merges a second SQLite DB into the target SQLite DB
          - reuses the same duplicate matcher used by import
          - remaps related application/session rows safely

        ## Current doc drift to keep in mind
        - `README.md` reflects the current Postgres-or-SQLite story
        - `docs/architecture.md` still says “SQLite-backed”
        """
    )


def render_job_pipeline() -> str:
    return format_md(
        """
        # Job Pipeline

        This is the current daily pipeline behind `jobs-ai run`.

        ## 1. Resolve runtime and workspace
        - Code: `src/jobs_ai/cli.py`, `src/jobs_ai/config.py`, `src/jobs_ai/workspace.py`
        - Reads:
          - `.env.example`-style env vars at runtime
          - workspace path rules
        - Writes:
          - creates missing workspace directories if needed

        ## 2. Choose intake mode
        - Code: `src/jobs_ai/run_workflow.py`
        - Discover-first:
          - `run_discover_command()`
        - Registry-first:
          - `collect_registry_sources()`

        ## 3. Discover or select sources
        - Discover-first:
          - search-backed ATS discovery
          - separates confirmed sources from manual-review results
        - Registry-first:
          - loads active registry sources
          - verifies if needed

        ## 4. Collect ATS jobs
        - Code: `src/jobs_ai/collect/harness.py`
        - Validates and normalizes input URLs
        - Selects a collector adapter
        - Produces:
          - collected leads
          - manual-review items
          - collect artifacts under `data/processed/...` or a user-chosen out dir

        ## 5. Import, normalize, and dedupe
        - Code: `src/jobs_ai/jobs/importer.py`, `src/jobs_ai/jobs/identity.py`, `src/jobs_ai/jobs/normalization.py`
        - Normalizes text fields
        - Stores original JSON as `raw_json`
        - Skips duplicates by `apply_url`, `canonical_apply_url`, and `identity_key`
        - Writes rows into `jobs`

        ## 6. Score, queue, and recommend
        - Code: `src/jobs_ai/jobs/scoring.py`, `src/jobs_ai/jobs/queue.py`, `src/jobs_ai/resume/recommendations.py`
        - Uses the `jobs.status = 'new'` subset only
        - Applies role, stack, geography, source, and actionability scoring
        - Attaches a recommended resume variant and profile snippet

        ## 7. Freeze a session
        - Code: `src/jobs_ai/session_start.py`, `src/jobs_ai/session_export.py`, `src/jobs_ai/session_manifest.py`, `src/jobs_ai/launch_plan.py`, `src/jobs_ai/launch_dry_run.py`
        - Exports a deterministic manifest JSON
        - Reloads and validates that manifest
        - Builds a launch plan and dry run
        - Records session metadata into `session_history`

        ## 8. Optional open/reopen execution
        - Code: `src/jobs_ai/launch_executor.py`, `src/jobs_ai/session_history.py`, `src/jobs_ai/session_open.py`
        - `noop`: no side effects
        - `browser_stub`: opens URLs in a browser
        - `remote_print`: prints URLs for remote-safe workflows

        ## 9. Manual review, prefill, submit, and tracking
        - Code: `src/jobs_ai/application_assist.py`, `src/jobs_ai/application_prefill.py`, `src/jobs_ai/application_log.py`, `src/jobs_ai/application_tracking.py`
        - Prefill can safely fill fields and upload the recommended resume
        - Prefill always stops before submit
        - Outcome tracking happens through `session mark`, `track mark`, or application log writes
        """
    )


def render_discovery_and_import() -> str:
    return format_md(
        """
        # Discovery and Import

        Discovery/import is split across `discover`, `collect`, `sources`, and `jobs`.

        ## Discover-first path
        - Entry: `src/jobs_ai/discover/cli.py`
        - Core engine: `src/jobs_ai/discover/harness.py`
        - Behavior:
          - normalize query
          - build a run id and output directory
          - plan and run search-backed ATS discovery
          - classify hits into confirmed sources, skipped results, and manual-review results
          - optionally run follow-on collect/import steps

        ## Collection path
        - Wrapper: `src/jobs_ai/collect/cli.py`
        - Core harness: `src/jobs_ai/collect/harness.py`
        - Behavior:
          - normalize source URLs
          - drop invalid or duplicate normalized sources
          - choose the right collector adapter
          - collect leads and manual-review items

        ## Supported ATS collector path
        - Greenhouse
        - Lever
        - Ashby

        ## Workday status
        - `src/jobs_ai/portal_support.py` can detect and normalize Workday URLs
        - current discovery and collection do not treat Workday like the native Greenhouse/Lever/Ashby path
        - Workday stays much more manual in the current repo

        ## Registry-first path
        - Entry: `src/jobs_ai/sources/workflow.py`
        - Pulls active registry entries from `source_registry`
        - Verifies entries when needed
        - Runs collection directly against those sources
        - Can import immediately after collection

        ## Import behavior
        - Entry: `src/jobs_ai/jobs/importer.py`
        - Accepts one JSON object or a JSON array
        - Required fields:
          - `source`
          - `company`
          - `title`
          - `location`
        - Optional fields include:
          - `apply_url`
          - `source_job_id`
          - `portal_type`
          - `salary_text`
          - `posted_at`
          - `found_at`

        ## Normalization and dedupe
        - `src/jobs_ai/jobs/normalization.py`
          - strips whitespace
          - collapses repeated whitespace on key text fields
          - lowercases `portal_type`
        - `src/jobs_ai/jobs/identity.py`
          - derives `canonical_apply_url`
          - derives `identity_key`
        - `src/jobs_ai/db.py`
          - checks duplicate rules before insert

        ## What gets written where
        - Collect artifacts:
          - `data/processed/...` or a user-specified out dir
        - Imported jobs:
          - database backend resolved by config/runtime
        - Session outputs:
          - `data/exports/...` or a user-specified out dir during `session start`
        """
    )


def render_scoring_queue_and_session_flow() -> str:
    return format_md(
        """
        # Scoring Queue and Session Flow

        Selection starts from the DB, not from raw collected files.

        ## Queue selection
        - Code: `src/jobs_ai/jobs/queue.py`
        - Input subset:
          - only rows where `jobs.status = 'new'`
        - Optional scope:
          - `ingest_batch_id`
          - free-text query filter

        ## Scoring
        - Code: `src/jobs_ai/jobs/scoring.py`
        - Major signal groups:
          - target role title matches
          - stack keyword matches
          - geography preferences
          - source quality/source type
          - actionability, especially missing `apply_url`

        ## Resume recommendation attachment
        - Code: `src/jobs_ai/resume/recommendations.py`
        - Maps scored jobs to:
          - `resume_variant_key`
          - `resume_variant_label`
          - `snippet_key`
          - `snippet_label`
          - `snippet_text`
          - explanation text

        ## Launch preview
        - Code: `src/jobs_ai/launch_preview.py`
        - Thin adapter from queue recommendations into the preview objects used by session export

        ## Session selection scope
        - Code: `src/jobs_ai/session_manifest.py`
        - Carries:
          - `batch_id`
          - `source_query`
          - `import_source`
          - `selection_mode`
          - `refresh_batch_id`
        - Used so later inspection can tell whether a manifest came from new imports, a registry refresh reuse, or a more generic selection

        ## Manifest freeze
        - Code: `src/jobs_ai/session_start.py`, `src/jobs_ai/session_export.py`
        - Exported manifest contains:
          - ranked jobs
          - apply URLs
          - portal type
          - recommended resume variant
          - recommended profile snippet text
        - It does not contain:
          - resolved resume file paths
          - runtime portal hints
          - browser session state

        ## Launchability rules
        - Code: `src/jobs_ai/session_manifest.py`, `src/jobs_ai/launch_plan.py`
        - Manifest validation converts incomplete items into warnings
        - Launch plan makes `launchable = not warnings`
        - Only clean items receive deterministic `launch_order`

        ## Session history
        - Code: `src/jobs_ai/db.py`, `src/jobs_ai/session_history.py`
        - Every frozen session records:
          - manifest path
          - item count
          - launchable count
          - ingest batch id
          - source query
          - created timestamp
        """
    )


def render_launch_execution_and_safety() -> str:
    return format_md(
        """
        # Launch Execution and Safety

        The current repo separates read-only planning from side-effectful opening or prefilling.

        ## Command meanings
        - `launch-preview`
          - read-only
          - shows ranked, recommended jobs before a manifest exists
        - `launch-plan`
          - read-only
          - evaluates one manifest and labels items as launchable or skipped
        - `launch-dry-run`
          - can stay read-only if used for planning only
          - converts launchable plan items into `OPEN_URL` steps
          - has the CLI safety cap and confirmation gate
        - `session start --open`
          - writes a manifest
          - writes session history
          - can immediately run executor steps
        - `session reopen`
          - reloads a prior manifest or session id
          - rebuilds dry-run steps
          - can immediately run executor steps
        - `open`
          - opens one manifest item directly by index
          - does not require the stricter launch-plan gate

        ## Executor modes
        - `noop`
          - no external side effect
        - `browser_stub`
          - opens URLs in a browser
        - `remote_print`
          - prints URLs instead of opening them
          - useful for SSH or remote environments

        ## Side-effect boundaries
        - Read-only:
          - `launch-preview`
          - `launch-plan`
          - manifest validation
          - session inspection
        - Filesystem writes:
          - manifest export JSON
        - DB writes:
          - `session_history`
          - application status tracking via `session mark` or `track mark`
        - Browser/OS side effects:
          - URL open via `browser_stub`
          - Playwright browser launch in `application-assist --prefill`

        ## Safety rules that actually exist
        - Launch plan blocks items with manifest warnings
        - Dry-run rechecks missing `apply_url`, resume selection, and snippet selection
        - Executor never submits forms
        - Prefill always stops before submit
        - `remote_print` exists for remote-safe workflows

        ## Important edge cases
        - `src/jobs_ai/session_open.py`
          - only requires a valid index and non-null `apply_url`
          - this means `open` can bypass the stricter launch-plan warning gate
        - `src/jobs_ai/session_mark.py`
          - `--all` means all launchable manifest items
          - explicit `--indexes` can still select items that are not launchable if they still have `job_id`s
        - `src/jobs_ai/session_start.py` and `src/jobs_ai/session_history.py`
          - `session start --open` and `session reopen` execute immediately
          - they do not have the same confirmation layer as `launch-dry-run`
        - `src/jobs_ai/application_tracking.py`
          - opening URLs does not auto-mark jobs as `opened`
          - status changes happen through tracking commands or explicit follow-up
        """
    )


def render_resume_and_application_assist() -> str:
    return format_md(
        """
        # Resume and Application Assist

        Resume selection and application assist sit on top of the ranked queue and session manifest.

        ## Resume variants
        - Code: `src/jobs_ai/resume/config.py`
        - Current variant keys:
          - `general-data`
          - `data-engineering`
          - `analytics-engineering`
          - `telemetry-observability`
        - Resolution order:
          1. variant-specific env var such as `JOBS_AI_RESUME_DATA_ENGINEERING_PATH`
          2. JSON map from `JOBS_AI_RESUME_MAP_PATH` or `.jobs_ai_resume_paths.json`
          3. default `resumes/<variant>.<suffix>` discovery

        ## Recommendation layer
        - Code: `src/jobs_ai/resume/recommendations.py`
        - Uses title, target-role match, and stack signals to choose:
          - a resume variant
          - a profile snippet

        ## Applicant profile
        - Code: `src/jobs_ai/applicant_profile.py`
        - Default file: `.jobs_ai_applicant_profile.json`
        - Override env var: `JOBS_AI_APPLICANT_PROFILE_PATH`
        - Holds:
          - name, email, phone, location
          - LinkedIn/GitHub/portfolio
          - work authorization fields
          - canned answers
          - optional resume path overrides

        ## `application-assist` without `--prefill`
        - Code: `src/jobs_ai/application_assist.py`
        - Read-only
        - Projects a launch plan into assist entries
        - Requires launchable items with complete `apply_url`, recommended resume selection, and recommended profile snippet

        ## `application-assist --prefill`
        - Code: `src/jobs_ai/application_prefill.py`
        - Steps:
          1. load manifest
          2. build launch plan
          3. choose one launchable entry
          4. load applicant profile
          5. normalize portal URL
          6. resolve actual resume file path
          7. open the page in the browser backend
          8. fill supported safe fields
          9. upload resume when possible
          10. optionally use the recommended snippet as short text
          11. report unresolved required fields
          12. stop before submit

        ## Portal prefill support
        - `src/jobs_ai/prefill_portals.py`
        - Supported:
          - Greenhouse
          - Lever
          - Ashby
        - Limited manual support:
          - Workday

        ## Browser backend behavior
        - Code: `src/jobs_ai/prefill_browser.py`, `src/jobs_ai/autofill/profile_config.py`
        - Current backend: Playwright
        - On local macOS runs, the default behavior is a dedicated local Chrome profile flow
        - Important browser env vars:
          - `JOBS_AI_BROWSER_CHANNEL`
          - `JOBS_AI_BROWSER_USER_DATA_DIR`
          - `JOBS_AI_BROWSER_PROFILE_DIRECTORY`

        ## Application logging and DB tracking
        - JSON log writer:
          - `src/jobs_ai/application_log.py`
          - writes one JSON file per handled application under `data/applications/`
        - DB status tracking:
          - `src/jobs_ai/application_tracking.py`
          - separate from the JSON application log

        ## Important current behavior
        - Prefill may open a normalized or company-scoped portal URL
        - Application logging still records the original manifest `apply_url`
        - If an applicant-profile resume override points to a missing file, prefill records that as a skipped resume field instead of silently falling back
        """
    )


def render_config_env_and_paths() -> str:
    return format_md(
        """
        # Config, Env, and Paths

        This file uses `.env.example`, README, and source code only. It intentionally does not copy the local `.env`.

        ## Database env vars
        - `JOBS_AI_DB_BACKEND`
          - `sqlite` or `postgres`
        - `JOBS_AI_SQLITE_PATH`
          - preferred SQLite file path
        - `JOBS_AI_DB_PATH`
          - backward-compatible alias for `JOBS_AI_SQLITE_PATH`
        - `DATABASE_URL`
          - preferred Postgres/Neon connection string
        - `PGHOST`
        - `PGPORT`
        - `PGDATABASE`
        - `PGUSER`
        - `PGPASSWORD`
        - `PGSSLMODE`

        ## Resume and applicant env vars
        - `JOBS_AI_RESUME_MAP_PATH`
        - `JOBS_AI_RESUME_<VARIANT>_PATH`
          - for example `JOBS_AI_RESUME_DATA_ENGINEERING_PATH`
        - `JOBS_AI_APPLICANT_PROFILE_PATH`

        ## Browser-assist env vars
        - `JOBS_AI_BROWSER_CHANNEL`
        - `JOBS_AI_BROWSER_USER_DATA_DIR`
        - `JOBS_AI_BROWSER_PROFILE_DIRECTORY`

        ## Repo/workspace paths
        Defined in `src/jobs_ai/workspace.py`.

        - project root: repo root
        - data dir: `data/`
        - raw collection dir: `data/raw/`
        - processed workflow dir: `data/processed/`
        - session export dir: `data/exports/`
        - sessions dir: `sessions/`
        - logs dir: `logs/`
        - database path: resolved from `JOBS_AI_SQLITE_PATH` or `JOBS_AI_DB_PATH`

        ## Current practical config combinations
        - Local SQLite-first:
          ```bash
          JOBS_AI_DB_BACKEND=sqlite
          JOBS_AI_SQLITE_PATH=data/jobs_ai.db
          ```
        - Postgres/Neon with SQLite fallback path still present:
          ```bash
          JOBS_AI_DB_BACKEND=postgres
          JOBS_AI_SQLITE_PATH=data/jobs_ai.db
          DATABASE_URL=postgresql://user:password@host/db?sslmode=require
          ```
        - Postgres from `PG*` pieces instead of one URL:
          ```bash
          PGHOST=example-host
          PGPORT=5432
          PGDATABASE=example-db
          PGUSER=example-user
          PGPASSWORD=replace-me
          PGSSLMODE=require
          ```

        ## Path resolution notes
        - Resume-path resolution allows relative paths and expands them against the project root or mapping file directory.
        - Applicant profile defaults to `.jobs_ai_applicant_profile.json` in the repo root.
        - Browser user data dir can be absolute or relative, but the browser profile directory must be a profile name, not a filesystem path.
        """
    )


def render_known_limitations() -> str:
    return format_md(
        """
        # Known Limitations and Gaps

        ## Doc drift
        - `docs/architecture.md` still opens with “SQLite-backed”
        - current code supports Postgres or SQLite, with fallback behavior

        ## Old bundle drift
        - `chatgpt_sources_core_v2/` is not current architecture
        - it misses important current files such as `config.py`, `db_runtime.py`, `launch_plan.py`, `launch_dry_run.py`, and the current application-assist stack

        ## Portal and collector limits
        - Native collector path is focused on Greenhouse, Lever, and Ashby
        - Workday is detected and normalized but remains more manual
        - Workday prefill is limited manual support, not a full supported adapter path

        ## Prefill limits
        - `application-assist --prefill` is explicitly stop-before-submit
        - it fills only safe fields defined by portal adapters
        - unresolved required fields are expected on many real application pages

        ## Launch/open edge cases
        - `open` can open a manifest item with an `apply_url` even if that item would not be launchable in the stricter launch plan
        - `session start --open` and `session reopen` execute immediately and do not share the confirmation layer used by `launch-dry-run`

        ## Backend-selection nuance
        - runtime/backend selection follows env/config first
        - passing a SQLite path into helper functions does not automatically force SQLite if the resolved backend is still Postgres

        ## Queue scope limits
        - ranking and selection only operate on rows where `jobs.status = 'new'`
        - already-opened or already-applied jobs are intentionally outside the default queue

        ## Local browser assumptions
        - the local Chrome-profile assist flow is optimized for macOS local runs
        - remote/server workflows should prefer non-browser or `remote_print` paths
        """
    )


def render_operator_quickstart() -> str:
    return format_md(
        """
        # Operator Quickstart

        ## 1. Install and initialize
        ```bash
        python3.12 -m venv .venv
        source .venv/bin/activate
        python -m pip install --upgrade pip
        python -m pip install -e .
        cp .env.example .env
        jobs-ai init
        jobs-ai db init
        ```

        ## 2. Daily discover-first workflow
        ```bash
        jobs-ai run "python backend engineer remote" --limit 25
        jobs-ai session recent
        jobs-ai session inspect 1
        ```

        ## 3. Daily registry-first workflow
        ```bash
        jobs-ai run "python backend engineer remote" --use-registry --limit 25
        jobs-ai session recent
        jobs-ai session inspect 1
        ```

        ## 4. Modular workflow when you want the stages separate
        ```bash
        jobs-ai discover "python backend engineer remote" --collect --import
        jobs-ai session start --limit 25
        jobs-ai session recent
        ```

        ## 5. Remote-safe open flow
        ```bash
        jobs-ai session start --limit 20 --open --executor remote_print
        ```

        ## 6. Reopen or open a prior session
        ```bash
        jobs-ai session reopen 1
        jobs-ai open data/exports/<session-manifest>.json 2
        ```

        ## 7. Review-first browser assist
        ```bash
        jobs-ai application-assist data/exports/<session-manifest>.json --prefill --launch-order 1
        ```

        Optional post-browser logging:
        ```bash
        jobs-ai application-assist data/exports/<session-manifest>.json --prefill --launch-order 1 --log-outcome
        jobs-ai application-log --manifest data/exports/<session-manifest>.json --launch-order 1 --status applied --notes "manual submit after review"
        ```

        ## 8. Mark outcomes in the DB
        ```bash
        jobs-ai session mark applied --manifest data/exports/<session-manifest>.json --all
        jobs-ai track list --status applied
        jobs-ai stats --days 7
        ```

        ## 9. Inspect backend state
        ```bash
        jobs-ai db backend-status
        jobs-ai db ping
        jobs-ai db status
        ```
        """
    )


def build_priority_map() -> dict[str, int]:
    return {
        file_name: index
        for index, file_name in enumerate(PRIORITY_ORDER, start=1)
    }


def render_file_index_json() -> str:
    priority_map = build_priority_map()
    files = [
        {
            "file_name": meta.name,
            "purpose": meta.purpose,
            "source_paths": list(meta.source_paths),
            "kind": meta.kind,
            "priority_rank": priority_map[meta.name],
        }
        for meta in FILE_METADATA
    ]
    payload = {
        "pack_folder": "sources/jobs_ai_sources_pack_latest",
        "file_count": len(FILE_METADATA),
        "priority_order": list(PRIORITY_ORDER),
        "files": files,
    }
    return format_json(payload)


def render_upload_recommendation() -> str:
    optional_after_first_ten = [
        file_name
        for file_name in PRIORITY_ORDER
        if file_name not in FIRST_TEN_UPLOAD
    ]
    lines = [
        "# Upload Recommendation",
        "",
        "Use the same pack-wide priority order as `13_FILE_INDEX.json` for the full 21-file ranking.",
        "",
        "## Upload only 5 files",
        *[f"{index}. `{file_name}`" for index, file_name in enumerate(FIRST_FIVE_UPLOAD, start=1)],
        "",
        "Why these five:",
        "- they establish the canonical entrypoint",
        "- they explain the current architecture",
        "- they explain DB/runtime behavior",
        "- they explain the most important launch and safety boundaries",
        "- this 5-file cut intentionally swaps in `08_LAUNCH_EXECUTION_AND_SAFETY.md` earlier because safety matters more than deeper pipeline detail when space is tight",
        "",
        "## Upload 10 files",
        *[f"{index}. `{file_name}`" for index, file_name in enumerate(FIRST_TEN_UPLOAD, start=1)],
        "",
        "Why these ten:",
        "- they cover the CLI, architecture, DB/runtime, full job pipeline, discovery/import behavior, scoring/session behavior, launch safety, resume/application assist, and config",
        "- ChatGPT can reason about most repo-level questions from these ten alone",
        "",
        "## Upload all files",
        "Upload the full pack in this order:",
        *[f"{index}. `{file_name}`" for index, file_name in enumerate(PRIORITY_ORDER, start=1)],
        "",
        "## Optional files after the first 10",
        *[f"{index}. `{file_name}`" for index, file_name in enumerate(optional_after_first_ten, start=1)],
        "",
        "Optional-file notes:",
        "- `12_OPERATOR_QUICKSTART.md` is useful when you want direct command recipes.",
        "- `01_REPO_MAP.md` helps broad navigation but is less important than the workflow docs.",
        "- `11_KNOWN_LIMITATIONS_AND_GAPS.md` helps when you want caveats and rough edges called out explicitly.",
        "- `14_...` through `19_...` are best when ChatGPT needs exact code behavior, not just summaries.",
        "- `13_FILE_INDEX.json` is mainly for machine-readable navigation.",
    ]
    return "\n".join(lines) + "\n"


def render_priority_list(file_names: tuple[str, ...]) -> str:
    lines = []
    for index, file_name in enumerate(file_names, start=1):
        lines.append(f"{index}. `{file_name}`")
    return "\n".join(lines)


def render_all_contents() -> dict[str, str]:
    validate_repo_assumptions()
    contents = {
        "00_README_START_HERE.md": render_readme_start(),
        "01_REPO_MAP.md": render_repo_map(),
        "02_CLI_COMMANDS_AND_FLOW.md": render_cli_commands_and_flow(),
        "03_ARCHITECTURE_OVERVIEW.md": render_architecture_overview(),
        "04_DATA_MODEL_AND_DB.md": render_data_model_and_db(),
        "05_JOB_PIPELINE.md": render_job_pipeline(),
        "06_DISCOVERY_AND_IMPORT.md": render_discovery_and_import(),
        "07_SCORING_QUEUE_AND_SESSION_FLOW.md": render_scoring_queue_and_session_flow(),
        "08_LAUNCH_EXECUTION_AND_SAFETY.md": render_launch_execution_and_safety(),
        "09_RESUME_AND_APPLICATION_ASSIST.md": render_resume_and_application_assist(),
        "10_CONFIG_ENV_AND_PATHS.md": render_config_env_and_paths(),
        "11_KNOWN_LIMITATIONS_AND_GAPS.md": render_known_limitations(),
        "12_OPERATOR_QUICKSTART.md": render_operator_quickstart(),
        "13_FILE_INDEX.json": render_file_index_json(),
        "14_CODE_EXCERPT_CLI_ENTRYPOINTS.md": render_excerpt_document(
            "Code Excerpt: CLI Entrypoints",
            "Exact excerpts from the current repo for the canonical CLI entrypoints and the operator-facing commands that matter most.",
            CLI_EXCERPTS,
        ),
        "15_CODE_EXCERPT_DB_RUNTIME_AND_SCHEMA.md": render_excerpt_document(
            "Code Excerpt: DB Runtime and Schema",
            "Exact excerpts from the current repo for backend selection, runtime fallback, canonical schema, dedupe, and backend inspection helpers.",
            DB_EXCERPTS,
        ),
        "16_CODE_EXCERPT_DISCOVERY_COLLECTION_IMPORT.md": render_excerpt_document(
            "Code Excerpt: Discovery, Collection, and Import",
            "Exact excerpts from the current repo for top-level workflow orchestration, ATS discovery, source collection, registry-first collect/import, and JSON import normalization.",
            DISCOVERY_EXCERPTS,
        ),
        "17_CODE_EXCERPT_SESSION_MANIFEST_AND_HISTORY.md": render_excerpt_document(
            "Code Excerpt: Session Manifest and History",
            "Exact excerpts from the current repo for manifest export, manifest validation, inspect/reopen behavior, direct open behavior, and session mark resolution.",
            SESSION_EXCERPTS,
        ),
        "18_CODE_EXCERPT_LAUNCH_AND_EXECUTOR.md": render_excerpt_document(
            "Code Excerpt: Launch and Executor",
            "Exact excerpts from the current repo for preview selection, launchability rules, dry-run generation, and executor modes.",
            LAUNCH_EXCERPTS,
        ),
        "19_CODE_EXCERPT_RESUME_AND_APPLICATION_ASSIST.md": render_excerpt_document(
            "Code Excerpt: Resume and Application Assist",
            "Exact excerpts from the current repo for resume selection, applicant profiles, application assist, browser prefill, portal rules, and JSON application logging.",
            RESUME_EXCERPTS,
        ),
        "99_UPLOAD_RECOMMENDATION.md": render_upload_recommendation(),
    }
    missing = [name for name in PACK_FILE_ORDER if name not in contents]
    if missing:
        raise BuildError(f"missing generated contents for: {', '.join(missing)}")
    return contents


def validate_repo_assumptions() -> None:
    required_anchors = {
        "README.md": (
            'jobs-ai run "python backend engineer remote" --limit 25',
            "application-assist --prefill",
            "Database Backends",
            "Neon Migration",
        ),
        "docs/architecture.md": (
            "SQLite-backed pipeline",
            "Workday",
        ),
        "scripts/README.md": (
            "canonical operator workflow is the main CLI",
        ),
        "pyproject.toml": (
            'jobs-ai = "jobs_ai.cli:run"',
        ),
        "src/jobs_ai/cli.py": (
            '@app.command("run")',
            '@session_app.command("start")',
            '@sources_app.command("collect")',
            '@app.command("application-assist")',
            '@db_app.command("backend-status")',
            'def run(argv: Sequence[str] | None = None) -> int:',
        ),
        "src/jobs_ai/run_workflow.py": (
            "def run_operator_workflow(",
            "intake_mode=\"registry\"",
            "intake_mode=\"discover\"",
        ),
        "src/jobs_ai/config.py": (
            'DEFAULT_DB_BACKEND = "postgres"',
            "POSTGRES_SQLITE_FALLBACK_WARNING",
        ),
        "src/jobs_ai/db_runtime.py": (
            "POSTGRES_SQLITE_RUNTIME_FALLBACK_WARNING",
            "def connect_database(",
        ),
        "src/jobs_ai/session_start.py": (
            "def start_session(",
            "record_session_history(",
        ),
        "src/jobs_ai/application_prefill.py": (
            "def run_application_prefill(",
            "stopped_before_submit=True",
        ),
    }
    for relative_path, anchors in required_anchors.items():
        text = read_text(relative_path)
        for anchor in anchors:
            if anchor not in text:
                raise BuildError(
                    f"expected anchor {anchor!r} was not found in {relative_path}"
                )


def write_pack(contents: dict[str, str]) -> None:
    shutil.rmtree(OUTPUT_DIR, ignore_errors=True)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    for file_name in PACK_FILE_ORDER:
        path = OUTPUT_DIR / file_name
        path.write_text(contents[file_name], encoding="utf-8")


def compute_hashes() -> dict[str, str]:
    hashes: dict[str, str] = {}
    for path in sorted(OUTPUT_DIR.glob("*")):
        hashes[path.name] = hashlib.sha256(path.read_bytes()).hexdigest()
    return hashes


def validate_pack() -> None:
    if not OUTPUT_DIR.exists():
        raise BuildError(f"output folder was not created: {OUTPUT_DIR}")

    files = sorted(path.name for path in OUTPUT_DIR.glob("*") if path.is_file())
    if len(files) != EXPECTED_FILE_COUNT:
        raise BuildError(
            f"expected {EXPECTED_FILE_COUNT} files, found {len(files)}"
        )
    unexpected = sorted(set(files) - set(PACK_FILE_ORDER))
    missing = [name for name in PACK_FILE_ORDER if name not in files]
    if unexpected:
        raise BuildError(f"unexpected files in pack: {', '.join(unexpected)}")
    if missing:
        raise BuildError(f"missing files in pack: {', '.join(missing)}")

    for file_name in PACK_FILE_ORDER:
        if file_name.endswith(".md") and file_name[:2].isdigit():
            if not (OUTPUT_DIR / file_name).read_text(encoding="utf-8").strip():
                raise BuildError(f"overview file is empty: {file_name}")
    if not (OUTPUT_DIR / "99_UPLOAD_RECOMMENDATION.md").read_text(encoding="utf-8").strip():
        raise BuildError("99_UPLOAD_RECOMMENDATION.md is empty")

    index_payload = json.loads((OUTPUT_DIR / "13_FILE_INDEX.json").read_text(encoding="utf-8"))
    if not isinstance(index_payload, dict):
        raise BuildError("13_FILE_INDEX.json must be a JSON object")
    files_payload = index_payload.get("files")
    if not isinstance(files_payload, list):
        raise BuildError("13_FILE_INDEX.json must contain a files list")
    if len(files_payload) != EXPECTED_FILE_COUNT:
        raise BuildError(
            "13_FILE_INDEX.json file_count does not match the generated file count"
        )
    required_fields = {"file_name", "purpose", "source_paths", "kind", "priority_rank"}
    for entry in files_payload:
        if not isinstance(entry, dict):
            raise BuildError("13_FILE_INDEX.json contains a non-object file entry")
        missing_fields = sorted(required_fields - set(entry))
        if missing_fields:
            raise BuildError(
                "13_FILE_INDEX.json entry is missing fields: "
                + ", ".join(missing_fields)
            )
        source_paths = entry["source_paths"]
        if not isinstance(source_paths, list):
            raise BuildError("13_FILE_INDEX.json source_paths must be lists")

    validate_excerpt_sources()


def validate_excerpt_sources() -> None:
    for group in (
        CLI_EXCERPTS,
        DB_EXCERPTS,
        DISCOVERY_EXCERPTS,
        SESSION_EXCERPTS,
        LAUNCH_EXCERPTS,
        RESUME_EXCERPTS,
    ):
        for spec in group:
            start_line, end_line, code = extract_excerpt(spec)
            if start_line < 1 or end_line < start_line:
                raise BuildError(f"invalid line range for {spec.title}")
            if not code.strip():
                raise BuildError(f"empty code excerpt for {spec.title}")


def build_and_hash() -> dict[str, str]:
    contents = render_all_contents()
    write_pack(contents)
    validate_pack()
    return compute_hashes()


def print_summary() -> None:
    print("PACK READY")
    print(str(OUTPUT_DIR.resolve()))
    print(str(EXPECTED_FILE_COUNT))
    for index, file_name in enumerate(PACK_FILE_ORDER, start=1):
        print(f"{index}. {file_name}")


def main() -> int:
    try:
        first_hashes = build_and_hash()
        second_hashes = build_and_hash()
        if first_hashes != second_hashes:
            raise BuildError("determinism check failed: output hashes changed between runs")
        validate_pack()
    except (BuildError, OSError, json.JSONDecodeError) as exc:
        print(f"failed to build sources pack: {exc}", file=sys.stderr)
        return 1

    print_summary()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
