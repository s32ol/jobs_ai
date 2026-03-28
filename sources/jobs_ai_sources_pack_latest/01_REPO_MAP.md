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
