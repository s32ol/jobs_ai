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
