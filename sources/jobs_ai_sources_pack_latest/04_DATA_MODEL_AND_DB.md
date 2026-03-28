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
