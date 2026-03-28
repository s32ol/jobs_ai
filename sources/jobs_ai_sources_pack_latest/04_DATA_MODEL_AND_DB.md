# Data Model and DB

The current backend story is defined by `src/jobs_ai/config.py`, `src/jobs_ai/db_runtime.py`, `src/jobs_ai/db.py`, `src/jobs_ai/db_postgres.py`, and `src/jobs_ai/maintenance.py`.

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
  - also stores ingest and dedupe metadata: `ingest_batch_id`, `source_query`, `import_source`, `source_registry_id`, `canonical_apply_url`, `identity_key`, `applied_at`
  - current tracked statuses in practice include `new`, `opened`, `applied`, `recruiter_screen`, `assessment`, `interview`, `offer`, `rejected`, `skipped`, `invalid_location`, and `superseded`
- `applications`
  - per-job application state, notes, resume variant, and timestamps
  - still contributes applied evidence when canonical duplicate groups are resolved
- `application_tracking`
  - append-only status history such as `opened`, `applied`, `interview`, `offer`, `rejected`, `invalid_location`, and `superseded`
- `session_history`
  - records exported manifest path, item counts, launchable counts, batch id, and source query
- `source_registry`
  - durable ATS source registry keyed by `normalized_url`

## Duplicate and canonical URL behavior
Duplicate matching is centralized in `src/jobs_ai/db.py` and `src/jobs_ai/jobs/identity.py`.

Match order:
1. exact `apply_url`
2. `canonical_apply_url`
3. `identity_key`

Identity behavior:
- if `source_job_id` exists, prefer `portal-or-host | job_id | source_job_id`
- otherwise use `portal-or-host-or-source | company | title | location`
- canonical apply URLs use `src/jobs_ai/portal_support.py` to strip tracking params and promote company-scoped Greenhouse/Ashby job URLs when possible

Import-time nuance:
- exact or identity duplicates can still be skipped before insert
- canonical URL siblings can still be inserted, then resolved into one preferred row with sibling rows marked `superseded`
- queue selection and session exports operate on the surviving actionable rows, not the superseded siblings

## Canonical duplicate repair behavior
- `resolve_canonical_duplicate_group()` chooses one winner per `canonical_apply_url` group
- winner selection prefers stronger downstream evidence such as applied state, then uses title quality, score, and stable id tie-breakers
- sibling rows are marked `superseded`
- if a duplicate group already has applied evidence, one preferred row can be reactivated or preserved as `applied` while siblings stay `superseded`
- `jobs-ai maintenance supersede-duplicates` reruns this logic across existing duplicate groups without requiring reimport

## Schema and backfill behavior
- `initialize_schema()` creates missing tables and indexes
- it also ensures newer `jobs` columns such as `canonical_apply_url`, `identity_key`, and `applied_at`
- it backfills `jobs.applied_at` from the newest `application_tracking` row whose status is `applied` when older DBs are missing that column
- it backfills `canonical_apply_url` and `identity_key` for older job rows
- SQLite uses `sqlite3` with foreign keys enabled
- Postgres uses `psycopg` with a thin adapter so the rest of the code can keep using DB-agnostic calls

## Migration and merge
- `src/jobs_ai/db_postgres.py`
  - `build_backend_status()`
  - `ping_database_target()`
  - `migrate_sqlite_to_postgres()`
  - migration carries `canonical_apply_url`, `identity_key`, and `applied_at`, and remaps duplicate-linked child rows safely
- `src/jobs_ai/db_merge.py`
  - merges a second SQLite DB into the target SQLite DB
  - reuses the same duplicate matcher used by import
  - preserves duplicate-aware identity fields and `applied_at` when present
  - remaps related application/session rows safely

## Current doc drift to keep in mind
- `README.md` reflects the current Postgres-or-SQLite story
- `docs/architecture.md` still says “SQLite-backed”
