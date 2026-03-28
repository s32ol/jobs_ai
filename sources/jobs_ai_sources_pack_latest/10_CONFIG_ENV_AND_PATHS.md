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
