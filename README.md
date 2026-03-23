# jobs_ai

A Python-first job search automation toolkit that discovers ATS job sources, collects postings, ranks opportunities, and prepares clean application sessions without becoming a blind auto-apply bot.

## Why this exists

Technical job searches usually break down in familiar ways: job sources are scattered across different portals, the same opening shows up multiple times, browser tabs turn into an unmanageable working set, resume targeting gets inconsistent from one application to the next, and manual follow-up tracking falls behind once applications are in flight.

`jobs_ai` is built to keep that process local, structured, and reviewable. It discovers ATS job sources, collects and normalizes jobs from supported portals, imports leads into SQLite or Postgres, ranks the queue, prepares deterministic session manifests, suggests resume and profile variants, and tracks manual outcomes after real applications happen.

## What makes it different

- ATS-first instead of scraping everything blindly
- deterministic local workflow
- human-in-the-loop by design
- scoped session batches
- ranking + resume recommendations included

## Core Features

### ATS Discovery Engine

- Search-driven discovery of job board roots
- Supports Greenhouse, Lever, and Ashby discovery paths
- Surfaces unsupported/manual-review results separately

### Structured ATS Collectors

- Native collectors for Greenhouse, Lever, and Ashby
- Produces importer-ready leads
- Handles normalization and portal-specific parsing

### Company-First Source Seeding

- Seed likely ATS board roots from company names/domains
- Confirm reusable sources
- Useful for building a reusable source registry

### Ranked Application Queue

- Scores jobs before action
- Uses title, stack, geography, and source signals
- Produces a focused working set

### Resume Recommendation Layer

- Suggests best-fit resume/profile snippets
- Covers data engineering, analytics engineering, and observability/telemetry paths

### Session-Based Workflow

- Freeze deterministic application batches
- Export, inspect, and reopen manifests
- Mark outcomes after manual apply work

### Manual Tracking and Stats

- Track opened, applied, interview, rejected, offer, etc.
- Review recent sessions and throughput

## High-Level Workflow

```text
discover -> collect -> import -> rank -> session start -> manual apply -> track -> stats
```

The automation handles sourcing, dedupe, ranking, and session prep. The human handles final submission, edge cases, and judgment calls.

## Quick Start

```bash
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e .
cp .env.example .env

jobs-ai init
jobs-ai db init
jobs-ai run "python backend engineer remote" --limit 25
```

## Configuration

`jobs_ai` uses a root `.env` file plus normal environment variables. The repo-native settings are:

- `JOBS_AI_DB_BACKEND=sqlite|postgres`
- `JOBS_AI_SQLITE_PATH=data/jobs_ai.db`
- `DATABASE_URL=postgresql://...`

Backward compatibility is preserved for the older `JOBS_AI_DB_PATH` variable, but `JOBS_AI_SQLITE_PATH` is now the preferred name.

If you do not want one `DATABASE_URL`, you can also provide standard Postgres pieces such as:

- `PGHOST`
- `PGPORT`
- `PGDATABASE`
- `PGUSER`
- `PGPASSWORD`
- `PGSSLMODE=require`

## VS Code Workspace

The repo includes `jobs_ai.code-workspace` for a repeatable local VS Code setup.

Open it with `code jobs_ai.code-workspace` or use VS Code's `File -> Open Workspace from File...`.

The workspace helps standardize the repo-local interpreter and a few practical project settings for this `src/`-layout, pytest-based workflow.

## Main Commands

### `jobs-ai run`

Preferred daily path: discover sources, collect jobs, import them, and freeze one session.

```bash
jobs-ai run "python backend engineer remote" --limit 25
```

### `jobs-ai fast-apply`

Build a compact likely-fit shortlist from already collected/imported jobs.

```bash
jobs-ai fast-apply --limit 20 --families data,backend,software --easy-apply-first
```

### `jobs-ai discover`

Search for ATS-backed sources from a query and optionally carry them into later stages.

```bash
jobs-ai discover "data engineer remote" --collect --import
```

### `jobs-ai collect`

Collect jobs from known ATS sources.

```bash
jobs-ai collect https://boards.greenhouse.io/example https://jobs.lever.co/example
```

### `jobs-ai seed-sources`

Infer likely ATS board roots from company names or domains.

```bash
jobs-ai seed-sources "Example Company | example.com"
```

### `jobs-ai session start`

Freeze a ranked application batch into a durable manifest.

```bash
jobs-ai session start --limit 20
```

For SSH or tunnel-based workflows, print remote-safe launch targets instead of trying to open a browser on the remote machine:

```bash
jobs-ai session start --limit 20 --open --executor remote_print
```

This prints the application URLs to stdout for local clicking/copying while preserving the normal manifest export, dry-run planning, and manual-review flow.

### `jobs-ai application-assist --prefill`

Open one application in a review-first Playwright browser session, fill supported safe fields, upload the recommended resume, and stop before submit.

On local macOS runs, `application-assist --prefill` reuses a dedicated Chrome profile by default:

- browser channel: `chrome`
- user data dir: `~/Library/Application Support/Google/Chrome`
- profile directory: `Profile 2`

Override those defaults with:

- `JOBS_AI_BROWSER_CHANNEL`
- `JOBS_AI_BROWSER_USER_DATA_DIR`
- `JOBS_AI_BROWSER_PROFILE_DIRECTORY`

Only the local autofill/browser-assist path uses these settings. Other commands and remote/server-safe workflows keep their existing behavior.

When you want a faster handoff after manual review, add `--log-outcome` to prompt for status/notes after the browser closes, or use `--log-status ... --log-notes ...` to write the application log non-interactively on exit.

### `jobs-ai application-log`

Write one JSON log per manually handled application under `data/applications/`.

```bash
jobs-ai application-log --manifest data/exports/<session-manifest>.json --launch-order 2 --status applied --notes "prefill + manual fix"
```

### `jobs-ai session recent`

Review recently created session manifests.

```bash
jobs-ai session recent
```

### `jobs-ai session inspect`

Inspect a past session by recorded session id or manifest path.

```bash
jobs-ai session inspect 1
```

### `jobs-ai session mark`

Record outcomes for jobs from a session.

```bash
jobs-ai session mark applied --manifest data/exports/<session-manifest>.json --all
```

### `jobs-ai track list`

List current tracked statuses, optionally filtered by status.

`jobs-ai applied` is the quick shortcut for viewing jobs already marked applied.

```bash
jobs-ai track list --status opened
```

### `jobs-ai stats`

Show recent throughput and application activity.

```bash
jobs-ai stats --days 7
```

## Database Backends

### Database backend behavior

Postgres is the default when `JOBS_AI_DB_BACKEND` is unset and a `DATABASE_URL` or equivalent `PG*` settings are available.

If Postgres connection settings are missing, `jobs_ai` automatically falls back to SQLite at `JOBS_AI_SQLITE_PATH`.

To override manually, set `JOBS_AI_DB_BACKEND=postgres` or `JOBS_AI_DB_BACKEND=sqlite`.

Use these commands to inspect whichever backend resolved:

```bash
python -m jobs_ai db backend-status
python -m jobs_ai db ping
python -m jobs_ai db status
```

To force Neon/Postgres explicitly, set:

```bash
JOBS_AI_DB_BACKEND=postgres
DATABASE_URL=postgresql://user:password@your-neon-host/neondb?sslmode=require
```

Then verify the connection:

```bash
python -m jobs_ai db backend-status
python -m jobs_ai db ping
python -m jobs_ai db status
```

`db init` creates or backfills the schema for whichever backend is active.

## Neon Migration

Use `jobs-ai db migrate-to-postgres` when the SQLite database on this machine is the canonical source of truth and you want to copy it into Neon/Postgres.

The command:

- initializes/backfills the source SQLite schema before reading it
- creates the equivalent Postgres schema when missing
- preserves `jobs`, `applications`, `application_tracking`, `session_history`, and `source_registry`
- keeps child relationships by remapping `job_id` safely when a target duplicate already exists
- reuses the existing job identity logic based on `apply_url`, `canonical_apply_url`, and `identity_key`
- is safe to rerun because rows are inserted or updated by deterministic ids, with duplicate-aware job remapping when needed

Preview the migration from the server SQLite database into Neon:

```bash
python -m jobs_ai db migrate-to-postgres --dry-run
```

Run the real migration:

```bash
python -m jobs_ai db migrate-to-postgres
```

After the data is in Neon, you can rely on the default Postgres selection or force it explicitly:

```bash
python -m jobs_ai db ping
python -m jobs_ai stats --days 7
```

If you prefer to stay local or need an offline fallback later, set:

```bash
export JOBS_AI_DB_BACKEND=sqlite
```

## SQLite-to-SQLite Merge

Use `jobs-ai db merge` when the server database is the canonical SQLite copy and you need to fold in a second SQLite file from another machine without blindly replacing the master.

```bash
python -m jobs_ai db merge /tmp/jobs_ai.macbook.db --dry-run
python -m jobs_ai db merge /tmp/jobs_ai.macbook.db --backup --vacuum
```

If the canonical SQLite database is not the default `data/jobs_ai.db`, point at it explicitly:

```bash
python -m jobs_ai db merge /tmp/jobs_ai.macbook.db --target /srv/jobs_ai/jobs_ai.db --backup
```

## Ubuntu Server Commands

Once your `.env` contains a full Neon `DATABASE_URL` or the equivalent `PGHOST`/`PGDATABASE` pieces, these are the exact commands to run on the server:

```bash
cd /home/your-user/Projects/jobs_ai
source .venv/bin/activate
python -m pip install -e .
PYTHONPATH=src python -m jobs_ai db backend-status
PYTHONPATH=src python -m jobs_ai db ping
PYTHONPATH=src python -m jobs_ai db migrate-to-postgres --dry-run
PYTHONPATH=src python -m jobs_ai db migrate-to-postgres
JOBS_AI_DB_BACKEND=postgres PYTHONPATH=src python -m jobs_ai db ping
JOBS_AI_DB_BACKEND=postgres PYTHONPATH=src python -m jobs_ai stats --days 7
```

If your canonical SQLite file lives somewhere else on the server:

```bash
PYTHONPATH=src python -m jobs_ai db migrate-to-postgres --source-sqlite /srv/jobs_ai/jobs_ai.db
```

## MacBook Setup

To point a MacBook at the same centralized Neon database later:

1. Pull the repo and install the project dependencies.
2. Create a local root `.env` with the same `DATABASE_URL` and `JOBS_AI_DB_BACKEND=postgres`.
3. Keep `JOBS_AI_SQLITE_PATH=data/jobs_ai.db` in that file so SQLite fallback still works locally.
4. Verify the shared database with `PYTHONPATH=src python -m jobs_ai db backend-status`.
5. Verify connectivity with `PYTHONPATH=src python -m jobs_ai db ping`.
6. Run normal commands such as `PYTHONPATH=src python -m jobs_ai stats --days 7` or `PYTHONPATH=src python -m jobs_ai track list`.

Example MacBook `.env` values:

```bash
JOBS_AI_DB_BACKEND=postgres
JOBS_AI_SQLITE_PATH=data/jobs_ai.db
DATABASE_URL=postgresql://user:password@your-neon-host/neondb?sslmode=require
```

## Architecture Snapshot

- `src/jobs_ai/discover`: search-planned ATS discovery, verification, and manual-review reporting
- `src/jobs_ai/collect`: portal-specific collection adapters and artifact writers for importer-ready leads
- `src/jobs_ai/jobs`: normalization, dedupe identity, import, scoring, filtering, and queue selection
- `src/jobs_ai/resume`: resume variant resolution and recommendation logic for ranked jobs
- `src/jobs_ai/application_tracking.py`: status transitions and timeline reads for manual application tracking
- Session-related modules: `session_start.py`, `session_manifest.py`, `session_export.py`, `session_history.py`, `session_mark.py`, `launch_plan.py`, and `launch_preview.py`
- Source seeding and registry workflows live in `src/jobs_ai/source_seed` and `src/jobs_ai/sources`

## Design Philosophy

- local-first
- deterministic
- human-in-the-loop
- no blind auto-apply
- safe launch behavior

## Current Limitations

- not a form submission bot
- depends on public ATS structure
- Workday is still more manual
- browser automation is intentionally limited

## Future Directions

- stronger source registry workflows
- richer manual-review and Workday paths
- better UI/feed layer
- stronger analytics/exports
