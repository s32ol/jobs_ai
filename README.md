# jobs_ai

`jobs_ai` is a local, operator-driven CLI for running a disciplined job-search pipeline from ATS discovery through manual application tracking.

It is built for a real human operator, not for blind auto-apply. The goal is to keep sourcing, dedupe, ranking, session scope, launch prep, and outcome tracking deterministic while leaving the risky submission steps manual and visible.

## What It Solves

Job searches usually break down in the same places:

- leads come from multiple portals and duplicate easily
- ad hoc browser sessions lose the working set
- resume and profile choices drift across tabs
- older `new` jobs bleed into today's sprint
- manual tracking falls behind what actually happened

`jobs_ai` addresses that with a local SQLite-backed workflow that supports:

- ATS-first discovery and collection
- deterministic import and dedupe
- lightweight scoring and queueing
- scoped session manifests for daily application batches
- portal hints and read-only launch planning
- manual status tracking and compact analytics

## Operator Positioning

This repository is now shaped around an operator-ready daily path:

```bash
jobs-ai run "python backend engineer remote" --limit 25 --open
```

That unified command drives:

```text
discover -> collect -> import -> session start
```

It writes a durable manifest for the selected batch, can optionally open launchable URLs with the safe browser executor, and keeps the session scoped to the jobs imported by that run.

The older manual staircase is still present for inspection-heavy workflows, but it is no longer the primary operator path.

## Quick Start

Create a venv and install the repo in editable mode:

```bash
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e .
```

Optional local config:

```bash
cp .env.example .env
```

Initialize the workspace and database:

```bash
jobs-ai init
jobs-ai doctor
jobs-ai db init
jobs-ai db status
```

Minimal local workflow using the bundled sample leads:

```bash
jobs-ai import data/raw/sample_job_leads.json
jobs-ai session start --limit 5
jobs-ai session recent
jobs-ai stats --days 7
```

Daily operator workflow from live ATS discovery:

```bash
jobs-ai run "python backend engineer remote" --limit 25 --open --portal-hints
jobs-ai session mark opened --manifest data/processed/<run-dir>/launch-preview-session-<timestamp>.json --all
jobs-ai track list --status opened
jobs-ai stats --days 7
```

After installation, `jobs-ai` is the preferred operator-facing entrypoint. `python -m jobs_ai ...` is also supported.

## Installation

The canonical local install path is:

```bash
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e .
```

Useful first checks:

```bash
jobs-ai --help
jobs-ai status
```

Environment knobs from `.env.example`:

- `JOBS_AI_ENV`
- `JOBS_AI_PROFILE`
- `JOBS_AI_DB_PATH`
- optional resume path overrides such as `JOBS_AI_RESUME_DATA_ENGINEERING_PATH`

## Primary Operator Workflow

`jobs-ai run` is the preferred daily command for a live search sprint.

Example:

```bash
jobs-ai run "python backend engineer remote" --limit 25 --open
```

What it does:

- discovers likely ATS sources for the query
- collects supported sources into importer-ready leads
- imports those leads into the local SQLite database
- freezes one scoped session manifest from that run's imported jobs
- optionally opens launchable application URLs using the selected executor

Useful options:

- `--discover-limit` caps how many ATS candidates are verified during discovery
- `--collect-limit` caps how many confirmed sources move into collection
- `--portal-hints` prints portal detection and normalization details when available
- `--open` reuses the current safe browser-open behavior after the session is frozen
- `--executor noop|browser_stub` controls whether launch actions are simulated or opened
- `--label` and `--out-dir` make run artifacts easier to organize
- `--json` prints the final run summary as JSON

After the run, the operator still performs the actual application steps manually, then records outcomes with `session mark` or `track`.

## Modular Workflow

If you want more control over each handoff, the repo supports a modular operator flow.

### 1. Discover Or Collect

`jobs-ai discover` starts from a search query and is the high-level upstream helper.

Example:

```bash
jobs-ai discover "data engineer remote"
jobs-ai discover "backend engineer remote" --collect --import
```

Artifacts written by `discover`:

- `discover_report.json`
- `confirmed_sources.txt`
- `manual_review_sources.json`

`jobs-ai collect` is the lower-level collector when you already know the source URLs.

Example:

```bash
jobs-ai collect https://boards.greenhouse.io/acme https://jobs.lever.co/northwind
jobs-ai collect --from-file sources.txt
```

Artifacts written by `collect`:

- `run_report.json`
- `leads.import.json`
- `manual_review.json`

`jobs-ai seed-sources` is the company-first helper for inferring likely ATS board roots from company names or domains.

### 2. Import

Use `jobs-ai import` for importer-shaped JSON, including the leads produced by `collect`.

```bash
jobs-ai import data/raw/sample_job_leads.json
jobs-ai import data/processed/<collect-run>/leads.import.json --batch-id morning-batch
```

The importer performs:

- required-field validation
- lightweight normalization
- canonical URL and identity-based dedupe
- optional batch tagging for later session scoping

### 3. Queue And Recommend

These commands are still available for inspection-heavy workflows:

```bash
jobs-ai score
jobs-ai queue --limit 10
jobs-ai recommend --limit 10
```

- `score` applies rule-based ranking to the stored jobs
- `queue` shows the top ranked jobs whose current status is `new`
- `recommend` attaches resume/profile suggestions to the current queue

### 4. Preview And Export A Session

The advanced manual path remains available:

```bash
jobs-ai launch-preview --limit 10 --portal-hints
jobs-ai export-session --limit 10
```

- `launch-preview` is read-only and shows the launchable working set
- `export-session` writes a JSON manifest for the current preview set

For most day-to-day work, `jobs-ai session start` is the better modular command because it freezes and exports the same batch immediately.

### 5. Freeze A Session

`jobs-ai session start` is the preferred modular batch-freeze command.

```bash
jobs-ai session start --limit 10
jobs-ai session start --batch-id discover-morning-20260315T101500Z --limit 10 --open
```

It can:

- select the top ranked `new` jobs
- scope the session to one import or discover batch with `--batch-id`
- attach resume recommendations
- include portal hint details
- export the exact selected batch to a manifest
- optionally open launchable URLs with `--open`

Session history utilities:

```bash
jobs-ai session recent
jobs-ai session inspect 12
jobs-ai session reopen 12
```

### 6. Preflight, Launch Planning, And Assist

These commands operate on an exported session manifest:

```bash
jobs-ai preflight data/exports/launch-preview-session-<timestamp>.json
jobs-ai launch-plan data/exports/launch-preview-session-<timestamp>.json
jobs-ai application-assist data/exports/launch-preview-session-<timestamp>.json --portal-hints
jobs-ai launch-dry-run data/exports/launch-preview-session-<timestamp>.json --executor noop
```

- `preflight` validates the manifest payload
- `launch-plan` identifies which manifest items are launchable
- `application-assist` shows read-only resume/profile guidance
- `launch-dry-run` prints the launch sequence and can optionally open tabs via `browser_stub`

### 7. Manual Apply And Tracking

Actual submission remains manual. After opening or applying, record what happened:

```bash
jobs-ai session mark opened --manifest data/exports/launch-preview-session-<timestamp>.json --all
jobs-ai session mark applied --manifest data/exports/launch-preview-session-<timestamp>.json --index 1
jobs-ai track mark 42 interview
jobs-ai track list --status applied
jobs-ai track status 42
```

Supported tracking states include:

- `new`
- `opened`
- `applied`
- `recruiter_screen`
- `assessment`
- `interview`
- `offer`
- `rejected`
- `skipped`

### 8. Stats And Maintenance

```bash
jobs-ai stats --days 7
jobs-ai stats --json
jobs-ai maintenance backfill --dry-run
```

- `stats` summarizes jobs, status distribution, recent imports, sessions, and portal counts
- `maintenance backfill` upgrades older rows with newer derived metadata without rewriting history

## Supported ATS Portals

Current portal handling is:

- Greenhouse: supported for source discovery, structured collection, direct-job normalization, portal hints, and company-scoped apply links when available.
- Lever: supported for source discovery, structured collection, normalization, and portal hints.
- Ashby: supported for source discovery, structured collection, normalization, and portal hints.
- Workday: detected and normalized for manual review and portal hints, but not auto-confirmed into `confirmed_sources.txt` and not collected into structured importer-ready leads.

In practice, that means Greenhouse, Lever, and Ashby are the structured ATS intake path today. Workday remains operator-assisted.

## Known Limitations

- This is not an auto-apply bot. Real form submission is intentionally manual.
- `discover` and `collect` depend on live network access and on public ATS pages exposing enough structure to parse safely.
- Workday support is partial by design: detection and hints are present, but structured automatic collection is not.
- Browser opening is limited to the current safe launch executor modes: `noop` and `browser_stub`.
- Resume resolution depends on local file paths or environment configuration; if a variant cannot be resolved, the CLI falls back to summary guidance instead of inventing a file.
- The advanced manual staircase (`score -> queue -> recommend -> launch-preview -> export-session`) is available, but `session start` is the safer way to freeze a deterministic batch because it avoids preview/export drift.

## Repo Map

- `src/jobs_ai`: package source for the CLI, collectors, importer, queueing, session flow, tracking, and analytics.
- `tests`: unittest coverage for CLI flows, collectors, importer, session handling, tracking, and analytics.
- `scripts`: small operational helpers that are not the primary operator entrypoint.
- `docs`: top-level release notes and operator-facing documentation landing area.

## Development Notes

- The installed console script is `jobs-ai`.
- The package also supports `python -m jobs_ai`.
- Workspace artifacts default to `data/`, `sessions/`, and `logs/` under the repo root.
- The local SQLite database defaults to `data/jobs_ai.db`.
