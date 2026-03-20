# jobs_ai

A Python-first job search automation toolkit that discovers ATS job sources, collects postings, ranks opportunities, and prepares clean application sessions without becoming a blind auto-apply bot.

## Why this exists

Technical job searches usually break down in familiar ways: job sources are scattered across different portals, the same opening shows up multiple times, browser tabs turn into an unmanageable working set, resume targeting gets inconsistent from one application to the next, and manual follow-up tracking falls behind once applications are in flight.

`jobs_ai` is built to keep that process local, structured, and reviewable. It discovers ATS job sources, collects and normalizes jobs from supported portals, imports leads into SQLite, ranks the queue, prepares deterministic session manifests, suggests resume and profile variants, and tracks manual outcomes after real applications happen.

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

jobs-ai init
jobs-ai db init
jobs-ai run "python backend engineer remote" --limit 25
```

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

```bash
jobs-ai track list --status opened
```

### `jobs-ai stats`

Show recent throughput and application activity.

```bash
jobs-ai stats --days 7
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
