# jobs_ai

`jobs_ai` is a pragmatic local Python tool for a semi-automated job application exoskeleton.

The intent is not "auto-apply everywhere." The tool is meant to help a human operator move faster by handling search ingestion, cleanup, scoring, queue building, session launch, and application tracking in small safe steps.

## Current status

Milestone 11 operational polish is now in place, while retry queue behavior remains deferred. The repo currently includes:

- a Typer-based local CLI control tower
- environment and path configuration
- workspace bootstrap helpers for upcoming milestones
- a local SQLite database backbone in the project data area
- minimal `jobs` and `applications` tables
- small database helpers for connect/init/schema checks
- `db init` and `db status` CLI commands
- a small JSON file import flow that inserts leads into `jobs`
- per-record validation for required import fields with clear skip/error reporting
- a light normalization pass for selected import fields before insert
- a first deterministic dedupe pass before insert using exact `apply_url` matches or an exact `source + company + title + location` fallback when `apply_url` is missing
- a lightweight rule-based scoring pass for stored jobs based on title fit, stack signals, geography, and source priority
- a read-only apply queue view that selects the top ranked `new` jobs as a deterministic working set
- a read-only recommendation view that suggests resume variants and profile snippets for queued jobs using transparent keyword rules
- a read-only launch preview that shows queued jobs plus the apply URL and recommendation inputs that would be used in an application session
- a read-only session export that writes the current launch-preview working set to JSON under `data/exports`
- a read-only manifest preflight command that validates exported session JSON and shows a compact item preview with warnings for incomplete entries
- a read-only launch planner that consumes preflighted manifest data, preserves item order, and marks which entries are safe for future launch automation
- a launch dry-run adapter that consumes launch-plan items only and prints a compact ordered summary of the launch sequence
- a tiny launch executor adapter that accepts explicit launch steps and routes them through either a no-op executor or a browser-backed executor
- a browser executor mode that opens launchable URLs through Python `webbrowser` without form fill or application state mutation
- CLI safety controls that can preview, limit, and confirm browser launches before any tabs open
- a read-only application assist command that surfaces resume and snippet guidance for launchable items
- an optional portal support helper layer for Greenhouse, Lever, Ashby, and Workday URL detection, hints, and safe link normalization
- a manual application tracking layer that records explicit status updates and timestamps through the CLI
- workflow-oriented CLI output with next-step guidance for common operator flows
- clearer help text and README examples tuned for repeated sprint use
- smoke tests for schema initialization and required tables
- preserved legacy resume materials for later snippet/profile work

Fuzzy matching, semantic matching, record merging, stateful queue advancement, and application mutation are intentionally deferred to later milestones.

## Search focus

Target roles:

- Data Engineer
- Analytics Engineer
- Telemetry / Observability Engineer
- Platform Data Engineer
- BigQuery / GCP-oriented roles

Search/application priority:

1. staffing agencies / recruiter-driven contract roles
2. contract platforms
3. vendor / consulting ecosystems
4. direct company portals

Geography priority:

1. Remote
2. Sacramento / Folsom
3. San Jose / Bay Area

## Quick start

Create a virtual environment and install the package:

```bash
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e .
```

Optional environment file:

```bash
cp .env.example .env
```

Show the command map and the recommended sprint flow:

```bash
python -m jobs_ai --help
```

## Recommended sprint flow

When you are working through a real batch, keep it small and deterministic:

1. Bootstrap the workspace and database once for the local checkout.
2. Import a fresh lead batch, then score it.
3. Work from a limited queue and recommendations set.
4. Preview and export one manifest for the next launch batch.
5. Preflight and review the manifest before any browser tabs open.
6. Use `application-assist` for copy/snippet guidance.
7. Launch with `launch-dry-run --executor browser_stub --limit <n> --confirm`.
8. Record progress immediately with `track mark`.

Example sprint loop:

```bash
python -m jobs_ai init
python -m jobs_ai db init
python -m jobs_ai import data/raw/sample_job_leads.json
python -m jobs_ai score
python -m jobs_ai queue --limit 10
python -m jobs_ai recommend --limit 10
python -m jobs_ai launch-preview --limit 10 --portal-hints
python -m jobs_ai export-session --limit 10
python -m jobs_ai preflight data/exports/launch-preview-session-<timestamp>.json
python -m jobs_ai application-assist data/exports/launch-preview-session-<timestamp>.json
python -m jobs_ai launch-dry-run --executor browser_stub --limit 5 --confirm data/exports/launch-preview-session-<timestamp>.json
python -m jobs_ai track mark <job_id> opened
python -m jobs_ai track mark <job_id> applied
```

Practical notes:

- Keep using `--limit` so one sprint batch maps cleanly to one manifest.
- Re-run `launch-preview --portal-hints` when a link looks portal-hosted or heavily tracked.
- Use `portal-hint <apply_url>` before launching a suspicious URL outside the normal batch flow.
- `queue`, `recommend`, and `launch-preview` only operate on jobs whose current status is `new`.

## CLI commands

Show the current control tower status:

```bash
python -m jobs_ai
```

Initialize local runtime folders:

```bash
python -m jobs_ai init
```

Check whether the workspace is ready for the next milestone:

```bash
python -m jobs_ai doctor
```

Initialize the SQLite database and schema:

```bash
python -m jobs_ai db init
```

Check whether the database schema is ready:

```bash
python -m jobs_ai db status
```

Record a manual application status update:

```bash
python -m jobs_ai track mark 42 opened
```

List current application statuses:

```bash
python -m jobs_ai track list
```

Show status history for one job:

```bash
python -m jobs_ai track status 42
```

Import local job leads from a JSON file:

```bash
python -m jobs_ai import data/raw/sample_job_leads.json
```

Rank stored jobs by fit and urgency:

```bash
python -m jobs_ai score
```

Show the current read-only apply queue:

```bash
python -m jobs_ai queue
```

Limit the working set size:

```bash
python -m jobs_ai queue --limit 10
```

Show read-only resume/profile recommendations for the queued jobs:

```bash
python -m jobs_ai recommend
```

Optionally trim the recommendation set after ranking:

```bash
python -m jobs_ai recommend --limit 10
```

Preview the queued jobs that would be used in a launch session without opening a browser:

```bash
python -m jobs_ai launch-preview
```

Optionally trim the preview set after ranking:

```bash
python -m jobs_ai launch-preview --limit 10
```

Show optional portal hints for supported job boards:

```bash
python -m jobs_ai launch-preview --portal-hints
```

Export the current launch-preview working set to a JSON manifest:

```bash
python -m jobs_ai export-session
```

Optionally trim the exported set after ranking:

```bash
python -m jobs_ai export-session --limit 10
```

Preflight an exported session manifest without opening a browser:

```bash
python -m jobs_ai preflight data/exports/launch-preview-session-20260313T173045000000Z.json
```

Build a read-only launch plan from an exported manifest:

```bash
python -m jobs_ai launch-plan data/exports/launch-preview-session-20260313T173045000000Z.json
```

Print a compact launch summary for an exported manifest:

```bash
python -m jobs_ai launch-dry-run data/exports/launch-preview-session-20260313T173045000000Z.json
```

Inspect one apply URL for supported portal hints and safe link normalization:

```bash
python -m jobs_ai portal-hint "https://boards.greenhouse.io/acme?gh_jid=12345&gh_src=linkedin"
```

If installed in editable mode, the console script also works:

```bash
jobs-ai status
```

```bash
jobs-ai import data/raw/sample_job_leads.json
```

```bash
jobs-ai score
```

```bash
jobs-ai queue
```

```bash
jobs-ai queue --limit 10
```

```bash
jobs-ai recommend
```

```bash
jobs-ai recommend --limit 10
```

```bash
jobs-ai launch-preview
```

```bash
jobs-ai launch-preview --limit 10
```

```bash
jobs-ai launch-preview --portal-hints
```

```bash
jobs-ai export-session
```

```bash
jobs-ai export-session --limit 10
```

```bash
jobs-ai preflight data/exports/launch-preview-session-20260313T173045000000Z.json
```

```bash
jobs-ai launch-plan data/exports/launch-preview-session-20260313T173045000000Z.json
```

```bash
jobs-ai launch-dry-run data/exports/launch-preview-session-20260313T173045000000Z.json
```

```bash
jobs-ai application-assist data/exports/launch-preview-session-20260313T173045000000Z.json
```

```bash
jobs-ai application-assist --portal-hints data/exports/launch-preview-session-20260313T173045000000Z.json
```

```bash
jobs-ai portal-hint "https://boards.greenhouse.io/acme?gh_jid=12345&gh_src=linkedin"
```

```bash
jobs-ai track mark 42 opened
```

```bash
jobs-ai track list
```

```bash
jobs-ai track status 42
```

## Import format

JSON import is the supported ingestion path for now. The file can contain either:

- one JSON object
- an array of JSON objects

Each record must include:

- `source`
- `company`
- `title`
- `location`

Optional fields are mapped when present:

- `apply_url`
- `source_job_id`
- `portal_type`
- `salary_text`
- `posted_at`
- `found_at`

Before insert, `source`, `source_job_id`, `company`, `title`, `location`, `apply_url`, `portal_type`, and `salary_text` get a small normalization pass. Surrounding whitespace is trimmed, repeated internal whitespace is collapsed for normal text fields, blank strings become `null`, `portal_type` is lowercased, and `apply_url` is only trimmed.

The current dedupe pass is intentionally exact and minimal. If normalized `apply_url` is present, import skips later records with the same exact `apply_url`. If `apply_url` is missing, import falls back to an exact `source + company + title + location` match. Duplicate records are skipped and reported; existing rows are never merged or overwritten.

The original input record is still stored in `raw_json`.

## Scoring

`jobs-ai score` reads the current `jobs` table and assigns a small transparent score to each row. The current score is fully rule-based and combines:

- target role/title match
- stack signals from stored job text (`Python`, `BigQuery`, `Looker`, `GCP`, `telemetry/observability`)
- geography preference (`Remote`, then `Sacramento / Folsom`, then `San Jose / Bay Area`)
- source priority (`staffing agencies / recruiter-driven contract roles`, then `contract platforms`, then `vendor / consulting ecosystems`, then `direct company portals`)

The report shows the total score plus the exact role, stack, geography, and source reasons used for ranking. There is no ML, embedding, stateful queue advancement, or browser automation in this step.

## Apply queue

`jobs-ai queue` is a minimal read-only session view over already stored jobs. It:

- includes only rows where `status = 'new'`
- reuses the current deterministic scoring rules
- sorts by score descending using the existing tie-breakers
- optionally trims the working set after ranking with `--limit`
- does not change job status or create application records

Each queue row includes rank, company, title, location, source, score, and a short reason summary pulled from the score signals.

Example:

```json
[
  {
    "source": "manual",
    "company": "Acme Data",
    "title": "Data Engineer",
    "location": "Remote",
    "apply_url": "https://example.com/jobs/data-engineer",
    "source_job_id": "acme-123",
    "portal_type": "greenhouse",
    "salary_text": "$140,000 - $170,000",
    "posted_at": "2026-03-10",
    "found_at": "2026-03-12T08:15:00Z"
  }
]
```

A runnable sample file lives at `data/raw/sample_job_leads.json`.

## Resume/profile recommendations

`jobs-ai recommend` is a minimal read-only recommendation layer on top of the ranked queue. It:

- reads the same ranked `new` jobs used by `jobs-ai queue`
- recommends a resume variant from a small local config catalog
- recommends a short profile snippet key plus snippet text from the same config-backed catalog
- uses transparent title/role keywords plus stack signals to separate telemetry/observability, analytics engineering, and data-engineering jobs
- explains each recommendation with the queue signals that triggered it
- does not launch a browser, upload files, write application rows, or change job status

## Launch preview

`jobs-ai launch-preview` is a minimal read-only preview layer on top of `jobs-ai queue` and `jobs-ai recommend`. It:

- includes only queued `new` jobs
- carries forward the ranked score already used by the queue
- shows the `apply_url` that would be used in a launch session
- shows the recommended resume variant and profile snippet for each queued job
- includes the same short explanation text from the recommendation layer
- optionally adds portal detection, hints, and normalized/company-specific apply links with `--portal-hints`
- optionally trims the preview set after ranking with `--limit`
- does not open a browser, upload files, write application rows, or change job status

## Session export

`jobs-ai export-session` is a minimal read-only export layer on top of the existing launch-preview flow. It:

- reads the same ranked preview set used by `jobs-ai launch-preview`
- writes a JSON manifest to `data/exports/launch-preview-session-<timestamp>.json`
- includes export metadata with `created_at` and `item_count`
- includes company, title, location, source, apply URL, score, recommended resume/profile selections, and explanation text for each item
- optionally trims the exported set after ranking with `--limit`
- does not launch a browser, upload files, write application rows, or change job status

## Manifest preflight

`jobs-ai preflight <manifest_path>` is a minimal read-only loader for JSON files produced by `jobs-ai export-session`. It:

- loads the manifest JSON from disk without mutating any application state
- validates the basic top-level contract (`created_at`, `item_count`, and `items`)
- rejects clearly invalid manifest shapes with a non-zero exit code
- shows manifest metadata with `created_at` and `item_count`
- shows a compact per-item preview with company, title, apply URL, recommended resume variant, and recommended profile snippet
- flags incomplete entries such as missing `apply_url` or missing recommendation fields
- does not open a browser, upload files, write application rows, or change job status

## Launch planning

`jobs-ai launch-plan <manifest_path>` is a minimal read-only planning layer on top of `load_session_manifest()`. It:

- consumes only the manifest loader output as the planning input
- preserves manifest item order
- assigns deterministic launch order numbers only to launchable items
- summarizes total items, launchable items, and skipped items
- shows company, title, apply URL, recommended resume variant, and recommended profile snippet for each planned entry
- clearly skips items with missing `apply_url` or missing recommendation fields
- does not open a browser, upload files, write application rows, or change job status

## Launch dry run

`jobs-ai launch-dry-run <manifest_path>` is a minimal executor adapter on top of the existing manifest loader and launch plan flow. It:

- loads the manifest through `load_session_manifest()`
- builds the launch plan through `build_launch_plan()`
- materializes explicit `LaunchDryRunStep` inputs from launchable `LaunchPlan` items
- routes those ordered steps through a tiny `LaunchStepExecutor` adapter, defaulting to `NoOpLaunchExecutor`
- includes a `BrowserLaunchExecutor` mode that opens launchable URLs with Python `webbrowser`
- supports `--limit` to cap browser launches by deterministic launch order
- supports `--confirm` to ask for approval after printing a pre-launch summary and before opening tabs
- prints a compact, ordered per-entry summary with company, title, apply URL, executor mode, and action label
- prints `URL: <missing>` when a summary entry does not have a usable URL
- does not autofill forms, upload files, write application rows, or change job status

### Executor Modes

`noop` (default)
Does nothing; used for safety and dry runs.

`browser_stub`
Opens each launchable URL in deterministic order through the default browser.

Example browser launch command with safety controls:

```bash
jobs-ai launch-dry-run --executor browser_stub --limit 5 --confirm data/exports/session.json
```

Example output:

```text
[1] Northwind Talent | Senior Data Engineer
URL: https://agency.example/jobs/2
Executor: noop
Action: OPEN_URL
```

## Application assist

`jobs-ai application-assist <manifest_path>` is a read-only operator helper on top of the existing manifest loader and launch plan flow. It:

- loads the manifest through `load_session_manifest()`
- builds the launch plan through `build_launch_plan()`
- shows only launchable entries in deterministic launch order
- surfaces the recommended resume variant, snippet selection, and snippet text for each launchable item
- optionally adds portal detection, hints, and normalized/company-specific apply links with `--portal-hints`
- does not open a browser, autofill forms, submit applications, handle login, or mutate application state

Example output:

```text
[1] Northwind Talent | Senior Data Engineer
Resume: data-engineering (Data Engineering Resume)
Snippet: pipeline-delivery (Pipeline Delivery)
Text: Python-first pipeline delivery across SQL warehouses, BigQuery/GCP, and production data systems.
```

## Portal support

Milestone 10 adds a small optional helper layer for common job boards without changing the launch executor contract. The helper currently:

- detects Greenhouse, Lever, Ashby, and Workday URLs
- removes common tracking-only query params when it is safe to do so
- extracts a more direct company-scoped apply URL for supported Greenhouse and Ashby link shapes
- surfaces human-facing hints through `jobs-ai portal-hint`, `jobs-ai launch-preview --portal-hints`, and `jobs-ai application-assist --portal-hints`
- does not fill forms, handle login, scrape pages, or automate any browser steps beyond the existing launch flow

## Application tracking

`jobs-ai track` is a minimal manual status layer on top of the existing `jobs` table. It:

- records explicit operator-driven status updates only
- supports the statuses `new`, `opened`, `applied`, and `skipped`
- appends a timestamped history row for each manual update
- updates the current `jobs.status` field so queue-style views stay deterministic
- does not infer status from browser behavior, handle logins, submit forms, or automate any application step

Example mark flow:

```bash
jobs-ai track mark 42 applied
```

Example list flow:

```bash
jobs-ai track list
```

Example per-job history flow:

```bash
jobs-ai track status 42
```

## Layout

```text
src/jobs_ai/              Python package
tests/                    CLI and workspace smoke tests
scripts/                  One-off operational helpers
data/raw/                 Raw lead inputs
data/processed/           Cleaned or derived outputs
data/exports/             Read-only JSON session manifests
legacy_resume_materials/  Archived source files from earlier resume work
```

## Run tests

```bash
python -m unittest discover -s tests -v
```

## Manual verification flow

Recommended real-world check after code changes:

1. Run `python -m jobs_ai init`, `python -m jobs_ai doctor`, and `python -m jobs_ai db init` in a clean workspace or temp database path.
2. Import `data/raw/sample_job_leads.json`, then run `score`, `queue --limit 2`, `recommend --limit 2`, and `launch-preview --limit 2 --portal-hints`.
3. Export a manifest, then run `preflight`, `launch-plan`, and `application-assist` against that exact export path.
4. Run `launch-dry-run <manifest>` in default `noop` mode first, then run `launch-dry-run --executor browser_stub --limit 1 --confirm <manifest>` only when you are ready to verify one real browser open.
5. Use `track mark`, `track list`, and `track status` to confirm manual status updates still line up with the queue behavior you expect.

## Milestone Tracker

- [x] Milestone 1: Create the project skeleton
- [x] Milestone 2: Build the database and job/application tables
- [x] Milestone 3: Add job ingestion
- [x] Milestone 4: Normalize and deduplicate jobs
- [x] Milestone 4A: Normalize imported job leads before deduplication
- [x] Milestone 4B: Add the first deduplication pass
- [x] Milestone 5: Rank jobs by fit and urgency
- [x] Milestone 6: Build the apply queue
- [x] Milestone 7: Add resume/profile snippet selection
- [x] Milestone 8A: Add read-only launch preview
- [x] Milestone 8A.1: Add read-only session export manifest
- [x] Milestone 8A.2: Add read-only manifest loader and preflight
- [x] Milestone 8B.0: Add read-only launch planner from manifest
- [x] Milestone 8B.1: Add read-only browser launch dry run
- [x] Milestone 8B.2: Add minimal launch executor adapter
- [x] Milestone 8B.3: Add disabled browser executor stub
- [x] Milestone 8B.4: Add compact launch dry-run summary formatter
- [x] Milestone 8B.5: Enable browser-backed launch execution
- [x] Milestone 8B.6: Add launch safety controls
- [x] Milestone 8C: Add read-only application assist layer
- [ ] Milestone 8B: Build browser-backed launch + application assist flow
- [x] Milestone 9A: Add application tracking
- [ ] Milestone 9B: Add retry queue
- [x] Milestone 10: Improve portal-specific support
- [x] Milestone 11: Operational polish
