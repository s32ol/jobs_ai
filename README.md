# jobs_ai

`jobs_ai` is a local Python CLI for running a human-in-the-loop job application sprint.

It is not an "auto-apply" bot. The repo is built to help one operator move a batch of leads from source discovery to ranked queue to controlled browser launch, while keeping the risky parts manual and visible.

## What problem it solves

Most job-search workflows break down in the same places:

- leads come from multiple places and are messy
- it is hard to keep a small, ranked working set
- resume/snippet choices drift across tabs and sessions
- browser launch gets chaotic without a manifest and a limit
- manual tracking falls behind what actually happened

`jobs_ai` addresses that with a local SQLite-backed workflow:

- optional source seeding and source collection
- deterministic import, dedupe, scoring, and queueing
- read-only recommendation, preview, and export steps
- manifest-based preflight and launch planning
- controlled launch execution with safety flags
- explicit manual tracking after real operator actions

## System at a glance

The repo has five practical layers:

1. Source collection / seeding
   `seed-sources` helps infer likely ATS board roots from company inputs.
   `collect` fetches supported ATS pages and writes importer-ready artifacts.
2. Import / scoring / queue
   `import` writes leads into `jobs`.
   `score` ranks them with transparent rules.
   `queue` shows the current working set of ranked `new` jobs.
3. Recommend / preview / export
   `recommend` picks a resume variant and profile snippet.
   `launch-preview` shows what would be used in a launch session.
   `export-session` writes that preview set to a JSON manifest.
4. Preflight / launch-plan / launch-dry-run
   `preflight` validates the manifest.
   `launch-plan` marks which manifest entries are launchable.
   `launch-dry-run` prints the launch sequence and optionally opens URLs.
5. Application-assist / track / portal-hint
   `application-assist` shows read-only resume/snippet guidance for launchable items.
   `track` records manual status updates.
   `portal-hint` inspects one apply URL for safe normalization and portal notes.

## Canonical workflow

The full upstream-to-launch path is:

```text
seed-sources -> collect -> import -> score -> queue -> recommend -> launch-preview -> export-session -> preflight -> launch-plan -> launch-dry-run -> track
```

If you already have importer-shaped JSON, the shorter path is:

```text
import -> score -> queue -> recommend -> launch-preview -> export-session -> preflight -> launch-plan -> launch-dry-run -> track
```

## Quickstart

From the repo root:

```bash
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e .
```

Optional local config file:

```bash
cp .env.example .env
```

Bootstrap the local workspace and database:

```bash
source .venv/bin/activate
python -m jobs_ai init
python -m jobs_ai doctor
python -m jobs_ai db init
python -m jobs_ai db status
```

See the command map:

```bash
source .venv/bin/activate
python -m jobs_ai --help
```

## Recommended operator loop

For a real application sprint, keep the batch small and deterministic:

```bash
source .venv/bin/activate
python -m jobs_ai seed-sources --from-file companies.txt
python -m jobs_ai collect --from-file data/processed/seed-sources-<timestamp>/confirmed_sources.txt
python -m jobs_ai import data/processed/collect-<timestamp>/leads.import.json
python -m jobs_ai score
python -m jobs_ai queue --limit 10
python -m jobs_ai recommend --limit 10
python -m jobs_ai launch-preview --limit 10 --portal-hints
python -m jobs_ai export-session --limit 10
python -m jobs_ai preflight data/exports/launch-preview-session-<timestamp>.json
python -m jobs_ai launch-plan data/exports/launch-preview-session-<timestamp>.json
python -m jobs_ai application-assist --portal-hints data/exports/launch-preview-session-<timestamp>.json
python -m jobs_ai launch-dry-run --executor noop data/exports/launch-preview-session-<timestamp>.json
python -m jobs_ai launch-dry-run --executor browser_stub --limit 5 --confirm data/exports/launch-preview-session-<timestamp>.json
python -m jobs_ai track mark <job_id> opened
python -m jobs_ai track mark <job_id> applied
```

Practical operating rules:

- Use `--limit` so one queue batch maps to one preview and one manifest.
- Run `launch-dry-run --executor noop` before any real browser launch.
- Use `browser_stub` only when you are ready to open real tabs.
- Record progress immediately with `track mark` after real operator actions.
- Re-run `launch-preview` or `export-session` if the underlying DB state changes.

## Workflow details

### 1. Source collection / seeding

`seed-sources` is an optional upstream helper. It takes company inputs and tries to confirm reusable ATS board-root URLs for supported portals. Its outputs are:

- `seed_report.json`
- `confirmed_sources.txt`
- `manual_review_sources.json`

Use it when you have company names or domains and want likely Greenhouse, Lever, or Ashby board roots without hand-building each URL.

`collect` is the canonical collection step once you have source URLs. It fetches supported ATS pages and writes:

- `run_report.json`
- `leads.import.json`
- `manual_review.json`

`collect` is the step that produces importer-ready lead records. `manual_review.json` is for accessible but unsupported or incomplete pages that still need human follow-up.

### 2. Import / scoring / queue

`import` consumes local JSON and inserts rows into `jobs`.

It performs:

- required-field validation
- light normalization
- deterministic dedupe
- raw record retention in `raw_json`

`score` applies a transparent rule-based ranking using title fit, stack signals, geography priority, and source priority.

`queue` shows the top ranked working set, but only for jobs whose current `status` is `new`.

### 3. Recommend / preview / export

`recommend` is read-only. It chooses:

- a resume variant key and label
- a profile snippet key, label, and text

`launch-preview` is also read-only. It combines ranked jobs with the recommendation layer and shows the apply URL that would be used in a launch session.

`export-session` writes the current launch-preview working set to `data/exports/launch-preview-session-<timestamp>.json`.

This export is the handoff point between live DB-backed selection and manifest-based launch preparation.

### 4. Preflight / launch-plan / launch-dry-run

These commands operate on an exported manifest, not directly on the live DB:

- `preflight` validates the JSON contract and flags incomplete items
- `launch-plan` preserves manifest order and marks which items are launchable
- `launch-dry-run` prints the ordered launch summary and runs the selected executor

This separation is intentional. The launch pipeline works best when you preview a small batch, export it, preflight it, and then launch from that frozen manifest rather than from a moving queue.

### 5. Application-assist / track / portal-hint

`application-assist` is a read-only overlay on top of the manifest and launch plan. It shows resume and snippet guidance for launchable items.

`portal-hint` is a spot-check tool for one URL. It helps when you want portal detection, a safer normalized link, or a company-scoped Greenhouse/Ashby link before you open tabs.

`track` is the manual state layer. It records what the operator actually did after opening or submitting an application.

## Launch pipeline

The launch path is:

```text
live DB -> queue -> recommend -> launch-preview -> export-session -> preflight -> launch-plan -> launch-dry-run
```

Important boundary:

- `queue`, `recommend`, and `launch-preview` recompute from the current database state
- `export-session` writes a snapshot manifest to disk
- `preflight`, `launch-plan`, `application-assist`, and `launch-dry-run` consume that manifest

In practice:

1. Build a small preview batch from the live DB.
2. Export it once.
3. Review the exported manifest.
4. Launch from that manifest.
5. Track the results manually right away.

## Executor modes

`launch-dry-run` supports two executor modes:

`noop`

- default mode
- prints the launch summary only
- opens nothing
- safest choice for verification

`browser_stub`

- opens launchable URLs in deterministic order through Python `webbrowser`
- supports `--limit` as a safety cap
- supports `--confirm` before tabs open
- does not fill forms, upload resumes, log in, or submit anything

Example:

```bash
source .venv/bin/activate
python -m jobs_ai launch-dry-run --executor browser_stub --limit 5 --confirm data/exports/launch-preview-session-<timestamp>.json
```

## Manual tracking

`track` is how runtime status is maintained today.

`track mark` does two things:

- appends a history row to `application_tracking`
- updates `jobs.status`

Supported statuses are:

- `new`
- `opened`
- `applied`
- `skipped`

Current queue behavior depends on that status field:

- `queue`
- `recommend`
- `launch-preview`

all operate only on jobs whose current status is `new`.

Useful commands:

```bash
source .venv/bin/activate
python -m jobs_ai track mark 42 opened
python -m jobs_ai track mark 42 applied
python -m jobs_ai track list
python -m jobs_ai track status 42
```

## Intentionally manual

The repo is designed to stop short of full automation. These remain manual on purpose:

- reviewing ambiguous sources and manual-review artifacts
- logging into portals or creating accounts
- uploading the actual resume file
- answering portal-specific form questions
- tailoring answers or cover-letter text
- final submission
- immediate post-action status marking with `track`

## Production-ready vs optional helpers

Production-ready core operator path:

- `db`
- `import`
- `score`
- `queue`
- `recommend`
- `launch-preview`
- `export-session`
- `preflight`
- `launch-plan`
- `launch-dry-run`
- `track`

Optional upstream helpers:

- `seed-sources`
- `collect`

Read-only helper overlays:

- `application-assist`
- `portal-hint`
- `--portal-hints`

Auxiliary or non-canonical tooling paths:

- `src/jobs_ai/source_seed_fast.py`
- `scripts/build_ats_seed_list.py`
- `legacy_resume_materials/`

## Command reference

Setup and readiness:

```bash
python -m jobs_ai status
python -m jobs_ai init
python -m jobs_ai doctor
python -m jobs_ai db init
python -m jobs_ai db status
```

Lead intake:

```bash
python -m jobs_ai seed-sources --from-file companies.txt
python -m jobs_ai collect --from-file sources.txt
python -m jobs_ai import data/raw/sample_job_leads.json
```

Queue building:

```bash
python -m jobs_ai score
python -m jobs_ai queue --limit 10
python -m jobs_ai recommend --limit 10
python -m jobs_ai launch-preview --limit 10 --portal-hints
python -m jobs_ai export-session --limit 10
```

Manifest and launch:

```bash
python -m jobs_ai preflight data/exports/launch-preview-session-<timestamp>.json
python -m jobs_ai launch-plan data/exports/launch-preview-session-<timestamp>.json
python -m jobs_ai application-assist --portal-hints data/exports/launch-preview-session-<timestamp>.json
python -m jobs_ai launch-dry-run --executor noop data/exports/launch-preview-session-<timestamp>.json
python -m jobs_ai launch-dry-run --executor browser_stub --limit 5 --confirm data/exports/launch-preview-session-<timestamp>.json
python -m jobs_ai portal-hint "https://boards.greenhouse.io/acme?gh_jid=12345&gh_src=linkedin"
```

Tracking:

```bash
python -m jobs_ai track mark 42 opened
python -m jobs_ai track mark 42 applied
python -m jobs_ai track list
python -m jobs_ai track status 42
```

## Repo map

```text
src/jobs_ai/              main package and CLI
tests/                    unit and CLI coverage
scripts/                  auxiliary operational wrappers
data/raw/                 sample or raw lead inputs
data/processed/           collection and source-seeding outputs
data/exports/             exported launch manifests
legacy_resume_materials/  archived resume/profile materials
```

## Known limitations

- The `applications` table exists in the schema, but current runtime behavior is driven mostly by `jobs.status` plus `application_tracking`.
- `export-session` recomputes from the live DB-backed preview path, so preview/export drift is possible if the database changes between commands.
- Resume recommendations currently point to variant keys and labels, not concrete resume file paths.
- The repo contains parallel or auxiliary tooling paths in addition to the canonical CLI workflow.

## Tests

Run the project test suite from the repo venv:

```bash
source .venv/bin/activate
python -m unittest discover -s tests -v
```
