# Job Pipeline

This is the current daily pipeline behind `jobs-ai run`.

## 1. Resolve runtime and workspace
- Code: `src/jobs_ai/cli.py`, `src/jobs_ai/config.py`, `src/jobs_ai/workspace.py`
- Reads:
  - `.env.example`-style env vars at runtime
  - workspace path rules
- Writes:
  - creates missing workspace directories if needed

## 2. Choose intake mode
- Code: `src/jobs_ai/run_workflow.py`
- Discover-first:
  - `run_discover_command()`
- Registry-first:
  - `collect_registry_sources()`

## 3. Discover or select sources
- Discover-first:
  - search-backed ATS discovery
  - separates confirmed sources from manual-review results
- Registry-first:
  - loads active registry sources
  - verifies if needed

## 4. Collect ATS jobs
- Code: `src/jobs_ai/collect/harness.py`
- Validates and normalizes input URLs
- Selects a collector adapter
- Produces:
  - collected leads
  - manual-review items
  - collect artifacts under `data/processed/...` or a user-chosen out dir

## 5. Import, normalize, and dedupe
- Code: `src/jobs_ai/jobs/importer.py`, `src/jobs_ai/jobs/identity.py`, `src/jobs_ai/jobs/normalization.py`
- Normalizes text fields
- Stores original JSON as `raw_json`
- Skips duplicates by `apply_url`, `canonical_apply_url`, and `identity_key`
- Writes rows into `jobs`

## 6. Score, queue, and recommend
- Code: `src/jobs_ai/jobs/scoring.py`, `src/jobs_ai/jobs/queue.py`, `src/jobs_ai/resume/recommendations.py`
- Uses the `jobs.status = 'new'` subset only
- Applies role, stack, geography, source, and actionability scoring
- Attaches a recommended resume variant and profile snippet

## 7. Freeze a session
- Code: `src/jobs_ai/session_start.py`, `src/jobs_ai/session_export.py`, `src/jobs_ai/session_manifest.py`, `src/jobs_ai/launch_plan.py`, `src/jobs_ai/launch_dry_run.py`
- Exports a deterministic manifest JSON
- Reloads and validates that manifest
- Builds a launch plan and dry run
- Records session metadata into `session_history`

## 8. Optional open/reopen execution
- Code: `src/jobs_ai/launch_executor.py`, `src/jobs_ai/session_history.py`, `src/jobs_ai/session_open.py`
- `noop`: no side effects
- `browser_stub`: opens URLs in a browser
- `remote_print`: prints URLs for remote-safe workflows

## 9. Manual review, prefill, submit, and tracking
- Code: `src/jobs_ai/application_assist.py`, `src/jobs_ai/application_prefill.py`, `src/jobs_ai/application_log.py`, `src/jobs_ai/application_tracking.py`
- Prefill can safely fill fields and upload the recommended resume
- Prefill always stops before submit
- Outcome tracking happens through `session mark`, `track mark`, or application log writes
