# Architecture Overview

`jobs_ai` is a local, deterministic job-application preparation toolkit. It discovers likely ATS sources, collects structured job data, imports normalized jobs into SQLite or Postgres, ranks the `new` queue, freezes deterministic session manifests, and helps the operator open or prefill applications without turning into a blind auto-submit bot.

## The two current intake modes
- Discover-first intake:
  - `src/jobs_ai/run_workflow.py`
  - `src/jobs_ai/discover/cli.py`
  - `src/jobs_ai/collect/harness.py`
  - `src/jobs_ai/jobs/importer.py`
- Registry-first intake:
  - `src/jobs_ai/sources/workflow.py`
  - `src/jobs_ai/sources/registry.py`
  - `src/jobs_ai/session_start.py`

## End-to-end architecture
```text
query or registry
  -> discovery or registry collect
  -> ATS collection
  -> import + dedupe
  -> score + queue + resume recommendation
  -> session manifest export
  -> launch plan / dry run / optional URL open
  -> manual review or browser prefill
  -> manual submit
  -> session mark / track / stats
```

## Major runtime layers
- Intake: `discover`, `collect`, `sources`
- Persistence: `config.py`, `db_runtime.py`, `db.py`, `db_postgres.py`, `db_merge.py`
- Selection: `jobs/scoring.py`, `jobs/queue.py`, `resume/recommendations.py`
- Session/launch: `session_export.py`, `session_manifest.py`, `session_start.py`, `launch_plan.py`, `launch_dry_run.py`, `launch_executor.py`
- Operator assist: `application_assist.py`, `application_prefill.py`, `application_log.py`, `application_tracking.py`

## Human-in-the-loop boundary
- Automated:
  - source discovery
  - ATS collection
  - import normalization and dedupe
  - ranking and recommendation
  - manifest creation
  - safe URL opening
  - safe field prefilling
- Manual by design:
  - final application judgment
  - final submit click
  - edge-case answers
  - downstream status updates

## ATS and portal support as of the current repo
- Native discovery/collection: Greenhouse, Lever, Ashby
- Detected but more manual: Workday
- Portal-aware prefill support:
  - supported: Greenhouse, Lever, Ashby
  - limited manual support: Workday

## Important repo-state note
`docs/architecture.md` is still useful for the broad picture, but its opening “SQLite-backed” wording is stale relative to the current code. The code path now supports both Postgres and SQLite.
