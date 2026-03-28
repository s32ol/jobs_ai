# CLI Commands and Flow

`jobs-ai` is the canonical operator entrypoint. `python -m jobs_ai` is the alternate module entrypoint. `src/jobs_ai/main.py` mostly renders reports; it is not the startup file.

## Canonical commands
- Daily default: `jobs-ai run "python backend engineer remote" --limit 25`
- Daily registry-first: `jobs-ai run "python backend engineer remote" --use-registry --limit 25`
- Daily US-only variant: `jobs-ai run "python backend engineer remote" --use-registry --us-only --limit 25`
- Modular intake: `jobs-ai discover "python backend engineer remote" --collect --import`
- Modular session freeze: `jobs-ai session start --limit 25`
- Reopen or inspect a prior batch: `jobs-ai session recent`, `jobs-ai session inspect 1`, `jobs-ai session reopen 1`
- Open one job directly: `jobs-ai open 123`, `jobs-ai open https://boards.greenhouse.io/example/jobs/1234567890`
- Open one manifest item: `jobs-ai open --manifest data/exports/<manifest>.json --index 2`
- Check or inspect one URL cluster: `jobs-ai check-url https://boards.greenhouse.io/example/jobs/1234567890`, `jobs-ai check-url https://boards.greenhouse.io/example/jobs/1234567890 --inspect`, `jobs-ai inspect https://boards.greenhouse.io/example/jobs/1234567890`
- Review-first browser handoff for one launchable item: `jobs-ai application-assist data/exports/<manifest>.json --prefill --launch-order 1`
- Post-browser logging: `jobs-ai application-assist data/exports/<manifest>.json --prefill --launch-order 1 --log-outcome`, `jobs-ai application-log --manifest data/exports/<manifest>.json --launch-order 1 --status applied`
- Direct status updates: `jobs-ai apply-url https://boards.greenhouse.io/example/jobs/1234567890`, `jobs-ai applied 123`, `jobs-ai invalid-location https://example.com/jobs/non-us-role`, `jobs-ai track mark interview 123`
- Maintenance and backend inspection: `jobs-ai maintenance supersede-duplicates --dry-run`, `jobs-ai maintenance mark-invalid-location --us-only --dry-run`, `jobs-ai db backend-status`, `jobs-ai db ping`, `jobs-ai db status`

## Command groups that matter most
- Top-level commands: `run`, `apply`, `fast-apply`, `discover`, `collect`, `import`, `open`, `inspect`, `check-url`, `apply-url`, `launch-preview`, `launch-plan`, `launch-dry-run`, `application-assist`, `application-log`, `portal-hint`, `applied`, `invalid-location`, `stats`
- `session` group: `start`, `recent`, `inspect`, `reopen`, `mark`
- `track` group: `mark`, `list`, `status`
- `maintenance` group: `backfill`, `supersede-duplicates`, `mark-invalid-location`
- `db` group: `init`, `status`, `backend-status`, `ping`, `migrate-to-postgres`, `merge`
- `sources` group: registry management, source seeding, verification, schema.org `JobPosting` import, and registry-first collect/import workflows

## Important coordination rule
- Manifest index is used by `open --manifest --index` and `session mark --manifest --indexes`
- Launch order is used by `application-assist --prefill --launch-order` and `application-log --manifest --launch-order`
- Launch order is counted across launchable items only, so it is not always the same as manifest index

## Delegation flow from the CLI
- `src/jobs_ai/cli.py` loads settings and workspace paths, then delegates to focused modules.
- `run_command` calls `src/jobs_ai/run_workflow.py`.
- `run_workflow.py` has two intake branches:
  - discover-first: `discover -> collect -> import -> optional invalid_location guard -> session start`
  - registry-first: `sources/workflow.collect_registry_sources -> optional invalid_location guard -> session start`
- Direct-reference helpers such as `open <job_id|apply_url>`, `inspect`, `applied`, and `invalid-location` delegate through `src/jobs_ai/job_reference.py`, `src/jobs_ai/application_tracking.py`, and DB lookup helpers.
- `session start` delegates to `src/jobs_ai/session_start.py`.
- `session_start.py` selects previews, exports the manifest, reloads it, builds a launch plan, builds a dry run, records session history, and optionally executes launch steps.
- `application-assist` uses `src/jobs_ai/application_assist.py` for read-only guidance and `src/jobs_ai/application_prefill.py` for one-application browser prefill.

## Practical command routing map
- `jobs-ai run` -> `src/jobs_ai/run_workflow.py`
- `jobs-ai discover` -> `src/jobs_ai/discover/cli.py`
- `jobs-ai collect` -> `src/jobs_ai/collect/cli.py`
- `jobs-ai sources collect` -> `src/jobs_ai/sources/workflow.py`
- `jobs-ai open <job_id|apply_url>` and `jobs-ai inspect` -> `src/jobs_ai/job_reference.py`
- `jobs-ai open --manifest --index` -> `src/jobs_ai/session_open.py`
- `jobs-ai session start` -> `src/jobs_ai/session_start.py`
- `jobs-ai session reopen` -> `src/jobs_ai/session_history.py`
- `jobs-ai session mark` -> `src/jobs_ai/session_mark.py`
- `jobs-ai track mark`, `jobs-ai applied`, and `jobs-ai invalid-location` -> `src/jobs_ai/application_tracking.py` and `src/jobs_ai/job_reference.py`
- `jobs-ai apply-url` -> URL lookup helpers in `src/jobs_ai/db.py` plus status-marking helpers in `src/jobs_ai/session_mark.py`
- `jobs-ai application-assist --prefill` -> `src/jobs_ai/application_prefill.py`
- `jobs-ai maintenance supersede-duplicates` and `jobs-ai maintenance mark-invalid-location` -> `src/jobs_ai/maintenance.py`

## Minimal examples
```bash
jobs-ai run "python backend engineer remote" --limit 25
jobs-ai run "python backend engineer remote" --use-registry --us-only --limit 25
jobs-ai discover "python backend engineer remote" --collect --import
jobs-ai session start --limit 20 --open --executor remote_print
jobs-ai open 123
jobs-ai application-assist data/exports/<manifest>.json --prefill --launch-order 1 --log-outcome
jobs-ai apply-url https://boards.greenhouse.io/example/jobs/1234567890
jobs-ai maintenance supersede-duplicates --dry-run
```
