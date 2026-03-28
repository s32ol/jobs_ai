# CLI Commands and Flow

`jobs-ai` is the canonical operator entrypoint. `python -m jobs_ai` is the alternate module entrypoint. `src/jobs_ai/main.py` mostly renders reports; it is not the startup file.

## Canonical commands
- Daily default: `jobs-ai run "python backend engineer remote" --limit 25`
- Daily registry-first: `jobs-ai run "python backend engineer remote" --use-registry --limit 25`
- Modular intake: `jobs-ai discover "python backend engineer remote" --collect --import`
- Modular session freeze: `jobs-ai session start --limit 25`
- Reopen or inspect a prior batch: `jobs-ai session recent`, `jobs-ai session inspect 1`, `jobs-ai session reopen 1`
- Launch one manifest item directly: `jobs-ai open data/exports/<manifest>.json 2`
- Review-first browser handoff: `jobs-ai application-assist data/exports/<manifest>.json --prefill --launch-order 1`
- Track outcomes: `jobs-ai session mark applied --manifest data/exports/<manifest>.json --all`
- Inspect DB backend: `jobs-ai db backend-status`, `jobs-ai db ping`, `jobs-ai db status`

## Command groups that matter most
- Top-level commands: `run`, `fast-apply`, `discover`, `collect`, `import`, `launch-preview`, `launch-plan`, `launch-dry-run`, `application-assist`, `application-log`, `stats`
- `session` group: `start`, `recent`, `inspect`, `reopen`, `mark`
- `track` group: `mark`, `list`, `status`
- `db` group: `init`, `status`, `backend-status`, `ping`, `migrate-to-postgres`, `merge`
- `sources` group: registry management and registry-first collect/import workflows

## Delegation flow from the CLI
- `src/jobs_ai/cli.py` loads settings and workspace paths, then delegates to focused modules.
- `run_command` calls `src/jobs_ai/run_workflow.py`.
- `run_workflow.py` has two intake branches:
  - discover-first: `discover -> collect -> import -> session start`
  - registry-first: `sources/workflow.collect_registry_sources -> session start`
- `session start` delegates to `src/jobs_ai/session_start.py`.
- `session_start.py` selects previews, exports the manifest, reloads it, builds a launch plan, builds a dry run, records session history, and optionally executes launch steps.
- `application-assist` uses `src/jobs_ai/application_assist.py` for read-only guidance and `src/jobs_ai/application_prefill.py` for browser prefill.

## Practical command routing map
- `jobs-ai run` -> `src/jobs_ai/run_workflow.py`
- `jobs-ai discover` -> `src/jobs_ai/discover/cli.py`
- `jobs-ai collect` -> `src/jobs_ai/collect/cli.py`
- `jobs-ai sources collect` -> `src/jobs_ai/sources/workflow.py`
- `jobs-ai session start` -> `src/jobs_ai/session_start.py`
- `jobs-ai session reopen` -> `src/jobs_ai/session_history.py`
- `jobs-ai open` -> `src/jobs_ai/session_open.py`
- `jobs-ai session mark` and `jobs-ai track mark` -> `src/jobs_ai/session_mark.py` and `src/jobs_ai/application_tracking.py`
- `jobs-ai application-assist --prefill` -> `src/jobs_ai/application_prefill.py`

## Minimal examples
```bash
jobs-ai run "python backend engineer remote" --limit 25
jobs-ai run "python backend engineer remote" --use-registry --limit 25
jobs-ai discover "python backend engineer remote" --collect --import
jobs-ai session start --limit 20 --open --executor remote_print
jobs-ai application-assist data/exports/<manifest>.json --prefill --launch-order 1
jobs-ai session mark applied --manifest data/exports/<manifest>.json --all
```
