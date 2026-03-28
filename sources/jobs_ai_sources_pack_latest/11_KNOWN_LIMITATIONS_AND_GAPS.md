# Known Limitations and Gaps

## Doc drift
- `docs/architecture.md` still opens with “SQLite-backed”
- current code supports Postgres or SQLite, with fallback behavior
- the older source-pack summaries under-described direct URL flows, canonical duplicate repair, and `applied_at`

## Old bundle drift
- `chatgpt_sources_core_v2/` is not current architecture
- it misses important current files such as `config.py`, `db_runtime.py`, `launch_plan.py`, `launch_dry_run.py`, and the current application-assist stack

## Portal and collector limits
- Native collector path is focused on Greenhouse, Lever, and Ashby
- Workday is detected and normalized but remains more manual
- Workday prefill is limited manual support, not a full supported adapter path

## Prefill limits
- `application-assist --prefill` is explicitly stop-before-submit
- it fills only safe fields defined by portal adapters
- unresolved required fields are expected on many real application pages

## Launch/open edge cases
- `open` can open a manifest item with an `apply_url` even if that item would not be launchable in the stricter launch plan
- direct `open <job_id|apply_url>` leaves application status unchanged
- `session start --open` and `session reopen` execute immediately and do not share the confirmation layer used by `launch-dry-run`

## URL command semantics
- `check-url` without `--inspect` is exact-match only; a canonical-equivalent tracking URL may still report no match
- `apply-url` and `applied <apply_url>` are intentionally different:
  - `apply-url` marks one selected row as applied and may supersede siblings
  - `applied <apply_url>` marks every matched row in the canonical URL group

## Backend-selection nuance
- runtime/backend selection follows env/config first
- passing a SQLite path into helper functions does not automatically force SQLite if the resolved backend is still Postgres

## Queue scope limits
- ranking and selection only operate on rows where `jobs.status = 'new'`
- already-opened or already-applied jobs are intentionally outside the default queue

## Local browser assumptions
- the local Chrome-profile assist flow is optimized for macOS local runs
- remote/server workflows should prefer non-browser or `remote_print` paths
