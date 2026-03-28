# Recent Updates

This file summarizes the main repo-state changes since the previous source-pack refresh.

## Newly added or newly important CLI surfaces
- Direct-reference helpers now matter in the daily workflow:
  - `jobs-ai open <job_id|apply_url>`
  - `jobs-ai inspect <job_id|apply_url>`
  - `jobs-ai check-url <apply_url>` and `jobs-ai check-url --inspect`
- Direct URL mark flow now exists:
  - `jobs-ai apply-url <apply_url>`
  - auto-marks `applied` when one match exists
  - requires `--job-id` when multiple jobs share the URL group
- Top-level single-reference status helpers now exist:
  - `jobs-ai applied <job_id|apply_url>`
  - `jobs-ai invalid-location <job_id|apply_url>`
- Maintenance helpers now exist for stabilized cleanup work:
  - `jobs-ai maintenance supersede-duplicates`
  - `jobs-ai maintenance mark-invalid-location --us-only`
- `jobs-ai run --us-only` is now part of the current operator surface, not just a maintenance-only concept

## Tracking and status behavior changes
- Manual tracking now uses a fuller status set:
  - `new`, `opened`, `applied`, `recruiter_screen`, `assessment`, `interview`, `offer`, `rejected`, `skipped`, `invalid_location`, `superseded`
- Status transitions are validated instead of being fully free-form
- `superseded` is now a first-class duplicate-resolution state, not just an informal idea
- `track mark` supports:
  - bulk `STATUS JOB_ID...`
  - legacy single-reference `JOB_ID STATUS`
  - URL reference `APPLY_URL STATUS`

## Applied timestamp behavior
- `jobs.applied_at` is now a first-class field on the `jobs` table
- Recording `applied` updates both tracking history and `jobs.applied_at`
- Moving from `applied` into downstream states such as `interview` preserves the earlier applied timestamp
- `initialize_schema()` backfills `jobs.applied_at` from the newest `application_tracking` row with status `applied` when older DBs are missing that field

## URL-based mark flows and canonical URL behavior
- URL references no longer behave like exact-string lookups only
- Canonical apply URLs are derived through `portal_support`:
  - tracking params are stripped
  - Greenhouse and Ashby links can be promoted to company-scoped job URLs
- Direct-reference helpers choose a canonical/preferred row for URL clusters
- `check-url` without `--inspect` stays exact-match oriented
- `check-url --inspect`, `inspect`, `open <apply_url>`, `applied <apply_url>`, `invalid-location <apply_url>`, and `track mark <apply_url> <status>` operate against the canonical/preferred group view

## Duplicate and preferred-row behavior
- Duplicate matching primitives are still:
  1. exact `apply_url`
  2. `canonical_apply_url`
  3. `identity_key`
- Import behavior is more nuanced than the older pack suggested:
  - outright duplicates can still be skipped
  - canonical URL siblings can also be inserted, then resolved into one preferred winner with sibling rows marked `superseded`
- Preferred-row selection now considers effective status, title quality, score, and stable ids
- Queue/session selection excludes `superseded` rows from the normal actionable flow
- `jobs-ai maintenance supersede-duplicates` exists so older duplicate groups can be repaired without reimporting

## Operator workflow changes for one-job-at-a-time handling
- Batch selection still happens in `run`, `fast-apply`, or `session start`
- Actual browser handling is now sharply one application at a time:
  - `open --manifest --index` uses manifest index
  - `application-assist --prefill --launch-order` uses launch order among launchable items only
  - `application-log --manifest --launch-order` uses the same launch-order coordinate system
- `application-assist --prefill` auto-selects only when exactly one launchable item exists; otherwise `--launch-order` is required
- Manifest-mode `open` can still prompt to mark `applied` or `skipped`
- Direct-reference `open <job_id|apply_url>` leaves status unchanged

## Application-assist and logging updates
- Prefill can open a normalized or company-scoped URL instead of the original raw manifest URL
- Prefill can fill safe fields, upload the resolved resume, use the recommended snippet as short text, and report unresolved required fields
- Prefill still always stops before submit
- Post-browser logging is now built into the assist flow:
  - `--log-outcome` for interactive logging
  - `--log-status` and `--log-notes` for non-interactive logging
- Logging still records the original manifest `apply_url`

## Doc drift corrections and caveats
- Older source-pack examples that used `jobs-ai open data/exports/<manifest>.json 2` are stale; current manifest-mode syntax is `jobs-ai open --manifest ... --index 2`
- `README.md` currently says `jobs-ai applied` is a viewer shortcut; current code makes it a marking command
- `docs/architecture.md` still says “SQLite-backed” even though the current code supports Postgres or SQLite
- `session start --open` and `session reopen` still execute immediately; the confirmation gate exists on `launch-dry-run --confirm`, not on those commands
- Local Chrome-profile defaults apply only to `application-assist --prefill`, not to remote-safe workflows such as `remote_print`
