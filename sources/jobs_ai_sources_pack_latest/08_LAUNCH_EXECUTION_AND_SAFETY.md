# Launch Execution and Safety

The current repo separates read-only planning from side-effectful opening, prefilling, or status writes.

## Command meanings
- `launch-preview`
  - read-only
  - shows ranked, recommended jobs before a manifest exists
- `launch-plan`
  - read-only
  - evaluates one manifest and labels items as launchable or skipped
- `launch-dry-run`
  - can stay read-only if used for planning only
  - converts launchable plan items into `OPEN_URL` steps
  - has the CLI safety cap and confirmation gate for `browser_stub`
- `session start --open`
  - writes a manifest
  - writes session history
  - can immediately run executor steps
- `session reopen`
  - reloads a prior manifest or session id
  - rebuilds dry-run steps
  - can immediately run executor steps
- `open --manifest --index`
  - opens one manifest item directly
  - then prompts whether to record `applied` or `skipped`
- `open <job_id|apply_url>`
  - opens one direct job reference
  - resolves canonical/preferred rows for URL references
  - leaves tracking status unchanged
- `check-url`, `inspect`, `portal-hint`
  - read-only single-job or single-URL helpers

## Executor modes
- `noop`
  - no external side effect
- `browser_stub`
  - opens URLs in a browser
- `remote_print`
  - prints URLs instead of opening them
  - useful for SSH or remote environments

## Side-effect boundaries
- Read-only:
  - `launch-preview`
  - `launch-plan`
  - manifest validation
  - session inspection
  - `check-url`
  - `inspect`
  - `portal-hint`
- Filesystem writes:
  - manifest export JSON
  - optional JSON application logs under `data/applications/`
- DB writes:
  - `session_history`
  - application status tracking via `session mark`, `track mark`, `apply-url`, `applied`, or `invalid-location`
- Browser/OS side effects:
  - URL open via `browser_stub`
  - Playwright browser launch in `application-assist --prefill`

## Safety rules that actually exist
- Launch plan blocks items with manifest warnings
- Dry-run rechecks missing `apply_url`, resume selection, and snippet selection
- Executor never submits forms
- Prefill always stops before submit
- `remote_print` exists for remote-safe workflows
- Opening a URL does not auto-mark a job as `opened`

## Important edge cases
- `src/jobs_ai/session_open.py`
  - manifest-mode open only requires a valid index and non-null `apply_url`
  - this means `open --manifest --index` can bypass the stricter launch-plan warning gate
- `src/jobs_ai/job_reference.py`
  - direct URL references resolve to canonical/preferred rows or duplicate groups, not just one exact raw `apply_url` row
- `src/jobs_ai/session_mark.py`
  - `--all` means all launchable manifest items
  - explicit `--indexes` can still select items that are not launchable if they still have `job_id`s
- `src/jobs_ai/application_assist.py` and `src/jobs_ai/application_log.py`
  - `application-assist --prefill --launch-order` and `application-log --launch-order` use launch order among launchable items only, not manifest index
- `src/jobs_ai/session_start.py` and `src/jobs_ai/session_history.py`
  - `session start --open` and `session reopen` execute immediately
  - they do not have the same confirmation layer as `launch-dry-run --confirm`
