# Launch Execution and Safety

The current repo separates read-only planning from side-effectful opening or prefilling.

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
  - has the CLI safety cap and confirmation gate
- `session start --open`
  - writes a manifest
  - writes session history
  - can immediately run executor steps
- `session reopen`
  - reloads a prior manifest or session id
  - rebuilds dry-run steps
  - can immediately run executor steps
- `open`
  - opens one manifest item directly by index
  - does not require the stricter launch-plan gate

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
- Filesystem writes:
  - manifest export JSON
- DB writes:
  - `session_history`
  - application status tracking via `session mark` or `track mark`
- Browser/OS side effects:
  - URL open via `browser_stub`
  - Playwright browser launch in `application-assist --prefill`

## Safety rules that actually exist
- Launch plan blocks items with manifest warnings
- Dry-run rechecks missing `apply_url`, resume selection, and snippet selection
- Executor never submits forms
- Prefill always stops before submit
- `remote_print` exists for remote-safe workflows

## Important edge cases
- `src/jobs_ai/session_open.py`
  - only requires a valid index and non-null `apply_url`
  - this means `open` can bypass the stricter launch-plan warning gate
- `src/jobs_ai/session_mark.py`
  - `--all` means all launchable manifest items
  - explicit `--indexes` can still select items that are not launchable if they still have `job_id`s
- `src/jobs_ai/session_start.py` and `src/jobs_ai/session_history.py`
  - `session start --open` and `session reopen` execute immediately
  - they do not have the same confirmation layer as `launch-dry-run`
- `src/jobs_ai/application_tracking.py`
  - opening URLs does not auto-mark jobs as `opened`
  - status changes happen through tracking commands or explicit follow-up
