# Resume and Application Assist

Resume selection and application assist sit on top of the ranked queue and session manifest.

## Resume variants
- Code: `src/jobs_ai/resume/config.py`
- Current variant keys:
  - `general-data`
  - `data-engineering`
  - `analytics-engineering`
  - `telemetry-observability`
- Resolution order:
  1. variant-specific env var such as `JOBS_AI_RESUME_DATA_ENGINEERING_PATH`
  2. JSON map from `JOBS_AI_RESUME_MAP_PATH` or `.jobs_ai_resume_paths.json`
  3. default `resumes/<variant>.<suffix>` discovery

## Recommendation layer
- Code: `src/jobs_ai/resume/recommendations.py`
- Uses title, target-role match, and stack signals to choose:
  - a resume variant
  - a profile snippet

## Applicant profile
- Code: `src/jobs_ai/applicant_profile.py`
- Default file: `.jobs_ai_applicant_profile.json`
- Override env var: `JOBS_AI_APPLICANT_PROFILE_PATH`
- Holds:
  - name, email, phone, location
  - LinkedIn/GitHub/portfolio
  - work authorization fields
  - canned answers
  - optional resume path overrides

## Operator coordinate systems
- `open --manifest --index` and `session mark --manifest --indexes` use manifest index
- `application-assist --prefill --launch-order` and `application-log --manifest --launch-order` use launch order among launchable items only
- Prefill auto-selects only when exactly one launchable item exists; otherwise it requires `--launch-order`

## `application-assist` without `--prefill`
- Code: `src/jobs_ai/application_assist.py`
- Read-only
- Projects a launch plan into assist entries
- Requires launchable items with complete `apply_url`, recommended resume selection, and recommended profile snippet

## `application-assist --prefill`
- Code: `src/jobs_ai/application_prefill.py`
- Steps:
  1. load manifest
  2. build launch plan
  3. choose one launchable entry
  4. load applicant profile
  5. normalize or company-scope the portal URL when appropriate
  6. resolve actual resume file path
  7. open the page in the browser backend
  8. fill supported safe fields
  9. upload resume when possible
  10. optionally use the recommended snippet as short text
  11. report unresolved required fields and detected submit controls
  12. stop before submit

## Portal prefill support
- `src/jobs_ai/prefill_portals.py`
- Supported:
  - Greenhouse
  - Lever
  - Ashby
- Manual handoff only:
  - Workday

## Browser backend behavior
- Code: `src/jobs_ai/prefill_browser.py`, `src/jobs_ai/autofill/profile_config.py`
- Current backend: Playwright
- On local macOS runs, the default behavior is a dedicated local Chrome profile flow
- Those local Chrome-profile defaults apply only to `application-assist --prefill`, not to remote-safe workflows such as `remote_print`
- Important browser env vars:
  - `JOBS_AI_BROWSER_CHANNEL`
  - `JOBS_AI_BROWSER_USER_DATA_DIR`
  - `JOBS_AI_BROWSER_PROFILE_DIRECTORY`

## Application logging and DB tracking
- JSON log writer:
  - `src/jobs_ai/application_log.py`
  - writes one JSON file per handled application under `data/applications/`
- `application-assist --prefill` can hand off directly into logging:
  - `--log-outcome` prompts after the browser closes
  - `--log-status` and `--log-notes` write the log non-interactively after the browser closes
- `application-log --manifest --launch-order` follows launch order among launchable items, not raw manifest position
- DB status tracking:
  - `src/jobs_ai/application_tracking.py`
  - separate from the JSON application log

## Important current behavior
- Prefill may open a normalized or company-scoped portal URL
- Application logging still records the original manifest `apply_url`
- Logging failures are reported after prefilling, but they do not undo a successful prefill run
- If an applicant-profile resume override points to a missing file, prefill records that as a skipped resume field instead of silently falling back
