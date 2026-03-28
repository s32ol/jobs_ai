# Discovery and Import

Discovery/import is split across `discover`, `collect`, `sources`, and `jobs`.

## Discover-first path
- Entry: `src/jobs_ai/discover/cli.py`
- Core engine: `src/jobs_ai/discover/harness.py`
- Behavior:
  - normalize query
  - build a run id and output directory
  - plan and run search-backed ATS discovery
  - classify hits into confirmed sources, skipped results, and manual-review results
  - optionally run follow-on collect/import steps

## Collection path
- Wrapper: `src/jobs_ai/collect/cli.py`
- Core harness: `src/jobs_ai/collect/harness.py`
- Behavior:
  - normalize source URLs
  - drop invalid or duplicate normalized sources
  - choose the right collector adapter
  - collect leads and manual-review items

## Supported ATS collector path
- Greenhouse
- Lever
- Ashby

## Workday status
- `src/jobs_ai/portal_support.py` can detect and normalize Workday URLs
- current discovery and collection do not treat Workday like the native Greenhouse/Lever/Ashby path
- Workday stays much more manual in the current repo

## Registry-first path
- Entry: `src/jobs_ai/sources/workflow.py`
- Pulls active registry entries from `source_registry`
- Verifies entries when needed
- Runs collection directly against those sources
- Can import immediately after collection

## Import behavior
- Entry: `src/jobs_ai/jobs/importer.py`
- Accepts one JSON object or a JSON array
- Required fields:
  - `source`
  - `company`
  - `title`
  - `location`
- Optional fields include:
  - `apply_url`
  - `source_job_id`
  - `portal_type`
  - `salary_text`
  - `posted_at`
  - `found_at`
- Some filtered titles can be auto-marked `skipped` at import time instead of entering the normal `new` queue

## Normalization and dedupe
- `src/jobs_ai/jobs/normalization.py`
  - strips whitespace
  - collapses repeated whitespace on key text fields
  - lowercases `portal_type`
- `src/jobs_ai/jobs/identity.py`
  - derives `canonical_apply_url`
  - derives `identity_key`
- `src/jobs_ai/db.py` and `src/jobs_ai/jobs/importer.py`
  - still support exact URL, canonical URL, and identity-key duplicate matching
  - can skip outright duplicates before insert
  - can also insert canonical URL siblings, then resolve the whole canonical group into one preferred row with sibling rows marked `superseded`
  - report `duplicate_count`, `canonical_duplicate_groups_resolved`, and `superseded_count` in the import result

## What gets written where
- Collect artifacts:
  - `data/processed/...` or a user-specified out dir
- Imported jobs:
  - database backend resolved by config/runtime
- Session outputs:
  - `data/exports/...` or a user-specified out dir during `session start`
