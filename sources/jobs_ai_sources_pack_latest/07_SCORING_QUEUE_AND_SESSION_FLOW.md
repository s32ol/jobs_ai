# Scoring Queue and Session Flow

Selection starts from the DB, not from raw collected files.

## Queue selection
- Code: `src/jobs_ai/jobs/queue.py`
- Input subset:
  - only rows where `jobs.status = 'new'`
- Optional scope:
  - `ingest_batch_id`
  - free-text query filter

## Scoring
- Code: `src/jobs_ai/jobs/scoring.py`
- Major signal groups:
  - target role title matches
  - stack keyword matches
  - geography preferences
  - source quality/source type
  - actionability, especially missing `apply_url`

## Resume recommendation attachment
- Code: `src/jobs_ai/resume/recommendations.py`
- Maps scored jobs to:
  - `resume_variant_key`
  - `resume_variant_label`
  - `snippet_key`
  - `snippet_label`
  - `snippet_text`
  - explanation text

## Launch preview
- Code: `src/jobs_ai/launch_preview.py`
- Thin adapter from queue recommendations into the preview objects used by session export

## Session selection scope
- Code: `src/jobs_ai/session_manifest.py`
- Carries:
  - `batch_id`
  - `source_query`
  - `import_source`
  - `selection_mode`
  - `refresh_batch_id`
- Used so later inspection can tell whether a manifest came from new imports, a registry refresh reuse, or a more generic selection

## Manifest freeze
- Code: `src/jobs_ai/session_start.py`, `src/jobs_ai/session_export.py`
- Exported manifest contains:
  - ranked jobs
  - apply URLs
  - portal type
  - recommended resume variant
  - recommended profile snippet text
- It does not contain:
  - resolved resume file paths
  - runtime portal hints
  - browser session state

## Launchability rules
- Code: `src/jobs_ai/session_manifest.py`, `src/jobs_ai/launch_plan.py`
- Manifest validation converts incomplete items into warnings
- Launch plan makes `launchable = not warnings`
- Only clean items receive deterministic `launch_order`

## Session history
- Code: `src/jobs_ai/db.py`, `src/jobs_ai/session_history.py`
- Every frozen session records:
  - manifest path
  - item count
  - launchable count
  - ingest batch id
  - source query
  - created timestamp
