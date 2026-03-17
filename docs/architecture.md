# jobs_ai Architecture

## Overview

`jobs_ai` is a local, SQLite-backed pipeline for turning raw job search inputs into a ranked, reviewable application queue. It starts with ATS source discovery or company-first seeding, collects structured postings from supported portals, imports normalized jobs into the database, ranks the backlog, freezes a session manifest, and then records manual outcomes after the operator applies.

## Pipeline Stages

1. `discovery`: search-driven ATS discovery finds likely job board roots and separates confirmed sources from manual-review results.
2. `source seeding`: company names, domains, careers pages, and direct ATS URLs are used to infer reusable source roots and grow the source registry.
3. `collection`: native adapters fetch supported ATS jobs and write importer-ready lead artifacts.
4. `import`: normalized leads are inserted into SQLite with duplicate checks, batch ids, and source metadata.
5. `ranking`: stored jobs are scored with rule-based signals such as role fit, stack keywords, geography, and source quality.
6. `session freeze/export`: ranked jobs become a deterministic manifest that can be inspected, reopened, or exported.
7. `manual application`: the operator reviews each opening and submits applications manually.
8. `tracking + stats`: status events and session history are stored so recent throughput and outcomes can be reviewed later.

## ATS Support

- `Greenhouse`: native discovery support, board-root normalization, and structured collection.
- `Lever`: native discovery support, board-root normalization, and structured collection.
- `Ashby`: native discovery support, board-root normalization, and structured collection.
- `Workday`: partial/manual-review support. Workday URLs can be detected and normalized, but discovery keeps them in manual review and they are not part of the native collector path.

## Data Model

- `jobs`: the core backlog of normalized job rows. Each row stores company, title, location, apply URL, portal type, ingest batch metadata, dedupe identity fields, raw JSON, and the current workflow status.
- `applications`: a per-job application record for operator notes, draft state, resume variant choice, and important timestamps such as last attempted or applied time.
- `application_tracking`: the append-only status timeline for a job. This is where events like `opened`, `applied`, `interview`, `offer`, and `rejected` are recorded.
- `session_history`: the durable log of frozen session manifests. It stores where the manifest lives, how many items it contains, how many were launchable, and which batch/query produced it.

The schema also includes `source_registry`, which stores reusable ATS board roots and their verification status for registry-first collection workflows.

## Why the system is human-in-the-loop

Final application submission stays manual on purpose. Hosted job portals vary widely, can include resume parsing, account creation, work authorization questions, and other high-risk edge cases, and they change without warning. `jobs_ai` automates the repeatable prep work, but leaves the final decision and submission step visible to the operator.

## Repo Map

- `src/jobs_ai/cli.py`: Typer-based CLI entrypoint and command wiring.
- `src/jobs_ai/discover`: search-backed ATS discovery and manual-review reporting.
- `src/jobs_ai/source_seed` and `src/jobs_ai/sources`: company-first seeding, site detection, registry maintenance, and bulk source workflows.
- `src/jobs_ai/collect`: ATS-specific collection adapters and collection artifact writers.
- `src/jobs_ai/jobs`: import, normalization, dedupe identity, ranking, filtering, and queue selection.
- `src/jobs_ai/resume`: resume/profile variant configuration and recommendation logic.
- `src/jobs_ai/application_tracking.py`: status recording and status history reads.
- Session modules: `session_start.py`, `session_manifest.py`, `session_export.py`, `session_history.py`, `session_mark.py`, `launch_plan.py`, and `launch_preview.py`.
