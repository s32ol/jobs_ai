# scripts

Place one-off or operational helper scripts here as the project grows.

`build_ats_seed_list.py` is an auxiliary wrapper around the fast source-seeding path in `src/jobs_ai/source_seed_fast.py`.

It is useful for batch ATS seed-list generation, but it is not the primary operator entrypoint for the repo. The canonical user-facing workflow is still the main CLI from the repo root, especially:

- `python -m jobs_ai seed-sources`
- `python -m jobs_ai collect`
