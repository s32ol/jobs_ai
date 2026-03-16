# scripts

Place one-off or operational helper scripts here as the project grows.

`build_ats_seed_list.py` is an auxiliary wrapper around the fast source-seeding path in `src/jobs_ai/source_seed_fast.py`.

It is useful for batch ATS seed-list generation, but it is not the primary operator entrypoint for the repo. The canonical operator workflow is the main CLI from the repo root, especially:

- `jobs-ai run "python backend engineer remote" --limit 25 --open`
- `jobs-ai discover "python backend engineer remote" --collect --import`
- `jobs-ai session start --limit 25 --open`

The older staircase commands (`queue`, `recommend`, `launch-preview`, `export-session`) still exist for advanced/manual inspection, but they are no longer the primary operator path.

After installation, `python -m jobs_ai ...` is also supported, but `jobs-ai ...` is the preferred operator-facing entrypoint.
