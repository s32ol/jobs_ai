# Upload Recommendation

Use the same pack-wide priority order as `13_FILE_INDEX.json` for the full 21-file ranking.

## Upload only 5 files
1. `00_README_START_HERE.md`
2. `02_CLI_COMMANDS_AND_FLOW.md`
3. `03_ARCHITECTURE_OVERVIEW.md`
4. `04_DATA_MODEL_AND_DB.md`
5. `08_LAUNCH_EXECUTION_AND_SAFETY.md`

Why these five:
- they establish the canonical entrypoint
- they explain the current architecture
- they explain DB/runtime behavior
- they explain the most important launch and safety boundaries
- this 5-file cut intentionally swaps in `08_LAUNCH_EXECUTION_AND_SAFETY.md` earlier because safety matters more than deeper pipeline detail when space is tight

## Upload 10 files
1. `00_README_START_HERE.md`
2. `02_CLI_COMMANDS_AND_FLOW.md`
3. `03_ARCHITECTURE_OVERVIEW.md`
4. `04_DATA_MODEL_AND_DB.md`
5. `05_JOB_PIPELINE.md`
6. `06_DISCOVERY_AND_IMPORT.md`
7. `07_SCORING_QUEUE_AND_SESSION_FLOW.md`
8. `08_LAUNCH_EXECUTION_AND_SAFETY.md`
9. `09_RESUME_AND_APPLICATION_ASSIST.md`
10. `10_CONFIG_ENV_AND_PATHS.md`

Why these ten:
- they cover the CLI, architecture, DB/runtime, full job pipeline, discovery/import behavior, scoring/session behavior, launch safety, resume/application assist, and config
- ChatGPT can reason about most repo-level questions from these ten alone

## Upload all files
Upload the full pack in this order:
1. `00_README_START_HERE.md`
2. `02_CLI_COMMANDS_AND_FLOW.md`
3. `03_ARCHITECTURE_OVERVIEW.md`
4. `04_DATA_MODEL_AND_DB.md`
5. `05_JOB_PIPELINE.md`
6. `06_DISCOVERY_AND_IMPORT.md`
7. `07_SCORING_QUEUE_AND_SESSION_FLOW.md`
8. `08_LAUNCH_EXECUTION_AND_SAFETY.md`
9. `09_RESUME_AND_APPLICATION_ASSIST.md`
10. `10_CONFIG_ENV_AND_PATHS.md`
11. `12_OPERATOR_QUICKSTART.md`
12. `01_REPO_MAP.md`
13. `11_KNOWN_LIMITATIONS_AND_GAPS.md`
14. `14_CODE_EXCERPT_CLI_ENTRYPOINTS.md`
15. `15_CODE_EXCERPT_DB_RUNTIME_AND_SCHEMA.md`
16. `16_CODE_EXCERPT_DISCOVERY_COLLECTION_IMPORT.md`
17. `17_CODE_EXCERPT_SESSION_MANIFEST_AND_HISTORY.md`
18. `18_CODE_EXCERPT_LAUNCH_AND_EXECUTOR.md`
19. `19_CODE_EXCERPT_RESUME_AND_APPLICATION_ASSIST.md`
20. `13_FILE_INDEX.json`
21. `99_UPLOAD_RECOMMENDATION.md`

## Optional files after the first 10
1. `12_OPERATOR_QUICKSTART.md`
2. `01_REPO_MAP.md`
3. `11_KNOWN_LIMITATIONS_AND_GAPS.md`
4. `14_CODE_EXCERPT_CLI_ENTRYPOINTS.md`
5. `15_CODE_EXCERPT_DB_RUNTIME_AND_SCHEMA.md`
6. `16_CODE_EXCERPT_DISCOVERY_COLLECTION_IMPORT.md`
7. `17_CODE_EXCERPT_SESSION_MANIFEST_AND_HISTORY.md`
8. `18_CODE_EXCERPT_LAUNCH_AND_EXECUTOR.md`
9. `19_CODE_EXCERPT_RESUME_AND_APPLICATION_ASSIST.md`
10. `13_FILE_INDEX.json`
11. `99_UPLOAD_RECOMMENDATION.md`

Optional-file notes:
- `12_OPERATOR_QUICKSTART.md` is useful when you want direct command recipes.
- `01_REPO_MAP.md` helps broad navigation but is less important than the workflow docs.
- `11_KNOWN_LIMITATIONS_AND_GAPS.md` helps when you want caveats and rough edges called out explicitly.
- `14_...` through `19_...` are best when ChatGPT needs exact code behavior, not just summaries.
- `13_FILE_INDEX.json` is mainly for machine-readable navigation.
