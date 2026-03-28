# Upload Recommendation

Use the same pack-wide priority order as `13_FILE_INDEX.json` for the full 22-file ranking.

## Upload only 5 files
1. `00_README_START_HERE.md`
2. `02_CLI_COMMANDS_AND_FLOW.md`
3. `20_RECENT_UPDATES.md`
4. `04_DATA_MODEL_AND_DB.md`
5. `08_LAUNCH_EXECUTION_AND_SAFETY.md`

Why these five:
- they establish the canonical entrypoint and command truth
- they front-load the newest behavior changes and doc-drift corrections
- they explain DB/runtime, canonical duplicate handling, and applied timestamp behavior
- they explain the most important launch and safety boundaries

## Upload 10 files
1. `00_README_START_HERE.md`
2. `02_CLI_COMMANDS_AND_FLOW.md`
3. `20_RECENT_UPDATES.md`
4. `03_ARCHITECTURE_OVERVIEW.md`
5. `04_DATA_MODEL_AND_DB.md`
6. `05_JOB_PIPELINE.md`
7. `06_DISCOVERY_AND_IMPORT.md`
8. `07_SCORING_QUEUE_AND_SESSION_FLOW.md`
9. `08_LAUNCH_EXECUTION_AND_SAFETY.md`
10. `09_RESUME_AND_APPLICATION_ASSIST.md`

Why these ten:
- they cover the CLI, recent repo drift, architecture, DB/runtime, pipeline, discovery/import, scoring/session behavior, launch safety, and application assist
- ChatGPT can reason about most repo-level questions from these ten alone

## Upload all files
Upload the full pack in this order:
1. `00_README_START_HERE.md`
2. `02_CLI_COMMANDS_AND_FLOW.md`
3. `20_RECENT_UPDATES.md`
4. `03_ARCHITECTURE_OVERVIEW.md`
5. `04_DATA_MODEL_AND_DB.md`
6. `05_JOB_PIPELINE.md`
7. `06_DISCOVERY_AND_IMPORT.md`
8. `07_SCORING_QUEUE_AND_SESSION_FLOW.md`
9. `08_LAUNCH_EXECUTION_AND_SAFETY.md`
10. `09_RESUME_AND_APPLICATION_ASSIST.md`
11. `10_CONFIG_ENV_AND_PATHS.md`
12. `12_OPERATOR_QUICKSTART.md`
13. `01_REPO_MAP.md`
14. `11_KNOWN_LIMITATIONS_AND_GAPS.md`
15. `14_CODE_EXCERPT_CLI_ENTRYPOINTS.md`
16. `15_CODE_EXCERPT_DB_RUNTIME_AND_SCHEMA.md`
17. `16_CODE_EXCERPT_DISCOVERY_COLLECTION_IMPORT.md`
18. `17_CODE_EXCERPT_SESSION_MANIFEST_AND_HISTORY.md`
19. `18_CODE_EXCERPT_LAUNCH_AND_EXECUTOR.md`
20. `19_CODE_EXCERPT_RESUME_AND_APPLICATION_ASSIST.md`
21. `13_FILE_INDEX.json`
22. `99_UPLOAD_RECOMMENDATION.md`

## Optional files after the first 10
1. `10_CONFIG_ENV_AND_PATHS.md`
2. `12_OPERATOR_QUICKSTART.md`
3. `01_REPO_MAP.md`
4. `11_KNOWN_LIMITATIONS_AND_GAPS.md`
5. `14_CODE_EXCERPT_CLI_ENTRYPOINTS.md`
6. `15_CODE_EXCERPT_DB_RUNTIME_AND_SCHEMA.md`
7. `16_CODE_EXCERPT_DISCOVERY_COLLECTION_IMPORT.md`
8. `17_CODE_EXCERPT_SESSION_MANIFEST_AND_HISTORY.md`
9. `18_CODE_EXCERPT_LAUNCH_AND_EXECUTOR.md`
10. `19_CODE_EXCERPT_RESUME_AND_APPLICATION_ASSIST.md`
11. `13_FILE_INDEX.json`
12. `99_UPLOAD_RECOMMENDATION.md`

Optional-file notes:
- `10_CONFIG_ENV_AND_PATHS.md` is useful when config, browser profile defaults, or resume-path resolution matter.
- `12_OPERATOR_QUICKSTART.md` is useful when you want direct command recipes for one-job handling.
- `01_REPO_MAP.md` helps broad navigation but is less important than the workflow docs.
- `11_KNOWN_LIMITATIONS_AND_GAPS.md` helps when you want caveats and rough edges called out explicitly.
- `14_...` through `19_...` are best when ChatGPT needs exact code behavior, not just summaries.
- `13_FILE_INDEX.json` is mainly for machine-readable navigation.
