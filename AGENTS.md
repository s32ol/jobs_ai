# Resume Template Subsystem Guide

## Purpose

This repository contains a universal Google Apps Script resume renderer that rebuilds a Google Doc from a strict structured payload. The canonical implementation for this subsystem is [resume/resume_template.gs](/Users/rob/jobs_ai/resume/resume_template.gs).

The goal of the subsystem is stable layout fidelity, not one-off resume parsing. Treat it as a contract between:

- the renderer
- the JSON/TXT payload formats
- the embedded ChatGPT prompt contract
- future maintenance work by Codex

## Canonical Files

- [resume/resume_template.gs](/Users/rob/jobs_ai/resume/resume_template.gs): single source of truth for the schema, layout spec, renderer, validation, TXT parser/serializer, prompt contract, and self-check.
- [resume/examples/blank-profile.json](/Users/rob/jobs_ai/resume/examples/blank-profile.json): blank starter payload for manual editing.
- [resume/examples/sample-profile.json](/Users/rob/jobs_ai/resume/examples/sample-profile.json): fully populated neutral sample payload.
- [resume/examples/sample-profile.txt](/Users/rob/jobs_ai/resume/examples/sample-profile.txt): canonical TXT example aligned to the parser.
- [resume/examples/minimal-renderable-profile.json](/Users/rob/jobs_ai/resume/examples/minimal-renderable-profile.json): smallest payload that should render successfully.
- [resume/#code.js](/Users/rob/jobs_ai/resume/%23code.js): legacy/source artifact only. Do not treat it as the maintained contract.
- [resume/2026_Morales_Robert_Resume.pdf](/Users/rob/jobs_ai/resume/2026_Morales_Robert_Resume.pdf): canonical visual reference for layout and design language.

## Human Workflow / Operator Instructions

A. Setup

- Open the target resume document in Google Docs.
- Click `Extensions > Apps Script`.
- Replace the default `Code.gs` contents with the full contents of [resume_template.gs](/Users/rob/jobs_ai/resume/resume_template.gs).
- Click `Save`.
- Copy the Google Doc ID from the document URL, or keep the full Google Doc URL if that is easier.
- Paste that value into `RESUME_TEMPLATE_CONFIG.docId`.
- Run `rebuildResumeFromSample_()` first to confirm the script is connected to the correct document.

B. Prepare Payload

- Upload [resume_template.gs](/Users/rob/jobs_ai/resume/resume_template.gs) and the source resume PDF or resume text into ChatGPT.
- Tell ChatGPT to read the embedded contract and return only the PROFILE payload.
- Prefer JSON unless there is a reason to use the strict TXT fallback format.
- Review the returned payload for factual accuracy before pasting it into the script.
- Paste raw JSON into `PROFILE_JSON_PAYLOAD` or canonical TXT into `PROFILE_TXT_PAYLOAD`.
- Set `RESUME_TEMPLATE_CONFIG.inputMode` to `json`, `txt`, or `sample`.

C. Render

- Run `renderConfiguredResume()` for the normal config-driven workflow.
- Inspect the output in Google Docs after each render.
- If the resume spills to two pages, shorten the longest bullets, headline text, or long header link labels first.
- Keep the payload factual. Do not shorten content by changing dates, titles, organizations, or responsibilities.

D. Maintain

- Safe edits for a normal operator: `RESUME_TEMPLATE_CONFIG`, `PROFILE_JSON_PAYLOAD`, `PROFILE_TXT_PAYLOAD`, and example payload content.
- Safe with maintenance care: payload headings, ordering, visibility flags, and sample/example files, as long as they remain aligned with the script helpers.
- Do not casually break `LAYOUT_SPEC`, validation rules, TXT parsing rules, the top-of-file contracts, or the public API names.
- [resume_template.gs](/Users/rob/jobs_ai/resume/resume_template.gs) remains the canonical source of truth.

## What Must Never Break

- `PROFILE_VERSION` must remain `resume-profile-v1` unless the schema is intentionally versioned everywhere.
- The renderer must remain Apps Script compatible and dependency-free.
- `resume/resume_template.gs` must stay self-describing through its top-of-file contract block.
- The three audience locks must stay distinct:
  - human operator instructions
  - ChatGPT payload-generation instructions
  - maintainer/Codex instructions
- JSON is the canonical payload format. TXT is a strict fallback authoring format only.
- `blank-profile.json` is a human starter template, not the strict schema target ChatGPT should imitate verbatim.
- `header.name` is required for rendering.
- Rendering must require at least one visible non-empty section after normalization.
- Hidden or empty sections must be omitted cleanly with no leftover spacing.
- Layout fidelity must remain close to the canonical PDF:
  - US Letter page
  - compact one-page rhythm
  - Georgia name
  - Arial body/meta text
  - centered header
  - unheaded summary by default
  - inline title/date styling
  - dense bullets
  - compact inline education footer behavior when appropriate
- No Robert-specific hardcoding may return:
  - no hardcoded personal source text
  - no marker-based remapping
  - no role-name matching
  - no project-name matching
  - no assumptions tied to one person’s section names or entry counts

## Supported Schema Values

- Contact item `type`: `location`, `phone`, `email`, `link`, `text`
- Custom section `sectionKind`: `inline_list`, `paragraphs`, `experience`, `projects`, `education`, `bulleted_list`
- Education `layoutVariant`: `auto`, `inline`, `stacked`

Keep these mirrored across:

- validation
- normalization
- TXT parser
- TXT serializer
- sample payloads
- ChatGPT contract text
- self-check coverage

## Editing Rules

- Preserve the explicit layout system in `LAYOUT_SPEC`. Do not casually change fonts, sizes, margins, spacing, or compaction thresholds.
- When changing the schema, update every mirror together:
  - blank template
  - sample payload
  - minimal renderable payload
  - validator
  - normalizer
  - TXT contract
  - TXT parser
  - TXT serializer
  - ChatGPT contract
  - self-check
  - example files under `resume/examples/`
- Keep the prompt contract honest. It must never imply fields or sample entries are required when they are just starter placeholders.
- Preserve the human/operator docs and the ChatGPT payload docs during edits. Do not collapse them into vague shared guidance.
- Keep link rendering conservative: only apply links when the rendered text contains the linked label verbatim.
- Do not convert the TXT parser into a fuzzy resume parser. It should stay strict and explicit.
- Do not add build tooling, packages, transpilation, or external services to this subsystem.
- Do not treat a generated Google Doc or exported PDF as the new source of truth unless the design is intentionally being re-baselined.

## Validation Expectations

- Malformed payloads should fail with readable, path-aware errors.
- Blank templates may be schema-valid without being renderable.
- The serializer and parser must stay aligned with the exact TXT contract.
- The pipe character `|` is reserved in TXT and must remain forbidden inside ordinary TXT field values.
- Sample JSON and sample TXT should normalize to the same render tree.

## Acceptance Checks For Future Edits

- Run `runResumeTemplateSelfCheck_()` after any schema, parser, serializer, prompt, or normalization change.
- If you edit layout/rendering code, manually verify the generated Google Doc still matches the compact design canon.
- If you add or change example payload files, confirm they still validate and remain consistent with the script helpers.
- If nearby Python resume-variant or recommendation files are touched in the same change, run the relevant targeted tests for those files as well. Do not assume the Apps Script self-check covers Python-side behavior.

## Anti-Patterns

- Reintroducing Robert-specific data or matching logic.
- Changing public API names casually.
- Making TXT more permissive without updating the documented contract.
- Silently dropping content to force one-page output.
- Weakening validation or self-checks to make malformed input pass.
- Duplicating canonical schema rules in multiple divergent places.
