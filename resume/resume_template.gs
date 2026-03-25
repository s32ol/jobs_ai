/*
 * CHATGPT INTEGRATION CONTRACT — READ THIS FIRST
 *
 * This file is the canonical Google Apps Script resume template system for this
 * project. It rebuilds a resume in Google Docs from structured payload data
 * while preserving the visual language of the current canonical resume PDF as
 * closely as Google Docs allows.
 *
 * PUBLIC API
 * - buildResumeFromProfile_(docId, profile)
 * - rebuildResumeFromSample_()
 * - validateProfile_(profile)
 * - getBlankResumeProfileTemplate_()
 * - getSampleResumeProfile_()
 * - getCanonicalResumeTxtTemplate_()
 * - getChatGPTPayloadContract_()
 * - runResumeTemplateSelfCheck_()
 * - renderConfiguredResume() as the recommended operator render function
 *
 * ONE TRUE RUN PATH
 * - Recommended smoke test: rebuildResumeFromSample_()
 * - Recommended real render: renderConfiguredResume()
 * - Recommended config fields to populate:
 *   RESUME_TEMPLATE_CONFIG.docId
 *   RESUME_TEMPLATE_CONFIG.inputMode
 *   PROFILE_JSON_PAYLOAD or PROFILE_TXT_PAYLOAD
 * - buildResumeFromProfile_(docId, profile) is the advanced/manual entrypoint.
 *
 * JSON PAYLOAD CONTRACT
 * - JSON is the canonical input format.
 * - The payload version must stay exactly "resume-profile-v1".
 * - The payload is hybrid: header + summary + core technologies + grouped
 *   experience sections + technical projects + education + optional
 *   customSections.
 * - Bullet strings must be plain text only. Do not include bullet glyphs,
 *   markdown list markers, or numbering prefixes in bullet text.
 * - Keep header link labels compact because they render in the centered header.
 * - The blank template returned by getBlankResumeProfileTemplate_() is for human
 *   editing convenience. It is schema-valid but not necessarily renderable.
 * - Rendering requires header.name and at least one visible, non-empty section.
 * - The strict schema reference used by ChatGPT is defined separately from the
 *   starter template so empty placeholder entries are not treated as required.
 *
 * TXT CONTRACT
 * - TXT is a strict fallback authoring format, not a freeform resume parser.
 * - TXT blocks must use labels like [HEADER], [SUMMARY], [EXPERIENCE_SECTION],
 *   [TECHNICAL_PROJECTS], [EDUCATION], and [CUSTOM_SECTION].
 * - Entry boundaries inside entry-based blocks use a line containing only ---.
 * - CONTACT and LINK values are pipe-delimited. The pipe character "|" is
 *   reserved and forbidden inside TXT values, including headlines and URLs.
 *
 * CHATGPT PAYLOAD CONTRACT
 * - When this file is uploaded to ChatGPT with a new resume, ChatGPT should
 *   read this contract and return only the payload unless the user explicitly
 *   asks for commentary.
 * - ChatGPT must preserve nuance, impact, dates, titles, organizations,
 *   locations, scope, and hierarchy.
 * - ChatGPT must not invent facts.
 * - If target role context is provided, ChatGPT may reorder, compress, and
 *   emphasize existing facts to fit that role, but it must not fabricate or
 *   distort the factual record.
 * - ChatGPT should keep the payload compact and one-page oriented.
 * - ChatGPT should return raw JSON only, not markdown fences and not a
 *   JavaScript variable assignment.
 *
 * CANONICAL FILES
 * - resume/resume_template.gs is the canonical maintained Apps Script file.
 * - resume/#code.js is a legacy/source reference artifact and is not the
 *   maintained contract.
 * - resume/2026_Morales_Robert_Resume.pdf is the canonical visual example.
 * - AGENTS.md in the repo root is the future-maintenance contract for Codex.
 *
 * LIMITATIONS
 * - Google Docs cannot guarantee PDF-identical wrap points, tab behavior, or
 *   export layout. This renderer preserves hierarchy, spacing rhythm, and
 *   typography as closely as Docs allows.
 * - One-page compaction is advisory. The renderer warns when content is dense,
 *   but it does not delete content or force-fit by rewriting facts.
 */

/*
 * HUMAN QUICKSTART — HOW TO USE THIS TEMPLATE
 *
 * This guide is for a normal human user who wants to rebuild a resume in
 * Google Docs from a structured PROFILE payload.
 *
 * SETUP IN GOOGLE DOCS
 * 1. Open Google Docs in your browser.
 * 2. Create a new document or open the document you want to turn into the
 *    resume output.
 * 3. In that Google Doc, click Extensions > Apps Script.
 * 4. In the Apps Script editor, open the default file such as Code.gs.
 * 5. Replace the default file contents with the full contents of
 *    resume_template.gs.
 * 6. Click Save.
 *
 * FIND AND SET THE GOOGLE DOC ID
 * 1. Go back to your Google Doc.
 * 2. Look at the URL in your browser.
 * 3. Copy the long document ID between "/d/" and "/edit".
 * 4. Back in Apps Script, paste that value into
 *    RESUME_TEMPLATE_CONFIG.docId below.
 *
 * PASTE THE PROFILE PAYLOAD
 * 1. Decide whether you are using JSON or TXT input.
 * 2. If using JSON, set RESUME_TEMPLATE_CONFIG.inputMode to "json" and paste
 *    only the raw JSON PROFILE payload into PROFILE_JSON_PAYLOAD below.
 * 3. If using TXT, set RESUME_TEMPLATE_CONFIG.inputMode to "txt" and paste
 *    only the canonical TXT payload into PROFILE_TXT_PAYLOAD below.
 * 4. Do not paste markdown fences, explanation text, or "const PROFILE =".
 *
 * WHAT TO RUN
 * 1. Run rebuildResumeFromSample_() first after setting docId. This is the
 *    easiest smoke test because it proves the renderer can reach your Google
 *    Doc and rebuild it.
 * 2. After that works, switch inputMode to "json" or "txt".
 * 3. Run renderConfiguredResume() to render the payload pasted into the config.
 * 4. Advanced users can call buildResumeFromProfile_(docId, profile) directly,
 *    but most users should use renderConfiguredResume().
 * 5. Run runResumeTemplateSelfCheck_() any time you want to verify that the
 *    template contract is still internally consistent.
 *
 * HOW TO START A NEW RESUME
 * 1. Use getBlankResumeProfileTemplate_() when you need a starter JSON shape.
 * 2. Use getSampleResumeProfile_() when you want a fully populated example.
 * 3. Use getCanonicalResumeTxtTemplate_() only if you want the strict TXT
 *    fallback format. JSON remains the canonical format.
 *
 * HOW TO USE CHATGPT WITH THIS FILE LATER
 * 1. Upload this script file to ChatGPT.
 * 2. Upload or paste the new person's resume PDF or resume text.
 * 3. Copy the prompt from the "COPY-PASTE PROMPT FOR CHATGPT" block below.
 *    If you want the machine-readable prompt text from inside Apps Script, use
 *    getChatGPTPayloadContract_().
 * 4. Ask ChatGPT to return only the structured PROFILE payload.
 * 5. Review the payload for factual accuracy.
 * 6. Copy only the payload back into PROFILE_JSON_PAYLOAD or
 *    PROFILE_TXT_PAYLOAD.
 * 7. Run renderConfiguredResume() again.
 *
 * IF THE RESUME SPILLS TO TWO PAGES
 * 1. Shorten the content, not the schema.
 * 2. Start by tightening the headline, contact links, and longest bullets.
 * 3. Reduce repeated phrasing and overly long project summaries.
 * 4. Keep factual content accurate. Do not invent shorter replacements.
 * 5. Re-run renderConfiguredResume() after each content change.
 *
 * WHAT IS SAFE TO EDIT
 * - Safe: the PROFILE payload, sample payloads, and example content.
 * - Safe with care: section order, wording, visibility flags, and headings in
 *   the payload.
 * - Keep canonical unless intentionally redesigning: LAYOUT_SPEC, validation,
 *   TXT parsing rules, public API names, and the ChatGPT contract blocks.
 *
 * TROUBLESHOOTING
 * - If the script is in the wrong file: go to Extensions > Apps Script and
 *   replace the default Code.gs content with this entire file.
 * - If the doc ID is missing: copy it from the Google Doc URL and paste it
 *   into RESUME_TEMPLATE_CONFIG.docId.
 * - If the PROFILE object is malformed: run runResumeTemplateSelfCheck_() and
 *   read the validation error path carefully.
 * - If the output is too long for one page: shorten content in the payload,
 *   especially long bullets, link labels, and headlines.
 * - If ChatGPT returned commentary instead of payload only: ask again and say
 *   "Return only the raw JSON PROFILE payload. No markdown, no explanation."
 */

/*
 * COPY-PASTE PROMPT FOR CHATGPT
 *
 * Read the uploaded resume_template.gs file and follow its embedded contract.
 * Analyze my uploaded resume and return only the structured PROFILE payload
 * required by this template system.
 *
 * Requirements:
 * - Produce only the payload.
 * - Prefer raw JSON unless I explicitly ask for TXT.
 * - Preserve nuance, impact, dates, titles, organizations, locations, scope,
 *   and hierarchy.
 * - Do not invent facts.
 * - Keep bullets as plain strings with no bullet characters or numbering.
 * - Keep header link labels compact so they fit the centered one-page layout.
 * - Keep the output compact and one-page oriented.
 * - Follow the schema exactly.
 * - Do not return markdown fences, explanations, or "const PROFILE =".
 *
 * If I provide target role context, adapt emphasis and ordering toward that
 * role without changing the factual record.
 */

const PROFILE_VERSION = 'resume-profile-v1';
const PIPE_SEPARATOR = ' | ';
const ROLE_META_DASH = ' — ';
const INLINE_LIST_SEPARATOR = ' • ';
const CONTACT_TYPE_VALUES = ['location', 'phone', 'email', 'link', 'text'];
const SECTION_KIND_VALUES = [
  'inline_list',
  'paragraphs',
  'experience',
  'projects',
  'education',
  'bulleted_list',
];
const EDUCATION_LAYOUT_VALUES = ['auto', 'inline', 'stacked'];

/*
 * SAFE TO EDIT
 *
 * PASTE RAW JSON HERE WHEN inputMode IS "json"
 * - Paste only the JSON object.
 * - Do not include markdown fences.
 * - Do not include "const PROFILE =" or a trailing semicolon.
 * - Template strings are used here on purpose so large payloads can be pasted
 *   directly without escaping quotation marks.
 */
const PROFILE_JSON_PAYLOAD = String.raw`
`;

/*
 * SAFE TO EDIT
 *
 * PASTE CANONICAL TXT HERE WHEN inputMode IS "txt"
 * - Keep the block labels and field labels exactly as defined by
 *   getCanonicalResumeTxtTemplate_().
 * - Do not include commentary above or below the payload.
 */
const PROFILE_TXT_PAYLOAD = String.raw`
`;

const RESUME_TEMPLATE_CONFIG = {
  docId: '',
  inputMode: 'sample',
  profileJson: PROFILE_JSON_PAYLOAD,
  profileTxt: PROFILE_TXT_PAYLOAD,
  documentTitleFallback: 'Universal Resume Template',
};

/*
 * EDIT WITH CARE
 *
 * LAYOUT_SPEC is the visual contract for page size, margins, typography,
 * spacing rhythm, compaction thresholds, and one-page behavior.
 * Change this only when intentionally adjusting the template design.
 */
const LAYOUT_SPEC = {
  page: {
    width: 612,
    height: 792,
    marginTop: 36,
    marginBottom: 32,
    marginLeft: 40,
    marginRight: 40,
  },
  fonts: {
    name: 'Georgia',
    body: 'Arial',
  },
  colors: {
    text: '#202124',
    muted: '#5f6368',
    link: '#1155cc',
  },
  header: {
    nameSize: 18,
    contactSize: 8.44,
    headlineSize: 9.5,
    nameSpacingAfter: 0,
    contactSpacingAfter: 1,
    headlineSpacingAfter: 3,
    lineSpacing: 1.0,
  },
  summary: {
    size: 9.05,
    lineSpacing: 1.02,
    spacingBeforeFirst: 0,
    spacingBeforeLater: 4,
    spacingAfter: 0,
  },
  sectionHeader: {
    size: 10,
    firstSpacingBefore: 4,
    spacingBefore: 5,
    spacingAfter: 1,
    lineSpacing: 1.0,
  },
  inlineList: {
    size: 9.0,
    lineSpacing: 1.02,
    spacingAfter: 0,
  },
  body: {
    size: 9.25,
    lineSpacing: 1.02,
    spacingAfter: 0,
  },
  meta: {
    size: 8.9,
    lineSpacing: 1.0,
    spacingAfter: 1,
  },
  bullet: {
    indentStart: 15,
    indentFirstLine: 0,
    spacingBefore: 0,
    spacingAfter: 0,
    lineSpacing: 1.01,
  },
  experience: {
    titleSize: 9.45,
    entrySpacingBefore: 4,
    lineSpacing: 1.0,
  },
  projects: {
    titleSize: 9.45,
    entrySpacingBefore: 4,
    lineSpacing: 1.0,
    summarySpacingAfter: 0,
  },
  education: {
    titleSize: 9.45,
    institutionSize: 8.9,
    entrySpacingBefore: 2,
    lineSpacing: 1.0,
    inlineThresholdChars: 125,
  },
  compaction: {
    preferredInlineListLines: 2,
    maxContactLines: 2,
    contactLineWidth: 88,
    headlineLineWidth: 86,
    summaryParagraphLineWidth: 100,
    titleLineWidth: 90,
    metaLineWidth: 92,
    bulletLineWidth: 92,
    onePageLineBudget: 57,
    tightModeLineBudget: 57,
    longBulletChars: 185,
    veryLongBulletChars: 245,
    longHeadlineChars: 100,
    longContactItemChars: 36,
    longLinkLabelChars: 38,
    longTitleLineChars: 96,
    contactReducedSize: 8.1,
    headlineReducedSize: 9.2,
    headlineMinimumSize: 9.0,
    compactSectionSpacingDelta: 1,
    warningPrefix:
        'Resume template warning: content is dense for the compact one-page layout.',
  },
};

/*
 * CANONICAL / DO NOT CASUALLY MODIFY
 *
 * The renderer, validator, parser, normalization helpers, and self-check below
 * are the canonical engine of the template system. Human operators should use
 * the config constants above rather than editing this logic during normal use.
 */
function renderConfiguredResume() {
  const config = cloneData_(RESUME_TEMPLATE_CONFIG);
  if (!cleanText_(config.docId)) {
    throw new Error(
        'RESUME_TEMPLATE_CONFIG.docId is required before running renderConfiguredResume(). ' +
        'Paste the Google Doc ID or full Google Doc URL into that field.');
  }

  const profile = resolveConfiguredProfile_(config);
  return buildResumeFromProfileInternal_(config.docId, profile, config);
}

function buildResumeFromProfile_(docId, profile) {
  return buildResumeFromProfileInternal_(docId, profile, {});
}

function buildResumeFromProfileInternal_(docId, profile, config) {
  const resolvedDocId = resolveGoogleDocId_(docId);
  if (!resolvedDocId) {
    throw new Error(
        'buildResumeFromProfile_(docId, profile) requires a Google Doc ID or full Google Doc URL.');
  }

  validateProfile_(profile);
  const normalized = normalizeResumeProfile_(profile);
  assertRenderableProfile_(normalized);
  logDensityWarningIfNeeded_(normalized);

  const resolvedConfig = config || {};
  const doc = DocumentApp.openById(resolvedDocId);
  const body = doc.getBody();

  resetDocument_(doc, body);
  maybeRenameDocument_(
      doc,
      normalized.metadata.documentTitle ||
      cleanText_(resolvedConfig.documentTitleFallback) ||
      RESUME_TEMPLATE_CONFIG.documentTitleFallback);
  appendHeader_(body, normalized.header);
  appendNormalizedSections_(body, normalized.sections);

  doc.saveAndClose();
  return normalized;
}

function rebuildResumeFromSample_() {
  if (!cleanText_(RESUME_TEMPLATE_CONFIG.docId)) {
    throw new Error(
        'Set RESUME_TEMPLATE_CONFIG.docId to a Google Doc ID or full Google Doc URL ' +
        'before running rebuildResumeFromSample_().');
  }
  return buildResumeFromProfileInternal_(
      RESUME_TEMPLATE_CONFIG.docId,
      getSampleResumeProfile_(),
      RESUME_TEMPLATE_CONFIG);
}

function validateProfile_(profile) {
  const errors = [];
  validateProfileInternal_(profile, 'profile', errors);
  if (errors.length) {
    throw new Error(
        'Resume profile validation failed:\n- ' + errors.join('\n- ') +
        '\n\nCommon fixes:\n- Paste only raw JSON into PROFILE_JSON_PAYLOAD or canonical TXT into PROFILE_TXT_PAYLOAD.\n- Keep dates, titles, and bullets quoted as strings.\n- Use [] for empty arrays instead of removing required array fields.\n- Use getBlankResumeProfileTemplate_() or getSampleResumeProfile_() as a starting point if you need a canonical shape.');
  }
  return profile;
}

/*
 * Human starter template:
 * - This includes placeholder blank entries on purpose so a human can see the
 *   expected shape quickly.
 * - ChatGPT should not copy these placeholder blank entries into a generated
 *   payload unless the user explicitly asks for a starter template.
 */
function getBlankResumeProfileTemplate_() {
  return {
    version: PROFILE_VERSION,
    metadata: {
      documentTitle: '',
      allowPageSpill: true,
    },
    header: {
      name: '',
      headline: '',
      contactItems: [
        { type: 'location', text: '', url: '' },
        { type: 'phone', text: '', url: '' },
        { type: 'email', text: '', url: '' },
        { type: 'link', text: '', url: '' },
      ],
    },
    summary: {
      id: 'summary',
      heading: 'SUMMARY',
      order: 10,
      visible: true,
      renderHeading: false,
      paragraphs: [''],
    },
    coreTechnologies: {
      id: 'core-technologies',
      heading: 'CORE TECHNOLOGIES',
      order: 20,
      visible: true,
      items: [''],
      preferredLineCount: 2,
    },
    experienceSections: [
      {
        id: 'professional-experience',
        heading: 'PROFESSIONAL EXPERIENCE',
        order: 30,
        visible: true,
        entries: [
          {
            title: '',
            org: '',
            location: '',
            date: '',
            subtitle: '',
            bullets: [''],
            links: [{ label: '', url: '' }],
          },
        ],
      },
    ],
    technicalProjects: {
      id: 'technical-projects',
      heading: 'TECHNICAL PROJECTS',
      order: 60,
      visible: true,
      entries: [
        {
          title: '',
          date: '',
          subtitle: '',
          summary: '',
          bullets: [''],
          links: [{ label: '', url: '' }],
        },
      ],
    },
    education: {
      id: 'education',
      heading: 'EDUCATION',
      order: 70,
      visible: true,
      layoutVariant: 'auto',
      entries: [
        {
          institution: '',
          credential: '',
          location: '',
          date: '',
          details: '',
          links: [{ label: '', url: '' }],
        },
      ],
    },
    customSections: [
      {
        id: 'custom-section',
        heading: 'CUSTOM SECTION',
        order: 80,
        visible: false,
        sectionKind: 'paragraphs',
        preferredLineCount: 2,
        layoutVariant: 'auto',
        items: [''],
        bullets: [''],
        paragraphs: [''],
        entries: [],
      },
    ],
  };
}

/*
 * SAFE TO EDIT
 *
 * This sample profile is demonstration content. It is safe to edit when you
 * want a different example payload, as long as the schema shape stays valid.
 */
/*
 * Neutral sample payload:
 * - This is a working example for smoke tests and reference.
 * - ChatGPT should copy the structure, not the sample content itself.
 */
function getSampleResumeProfile_() {
  return {
    version: PROFILE_VERSION,
    metadata: {
      documentTitle: 'Alex Rivera Resume',
      allowPageSpill: true,
    },
    header: {
      name: 'Alex Rivera',
      headline: 'Data Engineer, Analytics Infrastructure, Python, SQL',
      contactItems: [
        { type: 'location', text: 'Seattle, WA', url: '' },
        { type: 'phone', text: '555-0100', url: '' },
        { type: 'email', text: 'alex.rivera@example.com', url: '' },
        {
          type: 'link',
          text: 'github.com/alrivera',
          url: 'https://github.com/alrivera',
        },
        {
          type: 'link',
          text: 'linkedin.com/in/alrivera',
          url: 'https://linkedin.com/in/alrivera',
        },
      ],
    },
    summary: {
      id: 'summary',
      heading: 'SUMMARY',
      order: 10,
      visible: true,
      renderHeading: false,
      paragraphs: [
        'Data engineer focused on building reliable analytics pipelines, reusable data models, and monitoring workflows that help product and operations teams make faster decisions.',
        'Experienced translating noisy operational signals into clean reporting datasets, compact dashboards, and automation tools that reduce manual coordination work.',
      ],
    },
    coreTechnologies: {
      id: 'core-technologies',
      heading: 'CORE TECHNOLOGIES',
      order: 20,
      visible: true,
      items: [
        'Python',
        'SQL',
        'BigQuery',
        'dbt',
        'Airflow',
        'Looker',
        'Data Modeling',
        'Workflow Automation',
        'Incident Analysis',
        'Git',
      ],
      preferredLineCount: 2,
    },
    experienceSections: [
      {
        id: 'professional-experience',
        heading: 'PROFESSIONAL EXPERIENCE',
        order: 30,
        visible: true,
        entries: [
          {
            title: 'Senior Data Engineer',
            org: 'Northwind Health',
            location: 'Remote',
            date: '2022 - Present',
            subtitle: 'Analytics Platform',
            bullets: [
              'Built reusable ingestion and transformation jobs that consolidated product telemetry, support events, and release metadata into shared analytics tables.',
              'Published validation checks and downstream monitoring so partner teams could trust daily reporting without manual reconciliations.',
              'Designed stakeholder-facing dashboards that surfaced adoption, incident volume, and operational risk trends for product and operations leads.',
            ],
            links: [],
          },
          {
            title: 'Analytics Engineer',
            org: 'Cedar Systems',
            location: 'Austin, TX',
            date: '2020 - 2022',
            subtitle: 'Data Products',
            bullets: [
              'Modeled application and workflow data into consistent reporting schemas that reduced ad hoc spreadsheet analysis.',
              'Automated recurring metric refreshes and alerting so analysts could spend more time on investigations and less on data preparation.',
              'Partnered with engineering and customer success teams to define stable business logic for support, usage, and reliability metrics.',
            ],
            links: [],
          },
        ],
      },
      {
        id: 'additional-experience',
        heading: 'ADDITIONAL EXPERIENCE',
        order: 40,
        visible: true,
        entries: [
          {
            title: 'Data Systems Associate',
            org: 'Harbor Metrics',
            location: 'Portland, OR',
            date: '2018 - 2020',
            subtitle: '',
            bullets: [
              'Standardized recurring operational datasets and reporting definitions that reduced manual spreadsheet cleanup for partner teams.',
            ],
            links: [],
          },
        ],
      },
      {
        id: 'programs-fellowships',
        heading: 'PROGRAMS & FELLOWSHIPS',
        order: 50,
        visible: true,
        entries: [
          {
            title: 'Data Engineering Fellow',
            org: 'Applied Data Collective',
            location: 'Remote',
            date: '2019',
            subtitle: 'Cohort Program',
            bullets: [
              'Built end-to-end sample pipelines and documented tradeoffs across ingestion, warehousing, validation, and dashboard delivery.',
            ],
            links: [],
          },
        ],
      },
    ],
    technicalProjects: {
      id: 'technical-projects',
      heading: 'TECHNICAL PROJECTS',
      order: 60,
      visible: true,
      entries: [
        {
          title: 'Pulseboard',
          date: '2024',
          subtitle: 'Open-source telemetry dashboard starter',
          summary: 'Reusable monitoring starter built for small product teams that need compact reporting and lightweight alerting.',
          bullets: [
            'Included sample ingestion jobs, issue rollups, and templated warehouse models for feature and incident reporting.',
            'Added dashboard views and alert thresholds so teams could spot adoption changes and workflow regressions quickly.',
          ],
          links: [
            {
              label: 'Pulseboard',
              url: 'https://github.com/alrivera/pulseboard',
            },
          ],
        },
      ],
    },
    education: {
      id: 'education',
      heading: 'EDUCATION',
      order: 70,
      visible: true,
      layoutVariant: 'auto',
      entries: [
        {
          institution: 'University of Washington',
          credential: 'B.S. Informatics',
          location: 'Seattle, WA',
          date: '2016 - 2020',
          details: 'Data systems emphasis',
          links: [],
        },
      ],
    },
    customSections: [],
  };
}

/*
 * TXT is a strict human-editable fallback format.
 * JSON remains canonical for ChatGPT and for production use.
 * BULLET values should be plain sentences without manual bullet markers because
 * the renderer supplies bullets automatically.
 */
function getCanonicalResumeTxtTemplate_() {
  const lines = [
    '# Canonical TXT contract for the universal resume template system.',
    '# JSON is canonical. TXT is the strict fallback format.',
    '# Keep the block labels and field labels exactly as shown.',
    '# Delete any optional block you do not need. Hidden or empty sections are omitted.',
    '# [EXPERIENCE_SECTION] and [CUSTOM_SECTION] blocks may be repeated as needed.',
    '# CONTACT and LINK use pipe-delimited values.',
    '# The pipe character "|" is forbidden inside TXT field values, including headlines and URLs.',
    '# BULLET values must be plain sentences without bullet symbols or numbering.',
    '# Use --- to separate multiple entries in entry-based blocks.',
    '',
  ];
  return lines.join('\n') + serializeResumeProfileToTxt_(getSampleResumeProfile_());
}

function getChatGPTPayloadContract_() {
  return [
    'You are converting resume text into a strict JSON payload for a Google Apps Script resume template renderer.',
    '',
    'Output rules',
    '- Return only valid JSON.',
    '- Do not wrap the JSON in markdown fences.',
    '- Do not add commentary, notes, or explanations.',
    '- Do not prepend "PROFILE =", "const PROFILE =", or any other assignment wrapper.',
    '- Do not invent facts.',
    '- Preserve dates, titles, organizations, locations, responsibilities, and impact.',
    '- Preserve role hierarchy and section grouping when the source resume implies it.',
    '- Keep bullet values as plain strings with no bullet characters or numbering prefixes.',
    '- Keep header link labels compact so they fit the centered one-page layout.',
    '- Keep wording compact and one-page oriented without changing the factual record.',
    '- If target role context is provided, adapt emphasis, ordering, and phrasing toward that role while preserving factual truth.',
    '',
    'Schema rules',
    '- version must equal "' + PROFILE_VERSION + '".',
    '- Use contact type values only from: ' + CONTACT_TYPE_VALUES.join(', ') + '.',
    '- Use custom sectionKind values only from: ' + SECTION_KIND_VALUES.join(', ') + '.',
    '- Use education.layoutVariant only from: ' + EDUCATION_LAYOUT_VALUES.join(', ') + '.',
    '- Arrays may be empty. Do not insert placeholder blank entries.',
    '- A renderable payload must include header.name and at least one visible section with content.',
    '- Use summary.renderHeading=false unless the source explicitly needs a visible summary heading.',
    '',
    'Strict schema reference',
    JSON.stringify(getStrictResumeProfileSchemaReference_(), null, 2),
    '',
    'Minimal renderable example',
    JSON.stringify(getMinimalRenderableResumeProfile_(), null, 2),
  ].join('\n');
}

function runResumeTemplateSelfCheck_() {
  const blank = getBlankResumeProfileTemplate_();
  const sample = getSampleResumeProfile_();
  const minimal = getMinimalRenderableResumeProfile_();
  const txt = getCanonicalResumeTxtTemplate_();
  const strictSchema = getStrictResumeProfileSchemaReference_();
  const emptyProfile = createEmptyResumeProfile_();
  const contract = getChatGPTPayloadContract_();

  validateProfile_(blank);
  validateProfile_(sample);
  validateProfile_(minimal);
  validateProfile_(strictSchema);
  validateProfile_(emptyProfile);

  const parsedJson = parseResumeProfileJson_(JSON.stringify(sample));
  const parsedMinimalJson = parseResumeProfileJson_(JSON.stringify(minimal));
  const parsedBlankJson = parseResumeProfileJson_(JSON.stringify(blank));
  validateProfile_(parsedJson);
  validateProfile_(parsedMinimalJson);
  validateProfile_(parsedBlankJson);

  const parsedTxt = parseResumeProfileTxt_(txt);
  validateProfile_(parsedTxt);

  assertCondition_(
      stableStringify_(extractProfileSchemaShape_(blank)) ===
      stableStringify_(extractProfileSchemaShape_(sample)),
      'Blank and sample profile helpers must share the same schema key shape.');
  assertCondition_(
      stableStringify_(extractProfileSchemaShape_(blank)) ===
      stableStringify_(extractProfileSchemaShape_(minimal)),
      'Blank and minimal profile helpers must share the same schema key shape.');
  assertCondition_(
      stableStringify_(extractProfileSchemaShape_(blank)) ===
      stableStringify_(extractProfileSchemaShape_(strictSchema)),
      'Blank and strict schema helpers must share the same schema key shape.');
  assertCondition_(
      stableStringify_(extractProfileSchemaShape_(blank)) ===
      stableStringify_(extractProfileSchemaShape_(emptyProfile)),
      'Blank and createEmptyResumeProfile_() must share the same schema key shape.');
  assertCondition_(
      stableStringify_(extractProfileSchemaShape_(sample)) ===
      stableStringify_(extractProfileSchemaShape_(parsedJson)),
      'Sample helper and parsed sample JSON must share the same schema key shape.');
  assertCondition_(
      stableStringify_(extractProfileSchemaShape_(sample)) ===
      stableStringify_(extractProfileSchemaShape_(parsedTxt)),
      'Sample helper and parsed sample TXT must share the same schema key shape.');

  const normalizedBlank = normalizeResumeProfile_(blank);
  const normalizedSample = normalizeResumeProfile_(sample);
  const normalizedMinimal = normalizeResumeProfile_(minimal);
  const normalizedTxt = normalizeResumeProfile_(parsedTxt);
  assertCondition_(
      normalizedBlank.sections.length === 0,
      'Blank template should normalize to zero renderable sections.');
  assertCondition_(
      normalizedMinimal.sections.length > 0,
      'Minimal renderable example should normalize to at least one section.');
  assertCondition_(
      stableStringify_(normalizedSample) === stableStringify_(normalizedTxt),
      'TXT parser output does not match sample JSON after normalization.');

  assertCondition_(
      contract.indexOf('"experienceSections": []') !== -1,
      'ChatGPT contract must reference the strict schema without placeholder entries.');
  assertCondition_(
      contract.indexOf('"title": "",\n      "org": ""') === -1,
      'ChatGPT contract must not embed the starter template placeholder entry block.');
  assertCondition_(
      txt.indexOf('BULLET values must be plain sentences without bullet symbols or numbering.') !== -1,
      'TXT contract should explicitly tell humans and ChatGPT not to paste bullet symbols.');

  const hiddenSectionProfile = cloneData_(sample);
  hiddenSectionProfile.customSections.push({
    id: 'hidden-section',
    heading: 'HIDDEN',
    order: 99,
    visible: false,
    sectionKind: 'paragraphs',
    preferredLineCount: 2,
    layoutVariant: 'auto',
    items: [],
    bullets: [],
    paragraphs: ['This should not render.'],
    entries: [],
  });
  const normalizedHidden = normalizeResumeProfile_(hiddenSectionProfile);
  assertCondition_(
      !normalizedHidden.sections.some(function(section) {
        return section.id === 'hidden-section';
      }),
      'Hidden sections should not be included after normalization.');

  const inlineEducationProfile = cloneData_(sample);
  inlineEducationProfile.education.layoutVariant = 'inline';
  validateProfile_(inlineEducationProfile);
  normalizeResumeProfile_(inlineEducationProfile);

  const stackedEducationProfile = cloneData_(sample);
  stackedEducationProfile.education.layoutVariant = 'stacked';
  validateProfile_(stackedEducationProfile);
  normalizeResumeProfile_(stackedEducationProfile);

  const noisyProfile = cloneData_(minimal);
  noisyProfile.header.contactItems = [
    { type: 'link', text: 'very-long-portfolio-domain.example.com/avery/long/path', url: '' },
    { type: 'link', text: 'linkedin.com/in/example-person-with-a-very-long-handle', url: '' },
  ];
  noisyProfile.header.headline =
      'Data Engineer, Analytics Infrastructure, Data Modeling, Workflow Reliability, Reporting Systems';
  noisyProfile.experienceSections[0].entries[0].bullets = [
    '* Built a deeply integrated reporting workflow that consolidated data from many systems and added extensive monitoring, alerting, handoff documentation, stakeholder rollout guidance, training notes, exception handling, and recurring validation follow-up for cross-functional teams operating on tight delivery timelines.',
  ];
  const normalizedNoisy = normalizeResumeProfile_(noisyProfile);
  const noisyRisks = detectRenderRisks_(normalizedNoisy);
  assertCondition_(noisyRisks.contactLineCount > 1, 'Long contact items should be detected as a header wrap risk.');
  assertCondition_(noisyRisks.longBulletCount > 0, 'Very long bullets should be detected as render risks.');
  assertCondition_(
      normalizedNoisy.sections.length > 0,
      'Normalized noisy profile should still produce renderable sections.');
  assertCondition_(
      resolveHeadlineFontSize_(noisyProfile.header.headline) < LAYOUT_SPEC.header.headlineSize,
      'Long headlines should trigger headline compaction.');
  assertCondition_(
      resolveBulletRenderStyle_(noisyProfile.experienceSections[0].entries[0].bullets[0]).fontSize <
          LAYOUT_SPEC.body.size,
      'Very long bullets should trigger bullet compaction.');
  assertCondition_(
      resolveGoogleDocId_('https://docs.google.com/document/d/abc123XYZ/edit') === 'abc123XYZ',
      'Google Doc URLs should be reduced to document IDs for rendering.');

  assertCondition_(
      normalizeBulletText_('•  Built   pipelines  ') === 'Built pipelines',
      'Bullet normalization should strip bullet markers and normalize whitespace.');
  const normalizedLink = normalizeLinkArray_([{ label: '', url: 'example.com/demo' }]);
  assertCondition_(
      normalizedLink.length === 1 &&
      normalizedLink[0].label === 'example.com/demo' &&
      normalizedLink[0].url === 'https://example.com/demo',
      'Link normalization should preserve a usable label and normalized URL.');

  Logger.log('Resume template self-check passed.');
  return true;
}

function parseResumeProfileJson_(jsonText) {
  const source = String(jsonText || '').trim();
  if (!source) {
    throw new Error(
        'JSON input is empty. Paste only the raw PROFILE JSON into ' +
        'PROFILE_JSON_PAYLOAD.');
  }

  try {
    const parsed = JSON.parse(source);
    if (!isPlainObject_(parsed)) {
      throw new Error('Top-level JSON value must be an object.');
    }
    return parsed;
  } catch (error) {
    throw new Error(
        'Could not parse resume profile JSON: ' + error.message + ' ' +
        buildJsonInputHelp_(source));
  }
}

function parseResumeProfileTxt_(txtText) {
  const source = String(txtText || '');
  if (!source.trim()) {
    throw new Error(
        'TXT input is empty. Paste the canonical TXT payload into ' +
        'PROFILE_TXT_PAYLOAD.');
  }
  if (/^\s*{/.test(source)) {
    throw new Error(
        'TXT input appears to be JSON. Set RESUME_TEMPLATE_CONFIG.inputMode to ' +
        '"json" and paste that payload into PROFILE_JSON_PAYLOAD instead.');
  }

  const profile = createEmptyResumeProfile_();
  const blocks = splitTxtIntoBlocks_(source);
  assertTxtBlockMultiplicity_(blocks);
  blocks.forEach(function(block, index) {
    parseTxtBlockIntoProfile_(profile, block, index);
  });
  return profile;
}

function normalizeResumeProfile_(profile) {
  validateProfile_(profile);

  const source = cloneData_(profile);
  const normalized = {
    version: cleanText_(source.version) || PROFILE_VERSION,
    metadata: {
      documentTitle: cleanText_(source.metadata.documentTitle),
      allowPageSpill: source.metadata.allowPageSpill !== false,
    },
    header: {
      name: cleanText_(source.header.name),
      headline: cleanText_(source.header.headline),
      contactItems: normalizeContactItems_(source.header.contactItems),
    },
    sections: [],
  };

  appendNormalizedSectionIfPresent_(normalized.sections, {
    kind: 'summary',
    id: cleanText_(source.summary.id) || 'summary',
    heading: cleanText_(source.summary.heading) || 'SUMMARY',
    order: numberOrDefault_(source.summary.order, 10),
    visible: source.summary.visible !== false,
    renderHeading: !!source.summary.renderHeading,
    paragraphs: normalizeStringArray_(source.summary.paragraphs),
  });

  appendNormalizedSectionIfPresent_(normalized.sections, {
    kind: 'inline_list',
    id: cleanText_(source.coreTechnologies.id) || 'core-technologies',
    heading: cleanText_(source.coreTechnologies.heading) || 'CORE TECHNOLOGIES',
    order: numberOrDefault_(source.coreTechnologies.order, 20),
    visible: source.coreTechnologies.visible !== false,
    items: normalizeStringArray_(source.coreTechnologies.items),
    preferredLineCount: positiveIntegerOrDefault_(
        source.coreTechnologies.preferredLineCount,
        LAYOUT_SPEC.compaction.preferredInlineListLines),
  });

  (source.experienceSections || []).forEach(function(section, index) {
    appendNormalizedSectionIfPresent_(normalized.sections, {
      kind: 'experience',
      id: cleanText_(section.id) || ('experience-section-' + (index + 1)),
      heading: cleanText_(section.heading) || 'EXPERIENCE',
      order: numberOrDefault_(section.order, 30 + index),
      visible: section.visible !== false,
      entries: normalizeExperienceEntries_(section.entries),
    });
  });

  (source.customSections || []).forEach(function(section, index) {
    const kind = cleanText_(section.sectionKind);
    appendNormalizedSectionIfPresent_(normalized.sections, {
      kind: kind,
      id: cleanText_(section.id) || ('custom-section-' + (index + 1)),
      heading: cleanText_(section.heading) || 'CUSTOM SECTION',
      order: numberOrDefault_(section.order, 80 + index),
      visible: section.visible !== false,
      preferredLineCount: positiveIntegerOrDefault_(
          section.preferredLineCount,
          LAYOUT_SPEC.compaction.preferredInlineListLines),
      layoutVariant: cleanText_(section.layoutVariant) || 'auto',
      items: normalizeStringArray_(section.items),
      bullets: normalizeBulletArray_(section.bullets),
      paragraphs: normalizeStringArray_(section.paragraphs),
      entries: normalizeCustomSectionEntries_(kind, section.entries),
    });
  });

  appendNormalizedSectionIfPresent_(normalized.sections, {
    kind: 'projects',
    id: cleanText_(source.technicalProjects.id) || 'technical-projects',
    heading: cleanText_(source.technicalProjects.heading) || 'TECHNICAL PROJECTS',
    order: numberOrDefault_(source.technicalProjects.order, 60),
    visible: source.technicalProjects.visible !== false,
    entries: normalizeProjectEntries_(source.technicalProjects.entries),
  });

  appendNormalizedSectionIfPresent_(normalized.sections, {
    kind: 'education',
    id: cleanText_(source.education.id) || 'education',
    heading: cleanText_(source.education.heading) || 'EDUCATION',
    order: numberOrDefault_(source.education.order, 70),
    visible: source.education.visible !== false,
    layoutVariant: cleanText_(source.education.layoutVariant) || 'auto',
    entries: normalizeEducationEntries_(source.education.entries),
  });

  normalized.sections.sort(compareNormalizedSections_);
  return normalized;
}

function resolveConfiguredProfile_(config) {
  const mode = cleanText_(config.inputMode).toLowerCase();
  if (mode === 'sample') {
    return getSampleResumeProfile_();
  }
  if (mode === 'json') {
    if (!cleanText_(config.profileJson)) {
      throw new Error(
          'PROFILE_JSON_PAYLOAD is empty. Paste raw PROFILE JSON there or ' +
          'switch inputMode to "sample".');
    }
    return parseResumeProfileJson_(config.profileJson);
  }
  if (mode === 'txt') {
    if (!cleanText_(config.profileTxt)) {
      throw new Error(
          'PROFILE_TXT_PAYLOAD is empty. Paste canonical PROFILE TXT there or ' +
          'switch inputMode to "sample".');
    }
    return parseResumeProfileTxt_(config.profileTxt);
  }
  throw new Error(
      'Unsupported inputMode "' + config.inputMode +
      '". Use "sample", "json", or "txt".');
}

function validateProfileInternal_(profile, path, errors) {
  if (!isPlainObject_(profile)) {
    errors.push(path + ' must be an object.');
    return;
  }

  if (profile.version !== PROFILE_VERSION) {
    errors.push(path + '.version must equal "' + PROFILE_VERSION + '".');
  }

  validateMetadata_(profile.metadata, path + '.metadata', errors);
  validateHeader_(profile.header, path + '.header', errors);
  validateSummarySection_(profile.summary, path + '.summary', errors);
  validateInlineListSection_(profile.coreTechnologies, path + '.coreTechnologies', errors);
  validateExperienceSectionArray_(profile.experienceSections, path + '.experienceSections', errors);
  validateProjectsSection_(profile.technicalProjects, path + '.technicalProjects', errors);
  validateEducationSection_(profile.education, path + '.education', errors);
  validateCustomSectionArray_(profile.customSections, path + '.customSections', errors);
}

function validateMetadata_(metadata, path, errors) {
  if (!isPlainObject_(metadata)) {
    errors.push(path + ' must be an object.');
    return;
  }
  validateOptionalString_(metadata.documentTitle, path + '.documentTitle', errors);
  validateBoolean_(metadata.allowPageSpill, path + '.allowPageSpill', errors);
}

function validateHeader_(header, path, errors) {
  if (!isPlainObject_(header)) {
    errors.push(path + ' must be an object.');
    return;
  }
  validateOptionalString_(header.name, path + '.name', errors);
  validateOptionalString_(header.headline, path + '.headline', errors);

  if (!Array.isArray(header.contactItems)) {
    errors.push(path + '.contactItems must be an array.');
    return;
  }

  header.contactItems.forEach(function(item, index) {
    const itemPath = path + '.contactItems[' + index + ']';
    if (!isPlainObject_(item)) {
      errors.push(itemPath + ' must be an object.');
      return;
    }
    if (CONTACT_TYPE_VALUES.indexOf(item.type) === -1) {
      errors.push(itemPath + '.type must be one of: ' + CONTACT_TYPE_VALUES.join(', ') + '.');
    }
    validateOptionalString_(item.text, itemPath + '.text', errors);
    validateOptionalString_(item.url, itemPath + '.url', errors);
  });
}

function validateSummarySection_(section, path, errors) {
  validateCommonSectionShape_(section, path, errors);
  if (!isPlainObject_(section)) {
    return;
  }
  validateBoolean_(section.renderHeading, path + '.renderHeading', errors);
  validateStringArray_(section.paragraphs, path + '.paragraphs', errors);
}

function validateInlineListSection_(section, path, errors) {
  validateCommonSectionShape_(section, path, errors);
  if (!isPlainObject_(section)) {
    return;
  }
  validateStringArray_(section.items, path + '.items', errors);
  validateOptionalInteger_(section.preferredLineCount, path + '.preferredLineCount', errors);
}

function validateExperienceSectionArray_(sections, path, errors) {
  if (!Array.isArray(sections)) {
    errors.push(path + ' must be an array.');
    return;
  }
  sections.forEach(function(section, index) {
    const sectionPath = path + '[' + index + ']';
    validateCommonSectionShape_(section, sectionPath, errors);
    if (!isPlainObject_(section)) {
      return;
    }
    validateExperienceEntries_(section.entries, sectionPath + '.entries', errors);
  });
}

function validateProjectsSection_(section, path, errors) {
  validateCommonSectionShape_(section, path, errors);
  if (!isPlainObject_(section)) {
    return;
  }
  validateProjectEntries_(section.entries, path + '.entries', errors);
}

function validateEducationSection_(section, path, errors) {
  validateCommonSectionShape_(section, path, errors);
  if (!isPlainObject_(section)) {
    return;
  }
  if (EDUCATION_LAYOUT_VALUES.indexOf(section.layoutVariant) === -1) {
    errors.push(path + '.layoutVariant must be one of: ' + EDUCATION_LAYOUT_VALUES.join(', ') + '.');
  }
  validateEducationEntries_(section.entries, path + '.entries', errors);
}

function validateCustomSectionArray_(sections, path, errors) {
  if (!Array.isArray(sections)) {
    errors.push(path + ' must be an array.');
    return;
  }
  sections.forEach(function(section, index) {
    const sectionPath = path + '[' + index + ']';
    validateCommonSectionShape_(section, sectionPath, errors);
    if (!isPlainObject_(section)) {
      return;
    }

    if (SECTION_KIND_VALUES.indexOf(section.sectionKind) === -1) {
      errors.push(sectionPath + '.sectionKind must be one of: ' + SECTION_KIND_VALUES.join(', ') + '.');
    }
    validateOptionalInteger_(section.preferredLineCount, sectionPath + '.preferredLineCount', errors);
    validateOptionalString_(section.layoutVariant, sectionPath + '.layoutVariant', errors);
    validateStringArray_(section.items, sectionPath + '.items', errors);
    validateStringArray_(section.bullets, sectionPath + '.bullets', errors);
    validateStringArray_(section.paragraphs, sectionPath + '.paragraphs', errors);

    if (!Array.isArray(section.entries)) {
      errors.push(sectionPath + '.entries must be an array.');
      return;
    }

    if (section.sectionKind === 'experience') {
      validateExperienceEntries_(section.entries, sectionPath + '.entries', errors);
    } else if (section.sectionKind === 'projects') {
      validateProjectEntries_(section.entries, sectionPath + '.entries', errors);
    } else if (section.sectionKind === 'education') {
      validateEducationEntries_(section.entries, sectionPath + '.entries', errors);
    }
  });
}

function validateCommonSectionShape_(section, path, errors) {
  if (!isPlainObject_(section)) {
    errors.push(path + ' must be an object.');
    return;
  }
  validateOptionalString_(section.id, path + '.id', errors);
  validateOptionalString_(section.heading, path + '.heading', errors);
  validateOptionalInteger_(section.order, path + '.order', errors);
  validateBoolean_(section.visible, path + '.visible', errors);
}

function validateExperienceEntries_(entries, path, errors) {
  if (!Array.isArray(entries)) {
    errors.push(path + ' must be an array.');
    return;
  }
  entries.forEach(function(entry, index) {
    const entryPath = path + '[' + index + ']';
    if (!isPlainObject_(entry)) {
      errors.push(entryPath + ' must be an object.');
      return;
    }
    validateOptionalString_(entry.title, entryPath + '.title', errors);
    validateOptionalString_(entry.org, entryPath + '.org', errors);
    validateOptionalString_(entry.location, entryPath + '.location', errors);
    validateOptionalString_(entry.date, entryPath + '.date', errors);
    validateOptionalString_(entry.subtitle, entryPath + '.subtitle', errors);
    validateStringArray_(entry.bullets, entryPath + '.bullets', errors);
    validateLinkArray_(entry.links, entryPath + '.links', errors);
  });
}

function validateProjectEntries_(entries, path, errors) {
  if (!Array.isArray(entries)) {
    errors.push(path + ' must be an array.');
    return;
  }
  entries.forEach(function(entry, index) {
    const entryPath = path + '[' + index + ']';
    if (!isPlainObject_(entry)) {
      errors.push(entryPath + ' must be an object.');
      return;
    }
    validateOptionalString_(entry.title, entryPath + '.title', errors);
    validateOptionalString_(entry.date, entryPath + '.date', errors);
    validateOptionalString_(entry.subtitle, entryPath + '.subtitle', errors);
    validateOptionalString_(entry.summary, entryPath + '.summary', errors);
    validateStringArray_(entry.bullets, entryPath + '.bullets', errors);
    validateLinkArray_(entry.links, entryPath + '.links', errors);
  });
}

function validateEducationEntries_(entries, path, errors) {
  if (!Array.isArray(entries)) {
    errors.push(path + ' must be an array.');
    return;
  }
  entries.forEach(function(entry, index) {
    const entryPath = path + '[' + index + ']';
    if (!isPlainObject_(entry)) {
      errors.push(entryPath + ' must be an object.');
      return;
    }
    validateOptionalString_(entry.institution, entryPath + '.institution', errors);
    validateOptionalString_(entry.credential, entryPath + '.credential', errors);
    validateOptionalString_(entry.location, entryPath + '.location', errors);
    validateOptionalString_(entry.date, entryPath + '.date', errors);
    validateOptionalString_(entry.details, entryPath + '.details', errors);
    validateLinkArray_(entry.links, entryPath + '.links', errors);
  });
}

function validateLinkArray_(links, path, errors) {
  if (!Array.isArray(links)) {
    errors.push(path + ' must be an array.');
    return;
  }
  links.forEach(function(link, index) {
    const linkPath = path + '[' + index + ']';
    if (!isPlainObject_(link)) {
      errors.push(linkPath + ' must be an object.');
      return;
    }
    validateOptionalString_(link.label, linkPath + '.label', errors);
    validateOptionalString_(link.url, linkPath + '.url', errors);
  });
}

function validateStringArray_(value, path, errors) {
  if (!Array.isArray(value)) {
    errors.push(path + ' must be an array (received ' + describeType_(value) + ').');
    return;
  }
  value.forEach(function(item, index) {
    if (typeof item !== 'string') {
      errors.push(path + '[' + index + '] must be a string (received ' + describeType_(item) + ').');
    }
  });
}

function validateOptionalString_(value, path, errors) {
  if (typeof value !== 'string') {
    errors.push(path + ' must be a string (use "" if blank; received ' + describeType_(value) + ').');
  }
}

function validateOptionalNumber_(value, path, errors) {
  if (typeof value !== 'number' || !isFinite(value)) {
    errors.push(path + ' must be a finite number (received ' + describeType_(value) + ').');
  }
}

function validateBoolean_(value, path, errors) {
  if (typeof value !== 'boolean') {
    errors.push(path + ' must be a boolean (received ' + describeType_(value) + ').');
  }
}

function validateOptionalInteger_(value, path, errors) {
  if (typeof value !== 'number' || !isFinite(value) || Math.floor(value) !== value) {
    errors.push(path + ' must be an integer (received ' + describeType_(value) + ').');
  }
}

function appendNormalizedSections_(body, sections) {
  sections.forEach(function(section, index) {
    appendNormalizedSection_(body, section, index === 0);
  });
}

function appendNormalizedSection_(body, section, isFirstSection) {
  if (section.kind === 'summary') {
    appendSummarySection_(body, section, isFirstSection);
    return;
  }
  if (section.kind === 'inline_list') {
    appendInlineListSection_(body, section, isFirstSection);
    return;
  }
  if (section.kind === 'experience') {
    appendSectionHeader_(body, section.heading, isFirstSection);
    section.entries.forEach(function(entry, index) {
      appendExperienceEntry_(body, entry, index === 0 ? 0 : LAYOUT_SPEC.experience.entrySpacingBefore);
    });
    return;
  }
  if (section.kind === 'projects') {
    appendSectionHeader_(body, section.heading, isFirstSection);
    section.entries.forEach(function(entry, index) {
      appendProjectEntry_(body, entry, index === 0 ? 0 : LAYOUT_SPEC.projects.entrySpacingBefore);
    });
    return;
  }
  if (section.kind === 'education') {
    appendEducationSection_(body, section, isFirstSection);
    return;
  }
  if (section.kind === 'paragraphs') {
    appendSectionHeader_(body, section.heading, isFirstSection);
    section.paragraphs.forEach(function(paragraph, index) {
      appendBodyParagraph_(body, paragraph, {
        spacingBefore: index === 0 ? 0 : 0,
        spacingAfter: 0,
        lineSpacing: LAYOUT_SPEC.body.lineSpacing,
        size: LAYOUT_SPEC.body.size,
      });
    });
    return;
  }
  if (section.kind === 'bulleted_list') {
    appendSectionHeader_(body, section.heading, isFirstSection);
    section.bullets.forEach(function(bullet) {
      appendBullet_(body, bullet, [], {});
    });
    return;
  }
  throw new Error('Unsupported normalized section kind: ' + section.kind);
}

function appendHeader_(body, header) {
  const nameParagraph = takeReusableParagraph_(body, header.name);
  nameParagraph
      .setAlignment(DocumentApp.HorizontalAlignment.CENTER)
      .setSpacingBefore(0)
      .setSpacingAfter(LAYOUT_SPEC.header.nameSpacingAfter)
      .setLineSpacing(LAYOUT_SPEC.header.lineSpacing);
  applyTextStyle_(nameParagraph.editAsText(), {
    fontFamily: LAYOUT_SPEC.fonts.name,
    fontSize: LAYOUT_SPEC.header.nameSize,
    bold: true,
    color: LAYOUT_SPEC.colors.text,
  });

  const contactLines = splitHeaderContactItemsForLayout_(header.contactItems);
  const contactFontSize = contactLines.length > 1 ?
      LAYOUT_SPEC.compaction.contactReducedSize :
      LAYOUT_SPEC.header.contactSize;
  contactLines.forEach(function(lineItems, index) {
    const contactLine = lineItems.map(function(item) {
      return item.text;
    }).join(PIPE_SEPARATOR);
    const contactParagraph = body.appendParagraph(contactLine);
    contactParagraph
        .setAlignment(DocumentApp.HorizontalAlignment.CENTER)
        .setSpacingBefore(0)
        .setSpacingAfter(index === contactLines.length - 1 ?
            (contactLines.length > 1 ? 0 : LAYOUT_SPEC.header.contactSpacingAfter) : 0)
        .setLineSpacing(LAYOUT_SPEC.header.lineSpacing);
    const contactText = applyTextStyle_(contactParagraph.editAsText(), {
      fontFamily: LAYOUT_SPEC.fonts.body,
      fontSize: contactFontSize,
      color: LAYOUT_SPEC.colors.muted,
    });
    applyInlineLinks_(contactText, contactLine, toInlineLinkSpecs_(lineItems));
  });

  if (header.headline) {
    const headlineFontSize = resolveHeadlineFontSize_(header.headline);
    const headlineParagraph = body.appendParagraph(header.headline);
    headlineParagraph
        .setAlignment(DocumentApp.HorizontalAlignment.CENTER)
        .setSpacingBefore(0)
        .setSpacingAfter(
            headlineFontSize < LAYOUT_SPEC.header.headlineSize || contactLines.length > 1 ?
            Math.max(1, LAYOUT_SPEC.header.headlineSpacingAfter - 1) :
            LAYOUT_SPEC.header.headlineSpacingAfter)
        .setLineSpacing(LAYOUT_SPEC.header.lineSpacing);
    applyTextStyle_(headlineParagraph.editAsText(), {
      fontFamily: LAYOUT_SPEC.fonts.body,
      fontSize: headlineFontSize,
      bold: true,
      color: LAYOUT_SPEC.colors.text,
    });
  }
}

function appendSummarySection_(body, section, isFirstSection) {
  if (section.renderHeading) {
    appendSectionHeader_(body, section.heading, isFirstSection);
  }
  section.paragraphs.forEach(function(paragraph, index) {
    appendBodyParagraph_(body, paragraph, {
      spacingBefore: section.renderHeading ? 0 : (isFirstSection && index === 0 ?
          LAYOUT_SPEC.summary.spacingBeforeFirst :
          (index === 0 ? LAYOUT_SPEC.summary.spacingBeforeLater : 0)),
      spacingAfter: LAYOUT_SPEC.summary.spacingAfter,
      lineSpacing: LAYOUT_SPEC.summary.lineSpacing,
      size: LAYOUT_SPEC.summary.size,
    });
  });
}

function appendInlineListSection_(body, section, isFirstSection) {
  appendSectionHeader_(body, section.heading, isFirstSection);
  splitInlineListForLayout_(section.items, section.preferredLineCount).forEach(function(lineItems) {
    appendBodyParagraph_(body, lineItems.join(INLINE_LIST_SEPARATOR), {
      spacingBefore: 0,
      spacingAfter: LAYOUT_SPEC.inlineList.spacingAfter,
      lineSpacing: LAYOUT_SPEC.inlineList.lineSpacing,
      size: LAYOUT_SPEC.inlineList.size,
    });
  });
}

function appendSectionHeader_(body, title, isFirstSection) {
  const paragraph = body.appendParagraph(String(title).toUpperCase());
  paragraph
      .setAlignment(DocumentApp.HorizontalAlignment.LEFT)
      .setSpacingBefore(
          isFirstSection ? LAYOUT_SPEC.sectionHeader.firstSpacingBefore :
          LAYOUT_SPEC.sectionHeader.spacingBefore)
      .setSpacingAfter(LAYOUT_SPEC.sectionHeader.spacingAfter)
      .setLineSpacing(LAYOUT_SPEC.sectionHeader.lineSpacing);
  applyTextStyle_(paragraph.editAsText(), {
    fontFamily: LAYOUT_SPEC.fonts.body,
    fontSize: LAYOUT_SPEC.sectionHeader.size,
    bold: true,
    color: LAYOUT_SPEC.colors.text,
  });
  return paragraph;
}

function appendBodyParagraph_(body, text, options) {
  const config = options || {};
  const paragraph = body.appendParagraph(text);
  paragraph
      .setAlignment(DocumentApp.HorizontalAlignment.LEFT)
      .setSpacingBefore(config.spacingBefore == null ? 0 : config.spacingBefore)
      .setSpacingAfter(config.spacingAfter == null ? 0 : config.spacingAfter)
      .setLineSpacing(config.lineSpacing || LAYOUT_SPEC.body.lineSpacing);
  const paragraphText = applyTextStyle_(paragraph.editAsText(), {
    fontFamily: LAYOUT_SPEC.fonts.body,
    fontSize: config.size || LAYOUT_SPEC.body.size,
    color: config.color || LAYOUT_SPEC.colors.text,
    bold: !!config.bold,
    italic: !!config.italic,
  });
  applyInlineLinks_(paragraphText, paragraph.getText(), config.links || []);
  return paragraph;
}

function appendMetaParagraph_(body, text, options) {
  const config = options || {};
  const paragraph = body.appendParagraph(text);
  paragraph
      .setAlignment(DocumentApp.HorizontalAlignment.LEFT)
      .setSpacingBefore(config.spacingBefore == null ? 0 : config.spacingBefore)
      .setSpacingAfter(config.spacingAfter == null ? LAYOUT_SPEC.meta.spacingAfter : config.spacingAfter)
      .setLineSpacing(config.lineSpacing || LAYOUT_SPEC.meta.lineSpacing);
  const paragraphText = applyTextStyle_(paragraph.editAsText(), {
    fontFamily: LAYOUT_SPEC.fonts.body,
    fontSize: config.size || LAYOUT_SPEC.meta.size,
    color: config.color || LAYOUT_SPEC.colors.muted,
    bold: !!config.bold,
    italic: config.italic == null ? true : !!config.italic,
  });
  applyInlineLinks_(paragraphText, paragraph.getText(), config.links || []);
  return paragraph;
}

function appendBullet_(body, text, links, options) {
  const config = options || {};
  const bulletStyle = resolveBulletRenderStyle_(text);
  const item = body.appendListItem(text);
  item
      .setGlyphType(DocumentApp.GlyphType.BULLET)
      .setIndentStart(LAYOUT_SPEC.bullet.indentStart)
      .setIndentFirstLine(LAYOUT_SPEC.bullet.indentFirstLine)
      .setSpacingBefore(config.spacingBefore == null ? LAYOUT_SPEC.bullet.spacingBefore : config.spacingBefore)
      .setSpacingAfter(config.spacingAfter == null ? LAYOUT_SPEC.bullet.spacingAfter : config.spacingAfter)
      .setLineSpacing(config.lineSpacing || bulletStyle.lineSpacing);
  const bulletText = applyTextStyle_(item.editAsText(), {
    fontFamily: LAYOUT_SPEC.fonts.body,
    fontSize: config.fontSize || bulletStyle.fontSize,
    color: LAYOUT_SPEC.colors.text,
  });
  applyInlineLinks_(bulletText, item.getText(), links || []);
  return item;
}

function appendExperienceEntry_(body, entry, spacingBefore) {
  let hasLeadParagraph = false;
  const titleLineText = buildInlineTitleDateLine_(entry.title, entry.date);
  if (titleLineText) {
    const titleLine = body.appendParagraph(titleLineText);
    titleLine
        .setAlignment(DocumentApp.HorizontalAlignment.LEFT)
        .setSpacingBefore(spacingBefore || 0)
        .setSpacingAfter(0)
        .setLineSpacing(LAYOUT_SPEC.experience.lineSpacing);
    const titleText = applyTextStyle_(titleLine.editAsText(), {
      fontFamily: LAYOUT_SPEC.fonts.body,
      fontSize: LAYOUT_SPEC.experience.titleSize,
      color: LAYOUT_SPEC.colors.text,
    });
    styleInlineTitleDateText_(titleText, entry.title, entry.date);
    applyInlineLinks_(titleText, titleLineText, entry.links);
    hasLeadParagraph = true;
  }

  const metaLine = buildExperienceMetaLine_(entry);
  if (metaLine) {
    appendMetaParagraph_(body, metaLine, {
      spacingBefore: hasLeadParagraph ? 0 : spacingBefore || 0,
      spacingAfter: 1,
      italic: true,
      links: entry.links,
    });
    hasLeadParagraph = true;
  }

  entry.bullets.forEach(function(bullet, index) {
    appendBullet_(body, bullet, entry.links, {
      spacingBefore: !hasLeadParagraph && index === 0 ? spacingBefore || 0 : 0,
    });
  });
}

function appendProjectEntry_(body, entry, spacingBefore) {
  let hasLeadParagraph = false;
  const titleLineText = buildInlineTitleDateLine_(entry.title, entry.date);
  if (titleLineText) {
    const titleLine = body.appendParagraph(titleLineText);
    titleLine
        .setAlignment(DocumentApp.HorizontalAlignment.LEFT)
        .setSpacingBefore(spacingBefore || 0)
        .setSpacingAfter(0)
        .setLineSpacing(LAYOUT_SPEC.projects.lineSpacing);
    const titleText = applyTextStyle_(titleLine.editAsText(), {
      fontFamily: LAYOUT_SPEC.fonts.body,
      fontSize: LAYOUT_SPEC.projects.titleSize,
      color: LAYOUT_SPEC.colors.text,
    });
    styleInlineTitleDateText_(titleText, entry.title, entry.date);
    applyInlineLinks_(titleText, titleLineText, entry.links);
    hasLeadParagraph = true;
  }

  if (entry.subtitle) {
    appendMetaParagraph_(body, entry.subtitle, {
      spacingBefore: hasLeadParagraph ? 0 : spacingBefore || 0,
      spacingAfter: 0,
      italic: true,
      links: entry.links,
    });
    hasLeadParagraph = true;
  }

  if (entry.summary) {
    appendBodyParagraph_(body, entry.summary, {
      spacingBefore: hasLeadParagraph ? 0 : spacingBefore || 0,
      spacingAfter: LAYOUT_SPEC.projects.summarySpacingAfter,
      lineSpacing: LAYOUT_SPEC.body.lineSpacing,
      size: LAYOUT_SPEC.body.size,
      links: entry.links,
    });
    hasLeadParagraph = true;
  }

  entry.bullets.forEach(function(bullet, index) {
    appendBullet_(body, bullet, entry.links, {
      spacingBefore: !hasLeadParagraph && index === 0 ? spacingBefore || 0 : 0,
    });
  });
}

function appendEducationSection_(body, section, isFirstSection) {
  const layoutVariant = resolveEducationLayoutVariant_(section);
  if (layoutVariant === 'inline') {
    appendInlineEducationSection_(body, section, isFirstSection);
    return;
  }

  appendSectionHeader_(body, section.heading, isFirstSection);
  section.entries.forEach(function(entry, index) {
    appendEducationEntry_(body, entry, index === 0 ? 0 : LAYOUT_SPEC.education.entrySpacingBefore);
  });
}

function appendInlineEducationSection_(body, section, isFirstSection) {
  const entry = section.entries[0];
  const line = buildInlineEducationLine_(section.heading, entry);
  const paragraph = body.appendParagraph(line.text);
  paragraph
      .setAlignment(DocumentApp.HorizontalAlignment.LEFT)
      .setSpacingBefore(
          isFirstSection ? LAYOUT_SPEC.sectionHeader.firstSpacingBefore :
          LAYOUT_SPEC.sectionHeader.spacingBefore)
      .setSpacingAfter(0)
      .setLineSpacing(LAYOUT_SPEC.education.lineSpacing);

  const text = applyTextStyle_(paragraph.editAsText(), {
    fontFamily: LAYOUT_SPEC.fonts.body,
    fontSize: LAYOUT_SPEC.body.size,
    color: LAYOUT_SPEC.colors.text,
  });

  line.ranges.forEach(function(range) {
    if (range.end < range.start) {
      return;
    }
    text
        .setFontSize(range.start, range.end, range.fontSize)
        .setForegroundColor(range.start, range.end, range.color);
    if (range.bold != null) {
      text.setBold(range.start, range.end, !!range.bold);
    }
    if (range.italic != null) {
      text.setItalic(range.start, range.end, !!range.italic);
    }
  });

  applyInlineLinks_(text, line.text, entry.links);
}

function appendEducationEntry_(body, entry, spacingBefore) {
  let hasLeadParagraph = false;
  const titleLineText = buildInlineTitleDateLine_(entry.credential, entry.date);
  if (titleLineText) {
    const titleLine = body.appendParagraph(titleLineText);
    titleLine
        .setAlignment(DocumentApp.HorizontalAlignment.LEFT)
        .setSpacingBefore(spacingBefore || 0)
        .setSpacingAfter(0)
        .setLineSpacing(LAYOUT_SPEC.education.lineSpacing);
    const titleText = applyTextStyle_(titleLine.editAsText(), {
      fontFamily: LAYOUT_SPEC.fonts.body,
      fontSize: LAYOUT_SPEC.education.titleSize,
      color: LAYOUT_SPEC.colors.text,
    });
    styleInlineTitleDateText_(titleText, entry.credential, entry.date);
    applyInlineLinks_(titleText, titleLineText, entry.links);
    hasLeadParagraph = true;
  }

  const metaLineText = joinNonEmpty_([entry.institution, entry.location], PIPE_SEPARATOR);
  if (metaLineText) {
    appendMetaParagraph_(body, metaLineText, {
      spacingBefore: hasLeadParagraph ? 0 : spacingBefore || 0,
      spacingAfter: entry.details ? 0 : 0,
      italic: true,
      links: entry.links,
    });
    hasLeadParagraph = true;
  }

  if (entry.details) {
    appendBodyParagraph_(body, entry.details, {
      spacingBefore: hasLeadParagraph ? 0 : spacingBefore || 0,
      spacingAfter: 0,
      lineSpacing: LAYOUT_SPEC.body.lineSpacing,
      size: LAYOUT_SPEC.body.size,
      links: entry.links,
    });
  }
}

function applyTextStyle_(text, options) {
  text
      .setFontFamily(options.fontFamily || LAYOUT_SPEC.fonts.body)
      .setFontSize(options.fontSize || LAYOUT_SPEC.body.size)
      .setForegroundColor(options.color || LAYOUT_SPEC.colors.text)
      .setBold(!!options.bold)
      .setItalic(!!options.italic);
  return text;
}

function applyInlineLinks_(text, fullText, links) {
  (links || []).forEach(function(link) {
    if (!link || !cleanText_(link.label) || !cleanText_(link.url)) {
      return;
    }
    const range = findUniqueRangeInText_(fullText, link.label);
    if (!range) {
      return;
    }
    text
        .setLinkUrl(range.start, range.end, link.url)
        .setForegroundColor(range.start, range.end, LAYOUT_SPEC.colors.link);
  });
}

function resetDocument_(doc, body) {
  body.clear();

  const header = doc.getHeader();
  if (header) {
    header.clear();
  }
  const footer = doc.getFooter();
  if (footer) {
    footer.clear();
  }

  body
      .setPageWidth(LAYOUT_SPEC.page.width)
      .setPageHeight(LAYOUT_SPEC.page.height)
      .setMarginTop(LAYOUT_SPEC.page.marginTop)
      .setMarginBottom(LAYOUT_SPEC.page.marginBottom)
      .setMarginLeft(LAYOUT_SPEC.page.marginLeft)
      .setMarginRight(LAYOUT_SPEC.page.marginRight);
}

function maybeRenameDocument_(doc, title) {
  const cleanTitle = cleanText_(title);
  if (!cleanTitle) {
    return;
  }
  try {
    DriveApp.getFileById(doc.getId()).setName(cleanTitle);
  } catch (error) {
    Logger.log('Resume template note: could not rename the target Google Doc: ' + error.message);
  }
}

function takeReusableParagraph_(body, text) {
  if (body.getNumChildren() === 1) {
    const child = body.getChild(0);
    if (child.getType() === DocumentApp.ElementType.PARAGRAPH) {
      const paragraph = child.asParagraph();
      if (!paragraph.getText()) {
        paragraph.setText(text);
        return paragraph;
      }
    }
  }
  return body.appendParagraph(text);
}

function normalizeContactItems_(items) {
  return dedupeObjectsByKey_((items || []).map(function(item) {
    const type = cleanText_(item.type);
    const rawText = cleanText_(item.text);
    const rawUrl = cleanText_(item.url);
    const text = rawText || inferContactDisplayText_(type, rawUrl);
    return {
      type: type,
      text: text,
      url: normalizeContactItemUrl_(type, rawUrl || rawText),
    };
  }).filter(function(item) {
    return item.type && item.text;
  }), function(item) {
    return [item.type, item.text.toLowerCase(), String(item.url || '').toLowerCase()].join('||');
  });
}

function normalizeExperienceEntries_(entries) {
  return (entries || []).map(function(entry) {
    return {
      title: cleanText_(entry.title),
      org: cleanText_(entry.org),
      location: cleanText_(entry.location),
      date: cleanText_(entry.date),
      subtitle: cleanText_(entry.subtitle),
      bullets: normalizeBulletArray_(entry.bullets),
      links: normalizeLinkArray_(entry.links),
    };
  }).filter(hasExperienceEntryContent_);
}

function normalizeProjectEntries_(entries) {
  return (entries || []).map(function(entry) {
    return {
      title: cleanText_(entry.title),
      date: cleanText_(entry.date),
      subtitle: cleanText_(entry.subtitle),
      summary: cleanText_(entry.summary),
      bullets: normalizeBulletArray_(entry.bullets),
      links: normalizeLinkArray_(entry.links),
    };
  }).filter(hasProjectEntryContent_);
}

function normalizeEducationEntries_(entries) {
  return (entries || []).map(function(entry) {
    return {
      institution: cleanText_(entry.institution),
      credential: cleanText_(entry.credential),
      location: cleanText_(entry.location),
      date: cleanText_(entry.date),
      details: cleanText_(entry.details),
      links: normalizeLinkArray_(entry.links),
    };
  }).filter(hasEducationEntryContent_);
}

function normalizeCustomSectionEntries_(kind, entries) {
  if (kind === 'experience') {
    return normalizeExperienceEntries_(entries);
  }
  if (kind === 'projects') {
    return normalizeProjectEntries_(entries);
  }
  if (kind === 'education') {
    return normalizeEducationEntries_(entries);
  }
  return [];
}

function appendNormalizedSectionIfPresent_(target, section) {
  if (hasRenderableSectionContent_(section)) {
    target.push(section);
  }
}

function compareNormalizedSections_(left, right) {
  if (left.order !== right.order) {
    return left.order - right.order;
  }
  return String(left.heading).localeCompare(String(right.heading));
}

function normalizeStringArray_(items) {
  return (items || []).map(cleanText_).filter(Boolean);
}

function normalizeLinkArray_(links) {
  return dedupeObjectsByKey_((links || []).map(function(link) {
    const url = addHttps_(link && link.url);
    const label = cleanText_(link && link.label) || simplifyUrlForDisplay_(url);
    return {
      label: label,
      url: url,
    };
  }).filter(function(link) {
    return link.label && link.url;
  }), function(link) {
    return link.label.toLowerCase() + '||' + link.url.toLowerCase();
  });
}

function normalizeBulletArray_(items) {
  return (items || []).map(normalizeBulletText_).filter(Boolean);
}

function hasExperienceEntryContent_(entry) {
  return !!(
    entry.title || entry.org || entry.location || entry.date ||
    entry.subtitle || entry.bullets.length
  );
}

function hasProjectEntryContent_(entry) {
  return !!(
    entry.title || entry.date || entry.subtitle || entry.summary || entry.bullets.length
  );
}

function hasEducationEntryContent_(entry) {
  return !!(
    entry.institution || entry.credential || entry.location || entry.date || entry.details
  );
}

function buildInlineTitleDateLine_(title, date) {
  const cleanTitle = cleanText_(title);
  const cleanDate = cleanText_(date);
  if (cleanTitle && cleanDate) {
    return cleanTitle + ' ' + cleanDate;
  }
  return cleanTitle || cleanDate;
}

function styleInlineTitleDateText_(text, title, date) {
  const cleanTitle = cleanText_(title);
  const cleanDate = cleanText_(date);
  if (cleanTitle) {
    text.setBold(0, cleanTitle.length - 1, true);
  }

  if (cleanTitle && cleanDate) {
    const dateStart = cleanTitle.length + 1;
    const dateEnd = dateStart + cleanDate.length - 1;
    text
        .setBold(dateStart, dateEnd, false)
        .setForegroundColor(dateStart, dateEnd, LAYOUT_SPEC.colors.muted);
    return;
  }

  if (!cleanTitle && cleanDate) {
    text
        .setBold(0, cleanDate.length - 1, false)
        .setForegroundColor(0, cleanDate.length - 1, LAYOUT_SPEC.colors.muted);
  }
}

function buildExperienceMetaLine_(entry) {
  let line = cleanText_(entry.org);
  if (cleanText_(entry.subtitle)) {
    line += (line ? ROLE_META_DASH : '') + cleanText_(entry.subtitle);
  }
  if (cleanText_(entry.location)) {
    line += (line ? PIPE_SEPARATOR : '') + cleanText_(entry.location);
  }
  return line;
}

function buildInlineEducationLine_(heading, entry) {
  const ranges = [];
  const parts = [];
  let cursor = 0;

  function pushSegment(text, style) {
    if (!text) {
      return;
    }
    const start = cursor;
    parts.push(text);
    cursor += text.length;
    ranges.push({
      start: start,
      end: cursor - 1,
      fontSize: style.fontSize,
      color: style.color,
      bold: style.bold,
      italic: style.italic,
    });
  }

  const headingText = String(heading).toUpperCase() + ': ';
  pushSegment(headingText, {
    fontSize: LAYOUT_SPEC.sectionHeader.size,
    color: LAYOUT_SPEC.colors.text,
    bold: true,
    italic: false,
  });

  const institutionBlock = joinNonEmpty_(
      [entry.institution, entry.location],
      PIPE_SEPARATOR);
  if (institutionBlock) {
    pushSegment(institutionBlock, {
      fontSize: LAYOUT_SPEC.education.institutionSize,
      color: LAYOUT_SPEC.colors.muted,
      bold: false,
      italic: true,
    });
  }

  if (entry.date) {
    pushSegment((institutionBlock ? ' ' : '') + entry.date, {
      fontSize: LAYOUT_SPEC.education.titleSize,
      color: LAYOUT_SPEC.colors.muted,
      bold: false,
      italic: false,
    });
  }

  const credentialBlock = joinNonEmpty_([entry.credential, entry.details], ' ');
  if (credentialBlock) {
    const prefix = parts.length ? ' - ' : '';
    pushSegment(prefix + credentialBlock, {
      fontSize: LAYOUT_SPEC.education.titleSize,
      color: LAYOUT_SPEC.colors.text,
      bold: true,
      italic: false,
    });
  }

  return {
    text: parts.join(''),
    ranges: ranges,
  };
}

function resolveEducationLayoutVariant_(section) {
  if (section.layoutVariant === 'inline' || section.layoutVariant === 'stacked') {
    return section.layoutVariant;
  }
  if (section.entries.length !== 1) {
    return 'stacked';
  }

  const entry = section.entries[0];
  const inlineText = joinNonEmpty_(
      [entry.institution, entry.location, entry.date, entry.credential, entry.details],
      PIPE_SEPARATOR);
  return inlineText.length <= LAYOUT_SPEC.education.inlineThresholdChars ? 'inline' : 'stacked';
}

function toInlineLinkSpecs_(contactItems) {
  return (contactItems || []).map(function(item) {
    return {
      label: item.text,
      url: buildContactUrl_(item),
    };
  }).filter(function(link) {
    return link.label && link.url;
  });
}

function buildContactUrl_(item) {
  if (item.type === 'email') {
    return cleanText_(item.url) || ('mailto:' + item.text);
  }
  if (item.type === 'link') {
    return cleanText_(item.url) || addHttps_(item.text);
  }
  return cleanText_(item.url);
}

function addHttps_(url) {
  const cleanUrl = cleanText_(url);
  if (!cleanUrl) {
    return '';
  }
  return /^(https?:\/\/|mailto:)/i.test(cleanUrl) ? cleanUrl : 'https://' + cleanUrl;
}

function assertRenderableProfile_(normalized) {
  if (!normalized.header.name) {
    throw new Error(
        'Rendering requires header.name. Fill in profile.header.name or start ' +
        'from getSampleResumeProfile_().');
  }
  if (!normalized.sections.length) {
    throw new Error(
        'Rendering requires at least one visible section with content. ' +
        'Enable a section and add content. The blank template is schema-valid ' +
        'but not directly renderable.');
  }
}

function logDensityWarningIfNeeded_(normalized) {
  const risks = detectRenderRisks_(normalized);
  if (!risks.requiresWarning) {
    return;
  }

  Logger.log(buildRenderRiskWarningMessage_(risks));
}

function estimateRenderedLineCount_(normalized) {
  let lines = 0;
  lines += 1;
  if (normalized.header.contactItems.length) {
    lines += Math.max(1, splitHeaderContactItemsForLayout_(normalized.header.contactItems).length);
  }
  if (normalized.header.headline) {
    lines += estimateWrappedLines_(
        normalized.header.headline, LAYOUT_SPEC.compaction.headlineLineWidth);
  }

  normalized.sections.forEach(function(section) {
    if (section.kind === 'summary') {
      if (section.renderHeading) {
        lines += 1;
      }
      section.paragraphs.forEach(function(paragraph) {
        lines += estimateWrappedLines_(
            paragraph, LAYOUT_SPEC.compaction.summaryParagraphLineWidth);
      });
      return;
    }

    if (section.kind === 'education' && resolveEducationLayoutVariant_(section) === 'inline') {
      lines += estimateWrappedLines_(
          buildInlineEducationLine_(section.heading, section.entries[0]).text,
          LAYOUT_SPEC.compaction.metaLineWidth);
      return;
    }

    lines += 1;

    if (section.kind === 'inline_list') {
      lines += Math.max(
          1,
          splitInlineListForLayout_(section.items, section.preferredLineCount).length);
      return;
    }

    if (section.kind === 'experience') {
      section.entries.forEach(function(entry) {
        const titleLine = buildInlineTitleDateLine_(entry.title, entry.date);
        if (titleLine) {
          lines += estimateWrappedLines_(titleLine, LAYOUT_SPEC.compaction.titleLineWidth);
        }
        const metaLine = buildExperienceMetaLine_(entry);
        if (metaLine) {
          lines += estimateWrappedLines_(metaLine, LAYOUT_SPEC.compaction.metaLineWidth);
        }
        entry.bullets.forEach(function(bullet) {
          lines += estimateWrappedLines_(bullet, LAYOUT_SPEC.compaction.bulletLineWidth);
        });
      });
      return;
    }

    if (section.kind === 'projects') {
      section.entries.forEach(function(entry) {
        const titleLine = buildInlineTitleDateLine_(entry.title, entry.date);
        if (titleLine) {
          lines += estimateWrappedLines_(titleLine, LAYOUT_SPEC.compaction.titleLineWidth);
        }
        if (entry.subtitle) {
          lines += estimateWrappedLines_(entry.subtitle, LAYOUT_SPEC.compaction.metaLineWidth);
        }
        if (entry.summary) {
          lines += estimateWrappedLines_(
              entry.summary, LAYOUT_SPEC.compaction.summaryParagraphLineWidth);
        }
        entry.bullets.forEach(function(bullet) {
          lines += estimateWrappedLines_(bullet, LAYOUT_SPEC.compaction.bulletLineWidth);
        });
      });
      return;
    }

    if (section.kind === 'education') {
      section.entries.forEach(function(entry) {
        const titleLine = buildInlineTitleDateLine_(entry.credential, entry.date);
        if (titleLine) {
          lines += estimateWrappedLines_(titleLine, LAYOUT_SPEC.compaction.titleLineWidth);
        }
        const metaLine = joinNonEmpty_([entry.institution, entry.location], PIPE_SEPARATOR);
        if (metaLine) {
          lines += estimateWrappedLines_(metaLine, LAYOUT_SPEC.compaction.metaLineWidth);
        }
        if (entry.details) {
          lines += estimateWrappedLines_(entry.details, LAYOUT_SPEC.compaction.summaryParagraphLineWidth);
        }
      });
      return;
    }

    if (section.kind === 'paragraphs') {
      section.paragraphs.forEach(function(paragraph) {
        lines += estimateWrappedLines_(
            paragraph, LAYOUT_SPEC.compaction.summaryParagraphLineWidth);
      });
      return;
    }

    if (section.kind === 'bulleted_list') {
      section.bullets.forEach(function(bullet) {
        lines += estimateWrappedLines_(bullet, LAYOUT_SPEC.compaction.bulletLineWidth);
      });
    }
  });

  return lines;
}

function estimateWrappedLines_(text, width) {
  const clean = cleanText_(text);
  if (!clean) {
    return 0;
  }
  return Math.max(1, Math.ceil(clean.length / Math.max(width, 1)));
}

function resolveHeadlineFontSize_(headline) {
  const length = cleanText_(headline).length;
  if (length > LAYOUT_SPEC.compaction.longHeadlineChars) {
    return LAYOUT_SPEC.compaction.headlineMinimumSize;
  }
  if (length > LAYOUT_SPEC.compaction.headlineLineWidth) {
    return LAYOUT_SPEC.compaction.headlineReducedSize;
  }
  return LAYOUT_SPEC.header.headlineSize;
}

function resolveBulletRenderStyle_(text) {
  const bulletLength = cleanText_(text).length;
  if (bulletLength > LAYOUT_SPEC.compaction.veryLongBulletChars) {
    return {
      fontSize: 9.0,
      lineSpacing: 1.0,
    };
  }
  if (bulletLength > LAYOUT_SPEC.compaction.longBulletChars) {
    return {
      fontSize: 9.1,
      lineSpacing: 1.0,
    };
  }
  return {
    fontSize: LAYOUT_SPEC.body.size,
    lineSpacing: LAYOUT_SPEC.bullet.lineSpacing,
  };
}

function splitHeaderContactItemsForLayout_(items) {
  const cleanItems = normalizeContactItems_(items);
  if (!cleanItems.length) {
    return [];
  }

  const combinedLength = cleanItems.map(function(item) {
    return item.text;
  }).join(PIPE_SEPARATOR).length;
  if (combinedLength <= LAYOUT_SPEC.compaction.contactLineWidth || cleanItems.length === 1) {
    return [cleanItems];
  }

  const estimatedLineCount = Math.min(
      LAYOUT_SPEC.compaction.maxContactLines,
      Math.max(2, Math.ceil(combinedLength / LAYOUT_SPEC.compaction.contactLineWidth)));
  return splitObjectItemsSequentially_(
      cleanItems,
      estimatedLineCount,
      function(item) {
        return item.text;
      },
      PIPE_SEPARATOR);
}

function splitInlineListForLayout_(items, preferredLineCount) {
  const cleanItems = normalizeStringArray_(items);
  const lineCount = Math.max(
      1,
      positiveIntegerOrDefault_(
          preferredLineCount,
          LAYOUT_SPEC.compaction.preferredInlineListLines));

  if (!cleanItems.length) {
    return [];
  }
  if (lineCount === 1 || cleanItems.length === 1) {
    return [cleanItems];
  }

  return splitItemsSequentially_(cleanItems, Math.min(lineCount, cleanItems.length));
}

function splitItemsSequentially_(items, lineCount) {
  if (lineCount <= 1) {
    return [items.slice()];
  }

  const totalLength = items.reduce(function(sum, item) {
    return sum + item.length + INLINE_LIST_SEPARATOR.length;
  }, 0);
  const targetLength = totalLength / lineCount;
  const lines = [];
  let current = [];
  let currentLength = 0;

  items.forEach(function(item, index) {
    const projectedLength = currentLength + item.length + INLINE_LIST_SEPARATOR.length;
    const remainingItems = items.length - index;
    const remainingLines = lineCount - lines.length;
    const mustBreak = current.length &&
        projectedLength > targetLength &&
        remainingItems >= (remainingLines - 1);

    if (mustBreak) {
      lines.push(current);
      current = [];
      currentLength = 0;
    }

    current.push(item);
    currentLength += item.length + INLINE_LIST_SEPARATOR.length;
  });

  if (current.length) {
    lines.push(current);
  }

  return lines;
}

function splitObjectItemsSequentially_(items, lineCount, textGetter, separator) {
  if (lineCount <= 1) {
    return [items.slice()];
  }

  const joinerLength = String(separator || '').length;
  const totalLength = items.reduce(function(sum, item) {
    return sum + cleanText_(textGetter(item)).length + joinerLength;
  }, 0);
  const targetLength = totalLength / lineCount;
  const lines = [];
  let current = [];
  let currentLength = 0;

  items.forEach(function(item, index) {
    const itemLength = cleanText_(textGetter(item)).length + joinerLength;
    const projectedLength = currentLength + itemLength;
    const remainingItems = items.length - index;
    const remainingLines = lineCount - lines.length;
    const mustBreak = current.length &&
        projectedLength > targetLength &&
        remainingItems >= (remainingLines - 1);

    if (mustBreak) {
      lines.push(current);
      current = [];
      currentLength = 0;
    }

    current.push(item);
    currentLength += itemLength;
  });

  if (current.length) {
    lines.push(current);
  }

  return lines;
}

function hasRenderableSectionContent_(section) {
  if (!section || !section.visible) {
    return false;
  }
  if (section.kind === 'summary') {
    return !!section.paragraphs.length;
  }
  if (section.kind === 'inline_list') {
    return !!section.items.length;
  }
  if (section.kind === 'experience' || section.kind === 'projects' || section.kind === 'education') {
    return !!section.entries.length;
  }
  if (section.kind === 'paragraphs') {
    return !!section.paragraphs.length;
  }
  if (section.kind === 'bulleted_list') {
    return !!section.bullets.length;
  }
  return false;
}

function detectRenderRisks_(normalized) {
  const risks = {
    estimatedLines: estimateRenderedLineCount_(normalized),
    onePageLineBudget: LAYOUT_SPEC.compaction.onePageLineBudget,
    contactLineCount: splitHeaderContactItemsForLayout_(normalized.header.contactItems).length,
    headlineEstimatedLines: normalized.header.headline ?
        estimateWrappedLines_(normalized.header.headline, LAYOUT_SPEC.compaction.headlineLineWidth) : 0,
    longBulletCount: 0,
    longBulletExamples: [],
    longLinkLabelCount: 0,
    longLinkExamples: [],
    longTitleLineCount: 0,
    longTitleExamples: [],
    longContactItemCount: 0,
    longContactExamples: [],
  };

  normalized.header.contactItems.forEach(function(item) {
    if (item.text.length > LAYOUT_SPEC.compaction.longContactItemChars) {
      risks.longContactItemCount += 1;
      maybePushRiskExample_(risks.longContactExamples, item.text);
    }
  });

  normalized.sections.forEach(function(section) {
    if (section.kind === 'experience') {
      section.entries.forEach(function(entry) {
        collectEntryRenderRisks_(risks, buildInlineTitleDateLine_(entry.title, entry.date), entry.bullets, entry.links);
      });
      return;
    }

    if (section.kind === 'projects') {
      section.entries.forEach(function(entry) {
        collectEntryRenderRisks_(risks, buildInlineTitleDateLine_(entry.title, entry.date), entry.bullets, entry.links);
      });
      return;
    }

    if (section.kind === 'education') {
      section.entries.forEach(function(entry) {
        collectEntryRenderRisks_(risks, buildInlineTitleDateLine_(entry.credential, entry.date), [], entry.links);
      });
      return;
    }

    if (section.kind === 'bulleted_list') {
      section.bullets.forEach(function(bullet) {
        collectBulletRisk_(risks, bullet);
      });
    }
  });

  risks.requiresWarning = (
    risks.estimatedLines > risks.onePageLineBudget ||
    risks.contactLineCount > 1 ||
    risks.headlineEstimatedLines > 1 ||
    risks.longBulletCount > 0 ||
    risks.longLinkLabelCount > 0 ||
    risks.longTitleLineCount > 0 ||
    risks.longContactItemCount > 0
  );
  return risks;
}

function collectEntryRenderRisks_(risks, titleLine, bullets, links) {
  if (cleanText_(titleLine).length > LAYOUT_SPEC.compaction.longTitleLineChars) {
    risks.longTitleLineCount += 1;
    maybePushRiskExample_(risks.longTitleExamples, titleLine);
  }
  (bullets || []).forEach(function(bullet) {
    collectBulletRisk_(risks, bullet);
  });
  (links || []).forEach(function(link) {
    if (cleanText_(link.label).length > LAYOUT_SPEC.compaction.longLinkLabelChars) {
      risks.longLinkLabelCount += 1;
      maybePushRiskExample_(risks.longLinkExamples, link.label);
    }
  });
}

function collectBulletRisk_(risks, bullet) {
  if (cleanText_(bullet).length > LAYOUT_SPEC.compaction.longBulletChars) {
    risks.longBulletCount += 1;
    maybePushRiskExample_(risks.longBulletExamples, bullet);
  }
}

function maybePushRiskExample_(target, value) {
  if (target.length >= 2) {
    return;
  }
  target.push(cleanText_(value));
}

function buildRenderRiskWarningMessage_(risks) {
  const parts = [
    LAYOUT_SPEC.compaction.warningPrefix,
    'Estimated lines: ' + risks.estimatedLines + ' vs budget ' + risks.onePageLineBudget + '.',
  ];

  if (risks.contactLineCount > 1) {
    parts.push('Header contact line will likely wrap to ' + risks.contactLineCount + ' lines.');
  }
  if (risks.headlineEstimatedLines > 1) {
    parts.push('Headline may wrap to ' + risks.headlineEstimatedLines + ' lines.');
  }
  if (risks.longContactItemCount) {
    parts.push('Long contact items: ' + risks.longContactExamples.join('; ') + '.');
  }
  if (risks.longTitleLineCount) {
    parts.push('Long title/date lines: ' + risks.longTitleExamples.join('; ') + '.');
  }
  if (risks.longBulletCount) {
    parts.push('Long bullets: ' + risks.longBulletExamples.join('; ') + '.');
  }
  if (risks.longLinkLabelCount) {
    parts.push('Long link labels: ' + risks.longLinkExamples.join('; ') + '.');
  }

  parts.push('Rendering will continue. Shorten content if you need a stricter one-page result.');
  return parts.join(' ');
}

function serializeResumeProfileToTxt_(profile) {
  const lines = [];
  const source = cloneData_(profile);

  forbidPipeInTxtValue_(source.metadata.documentTitle, 'metadata.documentTitle');
  lines.push('[METADATA]');
  lines.push('DOCUMENT_TITLE: ' + cleanText_(source.metadata.documentTitle));
  lines.push('ALLOW_PAGE_SPILL: ' + String(source.metadata.allowPageSpill));
  lines.push('');

  forbidPipeInTxtValue_(source.header.name, 'header.name');
  forbidPipeInTxtValue_(source.header.headline, 'header.headline');
  lines.push('[HEADER]');
  lines.push('NAME: ' + cleanText_(source.header.name));
  lines.push('HEADLINE: ' + cleanText_(source.header.headline));
  (source.header.contactItems || []).forEach(function(item) {
    forbidPipeInTxtValue_(item.text, 'header.contactItems.text');
    forbidPipeInTxtValue_(item.url, 'header.contactItems.url');
    const parts = [cleanText_(item.type), cleanText_(item.text)];
    if (cleanText_(item.url)) {
      parts.push(cleanText_(item.url));
    }
    lines.push('CONTACT: ' + parts.join(PIPE_SEPARATOR));
  });
  lines.push('');

  pushSummarySectionTxt_(lines, source.summary);
  pushInlineListSectionTxt_(lines, 'CORE_TECHNOLOGIES', source.coreTechnologies);

  (source.experienceSections || []).forEach(function(section) {
    pushExperienceSectionTxt_(lines, section);
  });

  (source.customSections || []).forEach(function(section) {
    pushCustomSectionTxt_(lines, section);
  });

  pushProjectsSectionTxt_(lines, source.technicalProjects);
  pushEducationSectionTxt_(lines, source.education);

  return lines.join('\n').replace(/\n{3,}/g, '\n\n').trim() + '\n';
}

function pushSummarySectionTxt_(lines, section) {
  lines.push('[SUMMARY]');
  pushCommonSectionTxtFields_(lines, section);
  lines.push('RENDER_HEADING: ' + String(section.renderHeading));
  (section.paragraphs || []).forEach(function(paragraph) {
    forbidPipeInTxtValue_(paragraph, 'summary.paragraph');
    lines.push('PARAGRAPH: ' + cleanText_(paragraph));
  });
  lines.push('');
}

function pushInlineListSectionTxt_(lines, label, section) {
  lines.push('[' + label + ']');
  pushCommonSectionTxtFields_(lines, section);
  lines.push('PREFERRED_LINE_COUNT: ' + String(section.preferredLineCount));
  (section.items || []).forEach(function(item) {
    forbidPipeInTxtValue_(item, label + '.item');
    lines.push('ITEM: ' + cleanText_(item));
  });
  lines.push('');
}

function pushExperienceSectionTxt_(lines, section) {
  lines.push('[EXPERIENCE_SECTION]');
  pushCommonSectionTxtFields_(lines, section);
  (section.entries || []).forEach(function(entry, index) {
    if (index > 0) {
      lines.push('---');
    }
    forbidPipeInTxtValue_(entry.title, 'experience.title');
    forbidPipeInTxtValue_(entry.org, 'experience.org');
    forbidPipeInTxtValue_(entry.location, 'experience.location');
    forbidPipeInTxtValue_(entry.date, 'experience.date');
    forbidPipeInTxtValue_(entry.subtitle, 'experience.subtitle');
    lines.push('TITLE: ' + cleanText_(entry.title));
    lines.push('ORG: ' + cleanText_(entry.org));
    lines.push('LOCATION: ' + cleanText_(entry.location));
    lines.push('DATE: ' + cleanText_(entry.date));
    lines.push(formatTxtFieldLine_('SUBTITLE', entry.subtitle));
    (entry.bullets || []).forEach(function(bullet) {
      forbidPipeInTxtValue_(bullet, 'experience.bullet');
      lines.push('BULLET: ' + cleanText_(bullet));
    });
    (entry.links || []).forEach(function(link) {
      forbidPipeInTxtValue_(link.label, 'experience.link.label');
      forbidPipeInTxtValue_(link.url, 'experience.link.url');
      lines.push('LINK: ' + cleanText_(link.label) + PIPE_SEPARATOR + cleanText_(link.url));
    });
  });
  lines.push('');
}

function pushProjectsSectionTxt_(lines, section) {
  lines.push('[TECHNICAL_PROJECTS]');
  pushCommonSectionTxtFields_(lines, section);
  (section.entries || []).forEach(function(entry, index) {
    if (index > 0) {
      lines.push('---');
    }
    forbidPipeInTxtValue_(entry.title, 'projects.title');
    forbidPipeInTxtValue_(entry.date, 'projects.date');
    forbidPipeInTxtValue_(entry.subtitle, 'projects.subtitle');
    forbidPipeInTxtValue_(entry.summary, 'projects.summary');
    lines.push('TITLE: ' + cleanText_(entry.title));
    lines.push('DATE: ' + cleanText_(entry.date));
    lines.push(formatTxtFieldLine_('SUBTITLE', entry.subtitle));
    lines.push('SUMMARY: ' + cleanText_(entry.summary));
    (entry.bullets || []).forEach(function(bullet) {
      forbidPipeInTxtValue_(bullet, 'projects.bullet');
      lines.push('BULLET: ' + cleanText_(bullet));
    });
    (entry.links || []).forEach(function(link) {
      forbidPipeInTxtValue_(link.label, 'projects.link.label');
      forbidPipeInTxtValue_(link.url, 'projects.link.url');
      lines.push('LINK: ' + cleanText_(link.label) + PIPE_SEPARATOR + cleanText_(link.url));
    });
  });
  lines.push('');
}

function pushEducationSectionTxt_(lines, section) {
  lines.push('[EDUCATION]');
  pushCommonSectionTxtFields_(lines, section);
  forbidPipeInTxtValue_(section.layoutVariant, 'education.layoutVariant');
  lines.push('LAYOUT_VARIANT: ' + cleanText_(section.layoutVariant));
  (section.entries || []).forEach(function(entry, index) {
    if (index > 0) {
      lines.push('---');
    }
    forbidPipeInTxtValue_(entry.institution, 'education.institution');
    forbidPipeInTxtValue_(entry.credential, 'education.credential');
    forbidPipeInTxtValue_(entry.location, 'education.location');
    forbidPipeInTxtValue_(entry.date, 'education.date');
    forbidPipeInTxtValue_(entry.details, 'education.details');
    lines.push('INSTITUTION: ' + cleanText_(entry.institution));
    lines.push('CREDENTIAL: ' + cleanText_(entry.credential));
    lines.push('LOCATION: ' + cleanText_(entry.location));
    lines.push('DATE: ' + cleanText_(entry.date));
    lines.push('DETAILS: ' + cleanText_(entry.details));
    (entry.links || []).forEach(function(link) {
      forbidPipeInTxtValue_(link.label, 'education.link.label');
      forbidPipeInTxtValue_(link.url, 'education.link.url');
      lines.push('LINK: ' + cleanText_(link.label) + PIPE_SEPARATOR + cleanText_(link.url));
    });
  });
  lines.push('');
}

function pushCustomSectionTxt_(lines, section) {
  lines.push('[CUSTOM_SECTION]');
  pushCommonSectionTxtFields_(lines, section);
  forbidPipeInTxtValue_(section.sectionKind, 'custom.sectionKind');
  forbidPipeInTxtValue_(section.layoutVariant, 'custom.layoutVariant');
  lines.push('SECTION_KIND: ' + cleanText_(section.sectionKind));
  lines.push('PREFERRED_LINE_COUNT: ' + String(section.preferredLineCount));
  lines.push('LAYOUT_VARIANT: ' + cleanText_(section.layoutVariant));

  if (section.sectionKind === 'inline_list') {
    (section.items || []).forEach(function(item) {
      forbidPipeInTxtValue_(item, 'custom.item');
      lines.push('ITEM: ' + cleanText_(item));
    });
  } else if (section.sectionKind === 'paragraphs') {
    (section.paragraphs || []).forEach(function(paragraph) {
      forbidPipeInTxtValue_(paragraph, 'custom.paragraph');
      lines.push('PARAGRAPH: ' + cleanText_(paragraph));
    });
  } else if (section.sectionKind === 'bulleted_list') {
    (section.bullets || []).forEach(function(bullet) {
      forbidPipeInTxtValue_(bullet, 'custom.bullet');
      lines.push('BULLET: ' + cleanText_(bullet));
    });
  } else if (section.sectionKind === 'experience') {
    pushCustomExperienceEntriesTxt_(lines, section.entries);
  } else if (section.sectionKind === 'projects') {
    pushCustomProjectEntriesTxt_(lines, section.entries);
  } else if (section.sectionKind === 'education') {
    pushCustomEducationEntriesTxt_(lines, section.entries);
  }

  lines.push('');
}

function pushCustomExperienceEntriesTxt_(lines, entries) {
  (entries || []).forEach(function(entry, index) {
    if (index > 0) {
      lines.push('---');
    }
    forbidPipeInTxtValue_(entry.title, 'custom.experience.title');
    forbidPipeInTxtValue_(entry.org, 'custom.experience.org');
    forbidPipeInTxtValue_(entry.location, 'custom.experience.location');
    forbidPipeInTxtValue_(entry.date, 'custom.experience.date');
    forbidPipeInTxtValue_(entry.subtitle, 'custom.experience.subtitle');
    lines.push('TITLE: ' + cleanText_(entry.title));
    lines.push('ORG: ' + cleanText_(entry.org));
    lines.push('LOCATION: ' + cleanText_(entry.location));
    lines.push('DATE: ' + cleanText_(entry.date));
    lines.push(formatTxtFieldLine_('SUBTITLE', entry.subtitle));
    (entry.bullets || []).forEach(function(bullet) {
      forbidPipeInTxtValue_(bullet, 'custom.experience.bullet');
      lines.push('BULLET: ' + cleanText_(bullet));
    });
    (entry.links || []).forEach(function(link) {
      forbidPipeInTxtValue_(link.label, 'custom.experience.link.label');
      forbidPipeInTxtValue_(link.url, 'custom.experience.link.url');
      lines.push('LINK: ' + cleanText_(link.label) + PIPE_SEPARATOR + cleanText_(link.url));
    });
  });
}

function pushCustomProjectEntriesTxt_(lines, entries) {
  (entries || []).forEach(function(entry, index) {
    if (index > 0) {
      lines.push('---');
    }
    forbidPipeInTxtValue_(entry.title, 'custom.projects.title');
    forbidPipeInTxtValue_(entry.date, 'custom.projects.date');
    forbidPipeInTxtValue_(entry.subtitle, 'custom.projects.subtitle');
    forbidPipeInTxtValue_(entry.summary, 'custom.projects.summary');
    lines.push('TITLE: ' + cleanText_(entry.title));
    lines.push('DATE: ' + cleanText_(entry.date));
    lines.push(formatTxtFieldLine_('SUBTITLE', entry.subtitle));
    lines.push('SUMMARY: ' + cleanText_(entry.summary));
    (entry.bullets || []).forEach(function(bullet) {
      forbidPipeInTxtValue_(bullet, 'custom.projects.bullet');
      lines.push('BULLET: ' + cleanText_(bullet));
    });
    (entry.links || []).forEach(function(link) {
      forbidPipeInTxtValue_(link.label, 'custom.projects.link.label');
      forbidPipeInTxtValue_(link.url, 'custom.projects.link.url');
      lines.push('LINK: ' + cleanText_(link.label) + PIPE_SEPARATOR + cleanText_(link.url));
    });
  });
}

function pushCustomEducationEntriesTxt_(lines, entries) {
  (entries || []).forEach(function(entry, index) {
    if (index > 0) {
      lines.push('---');
    }
    forbidPipeInTxtValue_(entry.institution, 'custom.education.institution');
    forbidPipeInTxtValue_(entry.credential, 'custom.education.credential');
    forbidPipeInTxtValue_(entry.location, 'custom.education.location');
    forbidPipeInTxtValue_(entry.date, 'custom.education.date');
    forbidPipeInTxtValue_(entry.details, 'custom.education.details');
    lines.push('INSTITUTION: ' + cleanText_(entry.institution));
    lines.push('CREDENTIAL: ' + cleanText_(entry.credential));
    lines.push('LOCATION: ' + cleanText_(entry.location));
    lines.push('DATE: ' + cleanText_(entry.date));
    lines.push('DETAILS: ' + cleanText_(entry.details));
    (entry.links || []).forEach(function(link) {
      forbidPipeInTxtValue_(link.label, 'custom.education.link.label');
      forbidPipeInTxtValue_(link.url, 'custom.education.link.url');
      lines.push('LINK: ' + cleanText_(link.label) + PIPE_SEPARATOR + cleanText_(link.url));
    });
  });
}

function pushCommonSectionTxtFields_(lines, section) {
  forbidPipeInTxtValue_(section.id, 'section.id');
  forbidPipeInTxtValue_(section.heading, 'section.heading');
  lines.push('ID: ' + cleanText_(section.id));
  lines.push('HEADING: ' + cleanText_(section.heading));
  lines.push('ORDER: ' + String(section.order));
  lines.push('VISIBLE: ' + String(section.visible));
}

function formatTxtFieldLine_(key, value) {
  const cleanValue = cleanText_(value);
  return cleanValue ? (key + ': ' + cleanValue) : (key + ':');
}

function splitTxtIntoBlocks_(source) {
  const lines = source.replace(/\r/g, '').split('\n');
  const blocks = [];
  let current = null;

  lines.forEach(function(line) {
    const trimmed = line.trim();
    if (!trimmed || /^#/.test(trimmed)) {
      return;
    }

    const match = /^\[([A-Z_]+)\]$/.exec(trimmed);
    if (match) {
      current = { label: match[1], lines: [] };
      blocks.push(current);
      return;
    }

    if (!current) {
      throw new Error('TXT payload must begin with a block header like [HEADER].');
    }
    current.lines.push(trimmed);
  });

  return blocks;
}

function parseTxtBlockIntoProfile_(profile, block, blockIndex) {
  const sectionPath = '[' + block.label + '] block #' + (blockIndex + 1);

  if (block.label === 'METADATA') {
    parseMetadataTxtBlock_(profile, block.lines, sectionPath);
    return;
  }
  if (block.label === 'HEADER') {
    parseHeaderTxtBlock_(profile, block.lines, sectionPath);
    return;
  }
  if (block.label === 'SUMMARY') {
    profile.summary = parseSummaryTxtBlock_(block.lines, sectionPath);
    return;
  }
  if (block.label === 'CORE_TECHNOLOGIES') {
    profile.coreTechnologies = parseInlineListTxtBlock_(block.lines, sectionPath, {
      id: 'core-technologies',
      heading: 'CORE TECHNOLOGIES',
      order: 20,
      visible: true,
      items: [],
      preferredLineCount: 2,
    });
    return;
  }
  if (block.label === 'EXPERIENCE_SECTION') {
    profile.experienceSections.push(parseExperienceSectionTxtBlock_(block.lines, sectionPath));
    return;
  }
  if (block.label === 'TECHNICAL_PROJECTS') {
    profile.technicalProjects = parseProjectsSectionTxtBlock_(block.lines, sectionPath);
    return;
  }
  if (block.label === 'EDUCATION') {
    profile.education = parseEducationSectionTxtBlock_(block.lines, sectionPath);
    return;
  }
  if (block.label === 'CUSTOM_SECTION') {
    profile.customSections.push(parseCustomSectionTxtBlock_(block.lines, sectionPath));
    return;
  }

  throw new Error('Unsupported TXT block label ' + block.label + ' at ' + sectionPath + '.');
}

function parseMetadataTxtBlock_(profile, lines, sectionPath) {
  lines.forEach(function(line, index) {
    const pair = parseTxtKeyValueLine_(line, sectionPath, index);
    assertPlainTxtValuePair_(pair, sectionPath, index);
    if (pair.key === 'DOCUMENT_TITLE') {
      profile.metadata.documentTitle = pair.value;
      return;
    }
    if (pair.key === 'ALLOW_PAGE_SPILL') {
      profile.metadata.allowPageSpill = parseBooleanValue_(pair.value, sectionPath + ' line ' + (index + 1));
      return;
    }
    throw new Error('Unsupported key "' + pair.key + '" in ' + sectionPath + '.');
  });
}

function parseHeaderTxtBlock_(profile, lines, sectionPath) {
  profile.header.contactItems = [];
  lines.forEach(function(line, index) {
    const pair = parseTxtKeyValueLine_(line, sectionPath, index);
    assertPlainTxtValuePair_(pair, sectionPath, index);
    if (pair.key === 'NAME') {
      profile.header.name = pair.value;
      return;
    }
    if (pair.key === 'HEADLINE') {
      profile.header.headline = pair.value;
      return;
    }
    if (pair.key === 'CONTACT') {
      profile.header.contactItems.push(parseTxtContactValue_(pair.value, sectionPath, index));
      return;
    }
    throw new Error('Unsupported key "' + pair.key + '" in ' + sectionPath + '.');
  });
}

function parseSummaryTxtBlock_(lines, sectionPath) {
  const section = {
    id: 'summary',
    heading: 'SUMMARY',
    order: 10,
    visible: true,
    renderHeading: false,
    paragraphs: [],
  };

  lines.forEach(function(line, index) {
    const pair = parseTxtKeyValueLine_(line, sectionPath, index);
    assertPlainTxtValuePair_(pair, sectionPath, index);
    if (applyCommonSectionTxtKey_(section, pair, sectionPath, index)) {
      return;
    }
    if (pair.key === 'RENDER_HEADING') {
      section.renderHeading = parseBooleanValue_(pair.value, sectionPath + ' line ' + (index + 1));
      return;
    }
    if (pair.key === 'PARAGRAPH') {
      section.paragraphs.push(pair.value);
      return;
    }
    throw new Error('Unsupported key "' + pair.key + '" in ' + sectionPath + '.');
  });

  return section;
}

function parseInlineListTxtBlock_(lines, sectionPath, defaults) {
  const section = cloneData_(defaults);
  section.items = [];
  lines.forEach(function(line, index) {
    const pair = parseTxtKeyValueLine_(line, sectionPath, index);
    assertPlainTxtValuePair_(pair, sectionPath, index);
    if (applyCommonSectionTxtKey_(section, pair, sectionPath, index)) {
      return;
    }
    if (pair.key === 'PREFERRED_LINE_COUNT') {
      section.preferredLineCount = parseIntegerValue_(pair.value, sectionPath + ' line ' + (index + 1));
      return;
    }
    if (pair.key === 'ITEM') {
      section.items.push(pair.value);
      return;
    }
    throw new Error('Unsupported key "' + pair.key + '" in ' + sectionPath + '.');
  });
  return section;
}

function parseExperienceSectionTxtBlock_(lines, sectionPath) {
  const section = {
    id: 'experience-section',
    heading: 'EXPERIENCE',
    order: 30,
    visible: true,
    entries: [],
  };
  let currentEntry = null;

  lines.forEach(function(line, index) {
    if (line === '---') {
      finalizeTxtEntry_(section.entries, currentEntry, sectionPath, index);
      currentEntry = null;
      return;
    }

    const pair = parseTxtKeyValueLine_(line, sectionPath, index);
    assertPlainTxtValuePair_(pair, sectionPath, index);
    if (applyCommonSectionTxtKey_(section, pair, sectionPath, index)) {
      return;
    }

    currentEntry = currentEntry || createEmptyExperienceEntry_();
    if (pair.key === 'TITLE') {
      currentEntry.title = pair.value;
      return;
    }
    if (pair.key === 'ORG') {
      currentEntry.org = pair.value;
      return;
    }
    if (pair.key === 'LOCATION') {
      currentEntry.location = pair.value;
      return;
    }
    if (pair.key === 'DATE') {
      currentEntry.date = pair.value;
      return;
    }
    if (pair.key === 'SUBTITLE') {
      currentEntry.subtitle = pair.value;
      return;
    }
    if (pair.key === 'BULLET') {
      currentEntry.bullets.push(pair.value);
      return;
    }
    if (pair.key === 'LINK') {
      currentEntry.links.push(parseTxtLinkValue_(pair.value, sectionPath, index));
      return;
    }
    throw new Error('Unsupported key "' + pair.key + '" in ' + sectionPath + '.');
  });

  finalizeTxtEntry_(section.entries, currentEntry, sectionPath, lines.length);
  return section;
}

function parseProjectsSectionTxtBlock_(lines, sectionPath) {
  const section = {
    id: 'technical-projects',
    heading: 'TECHNICAL PROJECTS',
    order: 60,
    visible: true,
    entries: [],
  };
  let currentEntry = null;

  lines.forEach(function(line, index) {
    if (line === '---') {
      finalizeTxtEntry_(section.entries, currentEntry, sectionPath, index);
      currentEntry = null;
      return;
    }

    const pair = parseTxtKeyValueLine_(line, sectionPath, index);
    assertPlainTxtValuePair_(pair, sectionPath, index);
    if (applyCommonSectionTxtKey_(section, pair, sectionPath, index)) {
      return;
    }

    currentEntry = currentEntry || createEmptyProjectEntry_();
    if (pair.key === 'TITLE') {
      currentEntry.title = pair.value;
      return;
    }
    if (pair.key === 'DATE') {
      currentEntry.date = pair.value;
      return;
    }
    if (pair.key === 'SUBTITLE') {
      currentEntry.subtitle = pair.value;
      return;
    }
    if (pair.key === 'SUMMARY') {
      currentEntry.summary = pair.value;
      return;
    }
    if (pair.key === 'BULLET') {
      currentEntry.bullets.push(pair.value);
      return;
    }
    if (pair.key === 'LINK') {
      currentEntry.links.push(parseTxtLinkValue_(pair.value, sectionPath, index));
      return;
    }
    throw new Error('Unsupported key "' + pair.key + '" in ' + sectionPath + '.');
  });

  finalizeTxtEntry_(section.entries, currentEntry, sectionPath, lines.length);
  return section;
}

function parseEducationSectionTxtBlock_(lines, sectionPath) {
  const section = {
    id: 'education',
    heading: 'EDUCATION',
    order: 70,
    visible: true,
    layoutVariant: 'auto',
    entries: [],
  };
  let currentEntry = null;

  lines.forEach(function(line, index) {
    if (line === '---') {
      finalizeTxtEntry_(section.entries, currentEntry, sectionPath, index);
      currentEntry = null;
      return;
    }

    const pair = parseTxtKeyValueLine_(line, sectionPath, index);
    assertPlainTxtValuePair_(pair, sectionPath, index);
    if (applyCommonSectionTxtKey_(section, pair, sectionPath, index)) {
      return;
    }
    if (pair.key === 'LAYOUT_VARIANT') {
      section.layoutVariant = pair.value;
      return;
    }

    currentEntry = currentEntry || createEmptyEducationEntry_();
    if (pair.key === 'INSTITUTION') {
      currentEntry.institution = pair.value;
      return;
    }
    if (pair.key === 'CREDENTIAL') {
      currentEntry.credential = pair.value;
      return;
    }
    if (pair.key === 'LOCATION') {
      currentEntry.location = pair.value;
      return;
    }
    if (pair.key === 'DATE') {
      currentEntry.date = pair.value;
      return;
    }
    if (pair.key === 'DETAILS') {
      currentEntry.details = pair.value;
      return;
    }
    if (pair.key === 'LINK') {
      currentEntry.links.push(parseTxtLinkValue_(pair.value, sectionPath, index));
      return;
    }
    throw new Error('Unsupported key "' + pair.key + '" in ' + sectionPath + '.');
  });

  finalizeTxtEntry_(section.entries, currentEntry, sectionPath, lines.length);
  return section;
}

function parseCustomSectionTxtBlock_(lines, sectionPath) {
  const section = {
    id: 'custom-section',
    heading: 'CUSTOM SECTION',
    order: 80,
    visible: true,
    sectionKind: 'paragraphs',
    preferredLineCount: 2,
    layoutVariant: 'auto',
    items: [],
    bullets: [],
    paragraphs: [],
    entries: [],
  };
  let currentEntry = null;

  lines.forEach(function(line, index) {
    if (line === '---') {
      finalizeTxtEntry_(section.entries, currentEntry, sectionPath, index);
      currentEntry = null;
      return;
    }

    const pair = parseTxtKeyValueLine_(line, sectionPath, index);
    assertPlainTxtValuePair_(pair, sectionPath, index);
    if (applyCommonSectionTxtKey_(section, pair, sectionPath, index)) {
      return;
    }
    if (pair.key === 'SECTION_KIND') {
      section.sectionKind = pair.value;
      return;
    }
    if (pair.key === 'PREFERRED_LINE_COUNT') {
      section.preferredLineCount = parseIntegerValue_(pair.value, sectionPath + ' line ' + (index + 1));
      return;
    }
    if (pair.key === 'LAYOUT_VARIANT') {
      section.layoutVariant = pair.value;
      return;
    }
    if (pair.key === 'ITEM') {
      section.items.push(pair.value);
      return;
    }
    if (pair.key === 'PARAGRAPH') {
      section.paragraphs.push(pair.value);
      return;
    }

    if (pair.key === 'BULLET') {
      if (section.sectionKind === 'experience' || section.sectionKind === 'projects') {
        currentEntry = currentEntry || createCustomTxtEntryForKind_(section.sectionKind);
        currentEntry.bullets.push(pair.value);
      } else {
        section.bullets.push(pair.value);
      }
      return;
    }

    currentEntry = currentEntry || createCustomTxtEntryForKind_(section.sectionKind);
    if (pair.key === 'LINK') {
      currentEntry.links = currentEntry.links || [];
      currentEntry.links.push(parseTxtLinkValue_(pair.value, sectionPath, index));
      return;
    }
    if (pair.key === 'TITLE') {
      currentEntry.title = pair.value;
      return;
    }
    if (pair.key === 'ORG') {
      currentEntry.org = pair.value;
      return;
    }
    if (pair.key === 'LOCATION') {
      currentEntry.location = pair.value;
      return;
    }
    if (pair.key === 'DATE') {
      currentEntry.date = pair.value;
      return;
    }
    if (pair.key === 'SUBTITLE') {
      currentEntry.subtitle = pair.value;
      return;
    }
    if (pair.key === 'SUMMARY') {
      currentEntry.summary = pair.value;
      return;
    }
    if (pair.key === 'INSTITUTION') {
      currentEntry.institution = pair.value;
      return;
    }
    if (pair.key === 'CREDENTIAL') {
      currentEntry.credential = pair.value;
      return;
    }
    if (pair.key === 'DETAILS') {
      currentEntry.details = pair.value;
      return;
    }

    throw new Error('Unsupported key "' + pair.key + '" in ' + sectionPath + '.');
  });

  finalizeTxtEntry_(section.entries, currentEntry, sectionPath, lines.length);
  return section;
}

function applyCommonSectionTxtKey_(section, pair, sectionPath, index) {
  if (pair.key === 'ID') {
    section.id = pair.value;
    return true;
  }
  if (pair.key === 'HEADING') {
    section.heading = pair.value;
    return true;
  }
  if (pair.key === 'ORDER') {
    section.order = parseIntegerValue_(pair.value, sectionPath + ' line ' + (index + 1));
    return true;
  }
  if (pair.key === 'VISIBLE') {
    section.visible = parseBooleanValue_(pair.value, sectionPath + ' line ' + (index + 1));
    return true;
  }
  return false;
}

function parseTxtKeyValueLine_(line, sectionPath, index) {
  const match = /^([A-Z_]+):\s*(.*)$/.exec(line);
  if (!match) {
    throw new Error(
        'Could not parse line ' + (index + 1) + ' in ' + sectionPath +
        '. Expected KEY: value format.');
  }
  return {
    key: match[1],
    value: match[2],
  };
}

function parseTxtContactValue_(value, sectionPath, index) {
  const parts = splitPipeValue_(value, 2, 3, sectionPath + ' line ' + (index + 1));
  return {
    type: parts[0],
    text: parts[1],
    url: parts[2] || '',
  };
}

function parseTxtLinkValue_(value, sectionPath, index) {
  const parts = splitPipeValue_(value, 2, 2, sectionPath + ' line ' + (index + 1));
  return {
    label: parts[0],
    url: parts[1],
  };
}

function splitPipeValue_(value, minParts, maxParts, path) {
  const raw = String(value || '');
  const parts = raw.split('|').map(function(part) {
    return cleanText_(part);
  }).filter(Boolean);

  if (parts.length < minParts || parts.length > maxParts) {
    throw new Error(
        path + ' must contain between ' + minParts + ' and ' + maxParts +
        ' pipe-delimited value(s). The pipe character "|" is reserved in TXT.');
  }
  return parts;
}

function assertPlainTxtValuePair_(pair, sectionPath, index) {
  if ((pair.key === 'CONTACT') || (pair.key === 'LINK')) {
    return;
  }
  forbidPipeInTxtValue_(pair.value, sectionPath + ' line ' + (index + 1) + ' (' + pair.key + ')');
}

function finalizeTxtEntry_(entries, entry, sectionPath, index) {
  if (!entry) {
    return;
  }
  if (isEntryEffectivelyEmpty_(entry)) {
    throw new Error(
        'Encountered an empty entry separator near line ' + (index + 1) +
        ' in ' + sectionPath + '.');
  }
  entries.push(entry);
}

function isEntryEffectivelyEmpty_(entry) {
  return !normalizeStringArray_(Object.keys(entry).reduce(function(values, key) {
    const value = entry[key];
    if (typeof value === 'string') {
      values.push(value);
    } else if (Array.isArray(value)) {
      values = values.concat(value.map(function(item) {
        return typeof item === 'string' ? item : JSON.stringify(item);
      }));
    }
    return values;
  }, [])).length;
}

/*
 * Strict schema reference for ChatGPT:
 * - Arrays are intentionally empty here.
 * - This is the reference shape ChatGPT should follow when generating a real
 *   payload.
 * - It is not the same thing as the human starter template with placeholders.
 */
function getStrictResumeProfileSchemaReference_() {
  return {
    version: PROFILE_VERSION,
    metadata: {
      documentTitle: '',
      allowPageSpill: true,
    },
    header: {
      name: '',
      headline: '',
      contactItems: [
        { type: 'location', text: '', url: '' },
      ],
    },
    summary: {
      id: 'summary',
      heading: 'SUMMARY',
      order: 10,
      visible: true,
      renderHeading: false,
      paragraphs: [],
    },
    coreTechnologies: {
      id: 'core-technologies',
      heading: 'CORE TECHNOLOGIES',
      order: 20,
      visible: true,
      items: [],
      preferredLineCount: 2,
    },
    experienceSections: [],
    technicalProjects: {
      id: 'technical-projects',
      heading: 'TECHNICAL PROJECTS',
      order: 60,
      visible: true,
      entries: [],
    },
    education: {
      id: 'education',
      heading: 'EDUCATION',
      order: 70,
      visible: true,
      layoutVariant: 'auto',
      entries: [],
    },
    customSections: [],
  };
}

/*
 * Minimal renderable example:
 * - This is the smallest example that should still render successfully.
 * - It is useful when a human wants to sanity-check a new installation.
 */
function getMinimalRenderableResumeProfile_() {
  return {
    version: PROFILE_VERSION,
    metadata: {
      documentTitle: 'Minimal Resume Example',
      allowPageSpill: true,
    },
    header: {
      name: 'Jamie Example',
      headline: 'Data Engineer',
      contactItems: [
        { type: 'email', text: 'jamie@example.com', url: '' },
      ],
    },
    summary: {
      id: 'summary',
      heading: 'SUMMARY',
      order: 10,
      visible: true,
      renderHeading: false,
      paragraphs: [
        'Data engineer focused on reliable reporting pipelines and compact stakeholder communication.',
      ],
    },
    coreTechnologies: {
      id: 'core-technologies',
      heading: 'CORE TECHNOLOGIES',
      order: 20,
      visible: false,
      items: [],
      preferredLineCount: 2,
    },
    experienceSections: [
      {
        id: 'professional-experience',
        heading: 'PROFESSIONAL EXPERIENCE',
        order: 30,
        visible: true,
        entries: [
          {
            title: 'Data Engineer',
            org: 'Example Co',
            location: 'Remote',
            date: '2023 - Present',
            subtitle: '',
            bullets: [
              'Built reporting pipelines and validation checks for recurring business metrics.',
            ],
            links: [],
          },
        ],
      },
    ],
    technicalProjects: {
      id: 'technical-projects',
      heading: 'TECHNICAL PROJECTS',
      order: 60,
      visible: false,
      entries: [],
    },
    education: {
      id: 'education',
      heading: 'EDUCATION',
      order: 70,
      visible: false,
      layoutVariant: 'auto',
      entries: [],
    },
    customSections: [],
  };
}

function createEmptyResumeProfile_() {
  return {
    version: PROFILE_VERSION,
    metadata: {
      documentTitle: '',
      allowPageSpill: true,
    },
    header: {
      name: '',
      headline: '',
      contactItems: [],
    },
    summary: {
      id: 'summary',
      heading: 'SUMMARY',
      order: 10,
      visible: true,
      renderHeading: false,
      paragraphs: [],
    },
    coreTechnologies: {
      id: 'core-technologies',
      heading: 'CORE TECHNOLOGIES',
      order: 20,
      visible: true,
      items: [],
      preferredLineCount: 2,
    },
    experienceSections: [],
    technicalProjects: {
      id: 'technical-projects',
      heading: 'TECHNICAL PROJECTS',
      order: 60,
      visible: true,
      entries: [],
    },
    education: {
      id: 'education',
      heading: 'EDUCATION',
      order: 70,
      visible: true,
      layoutVariant: 'auto',
      entries: [],
    },
    customSections: [],
  };
}

function createEmptyExperienceEntry_() {
  return {
    title: '',
    org: '',
    location: '',
    date: '',
    subtitle: '',
    bullets: [],
    links: [],
  };
}

function createEmptyProjectEntry_() {
  return {
    title: '',
    date: '',
    subtitle: '',
    summary: '',
    bullets: [],
    links: [],
  };
}

function createEmptyEducationEntry_() {
  return {
    institution: '',
    credential: '',
    location: '',
    date: '',
    details: '',
    links: [],
  };
}

function createCustomTxtEntryForKind_(kind) {
  if (kind === 'education') {
    return createEmptyEducationEntry_();
  }
  if (kind === 'projects') {
    return createEmptyProjectEntry_();
  }
  return createEmptyExperienceEntry_();
}

function assertTxtBlockMultiplicity_(blocks) {
  const singletonLabels = {
    METADATA: true,
    HEADER: true,
    SUMMARY: true,
    CORE_TECHNOLOGIES: true,
    TECHNICAL_PROJECTS: true,
    EDUCATION: true,
  };
  const seen = {};

  (blocks || []).forEach(function(block) {
    if (!singletonLabels[block.label]) {
      return;
    }
    if (seen[block.label]) {
      throw new Error(
          'TXT payload contains multiple [' + block.label + '] blocks. ' +
          'That block type may appear only once.');
    }
    seen[block.label] = true;
  });
}

function forbidPipeInTxtValue_(value, label) {
  if (String(value || '').indexOf('|') !== -1) {
    throw new Error(
        'TXT values may not contain "|" because it is a reserved delimiter: ' + label);
  }
}

function cloneData_(value) {
  return JSON.parse(JSON.stringify(value));
}

function cleanText_(value) {
  return String(value == null ? '' : value).replace(/\s+/g, ' ').trim();
}

function resolveGoogleDocId_(value) {
  const cleanValue = cleanText_(value);
  if (!cleanValue) {
    return '';
  }
  const match = /\/document\/d\/([a-zA-Z0-9_-]+)/.exec(cleanValue);
  return match ? match[1] : cleanValue;
}

function isPlainObject_(value) {
  return !!value && typeof value === 'object' && !Array.isArray(value);
}

function describeType_(value) {
  if (value === null) {
    return 'null';
  }
  if (Array.isArray(value)) {
    return 'array';
  }
  return typeof value;
}

function numberOrDefault_(value, fallback) {
  return typeof value === 'number' && isFinite(value) ? value : fallback;
}

function positiveIntegerOrDefault_(value, fallback) {
  return Math.max(1, Math.round(numberOrDefault_(value, fallback)));
}

function joinNonEmpty_(parts, separator) {
  return (parts || []).filter(function(part) {
    return !!cleanText_(part);
  }).join(separator);
}

function parseBooleanValue_(value, path) {
  const normalized = cleanText_(value).toLowerCase();
  if (normalized === 'true') {
    return true;
  }
  if (normalized === 'false') {
    return false;
  }
  throw new Error(path + ' must be "true" or "false".');
}

function parseIntegerValue_(value, path) {
  const cleanValue = cleanText_(value);
  if (!/^-?\d+$/.test(cleanValue)) {
    throw new Error(path + ' must be an integer.');
  }
  return parseInt(cleanValue, 10);
}

function findRangeInText_(fullText, value) {
  const label = cleanText_(value);
  if (!label) {
    return null;
  }
  const start = String(fullText || '').indexOf(label);
  if (start === -1) {
    return null;
  }
  return {
    start: start,
    end: start + label.length - 1,
  };
}

function findUniqueRangeInText_(fullText, value) {
  const firstRange = findRangeInText_(fullText, value);
  if (!firstRange) {
    return null;
  }
  const label = cleanText_(value);
  const secondStart = String(fullText || '').indexOf(label, firstRange.end + 1);
  return secondStart === -1 ? firstRange : null;
}

function stableStringify_(value) {
  return JSON.stringify(sortKeysDeep_(value));
}

function sortKeysDeep_(value) {
  if (Array.isArray(value)) {
    return value.map(sortKeysDeep_);
  }
  if (!isPlainObject_(value)) {
    return value;
  }

  const sorted = {};
  Object.keys(value).sort().forEach(function(key) {
    sorted[key] = sortKeysDeep_(value[key]);
  });
  return sorted;
}

function assertCondition_(condition, message) {
  if (!condition) {
    throw new Error(message);
  }
}

function inferContactDisplayText_(type, rawUrl) {
  if (!rawUrl) {
    return '';
  }
  if (type === 'email') {
    return stripMailto_(rawUrl);
  }
  if (type === 'link') {
    return simplifyUrlForDisplay_(rawUrl);
  }
  return cleanText_(rawUrl);
}

function normalizeContactItemUrl_(type, rawUrl) {
  const cleanUrl = cleanText_(rawUrl);
  if (!cleanUrl) {
    return '';
  }
  if (type === 'email') {
    return 'mailto:' + stripMailto_(cleanUrl);
  }
  if (type === 'link') {
    return addHttps_(cleanUrl);
  }
  return cleanUrl;
}

function normalizeBulletText_(value) {
  return cleanText_(String(value == null ? '' : value)
      .replace(/^[\u2022•*\-]+\s*/, '')
      .replace(/^\d+[.)]\s+/, ''));
}

function simplifyUrlForDisplay_(url) {
  return cleanText_(url)
      .replace(/^mailto:/i, '')
      .replace(/^https?:\/\//i, '')
      .replace(/\/$/, '');
}

function stripMailto_(value) {
  return cleanText_(value).replace(/^mailto:/i, '');
}

function dedupeObjectsByKey_(items, keyFn) {
  const seen = {};
  return (items || []).filter(function(item) {
    const key = String(keyFn(item));
    if (seen[key]) {
      return false;
    }
    seen[key] = true;
    return true;
  });
}

function buildJsonInputHelp_(source) {
  const sample = String(source || '').trim();
  if (/^```/.test(sample)) {
    return 'Remove the markdown fence and paste only the raw JSON object into PROFILE_JSON_PAYLOAD.';
  }
  if (/^(const|let|var)\s+PROFILE\b/i.test(sample) || /^PROFILE\s*=/.test(sample)) {
    return 'Remove the variable assignment and paste only the raw JSON object into PROFILE_JSON_PAYLOAD.';
  }
  if (sample[0] !== '{') {
    return 'The value should begin with "{". If ChatGPT added commentary, ask it to return only the raw JSON payload.';
  }
  return 'Make sure the pasted value is raw JSON only, with no commentary before or after it, and keep it inside PROFILE_JSON_PAYLOAD.';
}

function extractProfileSchemaShape_(profile) {
  const customFallback = {
    id: '',
    heading: '',
    order: 0,
    visible: true,
    sectionKind: 'paragraphs',
    preferredLineCount: 2,
    layoutVariant: 'auto',
    items: [],
    bullets: [],
    paragraphs: [],
    entries: [],
  };
  return {
    topLevel: sortedKeysOf_(profile),
    metadata: sortedKeysOf_(profile.metadata),
    header: sortedKeysOf_(profile.header),
    contactItem: sortedKeysOf_(pickFirstArrayItem_(profile.header.contactItems, { type: '', text: '', url: '' })),
    summary: sortedKeysOf_(profile.summary),
    coreTechnologies: sortedKeysOf_(profile.coreTechnologies),
    experienceSection: sortedKeysOf_(pickFirstSectionLike_(profile.experienceSections, { id: '', heading: '', order: 0, visible: true, entries: [] })),
    experienceEntry: sortedKeysOf_(pickFirstExperienceEntry_(profile)),
    technicalProjects: sortedKeysOf_(profile.technicalProjects),
    projectEntry: sortedKeysOf_(pickFirstArrayItem_(profile.technicalProjects && profile.technicalProjects.entries, createEmptyProjectEntry_())),
    education: sortedKeysOf_(profile.education),
    educationEntry: sortedKeysOf_(pickFirstArrayItem_(profile.education && profile.education.entries, createEmptyEducationEntry_())),
    customSection: sortedKeysOf_(pickFirstArrayItem_(profile.customSections, customFallback)),
  };
}

function sortedKeysOf_(value) {
  return Object.keys(value || {}).sort();
}

function pickFirstArrayItem_(items, fallback) {
  return (items && items.length) ? items[0] : fallback;
}

function pickFirstSectionLike_(sections, fallback) {
  return pickFirstArrayItem_(sections, fallback);
}

function pickFirstExperienceEntry_(profile) {
  const experienceSection = pickFirstSectionLike_(
      profile.experienceSections,
      { entries: [createEmptyExperienceEntry_()] });
  return pickFirstArrayItem_(experienceSection.entries, createEmptyExperienceEntry_());
}
