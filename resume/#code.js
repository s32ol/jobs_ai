/*
 * Universal Resume Template System for Google Apps Script
 *
 * Usage
 * 1. Set RESUME_RENDER_CONFIG.docId to the Google Doc you want to rebuild.
 * 2. Choose RESUME_RENDER_CONFIG.inputMode: "json", "txt", or "sample".
 * 3. Paste a JSON payload into RESUME_RENDER_CONFIG.profileJson, or a TXT payload
 *    into RESUME_RENDER_CONFIG.profileTxt.
 * 4. Run renderConfiguredResume().
 * 5. Use getBlankResumeProfileTemplate_() for the starter JSON shape.
 * 6. Use getCanonicalResumeTxtTemplate_() for the fallback TXT contract.
 * 7. Use getChatGptResumePromptContract_() to generate a payload from plain
 *    resume text in ChatGPT. ChatGPT should return only the payload.
 *
 * Notes
 * - This renderer preserves the current design language as closely as Google
 *   Docs allows, but exported PDFs may still wrap a few lines differently.
 * - TXT support is a strict authoring format, not a freeform resume parser.
 */

const PROFILE_VERSION = 'resume-profile-v1';
const INLINE_LIST_SEPARATOR = ' • ';
const META_SEPARATOR = ' | ';
const SECTION_KIND_VALUES = [
  'inline_list',
  'paragraphs',
  'experience',
  'projects',
  'education',
  'bulleted_list',
];
const CONTACT_TYPE_VALUES = ['location', 'phone', 'email', 'link', 'text'];
const EDUCATION_LAYOUT_VALUES = ['auto', 'inline', 'stacked'];

const RESUME_RENDER_CONFIG = {
  docId: '',
  inputMode: 'sample',
  profileJson: '',
  profileTxt: '',
  documentTitleFallback: 'Universal Resume Template',
};

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
    contactSize: 8.5,
    headlineSize: 9.5,
    alignment: 'CENTER',
    nameSpacingAfter: 0,
    contactSpacingAfter: 1,
    headlineSpacingAfter: 4,
    lineSpacing: 1.0,
  },
  sectionHeader: {
    size: 10,
    firstSpacingBefore: 3,
    spacingBefore: 5,
    spacingAfter: 1,
    lineSpacing: 1.0,
    uppercase: true,
  },
  body: {
    size: 9.25,
    lineSpacing: 1.02,
    spacingBefore: 0,
    spacingAfter: 1,
  },
  meta: {
    size: 8.9,
    lineSpacing: 1.0,
    spacingBefore: 0,
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
    entrySpacingBefore: 2,
    inlineThresholdChars: 125,
    lineSpacing: 1.0,
  },
  compaction: {
    preferredInlineListLines: 2,
    summaryParagraphLineWidth: 100,
    bulletLineWidth: 92,
    inlineListLineWidth: 108,
    onePageLineBudget: 58,
    warningPrefix:
        'Resume template warning: content is dense for a compact one-page layout.',
  },
};

function renderConfiguredResume() {
  const config = cloneData_(RESUME_RENDER_CONFIG);
  if (!cleanText_(config.docId)) {
    throw new Error(
        'RESUME_RENDER_CONFIG.docId is required. Set it to the Google Doc ID ' +
        'you want to rebuild before running renderConfiguredResume().');
  }

  const profile = resolveConfiguredProfile_(config);
  const normalized = renderResumeFromProfile_(profile, config);
  Logger.log(
      'Rendered resume with ' + normalized.sections.length + ' visible section(s).');
  return normalized;
}

function renderResumeFromProfile_(profile, config) {
  validateResumeProfile_(profile);
  const normalized = normalizeResumeProfile_(profile);
  assertRenderableProfile_(normalized);
  logDensityWarningIfNeeded_(normalized);

  const doc = DocumentApp.openById(config.docId);
  const body = doc.getBody();

  resetDocument_(doc, body);
  maybeRenameDocument_(doc, normalized.metadata.documentTitle || config.documentTitleFallback);
  appendHeader_(body, normalized.header);

  normalized.sections.forEach(function(section, index) {
    appendNormalizedSection_(body, section, index === 0);
  });

  doc.saveAndClose();
  return normalized;
}

function parseResumeProfileJson_(jsonText) {
  const source = String(jsonText || '').trim();
  if (!source) {
    throw new Error(
        'JSON input is empty. Paste a payload into ' +
        'RESUME_RENDER_CONFIG.profileJson or use inputMode "sample".');
  }

  try {
    const parsed = JSON.parse(source);
    if (!isPlainObject_(parsed)) {
      throw new Error('Top-level JSON value must be an object.');
    }
    return parsed;
  } catch (error) {
    throw new Error('Could not parse resume profile JSON: ' + error.message);
  }
}

function parseResumeProfileTxt_(txtText) {
  const source = String(txtText || '');
  if (!source.trim()) {
    throw new Error(
        'TXT input is empty. Paste a payload into ' +
        'RESUME_RENDER_CONFIG.profileTxt or use inputMode "sample".');
  }

  const profile = createEmptyResumeProfile_();
  const blocks = splitTxtIntoBlocks_(source);

  blocks.forEach(function(block, index) {
    parseTxtBlockIntoProfile_(profile, block, index);
  });

  return profile;
}

function validateResumeProfile_(profile) {
  const errors = [];
  validateResumeProfileInternal_(profile, 'profile', errors);
  if (errors.length) {
    throw new Error('Resume profile validation failed:\n- ' + errors.join('\n- '));
  }
  return profile;
}

function normalizeResumeProfile_(profile) {
  validateResumeProfile_(profile);

  const source = cloneData_(profile);
  const normalized = {
    version: cleanText_(source.version) || PROFILE_VERSION,
    metadata: {
      documentTitle: cleanText_(source.metadata && source.metadata.documentTitle),
      allowPageSpill: source.metadata && source.metadata.allowPageSpill !== false,
    },
    header: {
      name: cleanText_(source.header && source.header.name),
      headline: cleanText_(source.header && source.header.headline),
      contactItems: normalizeContactItems_(source.header && source.header.contactItems),
    },
    sections: [],
  };

  appendNormalizedSectionIfPresent_(normalized.sections, {
    kind: 'paragraphs',
    id: cleanText_(source.summary && source.summary.id) || 'summary',
    heading: cleanText_(source.summary && source.summary.heading) || 'SUMMARY',
    order: numberOrDefault_(source.summary && source.summary.order, 10),
    visible: source.summary ? source.summary.visible !== false : false,
    paragraphs: normalizeStringArray_(source.summary && source.summary.paragraphs),
  });

  appendNormalizedSectionIfPresent_(normalized.sections, {
    kind: 'inline_list',
    id: cleanText_(source.coreTechnologies && source.coreTechnologies.id) ||
        'core-technologies',
    heading: cleanText_(source.coreTechnologies && source.coreTechnologies.heading) ||
        'CORE TECHNOLOGIES',
    order: numberOrDefault_(source.coreTechnologies && source.coreTechnologies.order, 20),
    visible: source.coreTechnologies ? source.coreTechnologies.visible !== false : false,
    items: normalizeStringArray_(source.coreTechnologies && source.coreTechnologies.items),
    preferredLineCount: positiveIntegerOrDefault_(
        source.coreTechnologies && source.coreTechnologies.preferredLineCount,
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
      bullets: normalizeStringArray_(section.bullets),
      paragraphs: normalizeStringArray_(section.paragraphs),
      entries: normalizeCustomSectionEntries_(kind, section.entries),
    });
  });

  appendNormalizedSectionIfPresent_(normalized.sections, {
    kind: 'projects',
    id: cleanText_(source.technicalProjects && source.technicalProjects.id) ||
        'technical-projects',
    heading: cleanText_(source.technicalProjects && source.technicalProjects.heading) ||
        'TECHNICAL PROJECTS',
    order: numberOrDefault_(source.technicalProjects && source.technicalProjects.order, 60),
    visible: source.technicalProjects ? source.technicalProjects.visible !== false : false,
    entries: normalizeProjectEntries_(source.technicalProjects && source.technicalProjects.entries),
  });

  appendNormalizedSectionIfPresent_(normalized.sections, {
    kind: 'education',
    id: cleanText_(source.education && source.education.id) || 'education',
    heading: cleanText_(source.education && source.education.heading) || 'EDUCATION',
    order: numberOrDefault_(source.education && source.education.order, 70),
    visible: source.education ? source.education.visible !== false : false,
    layoutVariant: cleanText_(source.education && source.education.layoutVariant) || 'auto',
    entries: normalizeEducationEntries_(source.education && source.education.entries),
  });

  normalized.sections.sort(compareNormalizedSections_);
  return normalized;
}

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

function getSampleResumeProfile_() {
  return {
    version: PROFILE_VERSION,
    metadata: {
      documentTitle: 'Alex Rivera Resume',
      allowPageSpill: true,
    },
    header: {
      name: 'Alex Rivera',
      headline: 'Data Engineer | Analytics Infrastructure | Python | SQL',
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
    customSections: [
      {
        id: 'certifications',
        heading: 'CERTIFICATIONS',
        order: 40,
        visible: true,
        sectionKind: 'bulleted_list',
        preferredLineCount: 2,
        layoutVariant: 'auto',
        items: [],
        bullets: [
          'Google Cloud Professional Data Engineer (sample placeholder)',
          'dbt Fundamentals (sample placeholder)',
        ],
        paragraphs: [],
        entries: [],
      },
    ],
  };
}

function getCanonicalResumeTxtTemplate_() {
  const lines = [
    '# Canonical TXT contract for the universal resume template system.',
    '# Replace the sample values with your own. Keep the labels exactly as shown.',
    '# Repeat CONTACT, ITEM, PARAGRAPH, BULLET, and LINK lines as needed.',
    '# Use --- to separate entries inside EXPERIENCE_SECTION, TECHNICAL_PROJECTS,',
    '# EDUCATION, and CUSTOM_SECTION blocks that use entry-based section kinds.',
    '# Avoid using the pipe character "|" inside field values unless it is part of',
    '# a URL. CONTACT uses: CONTACT: type | text | optional-url',
    '# LINK uses: LINK: label | url',
    '',
  ];
  return lines.join('\n') + serializeResumeProfileToTxt_(getSampleResumeProfile_());
}

function getChatGptResumePromptContract_() {
  return [
    'You are converting resume text into a strict JSON payload for a Google Apps Script resume renderer.',
    '',
    'Output requirements',
    '- Return only valid JSON.',
    '- Do not wrap the JSON in markdown fences.',
    '- Do not add commentary, notes, or explanations.',
    '- Do not invent facts.',
    '- Preserve titles, organizations, locations, dates, project names, education, and impact as written.',
    '- Keep bullets compact and resume-ready.',
    '- Prefer concise, high-signal phrasing that fits a compact one-page resume style.',
    '',
    'Rules',
    '- The top-level object must match the schema exactly.',
    '- Keep version exactly "' + PROFILE_VERSION + '".',
    '- Keep section ordering deliberate through the numeric order fields.',
    '- Use visible:false only when a section has no reliable content.',
    '- Put header contact data in header.contactItems using type values: ' +
        CONTACT_TYPE_VALUES.join(', ') + '.',
    '- Use sectionKind values only from: ' + SECTION_KIND_VALUES.join(', ') + '.',
    '- Use education.layoutVariant only from: ' +
        EDUCATION_LAYOUT_VALUES.join(', ') + '.',
    '- For links, include { "label": "...", "url": "..." } only when the label text actually appears in rendered text.',
    '- Preserve role hierarchy. If the source resume has multiple experience groupings, keep them as separate experienceSections.',
    '- Preserve dates exactly when possible instead of normalizing them.',
    '',
    'Schema',
    JSON.stringify(getBlankResumeProfileTemplate_(), null, 2),
  ].join('\n');
}

function runResumeTemplateSelfCheck_() {
  const blank = getBlankResumeProfileTemplate_();
  const sample = getSampleResumeProfile_();
  const txt = getCanonicalResumeTxtTemplate_();

  validateResumeProfile_(blank);
  validateResumeProfile_(sample);

  const parsedJson = parseResumeProfileJson_(JSON.stringify(sample));
  validateResumeProfile_(parsedJson);

  const parsedTxt = parseResumeProfileTxt_(txt);
  validateResumeProfile_(parsedTxt);

  const normalizedSample = normalizeResumeProfile_(sample);
  const normalizedTxt = normalizeResumeProfile_(parsedTxt);

  assertCondition_(
      stableStringify_(normalizedSample) === stableStringify_(normalizedTxt),
      'TXT parser output does not match the sample JSON payload after normalization.');

  const hiddenSectionProfile = cloneData_(sample);
  hiddenSectionProfile.customSections.push({
    id: 'hidden-paragraphs',
    heading: 'HIDDEN SECTION',
    order: 90,
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
        return section.id === 'hidden-paragraphs';
      }),
      'Hidden sections should be omitted.');

  const stackedEducation = cloneData_(sample);
  stackedEducation.education.layoutVariant = 'stacked';
  validateResumeProfile_(stackedEducation);
  normalizeResumeProfile_(stackedEducation);

  const inlineEducation = cloneData_(sample);
  inlineEducation.education.layoutVariant = 'inline';
  validateResumeProfile_(inlineEducation);
  normalizeResumeProfile_(inlineEducation);

  Logger.log('Resume template self-check passed.');
  return true;
}

function resolveConfiguredProfile_(config) {
  const mode = cleanText_(config.inputMode).toLowerCase();
  if (mode === 'sample') {
    return getSampleResumeProfile_();
  }
  if (mode === 'json') {
    return parseResumeProfileJson_(config.profileJson);
  }
  if (mode === 'txt') {
    return parseResumeProfileTxt_(config.profileTxt);
  }
  throw new Error(
      'Unsupported inputMode "' + config.inputMode +
      '". Use "json", "txt", or "sample".');
}

function validateResumeProfileInternal_(profile, path, errors) {
  if (!isPlainObject_(profile)) {
    errors.push(path + ' must be an object.');
    return;
  }

  if (profile.version !== PROFILE_VERSION) {
    errors.push(path + '.version must equal "' + PROFILE_VERSION + '".');
  }

  validateMetadata_(profile.metadata, path + '.metadata', errors);
  validateHeader_(profile.header, path + '.header', errors);
  validateParagraphSection_(profile.summary, path + '.summary', errors);
  validateInlineListSection_(profile.coreTechnologies, path + '.coreTechnologies', errors);
  validateExperienceSectionArray_(
      profile.experienceSections, path + '.experienceSections', errors);
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
      errors.push(
          itemPath + '.type must be one of: ' + CONTACT_TYPE_VALUES.join(', ') + '.');
    }
    validateOptionalString_(item.text, itemPath + '.text', errors);
    validateOptionalString_(item.url, itemPath + '.url', errors);
  });
}

function validateParagraphSection_(section, path, errors) {
  validateCommonSectionShape_(section, path, errors);
  if (!isPlainObject_(section)) {
    return;
  }
  validateStringArray_(section.paragraphs, path + '.paragraphs', errors);
}

function validateInlineListSection_(section, path, errors) {
  validateCommonSectionShape_(section, path, errors);
  if (!isPlainObject_(section)) {
    return;
  }
  validateStringArray_(section.items, path + '.items', errors);
  validateOptionalNumber_(section.preferredLineCount, path + '.preferredLineCount', errors);
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
    errors.push(
        path + '.layoutVariant must be one of: ' +
        EDUCATION_LAYOUT_VALUES.join(', ') + '.');
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
      errors.push(
          sectionPath + '.sectionKind must be one of: ' +
          SECTION_KIND_VALUES.join(', ') + '.');
    }
    validateOptionalNumber_(section.preferredLineCount, sectionPath + '.preferredLineCount', errors);
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
  validateOptionalNumber_(section.order, path + '.order', errors);
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
    errors.push(path + ' must be an array.');
    return;
  }
  value.forEach(function(item, index) {
    if (typeof item !== 'string') {
      errors.push(path + '[' + index + '] must be a string.');
    }
  });
}

function validateOptionalString_(value, path, errors) {
  if (typeof value !== 'string') {
    errors.push(path + ' must be a string.');
  }
}

function validateOptionalNumber_(value, path, errors) {
  if (typeof value !== 'number' || !isFinite(value)) {
    errors.push(path + ' must be a finite number.');
  }
}

function validateBoolean_(value, path, errors) {
  if (typeof value !== 'boolean') {
    errors.push(path + ' must be a boolean.');
  }
}

function appendNormalizedSection_(body, section, isFirstSection) {
  if (section.kind === 'paragraphs') {
    appendSectionHeader_(body, section.heading, isFirstSection);
    section.paragraphs.forEach(function(paragraph, index) {
      appendBodyParagraph_(body, paragraph, {
        spacingBefore: index === 0 ? 0 : LAYOUT_SPEC.body.spacingBefore,
        spacingAfter: 0,
        lineSpacing: LAYOUT_SPEC.body.lineSpacing,
      });
    });
    return;
  }

  if (section.kind === 'inline_list') {
    appendSectionHeader_(body, section.heading, isFirstSection);
    splitInlineListForLayout_(section.items, section.preferredLineCount).forEach(function(line) {
      appendBodyParagraph_(body, line.join(INLINE_LIST_SEPARATOR), {
        spacingBefore: 0,
        spacingAfter: 0,
        lineSpacing: LAYOUT_SPEC.body.lineSpacing,
      });
    });
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

  if (section.kind === 'bulleted_list') {
    appendSectionHeader_(body, section.heading, isFirstSection);
    section.bullets.forEach(function(bullet) {
      appendBullet_(body, bullet, []);
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

  if (header.contactItems.length) {
    const contactLine = header.contactItems.map(function(item) {
      return item.text;
    }).join(META_SEPARATOR);
    const contactParagraph = body.appendParagraph(contactLine);
    contactParagraph
        .setAlignment(DocumentApp.HorizontalAlignment.CENTER)
        .setSpacingBefore(0)
        .setSpacingAfter(LAYOUT_SPEC.header.contactSpacingAfter)
        .setLineSpacing(LAYOUT_SPEC.header.lineSpacing);
    const contactText = applyTextStyle_(contactParagraph.editAsText(), {
      fontFamily: LAYOUT_SPEC.fonts.body,
      fontSize: LAYOUT_SPEC.header.contactSize,
      color: LAYOUT_SPEC.colors.muted,
    });
    applyInlineLinks_(contactText, contactLine, toInlineLinkSpecs_(header.contactItems));
  }

  if (header.headline) {
    const headlineParagraph = body.appendParagraph(header.headline);
    headlineParagraph
        .setAlignment(DocumentApp.HorizontalAlignment.CENTER)
        .setSpacingBefore(0)
        .setSpacingAfter(LAYOUT_SPEC.header.headlineSpacingAfter)
        .setLineSpacing(LAYOUT_SPEC.header.lineSpacing);
    applyTextStyle_(headlineParagraph.editAsText(), {
      fontFamily: LAYOUT_SPEC.fonts.body,
      fontSize: LAYOUT_SPEC.header.headlineSize,
      bold: true,
      color: LAYOUT_SPEC.colors.text,
    });
  }
}

function appendSectionHeader_(body, title, isFirstSection) {
  const heading = LAYOUT_SPEC.sectionHeader.uppercase ? String(title).toUpperCase() : String(title);
  const paragraph = body.appendParagraph(heading);
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
      .setSpacingBefore(
          config.spacingBefore == null ? LAYOUT_SPEC.body.spacingBefore : config.spacingBefore)
      .setSpacingAfter(
          config.spacingAfter == null ? LAYOUT_SPEC.body.spacingAfter : config.spacingAfter)
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
      .setSpacingBefore(
          config.spacingBefore == null ? LAYOUT_SPEC.meta.spacingBefore : config.spacingBefore)
      .setSpacingAfter(
          config.spacingAfter == null ? LAYOUT_SPEC.meta.spacingAfter : config.spacingAfter)
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
  const item = body.appendListItem(text);
  item
      .setGlyphType(DocumentApp.GlyphType.BULLET)
      .setIndentStart(LAYOUT_SPEC.bullet.indentStart)
      .setIndentFirstLine(LAYOUT_SPEC.bullet.indentFirstLine)
      .setSpacingBefore(
          config.spacingBefore == null ? LAYOUT_SPEC.bullet.spacingBefore : config.spacingBefore)
      .setSpacingAfter(
          config.spacingAfter == null ? LAYOUT_SPEC.bullet.spacingAfter : config.spacingAfter)
      .setLineSpacing(config.lineSpacing || LAYOUT_SPEC.bullet.lineSpacing);
  const bulletText = applyTextStyle_(item.editAsText(), {
    fontFamily: LAYOUT_SPEC.fonts.body,
    fontSize: LAYOUT_SPEC.body.size,
    color: LAYOUT_SPEC.colors.text,
  });
  applyInlineLinks_(bulletText, item.getText(), links || []);
  return item;
}

function appendExperienceEntry_(body, entry, spacingBefore) {
  const titleLineText = buildTitleDateLine_(entry.title, entry.date);
  let hasLeadParagraph = false;
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
    styleTitleDateParagraph_(titleText, entry.title, entry.date);
    applyInlineLinks_(titleText, titleLineText, entry.links);
    hasLeadParagraph = true;
  }

  const metaLineText = joinNonEmpty_([entry.org, entry.subtitle, entry.location], META_SEPARATOR);
  if (metaLineText) {
    appendMetaParagraph_(body, metaLineText, {
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
  const titleLineText = buildTitleDateLine_(entry.title, entry.date);
  let hasLeadParagraph = false;
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
    styleTitleDateParagraph_(titleText, entry.title, entry.date);
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
    appendEducationInlineSection_(body, section, isFirstSection);
    return;
  }

  appendSectionHeader_(body, section.heading, isFirstSection);
  section.entries.forEach(function(entry, index) {
    appendEducationEntry_(body, entry, index === 0 ? 0 : LAYOUT_SPEC.education.entrySpacingBefore);
  });
}

function appendEducationInlineSection_(body, section, isFirstSection) {
  const entry = section.entries[0];
  const contentParts = [];
  if (entry.institution) {
    contentParts.push(entry.institution);
  }
  if (entry.location) {
    contentParts.push(entry.location);
  }
  if (entry.date) {
    contentParts.push(entry.date);
  }
  if (entry.credential) {
    contentParts.push(entry.credential);
  }
  if (entry.details) {
    contentParts.push(entry.details);
  }

  const heading = String(section.heading).toUpperCase();
  const paragraphText = heading + ': ' + contentParts.join(META_SEPARATOR);
  const paragraph = body.appendParagraph(paragraphText);
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

  if (heading.length > 0) {
    text
        .setBold(0, heading.length, true)
        .setFontSize(0, heading.length, LAYOUT_SPEC.sectionHeader.size);
  }

  const dateRange = findRangeInText_(paragraphText, entry.date);
  if (dateRange) {
    text.setForegroundColor(dateRange.start, dateRange.end, LAYOUT_SPEC.colors.muted);
  }

  applyInlineLinks_(text, paragraphText, entry.links);
}

function appendEducationEntry_(body, entry, spacingBefore) {
  const titleLineText = buildTitleDateLine_(entry.credential, entry.date);
  let hasLeadParagraph = false;
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
    styleTitleDateParagraph_(titleText, entry.credential, entry.date);
    applyInlineLinks_(titleText, titleLineText, entry.links);
    hasLeadParagraph = true;
  }

  const metaLineText = joinNonEmpty_([entry.institution, entry.location], META_SEPARATOR);
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
    const range = findRangeInText_(fullText, link.label);
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
    Logger.log('Resume template note: could not update document title: ' + error.message);
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
  return (items || []).map(function(item) {
    return {
      type: cleanText_(item.type),
      text: cleanText_(item.text),
      url: cleanText_(item.url),
    };
  }).filter(function(item) {
    return item.type && item.text;
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
      bullets: normalizeStringArray_(entry.bullets),
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
      bullets: normalizeStringArray_(entry.bullets),
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
  if (!section.visible) {
    return;
  }

  if (section.kind === 'paragraphs' && section.paragraphs.length) {
    target.push(section);
    return;
  }
  if (section.kind === 'inline_list' && section.items.length) {
    target.push(section);
    return;
  }
  if (section.kind === 'experience' && section.entries.length) {
    target.push(section);
    return;
  }
  if (section.kind === 'projects' && section.entries.length) {
    target.push(section);
    return;
  }
  if (section.kind === 'education' && section.entries.length) {
    target.push(section);
    return;
  }
  if (section.kind === 'bulleted_list' && section.bullets.length) {
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
  return (links || []).map(function(link) {
    return {
      label: cleanText_(link.label),
      url: cleanText_(link.url),
    };
  }).filter(function(link) {
    return link.label && link.url;
  });
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

function styleTitleDateParagraph_(text, title, date) {
  const cleanTitle = cleanText_(title);
  const cleanDate = cleanText_(date);
  if (cleanTitle) {
    text.setBold(0, cleanTitle.length - 1, true);
  }

  if (cleanTitle && cleanDate) {
    const dateStart = cleanTitle.length + 1;
    const dateEnd = cleanTitle.length + cleanDate.length;
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

function buildTitleDateLine_(title, date) {
  const cleanTitle = cleanText_(title);
  const cleanDate = cleanText_(date);
  if (cleanTitle && cleanDate) {
    return cleanTitle + '\t' + cleanDate;
  }
  return cleanTitle || cleanDate;
}

function toInlineLinkSpecs_(contactItems) {
  return contactItems.map(function(item) {
    const url = buildContactUrl_(item);
    return {
      label: item.text,
      url: url,
    };
  }).filter(function(link) {
    return link.url;
  });
}

function buildContactUrl_(item) {
  if (item.type === 'email') {
    return 'mailto:' + item.text;
  }
  if (item.type === 'link') {
    return addHttps_(item.url || item.text);
  }
  return cleanText_(item.url);
}

function addHttps_(url) {
  const cleanUrl = cleanText_(url);
  if (!cleanUrl) {
    return '';
  }
  return /^https?:\/\//i.test(cleanUrl) ? cleanUrl : 'https://' + cleanUrl;
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
      [
        entry.institution,
        entry.location,
        entry.date,
        entry.credential,
        entry.details,
      ],
      META_SEPARATOR);
  return inlineText.length <= LAYOUT_SPEC.education.inlineThresholdChars ? 'inline' : 'stacked';
}

function assertRenderableProfile_(normalized) {
  if (!normalized.header.name) {
    throw new Error(
        'header.name is required before rendering. Fill the blank template or use ' +
        'getSampleResumeProfile_() as a reference.');
  }

  if (!normalized.sections.length) {
    throw new Error(
        'No visible sections contain content. Add content to at least one section ' +
        'before rendering.');
  }
}

function logDensityWarningIfNeeded_(normalized) {
  const estimatedLines = estimateRenderedLineCount_(normalized);
  if (estimatedLines <= LAYOUT_SPEC.compaction.onePageLineBudget) {
    return;
  }

  Logger.log(
      LAYOUT_SPEC.compaction.warningPrefix +
      ' Estimated lines: ' + estimatedLines +
      ' vs budget ' + LAYOUT_SPEC.compaction.onePageLineBudget +
      '. Rendering will continue and may spill to a second page.');
}

function estimateRenderedLineCount_(normalized) {
  let lines = 0;

  lines += 1;
  if (normalized.header.contactItems.length) {
    lines += 1;
  }
  if (normalized.header.headline) {
    lines += estimateWrappedLines_(
        normalized.header.headline, LAYOUT_SPEC.compaction.summaryParagraphLineWidth);
  }

  normalized.sections.forEach(function(section) {
    if (section.kind === 'education' && resolveEducationLayoutVariant_(section) === 'inline') {
      lines += 1;
      return;
    }

    lines += 1;

    if (section.kind === 'paragraphs') {
      section.paragraphs.forEach(function(paragraph) {
        lines += estimateWrappedLines_(
            paragraph, LAYOUT_SPEC.compaction.summaryParagraphLineWidth);
      });
      return;
    }

    if (section.kind === 'inline_list') {
      lines += Math.max(
          1,
          splitInlineListForLayout_(section.items, section.preferredLineCount).length);
      return;
    }

    if (section.kind === 'experience') {
      section.entries.forEach(function(entry) {
        lines += 1;
        if (entry.org || entry.subtitle || entry.location) {
          lines += 1;
        }
        entry.bullets.forEach(function(bullet) {
          lines += estimateWrappedLines_(
              bullet, LAYOUT_SPEC.compaction.bulletLineWidth);
        });
      });
      return;
    }

    if (section.kind === 'projects') {
      section.entries.forEach(function(entry) {
        lines += 1;
        if (entry.subtitle) {
          lines += 1;
        }
        if (entry.summary) {
          lines += estimateWrappedLines_(
              entry.summary, LAYOUT_SPEC.compaction.summaryParagraphLineWidth);
        }
        entry.bullets.forEach(function(bullet) {
          lines += estimateWrappedLines_(
              bullet, LAYOUT_SPEC.compaction.bulletLineWidth);
        });
      });
      return;
    }

    if (section.kind === 'education') {
      section.entries.forEach(function(entry) {
        lines += 1;
        if (entry.institution || entry.location) {
          lines += 1;
        }
        if (entry.details) {
          lines += estimateWrappedLines_(
              entry.details, LAYOUT_SPEC.compaction.summaryParagraphLineWidth);
        }
      });
      return;
    }

    if (section.kind === 'bulleted_list') {
      section.bullets.forEach(function(bullet) {
        lines += estimateWrappedLines_(
            bullet, LAYOUT_SPEC.compaction.bulletLineWidth);
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

function splitInlineListForLayout_(items, preferredLineCount) {
  const cleanItems = normalizeStringArray_(items);
  const lineCount = Math.max(1, positiveIntegerOrDefault_(
      preferredLineCount, LAYOUT_SPEC.compaction.preferredInlineListLines));

  if (!cleanItems.length) {
    return [];
  }
  if (lineCount === 1 || cleanItems.length === 1) {
    return [cleanItems];
  }

  const buckets = [];
  for (let index = 0; index < lineCount; index += 1) {
    buckets.push([]);
  }

  const totals = buckets.map(function() {
    return 0;
  });

  cleanItems.forEach(function(item) {
    let bestIndex = 0;
    let bestSize = totals[0];
    for (let index = 1; index < totals.length; index += 1) {
      if (totals[index] < bestSize) {
        bestSize = totals[index];
        bestIndex = index;
      }
    }
    buckets[bestIndex].push(item);
    totals[bestIndex] += item.length + INLINE_LIST_SEPARATOR.length;
  });

  return buckets.filter(function(bucket) {
    return bucket.length;
  });
}

function serializeResumeProfileToTxt_(profile) {
  const lines = [];
  const source = cloneData_(profile);

  lines.push('[METADATA]');
  lines.push('DOCUMENT_TITLE: ' + cleanText_(source.metadata.documentTitle));
  lines.push('ALLOW_PAGE_SPILL: ' + String(source.metadata.allowPageSpill));
  lines.push('');

  lines.push('[HEADER]');
  lines.push('NAME: ' + cleanText_(source.header.name));
  lines.push('HEADLINE: ' + cleanText_(source.header.headline));
  (source.header.contactItems || []).forEach(function(item) {
    const parts = [cleanText_(item.type), cleanText_(item.text)];
    if (cleanText_(item.url)) {
      parts.push(cleanText_(item.url));
    }
    lines.push('CONTACT: ' + parts.join(META_SEPARATOR));
  });
  lines.push('');

  pushParagraphSectionTxt_(lines, 'SUMMARY', source.summary);
  pushInlineListSectionTxt_(lines, 'CORE_TECHNOLOGIES', source.coreTechnologies);

  (source.experienceSections || []).forEach(function(section) {
    pushExperienceSectionTxt_(lines, 'EXPERIENCE_SECTION', section);
  });

  (source.customSections || []).forEach(function(section) {
    pushCustomSectionTxt_(lines, 'CUSTOM_SECTION', section);
  });

  pushProjectsSectionTxt_(lines, 'TECHNICAL_PROJECTS', source.technicalProjects);
  pushEducationSectionTxt_(lines, 'EDUCATION', source.education);

  return lines.join('\n').replace(/\n{3,}/g, '\n\n').trim() + '\n';
}

function pushParagraphSectionTxt_(lines, label, section) {
  lines.push('[' + label + ']');
  pushCommonSectionTxtFields_(lines, section);
  (section.paragraphs || []).forEach(function(paragraph) {
    lines.push('PARAGRAPH: ' + cleanText_(paragraph));
  });
  lines.push('');
}

function pushInlineListSectionTxt_(lines, label, section) {
  lines.push('[' + label + ']');
  pushCommonSectionTxtFields_(lines, section);
  lines.push('PREFERRED_LINE_COUNT: ' + String(section.preferredLineCount));
  (section.items || []).forEach(function(item) {
    lines.push('ITEM: ' + cleanText_(item));
  });
  lines.push('');
}

function pushExperienceSectionTxt_(lines, label, section) {
  lines.push('[' + label + ']');
  pushCommonSectionTxtFields_(lines, section);
  (section.entries || []).forEach(function(entry, index) {
    if (index > 0) {
      lines.push('---');
    }
    lines.push('TITLE: ' + cleanText_(entry.title));
    lines.push('ORG: ' + cleanText_(entry.org));
    lines.push('LOCATION: ' + cleanText_(entry.location));
    lines.push('DATE: ' + cleanText_(entry.date));
    lines.push('SUBTITLE: ' + cleanText_(entry.subtitle));
    (entry.bullets || []).forEach(function(bullet) {
      lines.push('BULLET: ' + cleanText_(bullet));
    });
    (entry.links || []).forEach(function(link) {
      lines.push('LINK: ' + cleanText_(link.label) + META_SEPARATOR + cleanText_(link.url));
    });
  });
  lines.push('');
}

function pushProjectsSectionTxt_(lines, label, section) {
  lines.push('[' + label + ']');
  pushCommonSectionTxtFields_(lines, section);
  (section.entries || []).forEach(function(entry, index) {
    if (index > 0) {
      lines.push('---');
    }
    lines.push('TITLE: ' + cleanText_(entry.title));
    lines.push('DATE: ' + cleanText_(entry.date));
    lines.push('SUBTITLE: ' + cleanText_(entry.subtitle));
    lines.push('SUMMARY: ' + cleanText_(entry.summary));
    (entry.bullets || []).forEach(function(bullet) {
      lines.push('BULLET: ' + cleanText_(bullet));
    });
    (entry.links || []).forEach(function(link) {
      lines.push('LINK: ' + cleanText_(link.label) + META_SEPARATOR + cleanText_(link.url));
    });
  });
  lines.push('');
}

function pushEducationSectionTxt_(lines, label, section) {
  lines.push('[' + label + ']');
  pushCommonSectionTxtFields_(lines, section);
  lines.push('LAYOUT_VARIANT: ' + cleanText_(section.layoutVariant));
  (section.entries || []).forEach(function(entry, index) {
    if (index > 0) {
      lines.push('---');
    }
    lines.push('INSTITUTION: ' + cleanText_(entry.institution));
    lines.push('CREDENTIAL: ' + cleanText_(entry.credential));
    lines.push('LOCATION: ' + cleanText_(entry.location));
    lines.push('DATE: ' + cleanText_(entry.date));
    lines.push('DETAILS: ' + cleanText_(entry.details));
    (entry.links || []).forEach(function(link) {
      lines.push('LINK: ' + cleanText_(link.label) + META_SEPARATOR + cleanText_(link.url));
    });
  });
  lines.push('');
}

function pushCustomSectionTxt_(lines, label, section) {
  lines.push('[' + label + ']');
  pushCommonSectionTxtFields_(lines, section);
  lines.push('SECTION_KIND: ' + cleanText_(section.sectionKind));
  lines.push('PREFERRED_LINE_COUNT: ' + String(section.preferredLineCount));
  lines.push('LAYOUT_VARIANT: ' + cleanText_(section.layoutVariant));

  if (section.sectionKind === 'inline_list') {
    (section.items || []).forEach(function(item) {
      lines.push('ITEM: ' + cleanText_(item));
    });
  } else if (section.sectionKind === 'paragraphs') {
    (section.paragraphs || []).forEach(function(paragraph) {
      lines.push('PARAGRAPH: ' + cleanText_(paragraph));
    });
  } else if (section.sectionKind === 'bulleted_list') {
    (section.bullets || []).forEach(function(bullet) {
      lines.push('BULLET: ' + cleanText_(bullet));
    });
  } else if (section.sectionKind === 'experience') {
    (section.entries || []).forEach(function(entry, index) {
      if (index > 0) {
        lines.push('---');
      }
      lines.push('TITLE: ' + cleanText_(entry.title));
      lines.push('ORG: ' + cleanText_(entry.org));
      lines.push('LOCATION: ' + cleanText_(entry.location));
      lines.push('DATE: ' + cleanText_(entry.date));
      lines.push('SUBTITLE: ' + cleanText_(entry.subtitle));
      (entry.bullets || []).forEach(function(bullet) {
        lines.push('BULLET: ' + cleanText_(bullet));
      });
      (entry.links || []).forEach(function(link) {
        lines.push('LINK: ' + cleanText_(link.label) + META_SEPARATOR + cleanText_(link.url));
      });
    });
  } else if (section.sectionKind === 'projects') {
    (section.entries || []).forEach(function(entry, index) {
      if (index > 0) {
        lines.push('---');
      }
      lines.push('TITLE: ' + cleanText_(entry.title));
      lines.push('DATE: ' + cleanText_(entry.date));
      lines.push('SUBTITLE: ' + cleanText_(entry.subtitle));
      lines.push('SUMMARY: ' + cleanText_(entry.summary));
      (entry.bullets || []).forEach(function(bullet) {
        lines.push('BULLET: ' + cleanText_(bullet));
      });
      (entry.links || []).forEach(function(link) {
        lines.push('LINK: ' + cleanText_(link.label) + META_SEPARATOR + cleanText_(link.url));
      });
    });
  } else if (section.sectionKind === 'education') {
    (section.entries || []).forEach(function(entry, index) {
      if (index > 0) {
        lines.push('---');
      }
      lines.push('INSTITUTION: ' + cleanText_(entry.institution));
      lines.push('CREDENTIAL: ' + cleanText_(entry.credential));
      lines.push('LOCATION: ' + cleanText_(entry.location));
      lines.push('DATE: ' + cleanText_(entry.date));
      lines.push('DETAILS: ' + cleanText_(entry.details));
      (entry.links || []).forEach(function(link) {
        lines.push('LINK: ' + cleanText_(link.label) + META_SEPARATOR + cleanText_(link.url));
      });
    });
  }

  lines.push('');
}

function pushCommonSectionTxtFields_(lines, section) {
  lines.push('ID: ' + cleanText_(section.id));
  lines.push('HEADING: ' + cleanText_(section.heading));
  lines.push('ORDER: ' + String(section.order));
  lines.push('VISIBLE: ' + String(section.visible));
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

    const headerMatch = /^\[([A-Z_]+)\]$/.exec(trimmed);
    if (headerMatch) {
      current = {
        label: headerMatch[1],
        lines: [],
      };
      blocks.push(current);
      return;
    }

    if (!current) {
      throw new Error(
          'TXT payload must start with a section header like [HEADER] or [SUMMARY].');
    }
    current.lines.push(trimmed);
  });

  return blocks;
}

function parseTxtBlockIntoProfile_(profile, block, blockIndex) {
  const label = block.label;
  const sectionPath = '[' + label + '] block #' + (blockIndex + 1);

  if (label === 'METADATA') {
    parseMetadataTxtBlock_(profile, block.lines, sectionPath);
    return;
  }
  if (label === 'HEADER') {
    parseHeaderTxtBlock_(profile, block.lines, sectionPath);
    return;
  }
  if (label === 'SUMMARY') {
    profile.summary = parseParagraphSectionTxtBlock_(block.lines, sectionPath, {
      id: 'summary',
      heading: 'SUMMARY',
      order: 10,
      visible: true,
      paragraphs: [],
    });
    return;
  }
  if (label === 'CORE_TECHNOLOGIES') {
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
  if (label === 'EXPERIENCE_SECTION') {
    profile.experienceSections.push(parseExperienceSectionTxtBlock_(block.lines, sectionPath));
    return;
  }
  if (label === 'TECHNICAL_PROJECTS') {
    profile.technicalProjects = parseProjectsSectionTxtBlock_(block.lines, sectionPath);
    return;
  }
  if (label === 'EDUCATION') {
    profile.education = parseEducationSectionTxtBlock_(block.lines, sectionPath);
    return;
  }
  if (label === 'CUSTOM_SECTION') {
    profile.customSections.push(parseCustomSectionTxtBlock_(block.lines, sectionPath));
    return;
  }

  throw new Error('Unsupported TXT block label ' + label + ' at ' + sectionPath + '.');
}

function parseMetadataTxtBlock_(profile, lines, sectionPath) {
  lines.forEach(function(line, index) {
    const pair = parseTxtKeyValueLine_(line, sectionPath, index);
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

function parseParagraphSectionTxtBlock_(lines, sectionPath, defaults) {
  const section = cloneData_(defaults);
  section.paragraphs = [];
  lines.forEach(function(line, index) {
    const pair = parseTxtKeyValueLine_(line, sectionPath, index);
    if (applyCommonSectionTxtKey_(section, pair, sectionPath, index)) {
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
    if (applyCommonSectionTxtKey_(section, pair, sectionPath, index)) {
      return;
    }
    if (pair.key === 'PREFERRED_LINE_COUNT') {
      section.preferredLineCount = parseIntegerValue_(
          pair.value, sectionPath + ' line ' + (index + 1));
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
    if (applyCommonSectionTxtKey_(section, pair, sectionPath, index)) {
      return;
    }
    if (pair.key === 'SECTION_KIND') {
      section.sectionKind = pair.value;
      return;
    }
    if (pair.key === 'PREFERRED_LINE_COUNT') {
      section.preferredLineCount = parseIntegerValue_(
          pair.value, sectionPath + ' line ' + (index + 1));
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
      if (!currentEntry.links) {
        currentEntry.links = [];
      }
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
  const parts = String(value || '').split('|').map(function(part) {
    return cleanText_(part);
  }).filter(function(part) {
    return part !== '';
  });

  if (parts.length < minParts || parts.length > maxParts) {
    throw new Error(
        path + ' must contain between ' + minParts + ' and ' + maxParts +
        ' pipe-delimited value(s).');
  }
  return parts;
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

function cloneData_(value) {
  return JSON.parse(JSON.stringify(value));
}

function cleanText_(value) {
  return String(value == null ? '' : value).replace(/\s+/g, ' ').trim();
}

function isPlainObject_(value) {
  return !!value && typeof value === 'object' && !Array.isArray(value);
}

function numberOrDefault_(value, fallback) {
  return typeof value === 'number' && isFinite(value) ? value : fallback;
}

function positiveIntegerOrDefault_(value, fallback) {
  const number = numberOrDefault_(value, fallback);
  return Math.max(1, Math.round(number));
}

function joinNonEmpty_(parts, separator) {
  return parts.filter(function(part) {
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
  const parsed = parseInt(cleanText_(value), 10);
  if (!isFinite(parsed)) {
    throw new Error(path + ' must be an integer.');
  }
  return parsed;
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
