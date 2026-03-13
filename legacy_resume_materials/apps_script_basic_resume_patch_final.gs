const DOC_ID = '1X3M0dKiJ7GrGWJWKAzVNXorXzt8z52AAanPV8AvE5pE';

const RESUME_SOURCE = String.raw`ROBERT MORALES
Bay Area / Sacramento, CA | 415-900-2819 | robert.morales.eng@gmail.com | linkedin.com/in/s32ol

Available immediately

PROFILE

Reliable and adaptable team member with experience in retail operations, fabrication shop environments, and day-to-day operational support. Known for staying organized, following procedures, and helping teams keep work moving in fast-paced settings. Quick to learn routines, maintain accurate records, and provide dependable support to customers and coworkers.

CORE WORK STRENGTHS

Reliable Attendance • Fast Learner • Team-Oriented • Organized Work Habits • Safety Awareness • Inventory Accuracy

SKILLS

Customer Service • Inventory & Stocking • Materials Handling • Documentation & Recordkeeping
Data Entry • Administrative Support • Organization • Teamwork • Time Management • Reliability • Operational Support

WORK EXPERIENCE

Backroom Merchandise Handler
Target | San Francisco, CA | 2015 – 2017

• Stocked merchandise, organized backroom inventory, and kept product areas ready for daily store needs
• Unloaded shipments, sorted incoming items, and moved merchandise to the sales floor
• Kept storage areas clean and orderly so products could be found and restocked quickly
• Worked with team members to complete stocking tasks and keep inventory moving during busy shifts

Fabrication Assistant
Jaytech Fabrication | Chatsworth, CA | 2013 – 2015

• Assisted with fabrication and machining work in a busy metal shop environment
• Operated CNC machines, lathes, and cutting equipment while following shop safety procedures
• Measured, staged, and prepared materials for cutting, machining, and assembly work
• Supported TIG and arc welding projects and helped keep tools and work areas organized

Operations Support
Google Android Program | Sacramento, CA | 2023 – 2025

• Kept tracking reports, status logs, and internal records updated for ongoing project activity
• Organized documentation and data so teams could review work progress quickly
• Flagged items that required follow-up and helped keep records accurate and current
• Assisted with coordination tasks that kept day-to-day work moving on schedule

Quality Assurance Support
Google Nest | Sacramento, CA | 2020 – 2023

• Reviewed system reports and documented issues to support troubleshooting and follow-up
• Assisted with routine validation checks during product updates and testing cycles
• Kept records of reported issues, status changes, and follow-up activity organized
• Coordinated with team members to resolve open items and keep documentation current

Data Specialist
OnwardCA | Fresno, CA | 2020

• Organized and standardized 1,500+ records across statewide resource databases
• Improved data consistency and documentation to support accurate team use

Software Support Assistant
Learn to Achieve | Sacramento, CA | 2017 – 2020

• Assisted staff with reports, documentation, and routine recordkeeping for education programs
• Maintained internal records, updated tracking information, and supported day-to-day program reporting
• Kept spreadsheets and support documents organized, accurate, and up to date for staff use

EDUCATION

B.S. Finance — Information Systems & Technology Management
San Francisco State University`;

const PAGE = {
  width: 612,
  height: 792,
  marginTop: 30,
  marginBottom: 28,
  marginLeft: 36,
  marginRight: 36,
};

const STYLE = {
  nameFont: 'Georgia',
  bodyFont: 'Arial',
  textColor: '#202124',
  mutedColor: '#5f6368',
  linkColor: '#1155cc',
  nameSize: 17,
  contactSize: 8.9,
  availabilitySize: 8.5,
  sectionSize: 10,
  bodySize: 9,
  metaSize: 8.5,
};

function rebuildRobertMoralesResume() {
  const resume = parseResumeSource_(RESUME_SOURCE);
  const doc = DocumentApp.openById(DOC_ID);
  const body = doc.getBody();

  resetDocument_(doc, body);
  appendHeader_(body, resume.header);

  appendSectionHeader_(body, 'PROFILE', { spacingBefore: 4 });
  resume.profile.forEach(function(paragraph, index) {
    appendBodyParagraph_(body, paragraph, {
      spacingBefore: index === 0 ? 0 : 1,
      spacingAfter: 0,
      lineSpacing: 1.05,
    });
  });

  appendSectionHeader_(body, 'CORE WORK STRENGTHS', { spacingBefore: 4 });
  appendInlineListSection_(body, resume.coreWorkStrengths, { lineSpacing: 1.03 });

  appendSectionHeader_(body, 'SKILLS', { spacingBefore: 4 });
  appendInlineListSection_(body, resume.skills, { lineSpacing: 1.03 });

  appendSectionHeader_(body, 'WORK EXPERIENCE');
  resume.workExperience.forEach(function(entry, index) {
    appendWorkEntry_(body, entry, index === 0 ? 0 : 2);
  });

  appendSectionHeader_(body, 'EDUCATION', { spacingBefore: 4 });
  resume.education.forEach(function(entry, index) {
    appendEducationEntry_(body, entry, index === 0 ? 0 : 2);
  });

  doc.saveAndClose();
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
      .setPageWidth(PAGE.width)
      .setPageHeight(PAGE.height)
      .setMarginTop(PAGE.marginTop)
      .setMarginBottom(PAGE.marginBottom)
      .setMarginLeft(PAGE.marginLeft)
      .setMarginRight(PAGE.marginRight);
}

function appendHeader_(body, header) {
  const nameParagraph = takeReusableParagraph_(body, header.name);
  nameParagraph
      .setAlignment(DocumentApp.HorizontalAlignment.CENTER)
      .setSpacingBefore(0)
      .setSpacingAfter(0)
      .setLineSpacing(1.0);
  applyTextStyle_(nameParagraph.editAsText(), {
    fontFamily: STYLE.nameFont,
    fontSize: STYLE.nameSize,
    bold: true,
    color: STYLE.textColor,
  });

  const contactLine = [
    header.location,
    header.phone,
    header.email,
    header.linkedin,
  ].filter(Boolean).join(' | ');

  const contactParagraph = body.appendParagraph(contactLine);
  contactParagraph
      .setAlignment(DocumentApp.HorizontalAlignment.CENTER)
      .setSpacingBefore(0)
      .setSpacingAfter(1)
      .setLineSpacing(1.0);
  const contactText = applyTextStyle_(contactParagraph.editAsText(), {
    fontFamily: STYLE.bodyFont,
    fontSize: STYLE.contactSize,
    color: STYLE.mutedColor,
  });
  applyInlineLinks_(contactText, contactLine, [
    { label: header.email, url: 'mailto:' + header.email },
    { label: header.linkedin, url: addHttps_(header.linkedin) },
  ]);

  const availabilityParagraph = body.appendParagraph(header.availability);
  availabilityParagraph
      .setAlignment(DocumentApp.HorizontalAlignment.CENTER)
      .setSpacingBefore(0)
      .setSpacingAfter(3)
      .setLineSpacing(1.0);
  applyTextStyle_(availabilityParagraph.editAsText(), {
    fontFamily: STYLE.bodyFont,
    fontSize: STYLE.availabilitySize,
    color: STYLE.mutedColor,
  });
}

function appendSectionHeader_(body, title, options) {
  const config = options || {};
  const paragraph = body.appendParagraph(title);
  paragraph
      .setAlignment(DocumentApp.HorizontalAlignment.LEFT)
      .setSpacingBefore(config.spacingBefore == null ? 4 : config.spacingBefore)
      .setSpacingAfter(config.spacingAfter == null ? 1 : config.spacingAfter)
      .setLineSpacing(1.0);
  applyTextStyle_(paragraph.editAsText(), {
    fontFamily: STYLE.bodyFont,
    fontSize: STYLE.sectionSize,
    bold: true,
    color: STYLE.textColor,
  });
}

function appendBodyParagraph_(body, text, options) {
  const config = options || {};
  const paragraph = body.appendParagraph(text);
  paragraph
      .setAlignment(DocumentApp.HorizontalAlignment.LEFT)
      .setSpacingBefore(config.spacingBefore == null ? 0 : config.spacingBefore)
      .setSpacingAfter(config.spacingAfter == null ? 1 : config.spacingAfter)
      .setLineSpacing(config.lineSpacing || 1.05);
  applyTextStyle_(paragraph.editAsText(), {
    fontFamily: STYLE.bodyFont,
    fontSize: config.size || STYLE.bodySize,
    color: config.color || STYLE.textColor,
    bold: !!config.bold,
    italic: !!config.italic,
  });
  return paragraph;
}

function appendMetaParagraph_(body, text, options) {
  const config = options || {};
  const paragraph = body.appendParagraph(text);
  paragraph
      .setAlignment(DocumentApp.HorizontalAlignment.LEFT)
      .setSpacingBefore(config.spacingBefore == null ? 0 : config.spacingBefore)
      .setSpacingAfter(config.spacingAfter == null ? 1 : config.spacingAfter)
      .setLineSpacing(1.0);
  applyTextStyle_(paragraph.editAsText(), {
    fontFamily: STYLE.bodyFont,
    fontSize: config.size || STYLE.metaSize,
    color: config.color || STYLE.mutedColor,
    bold: !!config.bold,
    italic: !!config.italic,
  });
  return paragraph;
}

function appendBullet_(body, text) {
  const item = body.appendListItem(text);
  item
      .setGlyphType(DocumentApp.GlyphType.BULLET)
      .setIndentStart(14)
      .setIndentFirstLine(0)
      .setSpacingBefore(0)
      .setSpacingAfter(0)
      .setLineSpacing(1.05);
  applyTextStyle_(item.editAsText(), {
    fontFamily: STYLE.bodyFont,
    fontSize: STYLE.bodySize,
    color: STYLE.textColor,
  });
}

function appendInlineListSection_(body, items, options) {
  const config = options || {};
  splitInlineListForLayout_(items).forEach(function(itemLine) {
    appendBodyParagraph_(body, itemLine.join(' • '), {
      spacingBefore: 0,
      spacingAfter: 0,
      lineSpacing: config.lineSpacing || 1.05,
    });
  });
}

function appendWorkEntry_(body, entry, spacingBefore) {
  appendBodyParagraph_(body, entry.title, {
    bold: true,
    size: 9.1,
    spacingBefore: spacingBefore || 0,
    spacingAfter: 0,
    lineSpacing: 1.0,
  });
  appendMetaParagraph_(body, formatRoleMeta_(entry), {
    spacingAfter: 0,
  });
  entry.bullets.forEach(function(bullet) {
    appendBullet_(body, bullet);
  });
}

function appendEducationEntry_(body, entry, spacingBefore) {
  appendBodyParagraph_(body, entry.degree, {
    bold: true,
    size: 9.05,
    spacingBefore: spacingBefore || 0,
    spacingAfter: 0,
    lineSpacing: 1.0,
  });
  appendMetaParagraph_(body, entry.school, {
    spacingAfter: 0,
  });
}

function applyTextStyle_(text, options) {
  text
      .setFontFamily(options.fontFamily || STYLE.bodyFont)
      .setFontSize(options.fontSize || STYLE.bodySize)
      .setForegroundColor(options.color || STYLE.textColor)
      .setBold(!!options.bold)
      .setItalic(!!options.italic);
  return text;
}

function applyInlineLinks_(text, fullText, links) {
  links.forEach(function(link) {
    if (!link || !link.label || !link.url) {
      return;
    }
    const start = fullText.indexOf(link.label);
    if (start === -1) {
      return;
    }
    const end = start + link.label.length - 1;
    text
        .setLinkUrl(start, end, link.url)
        .setForegroundColor(start, end, STYLE.linkColor);
  });
}

function addHttps_(url) {
  return /^https?:\/\//i.test(url) ? url : 'https://' + url;
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

function parseResumeSource_(source) {
  const sections = splitIntoSections_(source);
  const resume = {
    header: parseHeader_(sections.__header__ || []),
    profile: parseParagraphs_(sections.PROFILE || []),
    coreWorkStrengths: parseInlineList_(sections['CORE WORK STRENGTHS'] || []),
    skills: parseInlineList_(sections.SKILLS || []),
    workExperience: parseWorkExperience_(sections['WORK EXPERIENCE'] || []),
    education: parseEducationEntries_(sections.EDUCATION || []),
  };

  assertValue_(resume.header.name, 'Missing header name.');
  assertValue_(resume.header.email, 'Missing header email.');
  assertValue_(resume.header.linkedin, 'Missing header LinkedIn.');
  assertValue_(resume.profile.length, 'Missing PROFILE content.');
  assertValue_(resume.coreWorkStrengths.length, 'Missing CORE WORK STRENGTHS content.');
  assertValue_(resume.skills.length, 'Missing SKILLS content.');
  assertValue_(resume.workExperience.length, 'Missing WORK EXPERIENCE entries.');
  assertValue_(resume.education.length, 'Missing EDUCATION entries.');

  return resume;
}

function splitIntoSections_(source) {
  const sectionNames = new Set([
    'PROFILE',
    'CORE WORK STRENGTHS',
    'SKILLS',
    'WORK EXPERIENCE',
    'EDUCATION',
  ]);

  const sections = { __header__: [] };
  let current = '__header__';

  source.replace(/\r/g, '').split('\n').forEach(function(line) {
    const trimmed = line.trim();
    if (sectionNames.has(trimmed)) {
      current = trimmed;
      sections[current] = [];
      return;
    }
    sections[current].push(line);
  });

  return sections;
}

function parseHeader_(lines) {
  const compact = lines.map(cleanLine_).filter(Boolean);
  const contactParts = splitPipeLine_(compact[1] || '');

  return {
    name: compact[0] || '',
    location: contactParts[0] || '',
    phone: contactParts[1] || '',
    email: contactParts[2] || '',
    linkedin: contactParts[3] || '',
    availability: compact[2] || '',
  };
}

function parseParagraphs_(lines) {
  return splitByBlankRuns_(lines, 1).map(joinWrappedLines_).filter(Boolean);
}

function parseInlineList_(lines) {
  return joinWrappedLines_(lines)
      .split(/\s*•\s*/)
      .map(cleanLine_)
      .filter(Boolean);
}

function parseWorkExperience_(lines) {
  return splitByBlankRuns_(lines, 1).map(function(block) {
    const clean = block.map(cleanLine_).filter(Boolean);
    const meta = parseRoleMeta_(clean[1] || '');
    return {
      title: clean[0] || '',
      company: meta.company,
      location: meta.location,
      date: meta.date,
      bullets: parseBullets_(clean.slice(2)),
    };
  }).filter(function(entry) {
    return entry.title;
  });
}

function parseEducationEntries_(lines) {
  return splitByBlankRuns_(lines, 1).map(function(block) {
    const clean = block.map(cleanLine_).filter(Boolean);
    return {
      degree: clean[0] || '',
      school: clean[1] || '',
    };
  }).filter(function(entry) {
    return entry.degree || entry.school;
  });
}

function parseRoleMeta_(line) {
  const parts = splitPipeLine_(line);
  return {
    company: parts[0] || '',
    location: parts[1] || '',
    date: parts.slice(2).join(' | '),
  };
}

function formatRoleMeta_(entry) {
  return [entry.company, entry.location, entry.date].filter(Boolean).join(' | ');
}

function splitPipeLine_(line) {
  return cleanLine_(line).split(/\s+\|\s+/).map(cleanLine_).filter(Boolean);
}

function splitInlineListForLayout_(items) {
  if (items.length <= 1) {
    return [items];
  }

  let bestIndex = Math.ceil(items.length / 2);
  let bestDifference = Number.POSITIVE_INFINITY;

  for (let index = 1; index < items.length; index += 1) {
    const firstLine = items.slice(0, index);
    const secondLine = items.slice(index);
    const difference = Math.abs(
        estimateJoinedLength_(firstLine) - estimateJoinedLength_(secondLine));
    if (difference < bestDifference) {
      bestDifference = difference;
      bestIndex = index;
    }
  }

  return [
    items.slice(0, bestIndex),
    items.slice(bestIndex),
  ].filter(function(line) {
    return line.length;
  });
}

function splitByBlankRuns_(lines, blankRunForSplit) {
  const blocks = [];
  let current = [];
  let blankRun = 0;

  lines.forEach(function(line) {
    if (!String(line || '').trim()) {
      blankRun += 1;
      if (blankRun >= blankRunForSplit && current.length) {
        blocks.push(current);
        current = [];
      }
      return;
    }

    blankRun = 0;
    current.push(line);
  });

  if (current.length) {
    blocks.push(current);
  }

  return blocks;
}

function parseBullets_(lines) {
  const bullets = [];
  let current = [];

  lines.forEach(function(line) {
    const clean = cleanLine_(line);
    if (!clean) {
      return;
    }

    if (/^[•*-]\s*/.test(clean)) {
      if (current.length) {
        bullets.push(joinWrappedLines_(current));
      }
      current = [clean.replace(/^[•*-]\s*/, '')];
      return;
    }

    current.push(clean);
  });

  if (current.length) {
    bullets.push(joinWrappedLines_(current));
  }

  return bullets;
}

function joinWrappedLines_(lines) {
  return lines.map(cleanLine_).filter(Boolean).join(' ').replace(/\s+/g, ' ').trim();
}

function estimateJoinedLength_(parts) {
  return parts.join(' • ').length;
}

function cleanLine_(line) {
  return String(line || '').replace(/\s+/g, ' ').trim();
}

function assertValue_(value, message) {
  if (!value) {
    throw new Error(message);
  }
}
