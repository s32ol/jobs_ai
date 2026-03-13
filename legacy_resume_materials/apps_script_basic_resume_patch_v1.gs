const DOC_ID = '1X3M0dKiJ7GrGWJWKAzVNXorXzt8z52AAanPV8AvE5pE';

const RESUME_SOURCE = String.raw`ROBERT MORALES
Bay Area / Sacramento, CA
415-900-2819
robert.morales.eng@gmail.com
linkedin.com/in/s32ol

Availability: Open schedule including evenings and weekends

PROFILE

Dependable team member with experience in retail, fabrication shop environments, and operations support. Known for reliability, teamwork, and keeping inventory, records, and work areas organized in fast-paced settings. Quick to learn routines, follow directions, and help teams stay on schedule.

SKILLS

Customer Service • Inventory & Stocking • Materials Handling • Documentation & Recordkeeping • Data Entry • Organization • Teamwork • Time Management • Reliability • Shop Safety • Operational Support

WORK EXPERIENCE

Backroom Merchandise Handler
Target | Sacramento, CA
2015 – 2017

• Stocked merchandise, organized backroom inventory, and kept product areas ready for daily store needs
• Unloaded shipments, sorted incoming items, and prepared merchandise for the sales floor
• Kept storage areas clean and orderly so products could be found and restocked quickly
• Worked with team members to complete stocking tasks during busy shifts

Fabrication Assistant
Jaytech Fabrication | Sacramento, CA
2013 – 2015

• Assisted with fabrication and machining work in a busy metal shop
• Operated CNC machines, lathes, and cutting equipment while following shop safety procedures
• Measured, staged, and prepared materials for cutting, machining, and assembly
• Supported TIG and arc welding work during fabrication projects
• Maintained tools, cleaned work areas, and helped keep the shop organized

Operations Support
Google Android Program | Sacramento, CA
2023 – 2025

• Updated internal reports, tracking logs, and status records used to monitor ongoing work
• Organized operational data and flagged items that needed follow-up
• Assisted with documentation and team coordination to keep work moving
• Helped keep records accurate, current, and easy for teams to review

Quality Assurance Support
Google Nest | Sacramento, CA
2020 – 2023

• Reviewed reports and documented issues to support follow-up and troubleshooting
• Assisted with testing and routine validation tasks during product updates
• Tracked issues, updated records, and kept information organized for team use
• Coordinated with team members to support timely resolution of open items

Data Specialist
OnwardCA | Fresno, CA
2020

• Organized and standardized 1,500+ records across statewide resource databases
• Improved data consistency and documentation to support accurate team use

Software Support Assistant
Learn to Achieve | Sacramento, CA
2017 – 2020

• Assisted staff with reports, documentation, and routine recordkeeping for education programs
• Maintained internal records, updated tracking information, and supported program reporting
• Helped organize information for staff use and keep records accurate and up to date

EDUCATION

B.S. Finance — Information Systems & Technology Management
San Francisco State University`;

const PAGE = {
  width: 612,
  height: 792,
  marginTop: 34,
  marginBottom: 30,
  marginLeft: 38,
  marginRight: 38,
};

const STYLE = {
  nameFont: 'Georgia',
  bodyFont: 'Arial',
  textColor: '#1f2933',
  mutedColor: '#667085',
  linkColor: '#1155cc',
  nameSize: 18,
  contactSize: 8.15,
  availabilitySize: 8.0,
  sectionSize: 9.7,
  bodySize: 8.85,
  bulletSize: 8.8,
  metaSize: 8.35,
};

function rebuildRobertMoralesResume() {
  const resume = parseResumeSource_(RESUME_SOURCE);
  const doc = DocumentApp.openById(DOC_ID);
  const body = doc.getBody();

  resetDocument_(doc, body);
  appendHeader_(body, resume.header);

  appendSectionHeader_(body, 'Profile', { spacingBefore: 4 });
  resume.profile.forEach(function(paragraph, index) {
    appendBodyParagraph_(body, paragraph, {
      spacingBefore: index === 0 ? 0 : 1,
      spacingAfter: 0,
      lineSpacing: 1.03,
    });
  });

  appendSectionHeader_(body, 'Skills');
  appendSkills_(body, resume.skills);

  appendSectionHeader_(body, 'Work Experience');
  resume.workExperience.forEach(function(entry, index) {
    appendExperienceEntry_(body, entry, index === 0 ? 0 : 3);
  });

  appendSectionHeader_(body, 'Education');
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

  if (contactLine) {
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
  }

  if (header.availability) {
    const availabilityParagraph = body.appendParagraph(header.availability);
    availabilityParagraph
        .setAlignment(DocumentApp.HorizontalAlignment.CENTER)
        .setSpacingBefore(0)
        .setSpacingAfter(4)
        .setLineSpacing(1.0);
    applyTextStyle_(availabilityParagraph.editAsText(), {
      fontFamily: STYLE.bodyFont,
      fontSize: STYLE.availabilitySize,
      italic: true,
      color: STYLE.mutedColor,
    });
  }
}

function appendSectionHeader_(body, title, options) {
  const config = options || {};
  const paragraph = body.appendParagraph(title.toUpperCase());
  paragraph
      .setAlignment(DocumentApp.HorizontalAlignment.LEFT)
      .setSpacingBefore(config.spacingBefore == null ? 5 : config.spacingBefore)
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
      .setLineSpacing(config.lineSpacing || 1.02);

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
    italic: !!config.italic,
    bold: !!config.bold,
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
      .setLineSpacing(1.0);

  applyTextStyle_(item.editAsText(), {
    fontFamily: STYLE.bodyFont,
    fontSize: STYLE.bulletSize,
    color: STYLE.textColor,
  });
}

function appendExperienceEntry_(body, entry, spacingBefore) {
  appendBodyParagraph_(body, entry.title, {
    size: 9.1,
    bold: true,
    spacingBefore: spacingBefore || 0,
    spacingAfter: 0,
    lineSpacing: 1.0,
  });

  appendMetaParagraph_(body, entry.organization + '  |  ' + entry.date, {
    spacingAfter: 1,
  });

  entry.bullets.forEach(function(bullet) {
    appendBullet_(body, bullet);
  });
}

function appendSkills_(body, skills) {
  splitSkillsForLayout_(skills).forEach(function(skillLine) {
    appendBodyParagraph_(body, skillLine.join(' • '), {
      size: 8.8,
      spacingBefore: 0,
      spacingAfter: 0,
      lineSpacing: 1.03,
    });
  });
}

function appendEducationEntry_(body, entry, spacingBefore) {
  appendBodyParagraph_(body, entry.degree, {
    size: 9.0,
    bold: true,
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
    skills: parseSkills_(sections.SKILLS || []),
    workExperience: parseWorkExperience_(sections['WORK EXPERIENCE'] || []),
    education: parseEducationEntries_(sections.EDUCATION || []),
  };

  assertValue_(resume.header.name, 'Missing resume name in header.');
  assertValue_(resume.header.email, 'Missing email in header.');
  assertValue_(resume.header.linkedin, 'Missing LinkedIn in header.');
  assertValue_(resume.profile.length, 'Missing PROFILE content.');
  assertValue_(resume.skills.length, 'Missing SKILLS content.');
  assertValue_(resume.workExperience.length, 'Missing WORK EXPERIENCE content.');
  assertValue_(resume.education.length, 'Missing EDUCATION content.');

  return resume;
}

function splitIntoSections_(source) {
  const sectionNames = new Set([
    'PROFILE',
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
  const remaining = compact.slice(4);
  const availabilityIndex = remaining.findIndex(function(line) {
    return /^Availability:/i.test(line);
  });
  const availability = availabilityIndex === -1 ? '' : remaining[availabilityIndex];
  const linkLine = remaining.find(function(line) {
    return !/^Availability:/i.test(line);
  }) || compact[4] || '';

  return {
    name: compact[0] || '',
    location: compact[1] || '',
    phone: compact[2] || '',
    email: compact[3] || '',
    linkedin: linkLine,
    availability: availability,
  };
}

function parseParagraphs_(lines) {
  return splitByBlankRuns_(lines, 1).map(joinWrappedLines_).filter(Boolean);
}

function parseSkills_(lines) {
  return joinWrappedLines_(lines)
      .split(/\s*•\s*/)
      .map(cleanLine_)
      .filter(Boolean);
}

function splitSkillsForLayout_(skills) {
  if (skills.length <= 1) {
    return [skills];
  }

  let bestIndex = Math.ceil(skills.length / 2);
  let bestDifference = Number.POSITIVE_INFINITY;

  for (let index = 1; index < skills.length; index += 1) {
    const firstLine = skills.slice(0, index);
    const secondLine = skills.slice(index);
    const difference = Math.abs(
        estimateJoinedLength_(firstLine) - estimateJoinedLength_(secondLine));
    if (difference < bestDifference) {
      bestDifference = difference;
      bestIndex = index;
    }
  }

  return [
    skills.slice(0, bestIndex),
    skills.slice(bestIndex),
  ].filter(function(line) {
    return line.length;
  });
}

function parseWorkExperience_(lines) {
  const cleanLines = lines.map(cleanLine_).filter(Boolean);
  const entries = [];
  let current = null;

  for (let index = 0; index < cleanLines.length; index += 1) {
    if (isLikelyRoleHeaderAt_(cleanLines, index)) {
      if (current) {
        entries.push(finalizeWorkEntry_(current));
      }

      current = {
        title: cleanLines[index],
        organization: cleanLines[index + 1],
        date: cleanLines[index + 2],
        bodyLines: [],
      };
      index += 2;
      continue;
    }

    if (current) {
      current.bodyLines.push(cleanLines[index]);
    }
  }

  if (current) {
    entries.push(finalizeWorkEntry_(current));
  }

  return entries;
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

    if (/^•\s*/.test(clean)) {
      if (current.length) {
        bullets.push(joinWrappedLines_(current));
      }
      current = [clean.replace(/^•\s*/, '')];
      return;
    }

    current.push(clean);
  });

  if (current.length) {
    bullets.push(joinWrappedLines_(current));
  }

  return bullets;
}

function finalizeWorkEntry_(entry) {
  return {
    title: entry.title,
    organization: entry.organization,
    date: entry.date,
    bullets: parseBullets_(entry.bodyLines || []),
  };
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

function isLikelyRoleHeaderAt_(lines, index) {
  const title = lines[index];
  const organization = lines[index + 1];
  const date = lines[index + 2];

  if (!title || !organization || !date) {
    return false;
  }

  if (/^•\s*/.test(title) || /^•\s*/.test(organization) || /^•\s*/.test(date)) {
    return false;
  }

  return looksLikeDateLine_(date);
}

function looksLikeDateLine_(line) {
  const text = cleanLine_(line);
  if (!text) {
    return false;
  }

  if (/^(19|20)\d{2}$/.test(text)) {
    return true;
  }

  return /^(?:[A-Za-z]+\s+)?(19|20)\d{2}\s*[–—-]\s*(?:[A-Za-z]+\s+)?(19|20)\d{2}$/.test(text);
}

function assertValue_(value, message) {
  if (!value) {
    throw new Error(message);
  }
}
