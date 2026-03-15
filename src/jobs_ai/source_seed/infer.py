from __future__ import annotations

from collections.abc import Sequence
import re
from urllib.parse import urlparse

from .models import CandidateConfidence, CompanySeedInput, SlugCandidate, SourceCandidate

_LEADING_ARTICLES = frozenset({"the"})
_LEGAL_SUFFIXES = frozenset(
    {
        "ag",
        "bv",
        "co",
        "company",
        "corp",
        "corporation",
        "gmbh",
        "inc",
        "incorporated",
        "limited",
        "llc",
        "llp",
        "lp",
        "ltd",
        "oy",
        "plc",
        "pte",
        "pty",
        "sa",
    }
)
_COMMON_SUBDOMAINS = frozenset({"apply", "boards", "careers", "jobs", "www"})
_MULTI_PART_PUBLIC_SUFFIXES = frozenset(
    {
        "co.in",
        "co.jp",
        "co.uk",
        "com.au",
        "com.br",
        "com.hk",
        "com.mx",
        "com.sg",
    }
)
_NON_ALNUM_RE = re.compile(r"[^a-z0-9]+")

_PORTAL_HOSTS = {
    "greenhouse": "https://boards.greenhouse.io",
    "lever": "https://jobs.lever.co",
    "ashby": "https://jobs.ashbyhq.com",
}


def parse_company_inputs(raw_values: Sequence[str]) -> tuple[CompanySeedInput, ...]:
    entries: list[CompanySeedInput] = []
    for raw_value in raw_values:
        parsed = parse_company_input_line(len(entries) + 1, raw_value)
        if parsed is None:
            continue
        entries.append(parsed)
    return tuple(entries)


def parse_company_input_line(index: int, raw_value: str) -> CompanySeedInput | None:
    stripped = raw_value.strip()
    if not stripped or stripped.startswith("#"):
        return None

    if "|" not in stripped:
        company = _normalize_company_display(stripped)
        return CompanySeedInput(
            index=index,
            raw_value=stripped,
            company=company,
            domain=None,
            notes=None,
        )

    parts = [part.strip() for part in stripped.split("|")]
    company = _normalize_company_display(parts[0]) if parts else None
    domain = _normalize_domain(parts[1]) if len(parts) >= 2 and parts[1] else None
    notes = _normalize_notes(parts[2:]) if len(parts) >= 3 else None
    return CompanySeedInput(
        index=index,
        raw_value=stripped,
        company=company,
        domain=domain,
        notes=notes,
    )


def build_source_candidates(company_input: CompanySeedInput) -> tuple[SourceCandidate, ...]:
    slug_candidates = infer_slug_candidates(company_input)
    if not slug_candidates:
        return ()

    candidates: list[SourceCandidate] = []
    seen_urls: set[str] = set()
    for portal_type in ("greenhouse", "lever", "ashby"):
        base_url = _PORTAL_HOSTS[portal_type]
        for slug_candidate in slug_candidates:
            url = f"{base_url}/{slug_candidate.slug}"
            if url in seen_urls:
                continue
            seen_urls.add(url)
            candidates.append(
                SourceCandidate(
                    index=len(candidates) + 1,
                    portal_type=portal_type,
                    slug=slug_candidate.slug,
                    url=url,
                    slug_source=slug_candidate.slug_source,
                    confidence=slug_candidate.confidence,
                )
            )
    return tuple(candidates)


def infer_slug_candidates(company_input: CompanySeedInput) -> tuple[SlugCandidate, ...]:
    candidates: list[SlugCandidate] = []
    seen: set[str] = set()

    for slug, slug_source, confidence in _domain_slug_variants(company_input.domain):
        _append_slug_candidate(candidates, seen, slug, slug_source, confidence)

    company_tokens = normalize_company_tokens(company_input.company)
    if company_tokens:
        compact_slug = "".join(company_tokens)
        _append_slug_candidate(candidates, seen, compact_slug, "company_compact", "medium")
        if len(company_tokens) > 1:
            _append_slug_candidate(
                candidates,
                seen,
                "-".join(company_tokens),
                "company_hyphen",
                "medium",
            )
            _append_slug_candidate(
                candidates,
                seen,
                company_tokens[0],
                "company_primary_token",
                "low",
            )

    return tuple(candidates)


def normalize_company_tokens(value: str | None) -> tuple[str, ...]:
    normalized_text = _normalize_ascii_text(value)
    if normalized_text is None:
        return ()

    tokens = [token for token in normalized_text.split() if token]
    if tokens and tokens[0] in _LEADING_ARTICLES:
        tokens = tokens[1:]
    while tokens and tokens[-1] in _LEGAL_SUFFIXES:
        tokens.pop()
    return tuple(tokens)


def company_identity_key(value: str | None) -> str | None:
    tokens = normalize_company_tokens(value)
    if not tokens:
        return None
    return "".join(tokens)


def domain_identity_key(value: str | None) -> str | None:
    label = primary_domain_label(value)
    if label is None:
        return None
    return _NON_ALNUM_RE.sub("", label) or None


def primary_domain_label(value: str | None) -> str | None:
    normalized_domain = _normalize_domain(value)
    if normalized_domain is None:
        return None

    labels = [label for label in normalized_domain.split(".") if label]
    while len(labels) > 2 and labels[0] in _COMMON_SUBDOMAINS:
        labels.pop(0)
    if len(labels) < 2:
        return None

    public_suffix = ".".join(labels[-2:])
    if public_suffix in _MULTI_PART_PUBLIC_SUFFIXES and len(labels) >= 3:
        return labels[-3]
    return labels[-2]


def _append_slug_candidate(
    candidates: list[SlugCandidate],
    seen: set[str],
    slug: str | None,
    slug_source: str,
    confidence: CandidateConfidence,
) -> None:
    if slug is None:
        return
    normalized_slug = _normalize_slug(slug)
    if normalized_slug is None or normalized_slug in seen:
        return
    seen.add(normalized_slug)
    candidates.append(
        SlugCandidate(
            slug=normalized_slug,
            slug_source=slug_source,
            confidence=confidence,
        )
    )


def _domain_slug_variants(domain: str | None) -> tuple[tuple[str, str, str], ...]:
    label = primary_domain_label(domain)
    if label is None:
        return ()

    candidates: list[tuple[str, str, str]] = [(label, "domain_label", "high")]
    compact_label = _NON_ALNUM_RE.sub("", label)
    if compact_label and compact_label != label:
        candidates.append((compact_label, "domain_label_compact", "medium"))
    return tuple(candidates)


def _normalize_company_display(value: str | None) -> str | None:
    if value is None:
        return None
    normalized_value = re.sub(r"\s+", " ", value).strip()
    return normalized_value or None


def _normalize_notes(values: Sequence[str]) -> str | None:
    note = " | ".join(part for part in values if part)
    return _normalize_company_display(note)


def _normalize_domain(value: str | None) -> str | None:
    if value is None:
        return None

    normalized_value = value.strip().lower()
    if not normalized_value:
        return None

    parse_target = normalized_value if "://" in normalized_value else f"https://{normalized_value}"
    parsed = urlparse(parse_target)
    host = (parsed.netloc or parsed.path).strip().lower().strip("/")
    if not host:
        return None
    if "@" in host:
        return None
    if ":" in host:
        host = host.split(":", 1)[0]
    return host or None


def _normalize_ascii_text(value: str | None) -> str | None:
    if value is None:
        return None
    normalized_value = value.lower().replace("&", " and ")
    normalized_value = normalized_value.replace("'", "").replace('"', "")
    normalized_value = _NON_ALNUM_RE.sub(" ", normalized_value)
    normalized_value = re.sub(r"\s+", " ", normalized_value).strip()
    return normalized_value or None


def _normalize_slug(value: str | None) -> str | None:
    normalized_text = _normalize_ascii_text(value)
    if normalized_text is None:
        return None
    slug = normalized_text.replace(" ", "-")
    slug = re.sub(r"-{2,}", "-", slug).strip("-")
    return slug or None
