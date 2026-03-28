from __future__ import annotations

from dataclasses import dataclass
import re

from .identity import normalize_optional_metadata

US_ALLOWED_LOCATION_CLASSIFICATION = "us_allowed"
NON_US_LOCATION_CLASSIFICATION = "non_us"
AMBIGUOUS_LOCATION_CLASSIFICATION = "ambiguous"

_SEARCHABLE_TEXT_RE = re.compile(r"[^a-z0-9]+")
_US_CITY_STATE_RE = re.compile(
    r"\b[a-z][a-z .'-]+,\s*(?:AL|AK|AZ|AR|AS|CA|CO|CT|DC|DE|FL|FM|GA|GU|HI|IA|ID|IL|IN|KS|KY|LA|MA|MD|ME|MI|MN|MO|MP|MS|MT|NC|ND|NE|NH|NJ|NM|NV|NY|OH|OK|OR|PA|PR|RI|SC|SD|TN|TX|UM|UT|VA|VI|VT|WA|WI|WV|WY)\b",
    re.IGNORECASE,
)
_US_COUNTRY_SUFFIX_RE = re.compile(
    r",\s*u\.?s\.?(?:a\.?)?(?:$|[\s,)])",
    re.IGNORECASE,
)

_US_STATE_NAMES = (
    "alabama",
    "alaska",
    "arizona",
    "arkansas",
    "california",
    "colorado",
    "connecticut",
    "delaware",
    "district of columbia",
    "florida",
    "georgia",
    "hawaii",
    "idaho",
    "illinois",
    "indiana",
    "iowa",
    "kansas",
    "kentucky",
    "louisiana",
    "maine",
    "maryland",
    "massachusetts",
    "michigan",
    "minnesota",
    "mississippi",
    "missouri",
    "montana",
    "nebraska",
    "nevada",
    "new hampshire",
    "new jersey",
    "new mexico",
    "new york",
    "north carolina",
    "north dakota",
    "ohio",
    "oklahoma",
    "oregon",
    "pennsylvania",
    "rhode island",
    "south carolina",
    "south dakota",
    "tennessee",
    "texas",
    "utah",
    "vermont",
    "virginia",
    "washington",
    "west virginia",
    "wisconsin",
    "wyoming",
)
_US_STATE_NAME_RE = re.compile(
    r"\b(?:"
    + "|".join(re.escape(state_name) for state_name in sorted(_US_STATE_NAMES, key=len, reverse=True))
    + r")\b",
    re.IGNORECASE,
)

_NON_US_SIGNALS = (
    "united kingdom",
    "european union",
    "latin america",
    "new zealand",
    "south africa",
    "saudi arabia",
    "united arab emirates",
    "czech republic",
    "hong kong",
    "south korea",
    "australia",
    "singapore",
    "philippines",
    "netherlands",
    "switzerland",
    "argentina",
    "colombia",
    "portugal",
    "romania",
    "slovakia",
    "slovenia",
    "lithuania",
    "austria",
    "belgium",
    "bulgaria",
    "croatia",
    "czechia",
    "denmark",
    "estonia",
    "finland",
    "france",
    "germany",
    "greece",
    "hungary",
    "ireland",
    "israel",
    "italy",
    "latvia",
    "luxembourg",
    "malaysia",
    "mexico",
    "norway",
    "poland",
    "spain",
    "sweden",
    "taiwan",
    "thailand",
    "turkey",
    "ukraine",
    "vietnam",
    "brazil",
    "canada",
    "chile",
    "china",
    "egypt",
    "india",
    "japan",
    "korea",
    "peru",
    "uk",
    "eu",
    "uae",
    "emea",
    "apac",
    "latam",
    "europe",
)
_NON_US_SIGNAL_RE = re.compile(
    r"\b(?:"
    + "|".join(re.escape(signal) for signal in sorted(_NON_US_SIGNALS, key=len, reverse=True))
    + r")\b",
    re.IGNORECASE,
)


@dataclass(frozen=True, slots=True)
class LocationClassification:
    original_location: str | None
    normalized_location: str | None
    classification: str
    reason: str

    @property
    def is_us_allowed(self) -> bool:
        return self.classification == US_ALLOWED_LOCATION_CLASSIFICATION

    @property
    def is_non_us(self) -> bool:
        return self.classification == NON_US_LOCATION_CLASSIFICATION

    @property
    def is_ambiguous(self) -> bool:
        return self.classification == AMBIGUOUS_LOCATION_CLASSIFICATION


def classify_job_location(location: str | None) -> LocationClassification:
    normalized_location = normalize_optional_metadata(location)
    if normalized_location is None:
        return LocationClassification(
            original_location=location,
            normalized_location=None,
            classification=AMBIGUOUS_LOCATION_CLASSIFICATION,
            reason="location missing",
        )

    searchable_text = _searchable_text(normalized_location)
    lowered_location = normalized_location.lower()
    if not searchable_text:
        return LocationClassification(
            original_location=location,
            normalized_location=normalized_location,
            classification=AMBIGUOUS_LOCATION_CLASSIFICATION,
            reason="location missing",
        )

    if searchable_text == "remote":
        return LocationClassification(
            original_location=location,
            normalized_location=normalized_location,
            classification=AMBIGUOUS_LOCATION_CLASSIFICATION,
            reason='location "Remote" is ambiguous without a country',
        )

    if _contains_explicit_us_signal(lowered_location, searchable_text):
        return LocationClassification(
            original_location=location,
            normalized_location=normalized_location,
            classification=US_ALLOWED_LOCATION_CLASSIFICATION,
            reason="location explicitly names the United States",
        )

    non_us_match = _NON_US_SIGNAL_RE.search(searchable_text)
    if non_us_match is not None:
        return LocationClassification(
            original_location=location,
            normalized_location=normalized_location,
            classification=NON_US_LOCATION_CLASSIFICATION,
            reason=f'location matched non-US signal "{non_us_match.group(0)}"',
        )

    if _US_CITY_STATE_RE.search(normalized_location) is not None:
        return LocationClassification(
            original_location=location,
            normalized_location=normalized_location,
            classification=US_ALLOWED_LOCATION_CLASSIFICATION,
            reason="location matched a US city/state abbreviation pattern",
        )

    state_name_match = _US_STATE_NAME_RE.search(searchable_text)
    if state_name_match is not None:
        return LocationClassification(
            original_location=location,
            normalized_location=normalized_location,
            classification=US_ALLOWED_LOCATION_CLASSIFICATION,
            reason=f'location matched US state name "{state_name_match.group(0)}"',
        )

    if "remote" in searchable_text:
        return LocationClassification(
            original_location=location,
            normalized_location=normalized_location,
            classification=AMBIGUOUS_LOCATION_CLASSIFICATION,
            reason="remote location is ambiguous without an explicit geography",
        )

    return LocationClassification(
        original_location=location,
        normalized_location=normalized_location,
        classification=AMBIGUOUS_LOCATION_CLASSIFICATION,
        reason="location does not clearly indicate a supported or unsupported geography",
    )


def location_allowed_in_us_only_mode(location: str | None) -> bool:
    return not classify_job_location(location).is_non_us


def _contains_explicit_us_signal(
    lowered_location: str,
    searchable_text: str,
) -> bool:
    if searchable_text in {"us", "usa", "united states"}:
        return True
    if "united states" in searchable_text:
        return True
    if re.search(r"\busa\b", searchable_text) is not None:
        return True
    if re.search(r"\bremote\b.*\bus\b", searchable_text) is not None:
        return True
    if re.search(r"\bus\b.*\bremote\b", searchable_text) is not None:
        return True
    return _US_COUNTRY_SUFFIX_RE.search(lowered_location) is not None


def _searchable_text(value: str) -> str:
    return _SEARCHABLE_TEXT_RE.sub(" ", value.lower()).strip()
