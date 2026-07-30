"""Shared parsing and safe matching for competition document headers."""

from __future__ import annotations

import re
import unicodedata
from typing import Optional, Tuple

from natacion_chile.domain.normalization import normalize_string


DATE_DMY_RE = re.compile(
    r"(?P<day>\d{1,2})[-/](?P<month>\d{1,2})[-/](?P<year>\d{4})"
)
COMPETITION_HEADER_WITH_DATE_RE = re.compile(
    r"^(?P<name>.+?)\s+-\s+(?P<date>\d{1,2}[-/]\d{1,2}[-/]\d{4})$"
)
COMPETITION_HEADER_WITH_DATE_RANGE_RE = re.compile(
    r"^(?P<name>.+?)\s+-\s+"
    r"(?P<start_date>\d{1,2}[-/]\d{1,2}[-/]\d{4})\s+"
    r"(?:a|to)\s+"
    r"(?P<end_date>\d{1,2}[-/]\d{1,2}[-/]\d{4})$",
    re.IGNORECASE,
)
NON_COMPETITION_HEADER_RE = re.compile(
    r"HY-TEK|MEET MANAGER|\bPage\s+\d+\b|\bP[aá]gina\s+\d+\b|"
    r"^Results\b|^Resultados\b|^Event\s+\d+\b|^Evento\s+\d+\b",
    re.IGNORECASE,
)
ROMAN_NUMERAL_RE = re.compile(r"^[ivxlcdm]+$")
SAN_BERNARDO_TYPE_VARIANTS = frozenset({"copa", "torneo"})


def _clean_header_text(value: str | None) -> str | None:
    normalized = normalize_string(value)
    if normalized is None:
        return None
    return unicodedata.normalize("NFC", normalized)


def parse_dmy_date(value: Optional[str]) -> Optional[str]:
    candidate = normalize_string(value)
    if candidate is None:
        return None
    match = DATE_DMY_RE.search(candidate)
    if not match:
        return None
    return (
        f"{int(match.group('year')):04d}-"
        f"{int(match.group('month')):02d}-"
        f"{int(match.group('day')):02d}"
    )


def parse_competition_header(
    line: str,
) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """Return competition name and ISO date range from a HY-TEK header."""

    candidate = _clean_header_text(line)
    if candidate is None or NON_COMPETITION_HEADER_RE.search(candidate):
        return None, None, None

    match = COMPETITION_HEADER_WITH_DATE_RANGE_RE.match(candidate)
    if match:
        return (
            _clean_header_text(match.group("name")),
            parse_dmy_date(match.group("start_date")),
            parse_dmy_date(match.group("end_date")),
        )

    # Common FCHMN/HY-TEK form: "VI Torneo Smart Swim Team - 24-05-2025".
    match = COMPETITION_HEADER_WITH_DATE_RE.match(candidate)
    if match:
        date_iso = parse_dmy_date(match.group("date"))
        return _clean_header_text(match.group("name")), date_iso, date_iso

    # Preserve the results parser's legacy support for undated Copa headers.
    if re.search(r"\bCopa\b", candidate, re.IGNORECASE):
        return candidate, None, None
    return None, None, None


def distinctive_competition_name_tokens(value: str | None) -> tuple[str, ...]:
    """Normalize a name while retaining event-type tokens that carry identity."""

    if not value:
        return ()
    folded = "".join(
        character
        for character in unicodedata.normalize("NFKD", value.casefold())
        if not unicodedata.combining(character)
    )
    tokens = re.findall(r"[a-z0-9]+", folded)
    return tuple(
        token
        for token in tokens
        if not token.isdigit() and not ROMAN_NUMERAL_RE.fullmatch(token)
    )


def competition_names_match(source_name: str | None, database_name: str | None) -> bool:
    """Match normalized names, with one documented FCHMN naming exception."""

    source_tokens = distinctive_competition_name_tokens(source_name)
    database_tokens = distinctive_competition_name_tokens(database_name)
    if not source_tokens or not database_tokens:
        return False
    if source_tokens == database_tokens:
        return True
    # FCHMN's 2026 PDF says "Torneo" while core records this one meet as "Copa".
    return (
        source_tokens[1:] == database_tokens[1:] == ("master", "san", "bernardo")
        and source_tokens[0] in SAN_BERNARDO_TYPE_VARIANTS
        and database_tokens[0] in SAN_BERNARDO_TYPE_VARIANTS
    )
