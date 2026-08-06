#!/usr/bin/env python3
"""Parse, validate, and publish HY-TEK Meet Program PDFs."""

from __future__ import annotations

import argparse
import csv
from dataclasses import asdict, dataclass, field
import hashlib
import io
import json
from pathlib import Path
import re
import sys
from typing import Any, Iterable

import pdfplumber

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from natacion_chile.domain.competition_header import (
    competition_names_match,
    parse_competition_header,
)
from natacion_chile.domain.extracted_text import clean_extracted_text
from natacion_chile.domain.person_name import clean_athlete_name
from natacion_chile.domain.normalization import parse_hytek_event_identity


# Subir esta version es lo que habilita republicar un PDF ya cargado: la
# unicidad de publicacion es competencia + checksum + parser_version. Cualquier
# cambio que altere la salida del parser debe subirla, o el mismo archivo se
# rechaza como "ya publicado" y la correccion nunca llega a la base.
PARSER_VERSION = "0.5.3"
ENTRY_COLUMNS = [
    "session_number",
    "session_name",
    "event_number",
    "event_name",
    "heat_number",
    "heat_total",
    "estimated_start_time",
    "lane",
    "display_name",
    "age",
    "team_name",
    "seed_time_text",
    "seed_time_ms",
    "entry_type",
    "relay_members",
    "page_number",
    "column_number",
    "line_number",
]
DEBUG_COLUMNS = ["page_number", "column_number", "line_number", "raw_line", "reason"]
EVENT_RE = re.compile(
    r"^(?:#|Event\s+)(?P<number>\d+)\s+(?P<name>.+?)\s*$",
    re.IGNORECASE,
)
# Meet Manager rotula en el idioma del torneo: los programas FCHMN salen en
# espanol ("Serie 1 of 38 Finales Inicia a las 09:30 AM"). Sin estas variantes no
# se establece contexto de serie y toda inscripcion cae como linea no parseada.
CONTINUATION_RE = re.compile(
    r"^(?:Heat|Serie)\s+(?P<heat>\d+)\s+\(#(?P<number>\d+)\s+(?P<name>.+?)\)?\s*$",
    re.IGNORECASE,
)
HEAT_RE = re.compile(
    r"(?:^|.*\))(?:Heat|Serie)\s+(?P<number>\d+)"
    r"(?:\s+of\s+(?P<total>\d+))?\s+(?:Finals|Finales)\b"
    r"(?:\s+(?:Starts\s+at|Inicia\s+a\s+las)"
    r"\s+(?P<estimated_time>(?:0?[1-9]|1[0-2]):[0-5]\d\s*[AP]M))?",
    re.IGNORECASE,
)
SESSION_RE = re.compile(r"^Meet Program\s*-\s*(?P<name>.+?)\s*$", re.IGNORECASE)
SEED_RE = re.compile(
    r"^(?:X?NT|X?\d{1,3}(?::\d{2})?(?:[,.]\d{2}))$",
    re.IGNORECASE,
)
AGE_RE = re.compile(r"^(?:[MWX])?(?P<age>\d{1,3})$", re.IGNORECASE)
OVERLAPPED_NAME_AGE_RE = re.compile(
    r"^(?P<name>.+?)(?P<gender>[MWX])(?P<tail>(?:[A-Za-zÁÉÍÓÚÜÑ]*\d){1,3})$",
    re.IGNORECASE,
)
SCHEMA_RE = re.compile(r"^[a-z_][a-z0-9_]*$")
CHECKSUM_RE = re.compile(r"^[0-9a-f]{64}$")
SKIP_PATTERNS = [
    re.compile(r"^Natacion Stadio Italiano$", re.IGNORECASE),
    re.compile(r"^HY-TEK'?s MEET$", re.IGNORECASE),
    re.compile(r"^MANAGER\b.*\bPage\s+\d+$", re.IGNORECASE),
    re.compile(r"^Lane\s+(?:Name Age Team|Team Relay)\s+Seed Time$", re.IGNORECASE),
    # El encabezado de columnas en espanol llega con las celdas entrelazadas
    # ("Carril Nombre EdadT Eieqmuippoo para Sembrado"), asi que solo se ancla
    # el prefijo. "Carriles," como apellido no matchea por el espacio exigido.
    re.compile(r"^Carril\s+(?:Nombre|Equipo)\b", re.IGNORECASE),
    re.compile(r"^.+\s+-\s+\d{1,2}-\d{1,2}-\d{4}$"),
]
GENDER_MEMBER_RE = re.compile(
    r"(?P<name>[A-ZÁÉÍÓÚÜÑ][^,]+,\s+.*?)(?:\s+)(?P<gender>[MW])"
    r"(?P<age>\d{1,3})(?=\s*[A-ZÁÉÍÓÚÜÑ]|$)",
    re.IGNORECASE,
)
AGE_MEMBER_RE = re.compile(
    r"(?P<name>[A-ZÁÉÍÓÚÜÑ][^,]+,\s+.*?)(?:\s+)(?P<age>\d{1,3})"
    r"(?=\s*[A-ZÁÉÍÓÚÜÑ]|$)",
    re.IGNORECASE,
)


class MeetProgramError(RuntimeError):
    pass


@dataclass
class SourceLine:
    page_number: int
    column_number: int
    line_number: int
    text: str


@dataclass
class DebugLine:
    page_number: int
    column_number: int
    line_number: int
    raw_line: str
    reason: str


@dataclass
class MeetProgramEntry:
    session_number: int
    session_name: str
    event_number: int
    event_name: str
    heat_number: int
    heat_total: int | None
    lane: int
    display_name: str
    age: int | None
    team_name: str | None
    seed_time_text: str | None
    seed_time_ms: int | None
    entry_type: str
    relay_members: list[str]
    page_number: int
    column_number: int
    line_number: int
    estimated_start_time: str | None = None


@dataclass
class ParsedMeetProgram:
    entries: list[MeetProgramEntry]
    unparsed: list[DebugLine]
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class ValidationIssue:
    severity: str
    issue_key: str
    message: str
    count: int = 1


@dataclass
class ValidationSummary:
    state: str
    input_dir: str
    counts: dict[str, int]
    issues: list[ValidationIssue]
    metadata: dict[str, Any]
    publication_id: int | None = None
    publication_created: bool | None = None


def clean_text(value: str) -> str:
    return " ".join(value.replace("\u00a0", " ").split()).strip()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def seed_time_ms(value: str | None) -> int | None:
    if not value:
        return None
    normalized = value.upper().removeprefix("X")
    if normalized == "NT":
        return None
    normalized = normalized.replace(",", ".")
    parts = normalized.split(":")
    try:
        if len(parts) == 1:
            seconds = float(parts[0])
        elif len(parts) == 2:
            seconds = int(parts[0]) * 60 + float(parts[1])
        else:
            return None
    except ValueError:
        return None
    return round(seconds * 1000)


def normalize_estimated_start_time(value: str | None) -> str | None:
    if not value:
        return None
    match = re.fullmatch(
        r"(?P<hour>0?[1-9]|1[0-2]):(?P<minute>[0-5]\d)\s*(?P<period>[AP]M)",
        value.strip(),
        re.IGNORECASE,
    )
    if not match:
        return None
    hour = int(match.group("hour")) % 12
    if match.group("period").upper() == "PM":
        hour += 12
    return f"{hour:02d}:{match.group('minute')}"


def infer_program_segment(session_name: str) -> tuple[int, str]:
    """Map a document session label to its stable stage and pool stream."""

    normalized = clean_text(session_name).casefold()
    ordinals = {
        "primera": 1,
        "segunda": 2,
        "tercera": 3,
        "cuarta": 4,
        "quinta": 5,
        "sexta": 6,
    }
    stage_number = next(
        (number for word, number in ordinals.items() if re.search(rf"\b{word}\b", normalized)),
        1,
    )
    if "entrenamiento" in normalized:
        pool_role = "training"
    elif "competencia" in normalized:
        pool_role = "competition"
    else:
        pool_role = "main"
    return stage_number, pool_role


def _group_band_words(words: list[dict[str, Any]], tolerance: float = 2.0) -> list[str]:
    groups: list[tuple[float, list[dict[str, Any]]]] = []
    for word in sorted(words, key=lambda item: (float(item["top"]), float(item["x0"]))):
        top = float(word["top"])
        group = next((candidate for candidate in groups if abs(candidate[0] - top) <= tolerance), None)
        if group is None:
            group = (top, [])
            groups.append(group)
        group[1].append(word)
    return [
        clean_text(" ".join(word["text"] for word in sorted(group, key=lambda item: float(item["x0"]))))
        for _top, group in sorted(groups, key=lambda item: item[0])
        if group
    ]


def detect_page_column_count(
    words: list[dict[str, Any]], page_width: float = 612.0
) -> int:
    """Detect HY-TEK's two- or three-column page from repeated heat anchors.

    Acepta el rotulo en ingles y en espanol: sin "serie" un programa en espanol
    no encuentra anclas y cae al valor por defecto de tres columnas, que acierta
    solo por casualidad cuando el documento efectivamente tiene tres.
    """

    anchors: list[float] = []
    for word in sorted(
        (
            item for item in words
            if str(item.get("text", "")).casefold() in {"heat", "serie"}
        ),
        key=lambda item: float(item["x0"]),
    ):
        x0 = float(word["x0"])
        if not anchors or abs(x0 - anchors[-1]) > 40:
            anchors.append(x0)
    if len(anchors) >= 3:
        return 3
    if len(anchors) == 2:
        # Some FECHIDA one-page programs occupy only two cells of a three-cell
        # grid. The anchor distance distinguishes that from a true two-column page.
        return 3 if anchors[1] - anchors[0] < page_width * 0.42 else 2
    return 3


def extract_source_lines(pdf_path: Path) -> tuple[list[SourceLine], int]:
    lines: list[SourceLine] = []
    word_count = 0
    document_column_count: int | None = None
    with pdfplumber.open(pdf_path) as pdf:
        for page_number, page in enumerate(pdf.pages, start=1):
            words = page.extract_words(x_tolerance=2, y_tolerance=3, keep_blank_chars=False)
            word_count += len(words)
            width = float(page.width)
            # El corte entre encabezado de pagina y cuerpo se ancla a la primera
            # palabra de contenido. "Serie" es la variante en espanol: sin ella,
            # los encabezados que HY-TEK repite arriba de cada columna quedan en
            # la banda de ancho completo, se fusionan entre columnas y las
            # inscripciones siguientes heredan la serie de la columna anterior.
            body_tops = [
                float(word["top"])
                for word in words
                if str(word["text"]).casefold() in {"heat", "event", "serie"}
                or str(word["text"]).startswith("#")
            ]
            body_top = min(body_tops, default=float("inf"))
            header_words = [word for word in words if float(word["top"]) < body_top]
            for line_number, text in enumerate(_group_band_words(header_words), start=1):
                if text:
                    lines.append(SourceLine(page_number, 1, line_number, text))

            if document_column_count is None:
                document_column_count = detect_page_column_count(words, width)
            column_count = document_column_count
            body_words = [word for word in words if float(word["top"]) >= body_top]
            for column_number in range(1, column_count + 1):
                lower = width * (column_number - 1) / column_count
                upper = width * column_number / column_count
                band = [
                    word
                    for word in body_words
                    if lower
                    <= (float(word["x0"]) + float(word["x1"])) / 2
                    < upper
                ]
                for line_number, text in enumerate(_group_band_words(band), start=1):
                    if text:
                        lines.append(SourceLine(page_number, column_number, line_number, text))
    return lines, word_count


def extract_competition_header_metadata(
    lines: Iterable[SourceLine],
) -> dict[str, Any]:
    headers: list[tuple[str, str | None, str | None]] = []
    for line in lines:
        derived_text = clean_extracted_text(line.text) or ""
        name, start_date, end_date = parse_competition_header(derived_text)
        if name is not None:
            header = (name, start_date, end_date)
            if header not in headers:
                headers.append(header)
    # Solo el encabezado de competencia trae fechas. El titulo del reporte
    # ("Programa de Competencias - ...") tambien parsea como nombre, y sin esta
    # preferencia los dos compiten y disparan un conflicto falso.
    dated = [header for header in headers if header[1]]
    candidates = dated or headers
    selected = candidates[0] if candidates else (None, None, None)
    metadata: dict[str, Any] = {
        "source_competition_name": selected[0],
        "source_competition_start_date": selected[1],
        "source_competition_end_date": selected[2],
    }
    if len(candidates) > 1:
        metadata["source_competition_header_conflict"] = True
    return metadata


def _event_parts(name: str) -> tuple[str, str]:
    cleaned = clean_text(name).rstrip(")")
    # "Relevo" es el rotulo en espanol. Sin el, el evento de postas se procesa
    # como individual: la edad sumada del equipo (X240) se guarda como edad de
    # nadador y los integrantes no se capturan.
    entry_type = (
        "relay"
        if re.search(r"\b(?:Relay|Relevo)\b", cleaned, re.IGNORECASE)
        else "individual"
    )
    return cleaned, entry_type


def _parse_fechida_overlap(tokens: list[str]) -> tuple[str, int, str] | None:
    """Recover fixed-column FECHIDA rows whose long name overlaps age/team."""

    before_seed = tokens[1:-1]
    if len(before_seed) < 2:
        return None
    overlap_index = next(
        (
            index
            for index, token in enumerate(before_seed)
            if any(character.isdigit() for character in token)
        ),
        None,
    )
    if overlap_index is None or overlap_index == 0:
        return None
    prefix = before_seed[:overlap_index]
    overlap_tokens = before_seed[overlap_index:]
    explicit_team = (
        overlap_tokens[-1]
        if len(overlap_tokens) > 1
        and not any(character.islower() for character in overlap_tokens[-1])
        else None
    )
    encoded_tokens = overlap_tokens[:-1] if explicit_team else overlap_tokens
    overlap = "".join(encoded_tokens)
    team = explicit_team or ""
    attached_team = explicit_team is None

    digit_indexes = [index for index, character in enumerate(overlap) if character.isdigit()]
    if len(digit_indexes) < 2:
        return None
    age_indexes = set(digit_indexes[:2])
    age = int("".join(overlap[index] for index in digit_indexes[:2]))
    if age <= 0 or age > 120:
        return None
    first_uppercase = next(
        (index for index, character in enumerate(overlap) if character.isupper()),
        None,
    )
    first_lowercase = next(
        (index for index, character in enumerate(overlap) if character.islower()),
        None,
    )
    first_uppercase_is_name = (
        first_uppercase
        if first_uppercase is not None
        and (first_lowercase is None or first_uppercase < first_lowercase)
        else None
    )
    # La celda de edad se imprime como "W67", y al entrelazarse su marca de
    # genero queda suelta entre las letras del nombre. Pertenece a la edad, no
    # al codigo de equipo: sin descartarla PEMAS se guardaba como WPEMAS y el
    # club perdia esas inscripciones. Es la primera mayuscula candidata a equipo
    # antes del primer digito de la edad; el resto del codigo va despues.
    gender_index = next(
        (
            index
            for index in range(digit_indexes[0] - 1, -1, -1)
            if overlap[index].isupper() and index != first_uppercase_is_name
        ),
        None,
    )
    if gender_index is not None and overlap[gender_index].upper() not in {"M", "W", "X"}:
        gender_index = None
    name_characters: list[str] = []
    team_characters: list[str] = []
    for index, character in enumerate(overlap):
        if index in age_indexes or index == gender_index:
            continue
        if character.islower() or index == first_uppercase_is_name:
            name_characters.append(character)
        elif attached_team:
            team_characters.append(character)
    name_tail = "".join(name_characters)
    if attached_team:
        team = "".join(team_characters)
    if not name_tail or not team:
        return None
    if not prefix:
        return None
    if name_tail[:1].isupper():
        display_name = clean_text(" ".join([*prefix, name_tail]))
    else:
        display_name = clean_text(" ".join([*prefix[:-1], prefix[-1] + name_tail]))
    return display_name, age, team


def _parse_individual_entry(
    line: SourceLine,
    *,
    session_number: int,
    session_name: str,
    event_number: int,
    event_name: str,
    heat_number: int,
    heat_total: int | None,
    estimated_start_time: str | None,
) -> MeetProgramEntry | None:
    tokens = line.text.split()
    if len(tokens) < 4 or not tokens[0].isdigit() or not SEED_RE.fullmatch(tokens[-1]):
        return None
    team_name = tokens[-2]
    name_age = " ".join(tokens[1:-2]).strip()
    age_match = AGE_RE.fullmatch(tokens[-3])
    if age_match:
        display_name = " ".join(tokens[1:-3]).strip()
        age = int(age_match.group("age"))
    else:
        # HY-TEK's fixed age column can overlap long names. PDF extraction then
        # interleaves the remaining name letters with W/M and age digits.
        overlap = OVERLAPPED_NAME_AGE_RE.fullmatch(name_age)
        if overlap:
            tail = overlap.group("tail")
            display_name = clean_text(
                overlap.group("name")
                + "".join(character for character in tail if character.isalpha())
            )
            age_digits = "".join(character for character in tail if character.isdigit())
            if not age_digits:
                return None
            age = int(age_digits)
        else:
            # FECHIDA's narrow fixed columns can interleave lowercase name
            # letters, age digits, and uppercase team code in one PDF token.
            repaired = _parse_fechida_overlap(tokens)
            if repaired is None:
                return None
            display_name, age, team_name = repaired
    if not display_name:
        return None
    display_name = clean_athlete_name(display_name) or ""
    team_name = clean_extracted_text(team_name)
    seed = tokens[-1]
    return MeetProgramEntry(
        session_number=session_number,
        session_name=session_name,
        event_number=event_number,
        event_name=event_name,
        heat_number=heat_number,
        heat_total=heat_total,
        estimated_start_time=estimated_start_time,
        lane=int(tokens[0]),
        display_name=display_name,
        age=age,
        team_name=team_name,
        seed_time_text=seed,
        seed_time_ms=seed_time_ms(seed),
        entry_type="individual",
        relay_members=[],
        page_number=line.page_number,
        column_number=line.column_number,
        line_number=line.line_number,
    )


def _parse_relay_entry(
    line: SourceLine,
    *,
    session_number: int,
    session_name: str,
    event_number: int,
    event_name: str,
    heat_number: int,
    heat_total: int | None,
    estimated_start_time: str | None,
) -> MeetProgramEntry | None:
    tokens = line.text.split()
    if len(tokens) < 3 or not tokens[0].isdigit() or not SEED_RE.fullmatch(tokens[-1]):
        return None
    display_name = " ".join(tokens[1:-1]).strip()
    if not display_name:
        return None
    # "SDEPO X240 E" rotula equipo, edad sumada y letra de posta: no es nombre
    # de persona. clean_athlete_name lo trataria como tal y ademas de pegar la
    # letra al codigo de edad ("X240E"), consultaria el diccionario canonico de
    # apellidos de forma difusa sobre rotulos que no lo son.
    display_name = clean_extracted_text(display_name) or ""
    team_name = clean_extracted_text(tokens[1])
    seed = tokens[-1]
    return MeetProgramEntry(
        session_number=session_number,
        session_name=session_name,
        event_number=event_number,
        event_name=event_name,
        heat_number=heat_number,
        heat_total=heat_total,
        estimated_start_time=estimated_start_time,
        lane=int(tokens[0]),
        display_name=display_name,
        age=None,
        team_name=team_name,
        seed_time_text=seed,
        seed_time_ms=seed_time_ms(seed),
        entry_type="relay",
        relay_members=[],
        page_number=line.page_number,
        column_number=line.column_number,
        line_number=line.line_number,
    )


def parse_relay_member_names(text: str) -> list[str]:
    for pattern in (GENDER_MEMBER_RE, AGE_MEMBER_RE):
        names = [
            clean_athlete_name(match.group("name")) or ""
            for match in pattern.finditer(text)
        ]
        if len(names) == 2 and all("," in name for name in names):
            return names
    return []


def _is_skippable(text: str) -> bool:
    return any(pattern.fullmatch(text) for pattern in SKIP_PATTERNS)


def parse_source_lines(lines: Iterable[SourceLine]) -> ParsedMeetProgram:
    materialized_lines = list(lines)
    entries: list[MeetProgramEntry] = []
    unparsed: list[DebugLine] = []
    session_by_page: dict[int, str] = {}
    for source_line in materialized_lines:
        match = SESSION_RE.fullmatch(clean_text(source_line.text))
        if match:
            session_by_page.setdefault(
                source_line.page_number,
                clean_athlete_name(match.group("name")) or "",
            )
    ordered_session_names = list(dict.fromkeys(session_by_page.values()))
    if ordered_session_names:
        known_sessions = {
            name: index for index, name in enumerate(ordered_session_names, start=1)
        }
        session_name = ordered_session_names[0]
    else:
        session_name = "Jornada Unica"
        known_sessions = {session_name: 1}
    session_number = known_sessions[session_name]
    active_page: int | None = None
    canonical_events: dict[tuple[int, int], str] = {}
    event_number: int | None = None
    event_name: str | None = None
    entry_type: str | None = None
    heat_number: int | None = None
    heat_total: int | None = None
    estimated_start_time: str | None = None
    active_relay: MeetProgramEntry | None = None

    for line in materialized_lines:
        if line.page_number != active_page:
            active_page = line.page_number
            page_session = session_by_page.get(active_page)
            if page_session:
                session_name = page_session
                session_number = known_sessions[page_session]
        text = clean_text(line.text)
        session_match = SESSION_RE.fullmatch(text)
        if session_match:
            candidate = clean_extracted_text(session_match.group("name")) or ""
            if candidate not in known_sessions:
                known_sessions[candidate] = len(known_sessions) + 1
            session_name = candidate
            session_number = known_sessions[candidate]
            continue

        continuation = CONTINUATION_RE.fullmatch(text)
        if continuation:
            event_number = int(continuation.group("number"))
            candidate_name, _ = _event_parts(continuation.group("name"))
            event_key = (session_number, event_number)
            event_name = canonical_events.setdefault(event_key, candidate_name)
            _, entry_type = _event_parts(event_name)
            heat_number = int(continuation.group("heat"))
            heat_total = None
            estimated_start_time = None
            active_relay = None
            continue

        event_match = EVENT_RE.fullmatch(text)
        if event_match:
            event_number = int(event_match.group("number"))
            candidate_name, _ = _event_parts(event_match.group("name"))
            event_key = (session_number, event_number)
            known_name = canonical_events.get(event_key, "")
            event_name = max((known_name, candidate_name), key=len)
            canonical_events[event_key] = event_name
            _, entry_type = _event_parts(event_name)
            heat_number = None
            heat_total = None
            estimated_start_time = None
            active_relay = None
            continue

        heat_match = HEAT_RE.search(text)
        if heat_match:
            heat_number = int(heat_match.group("number"))
            heat_total = int(heat_match.group("total")) if heat_match.group("total") else None
            # HY-TEK prints the estimated start on the heat header, not on lane rows.
            estimated_start_time = normalize_estimated_start_time(
                heat_match.group("estimated_time")
            )
            active_relay = None
            continue

        if _is_skippable(text):
            continue

        if event_number and event_name and heat_number and text[:1].isdigit():
            common = {
                "session_number": session_number,
                "session_name": session_name,
                "event_number": event_number,
                "event_name": event_name,
                "heat_number": heat_number,
                "heat_total": heat_total,
                "estimated_start_time": estimated_start_time,
            }
            if entry_type == "relay":
                entry = _parse_relay_entry(line, **common)
            else:
                entry = _parse_individual_entry(line, **common)
            if entry:
                entries.append(entry)
                active_relay = entry if entry.entry_type == "relay" else None
                continue

        if active_relay is not None and len(active_relay.relay_members) < 4:
            members = parse_relay_member_names(text)
            if members and len(active_relay.relay_members) + len(members) <= 4:
                active_relay.relay_members.extend(members)
                continue

        if text.startswith(("#", "Heat")) or text[:1].isdigit():
            unparsed.append(
                DebugLine(
                    line.page_number,
                    line.column_number,
                    line.line_number,
                    line.text,
                    "relevant_line_not_parsed",
                )
            )

    return ParsedMeetProgram(entries=entries, unparsed=unparsed)


def parse_pdf(pdf_path: Path) -> ParsedMeetProgram:
    if not pdf_path.exists() or not pdf_path.is_file():
        raise MeetProgramError(f"PDF not found: {pdf_path}")
    lines, word_count = extract_source_lines(pdf_path)
    parsed = parse_source_lines(lines)
    session_name = parsed.entries[0].session_name if parsed.entries else "Jornada Unica"
    stage_number, pool_role = infer_program_segment(session_name)
    header_metadata = extract_competition_header_metadata(lines)
    start_date = header_metadata.get("source_competition_start_date")
    end_date = header_metadata.get("source_competition_end_date")
    parsed.metadata = {
        "pdf_name": pdf_path.name,
        "pdf_path": str(pdf_path.resolve()),
        "pdf_sha256": sha256_file(pdf_path),
        "parser_version": PARSER_VERSION,
        "text_word_count": word_count,
        "page_count": max((line.page_number for line in lines), default=0),
        "stage_number": stage_number,
        "pool_role": pool_role,
        "scheduled_date": start_date if start_date == end_date else None,
        **header_metadata,
    }
    return parsed


# --- Export CSV de HY-TEK Meet Manager -------------------------------------
# El reporte "Programa de Competencias" tambien se exporta como CSV. Es el mismo
# reporte, no una tabla: cada fila repite el encabezado completo y lleva una sola
# inscripcion en un bloque de celdas. El ancho depende de a cuantas columnas se
# imprima el reporte (2, 3, ...), asi que las posiciones NO son fijas y se anclan
# a la etiqueta "Carril", que precede al bloque de datos en toda fila.
CSV_ENCODINGS = ("utf-8-sig", "cp1252")
CSV_LANE_LABEL = "carril"
CSV_RELAY_LABEL = "relevo"
CSV_LABEL_WIDTH = 5
CSV_RELAY_MEMBER_SLOTS = 4
CSV_HEAT_RE = re.compile(r"^Serie\s+(?P<heat>\d+)\b", re.IGNORECASE)
CSV_HEAT_TOTAL_RE = re.compile(r"\bof\s+(?P<total>\d+)\b", re.IGNORECASE)
CSV_HEAT_TIME_RE = re.compile(
    r"Inicia a las\s+(?P<time>\d{1,2}:[0-5]\d\s*[AP]M)", re.IGNORECASE
)
# Edad viene con el genero pegado: "M24", "W64". En relevos es la edad sumada del
# equipo ("X240"), que no es la edad de un nadador y por eso no se persiste.
CSV_AGE_RE = re.compile(r"^(?P<gender>[MWX])(?P<age>\d+)$", re.IGNORECASE)
CSV_MEMBER_AGE_RE = re.compile(r"\s+[MWX]\d{1,3}\s*$", re.IGNORECASE)


def _decode_csv(path: Path) -> str:
    raw = path.read_bytes()
    for encoding in CSV_ENCODINGS:
        try:
            return raw.decode(encoding)
        except UnicodeDecodeError:
            continue
    raise MeetProgramError(f"Unsupported CSV encoding: {path}")


def _csv_anchor(row: list[str]) -> int | None:
    for index, cell in enumerate(row):
        if clean_text(cell).casefold() == CSV_LANE_LABEL:
            return index
    return None


def _csv_cell(row: list[str], index: int) -> str:
    return clean_text(row[index]) if 0 <= index < len(row) else ""


def _csv_relay_members(row: list[str], anchor: int) -> list[str]:
    first = anchor + CSV_LABEL_WIDTH + 7
    members = [
        _csv_cell(row, first + offset) for offset in range(CSV_RELAY_MEMBER_SLOTS)
    ]
    # El export pega genero y edad al integrante ("Mora, Ivonne W53"). Se
    # descartan para que el campo signifique lo mismo que en la ruta PDF, y el
    # nombre pasa por la misma curaduria compartida con resultados.
    return [
        clean_athlete_name(CSV_MEMBER_AGE_RE.sub("", member)) or ""
        for member in members
        if member
    ]


def parse_meet_manager_csv(csv_path: Path) -> ParsedMeetProgram:
    if not csv_path.exists() or not csv_path.is_file():
        raise MeetProgramError(f"CSV not found: {csv_path}")
    rows = list(csv.reader(io.StringIO(_decode_csv(csv_path))))
    entries: list[MeetProgramEntry] = []
    unparsed: list[DebugLine] = []
    header_lines: list[SourceLine] = []
    header_seen: set[str] = set()
    word_count = 0
    # La hora de inicio esta corrida una fila: la primera fila de cada serie trae
    # su hora real y las siguientes traen la de la serie que viene. Se toma la
    # primera aparicion de cada (evento, serie) y se ignoran las posteriores.
    seen_heats: set[tuple[int, int]] = set()

    for line_number, row in enumerate(rows, start=1):
        word_count += sum(len(clean_text(cell).split()) for cell in row)
        anchor = _csv_anchor(row)
        if anchor is None:
            unparsed.append(DebugLine(1, 1, line_number, ";".join(row)[:500], "missing_lane_label"))
            continue
        for cell in row[:anchor]:
            text = clean_text(cell)
            if not text or text in header_seen:
                continue
            header_seen.add(text)
            header_lines.append(SourceLine(1, 1, line_number, text))

        event_cell = next(
            (clean_text(cell) for cell in row[:anchor] if EVENT_RE.match(clean_text(cell))),
            "",
        )
        event_match = EVENT_RE.match(event_cell)
        heat_cell = _csv_cell(row, anchor + CSV_LABEL_WIDTH)
        heat_match = CSV_HEAT_RE.match(heat_cell)
        lane_cell = _csv_cell(row, anchor + CSV_LABEL_WIDTH + 1)
        if not event_match or not heat_match or not lane_cell.isdigit():
            unparsed.append(
                DebugLine(1, 1, line_number, ";".join(row)[:500], "incomplete_entry_row")
            )
            continue

        event_number = int(event_match.group("number"))
        heat_number = int(heat_match.group("heat"))
        total_match = CSV_HEAT_TOTAL_RE.search(heat_cell)
        time_match = CSV_HEAT_TIME_RE.search(heat_cell)
        is_relay = (
            _csv_cell(row, anchor + 3).casefold() == CSV_RELAY_LABEL
            or bool(_csv_relay_members(row, anchor))
        )
        name = _csv_cell(row, anchor + CSV_LABEL_WIDTH + 2)
        age_match = CSV_AGE_RE.match(_csv_cell(row, anchor + CSV_LABEL_WIDTH + 3))
        team = _csv_cell(row, anchor + CSV_LABEL_WIDTH + 4)
        seed_text = _csv_cell(row, anchor + CSV_LABEL_WIDTH + 5) or None
        # En relevos las celdas corren: el nombre es el club y el "equipo" es la
        # letra del relevo (A, B, ...). Se muestran juntos como en el programa.
        display_name = (
            clean_extracted_text(f"{name} {team}") if is_relay and team
            else clean_athlete_name(name)
        ) or ""
        heat_key = (event_number, heat_number)
        estimated = (
            normalize_estimated_start_time(time_match.group("time"))
            if time_match and heat_key not in seen_heats
            else None
        )
        seen_heats.add(heat_key)

        entries.append(
            MeetProgramEntry(
                session_number=1,
                session_name="Jornada Unica",
                event_number=event_number,
                event_name=clean_text(event_match.group("name")),
                heat_number=heat_number,
                heat_total=int(total_match.group("total")) if total_match else None,
                lane=int(lane_cell),
                display_name=display_name,
                age=None if is_relay or not age_match else int(age_match.group("age")),
                team_name=(name if is_relay else team) or None,
                seed_time_text=seed_text,
                seed_time_ms=seed_time_ms(seed_text),
                entry_type="relay" if is_relay else "individual",
                relay_members=_csv_relay_members(row, anchor) if is_relay else [],
                page_number=1,
                column_number=1,
                line_number=line_number,
                estimated_start_time=estimated,
            )
        )

    session_name = entries[0].session_name if entries else "Jornada Unica"
    stage_number, pool_role = infer_program_segment(session_name)
    header_metadata = extract_competition_header_metadata(header_lines)
    start_date = header_metadata.get("source_competition_start_date")
    end_date = header_metadata.get("source_competition_end_date")
    return ParsedMeetProgram(
        entries=entries,
        unparsed=unparsed,
        metadata={
            # `pdf_*` conserva su nombre porque es la identidad ya ligada de los
            # artefactos existentes; `source_kind` distingue el formato real.
            "source_kind": "meet_manager_csv",
            "pdf_name": csv_path.name,
            "pdf_path": str(csv_path.resolve()),
            "pdf_sha256": sha256_file(csv_path),
            "parser_version": PARSER_VERSION,
            "text_word_count": word_count,
            "page_count": 1,
            "stage_number": stage_number,
            "pool_role": pool_role,
            "scheduled_date": start_date if start_date == end_date else None,
            **header_metadata,
        },
    )


def _header_metadata_issues(metadata: dict[str, Any]) -> list[ValidationIssue]:
    if metadata.get("source_competition_header_conflict") is True:
        return [
            ValidationIssue(
                "error",
                "conflicting_competition_headers",
                "The PDF contains multiple distinct competition headers.",
            )
        ]
    if not clean_text(str(metadata.get("source_competition_name") or "")):
        return [
            ValidationIssue(
                "error",
                "missing_competition_header",
                "A competition header could not be extracted from the PDF.",
            )
        ]
    if not metadata.get("source_competition_start_date") or not metadata.get(
        "source_competition_end_date"
    ):
        return [
            ValidationIssue(
                "error",
                "missing_competition_header_date",
                "The PDF competition header requires a complete date range.",
            )
        ]
    return []


def _segment_metadata_issues(metadata: dict[str, Any]) -> list[ValidationIssue]:
    start_date = str(metadata.get("source_competition_start_date") or "")
    end_date = str(metadata.get("source_competition_end_date") or "")
    scheduled_date = str(metadata.get("scheduled_date") or "")
    if start_date and end_date and start_date != end_date and not scheduled_date:
        return [
            ValidationIssue(
                "error",
                "missing_segment_date",
                "Multi-day meet-program artifacts require scheduled_date.",
            )
        ]
    if scheduled_date and (
        not re.fullmatch(r"\d{4}-\d{2}-\d{2}", scheduled_date)
        or (start_date and scheduled_date < start_date)
        or (end_date and scheduled_date > end_date)
    ):
        return [
            ValidationIssue(
                "error",
                "segment_date_outside_competition",
                "scheduled_date must be an ISO date inside the competition range.",
            )
        ]
    return []


def validate_entries(
    entries: list[MeetProgramEntry],
    *,
    text_word_count: int,
    unparsed_count: int = 0,
) -> list[ValidationIssue]:
    issues: list[ValidationIssue] = []
    if text_word_count <= 0:
        issues.append(
            ValidationIssue(
                "error",
                "image_only_or_no_text",
                "The PDF has no extractable text; OCR is outside the v1 contract.",
            )
        )
    if not entries:
        issues.append(
            ValidationIssue("error", "no_entries_found", "No meet-program entries were parsed.")
        )
    checks = [
        ("session_number", "invalid_session_number"),
        ("event_number", "invalid_event_number"),
        ("heat_number", "invalid_heat_number"),
    ]
    for attribute, key in checks:
        count = sum(
            1
            for entry in entries
            if not isinstance(getattr(entry, attribute), int)
            or getattr(entry, attribute) <= 0
        )
        if count:
            issues.append(ValidationIssue("error", key, f"{attribute} must be positive.", count))
    invalid_lane = sum(
        1 for entry in entries if not isinstance(entry.lane, int) or entry.lane < 0
    )
    if invalid_lane:
        issues.append(
            ValidationIssue("error", "invalid_lane", "lane must be non-negative.", invalid_lane)
        )
    missing_event = sum(1 for entry in entries if not clean_text(entry.event_name))
    if missing_event:
        issues.append(
            ValidationIssue("error", "missing_event_name", "event_name is required.", missing_event)
        )
    unparseable_event = sum(
        1
        for entry in entries
        if clean_text(entry.event_name)
        and parse_hytek_event_identity(entry.event_name) is None
    )
    if unparseable_event:
        issues.append(
            ValidationIssue(
                "error",
                "unparseable_event_identity",
                "event_name must expose a canonical HY-TEK distance and stroke.",
                unparseable_event,
            )
        )
    missing_name = sum(1 for entry in entries if not clean_text(entry.display_name))
    if missing_name:
        issues.append(
            ValidationIssue(
                "error", "missing_display_name", "display_name is required.", missing_name
            )
        )
    invalid_age = sum(1 for entry in entries if entry.age is not None and entry.age <= 0)
    if invalid_age:
        issues.append(ValidationIssue("error", "invalid_age", "age must be positive.", invalid_age))
    invalid_seed = sum(
        1
        for entry in entries
        if entry.seed_time_text
        and not SEED_RE.fullmatch(entry.seed_time_text)
    )
    if invalid_seed:
        issues.append(
            ValidationIssue("error", "invalid_seed_time", "seed_time_text is invalid.", invalid_seed)
        )
    seen: set[tuple[int, int, int, int]] = set()
    duplicate_count = 0
    for entry in entries:
        key = (
            entry.session_number,
            entry.event_number,
            entry.heat_number,
            entry.lane,
        )
        if key in seen:
            duplicate_count += 1
        seen.add(key)
    if duplicate_count:
        issues.append(
            ValidationIssue(
                "error",
                "duplicate_lane_assignment",
                "Duplicate session/event/heat/lane assignment.",
                duplicate_count,
            )
        )
    if unparsed_count:
        issues.append(
            ValidationIssue(
                "error",
                "unparsed_relevant_lines",
                "Relevant event, heat, or lane lines were not parsed.",
                unparsed_count,
            )
        )
    return issues


def _summary(
    input_dir: Path,
    entries: list[MeetProgramEntry],
    metadata: dict[str, Any],
    *,
    unparsed_count: int,
) -> ValidationSummary:
    issues = validate_entries(
        entries,
        text_word_count=int(metadata.get("text_word_count") or 0),
        unparsed_count=unparsed_count,
    )
    issues.extend(_header_metadata_issues(metadata))
    issues.extend(_segment_metadata_issues(metadata))
    state = "requires_review" if any(issue.severity == "error" for issue in issues) else "validated"
    return ValidationSummary(
        state=state,
        input_dir=str(input_dir),
        counts={"entries": len(entries), "debug_unparsed_lines": unparsed_count},
        issues=issues,
        metadata=metadata,
    )


def _entry_csv_row(entry: MeetProgramEntry) -> dict[str, Any]:
    row = asdict(entry)
    row["relay_members"] = json.dumps(entry.relay_members, ensure_ascii=False)
    return row


def _write_csv(path: Path, columns: list[str], rows: Iterable[dict[str, Any]]) -> None:
    temporary = path.with_name(f".{path.name}.tmp")
    with temporary.open("w", encoding="utf-8-sig", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    temporary.replace(path)


def _atomic_write_text(path: Path, content: str) -> None:
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(content, encoding="utf-8")
    temporary.replace(path)


def write_summary(summary: ValidationSummary, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    _atomic_write_text(path, json.dumps(asdict(summary), ensure_ascii=False, indent=2))


def write_artifacts(parsed: ParsedMeetProgram, out_dir: Path) -> ValidationSummary:
    out_dir.mkdir(parents=True, exist_ok=True)
    _write_csv(
        out_dir / "meet_program_entries.csv",
        ENTRY_COLUMNS,
        (_entry_csv_row(entry) for entry in parsed.entries),
    )
    _write_csv(
        out_dir / "debug_unparsed_lines.csv",
        DEBUG_COLUMNS,
        (asdict(line) for line in parsed.unparsed),
    )
    parsed.metadata["artifact_binding"] = {
        name: {"sha256": sha256_file(out_dir / name), "rows": rows}
        for name, rows in (
            ("meet_program_entries.csv", len(parsed.entries)),
            ("debug_unparsed_lines.csv", len(parsed.unparsed)),
        )
    }
    parsed.metadata["artifact_binding"]["artifact_identity"] = {
        "sha256": _artifact_identity_sha256(parsed.metadata)
    }
    _atomic_write_text(
        out_dir / "metadata.json",
        json.dumps(parsed.metadata, ensure_ascii=False, indent=2),
    )
    summary = _summary(
        out_dir,
        parsed.entries,
        parsed.metadata,
        unparsed_count=len(parsed.unparsed),
    )
    write_summary(summary, out_dir / "validation_summary.json")
    return summary


def _parse_optional_int(value: str | None) -> int | None:
    cleaned = (value or "").strip()
    return int(cleaned) if cleaned else None


def _load_entries(path: Path) -> list[MeetProgramEntry]:
    with path.open(encoding="utf-8-sig", newline="") as stream:
        reader = csv.DictReader(stream)
        missing = [column for column in ENTRY_COLUMNS if column not in (reader.fieldnames or [])]
        if missing:
            raise MeetProgramError(f"meet_program_entries.csv missing columns: {missing}")
        entries = []
        for row in reader:
            try:
                members = json.loads(row["relay_members"] or "[]")
                entries.append(
                    MeetProgramEntry(
                        session_number=int(row["session_number"]),
                        session_name=row["session_name"],
                        event_number=int(row["event_number"]),
                        event_name=row["event_name"],
                        heat_number=int(row["heat_number"]),
                        heat_total=_parse_optional_int(row["heat_total"]),
                        estimated_start_time=row["estimated_start_time"] or None,
                        lane=int(row["lane"]),
                        display_name=row["display_name"],
                        age=_parse_optional_int(row["age"]),
                        team_name=row["team_name"] or None,
                        seed_time_text=row["seed_time_text"] or None,
                        seed_time_ms=_parse_optional_int(row["seed_time_ms"]),
                        entry_type=row["entry_type"],
                        relay_members=members,
                        page_number=int(row["page_number"]),
                        column_number=int(row["column_number"]),
                        line_number=int(row["line_number"]),
                    )
                )
            except (TypeError, ValueError, json.JSONDecodeError) as exc:
                raise MeetProgramError(f"Invalid meet-program CSV row: {exc}") from exc
    return entries


def _verify_artifact_binding(input_dir: Path, metadata: dict[str, Any]) -> None:
    binding = metadata.get("artifact_binding")
    if not isinstance(binding, dict):
        raise MeetProgramError("Artifact binding is missing.")
    identity_binding = binding.get("artifact_identity")
    if (
        not isinstance(identity_binding, dict)
        or identity_binding.get("sha256") != _artifact_identity_sha256(metadata)
    ):
        raise MeetProgramError("Artifact identity binding mismatch.")
    for name in ("meet_program_entries.csv", "debug_unparsed_lines.csv"):
        expected = binding.get(name)
        path = input_dir / name
        if not isinstance(expected, dict) or sha256_file(path) != expected.get("sha256"):
            raise MeetProgramError(f"Artifact binding mismatch: {name}")
        with path.open(encoding="utf-8-sig", newline="") as stream:
            row_count = sum(1 for _row in csv.DictReader(stream))
        if row_count != expected.get("rows"):
            raise MeetProgramError(f"Artifact binding row-count mismatch: {name}")


def _artifact_identity_sha256(metadata: dict[str, Any]) -> str:
    identity = {
        key: metadata.get(key)
        for key in (
            "pdf_sha256",
            "parser_version",
            "source_competition_name",
            "source_competition_start_date",
            "source_competition_end_date",
            "source_competition_header_conflict",
        )
    }
    canonical = json.dumps(identity, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def validate_input_dir(input_dir: Path) -> ValidationSummary:
    metadata_path = input_dir / "metadata.json"
    entries_path = input_dir / "meet_program_entries.csv"
    debug_path = input_dir / "debug_unparsed_lines.csv"
    if not metadata_path.exists() or not entries_path.exists() or not debug_path.exists():
        raise MeetProgramError(
            "Input directory requires metadata.json, meet_program_entries.csv, "
            "and debug_unparsed_lines.csv."
        )
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    _verify_artifact_binding(input_dir, metadata)
    entries = _load_entries(entries_path)
    with debug_path.open(encoding="utf-8-sig", newline="") as stream:
        unparsed_count = sum(1 for _row in csv.DictReader(stream))
    summary = _summary(input_dir, entries, metadata, unparsed_count=unparsed_count)
    write_summary(summary, input_dir / "validation_summary.json")
    return summary


def _qualified(schema: str, table: str) -> str:
    if not SCHEMA_RE.fullmatch(schema):
        raise MeetProgramError(f"Invalid schema: {schema}")
    return f"{schema}.{table}"


def _date_text(value: Any) -> str | None:
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        return str(value.isoformat())
    cleaned = str(value).strip()
    return cleaned or None


def validate_competition_identity(
    metadata: dict[str, Any],
    competition: dict[str, Any],
) -> None:
    source_name = clean_text(str(metadata.get("source_competition_name") or ""))
    if not source_name:
        raise MeetProgramError("Competition header missing from PDF metadata.")
    database_name = clean_text(str(competition.get("name") or ""))
    if not competition_names_match(source_name, database_name):
        raise MeetProgramError(
            "Competition name mismatch: "
            f"PDF '{source_name}' does not match database '{database_name}'."
        )

    source_start = _date_text(metadata.get("source_competition_start_date"))
    source_end = _date_text(metadata.get("source_competition_end_date"))
    database_start = _date_text(competition.get("start_date"))
    database_end = _date_text(competition.get("end_date"))
    if not source_start or not source_end:
        raise MeetProgramError("Competition header is missing a complete PDF date range.")
    if not database_start or not database_end:
        raise MeetProgramError("Selected competition is missing a complete database date range.")
    if (source_start, source_end) != (database_start, database_end):
        raise MeetProgramError(
            "Competition date mismatch: "
            f"PDF {source_start} to {source_end}; "
            f"database {database_start} to {database_end}."
        )


def publish_validated_program(
    connection,
    entries: list[MeetProgramEntry],
    metadata: dict[str, Any],
    *,
    competition_id: int,
    source_url: str | None,
    schema: str,
) -> tuple[int, bool]:
    invalid_event_names = sorted(
        {
            entry.event_name
            for entry in entries
            if parse_hytek_event_identity(entry.event_name) is None
        }
    )
    if invalid_event_names:
        raise MeetProgramError(
            "Cannot publish entries with unparseable event identity: "
            + ", ".join(invalid_event_names)
        )
    checksum = str(metadata.get("pdf_sha256") or "")
    if not CHECKSUM_RE.fullmatch(checksum):
        raise MeetProgramError("metadata.json requires a lowercase SHA-256 checksum.")
    parser_version = str(metadata.get("parser_version") or "").strip()
    if not parser_version:
        raise MeetProgramError("metadata.json requires a nonblank parser_version.")
    if competition_id <= 0:
        raise MeetProgramError("competition_id must be positive.")
    try:
        stage_number = int(metadata.get("stage_number") or 1)
    except (TypeError, ValueError) as exc:
        raise MeetProgramError("stage_number must be a positive integer.") from exc
    if stage_number <= 0:
        raise MeetProgramError("stage_number must be a positive integer.")
    pool_role = str(metadata.get("pool_role") or "main").strip().lower()
    if pool_role not in {"main", "competition", "training"}:
        raise MeetProgramError("pool_role must be main, competition, or training.")
    scheduled_date = metadata.get("scheduled_date") or None

    competition_table = _qualified(schema, "competition")
    publication_table = _qualified(schema, "meet_program_publication")
    entry_table = _qualified(schema, "meet_program_entry")
    document_table = _qualified(schema, "source_document")
    with connection.transaction():
        with connection.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT id, source_id, name, start_date, end_date
                FROM {competition_table}
                WHERE id = %s
                FOR UPDATE;
                """,
                (competition_id,),
            )
            competition = cursor.fetchone()
            if not competition:
                raise MeetProgramError(f"Competition not found: {competition_id}")
            source_id = competition[1]
            validate_competition_identity(
                metadata,
                {
                    "name": competition[2],
                    "start_date": competition[3],
                    "end_date": competition[4],
                },
            )

            cursor.execute(
                f"""
                SELECT id
                FROM {publication_table}
                WHERE competition_id = %s
                  AND source_checksum_sha256 = %s
                  AND parser_version = %s;
                """,
                (competition_id, checksum, parser_version),
            )
            existing = cursor.fetchone()
            if existing:
                return int(existing[0]), False

            cursor.execute(
                f"""
                INSERT INTO {document_table} (
                    source_id, document_name, document_type, source_url,
                    storage_path, checksum_sha256, parser_version, metadata
                )
                VALUES (%s, %s, 'meet_program_pdf', NULL, %s, %s, %s, %s::jsonb)
                ON CONFLICT (checksum_sha256) WHERE checksum_sha256 IS NOT NULL
                DO UPDATE SET last_seen_at = NOW()
                RETURNING id;
                """,
                (
                    source_id,
                    metadata.get("pdf_name") or "meet-program.pdf",
                    metadata.get("pdf_path"),
                    checksum,
                    parser_version,
                    json.dumps(metadata, ensure_ascii=False),
                ),
            )
            source_document_id = int(cursor.fetchone()[0])

            cursor.execute(
                f"""
                INSERT INTO {publication_table} (
                    competition_id, stage_number, pool_role, scheduled_date,
                    source_document_id, source_checksum_sha256,
                    source_url, parser_version, status, metadata
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'pending', %s::jsonb)
                RETURNING id;
                """,
                (
                    competition_id,
                    stage_number,
                    pool_role,
                    scheduled_date,
                    source_document_id,
                    checksum,
                    source_url,
                    parser_version,
                    json.dumps(metadata, ensure_ascii=False),
                ),
            )
            publication_id = int(cursor.fetchone()[0])

            cursor.executemany(
                f"""
                INSERT INTO {entry_table} (
                    publication_id, session_number, session_name,
                    event_number, event_name, heat_number, heat_total,
                    estimated_start_time, lane, display_name, age, team_name, seed_time_text,
                    seed_time_ms, entry_type, relay_members,
                    page_number, column_number, line_number
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s
                );
                """,
                [
                    (
                        publication_id,
                        entry.session_number,
                        entry.session_name,
                        entry.event_number,
                        entry.event_name,
                        entry.heat_number,
                        entry.heat_total,
                        entry.estimated_start_time,
                        entry.lane,
                        entry.display_name,
                        entry.age,
                        entry.team_name,
                        entry.seed_time_text,
                        entry.seed_time_ms,
                        entry.entry_type,
                        json.dumps(entry.relay_members, ensure_ascii=False),
                        entry.page_number,
                        entry.column_number,
                        entry.line_number,
                    )
                    for entry in entries
                ],
            )
            # Entries are complete before the public pointer changes. Any later
            # failure rolls the whole transaction back to the previous version.
            cursor.execute(
                f"""
                UPDATE {publication_table}
                SET status = 'superseded', superseded_at = NOW()
                WHERE competition_id = %s
                  AND stage_number = %s
                  AND pool_role = %s
                  AND status = 'published';
                """,
                (competition_id, stage_number, pool_role),
            )
            cursor.execute(
                f"""
                UPDATE {publication_table}
                SET status = 'published', published_at = NOW()
                WHERE id = %s;
                """,
                (publication_id,),
            )
    return publication_id, True


def connect_database(args):
    try:
        import psycopg
    except ImportError as exc:  # pragma: no cover
        raise MeetProgramError("psycopg is required for --publish.") from exc
    return psycopg.connect(
        host=args.host,
        port=args.port,
        dbname=args.dbname,
        user=args.user,
        password=args.password,
    )


def publish_from_artifacts(
    input_dir: Path,
    *,
    competition_id: int,
    source_url: str | None,
    args,
) -> tuple[int, bool]:
    summary_path = input_dir / "validation_summary.json"
    if not summary_path.exists():
        raise MeetProgramError("Publishing requires validation_summary.json.")
    recorded = json.loads(summary_path.read_text(encoding="utf-8"))
    if recorded.get("state") != "validated":
        raise MeetProgramError("Publishing requires validated artifacts.")
    summary = validate_input_dir(input_dir)
    if summary.state != "validated":
        raise MeetProgramError("Artifacts no longer satisfy the validated contract.")
    if not str(summary.metadata.get("parser_version") or "").strip():
        raise MeetProgramError("Publishing requires a nonblank artifact parser_version.")
    entries = _load_entries(input_dir / "meet_program_entries.csv")
    connection = connect_database(args)
    try:
        return publish_validated_program(
            connection,
            entries,
            summary.metadata,
            competition_id=competition_id,
            source_url=source_url,
            schema=args.schema,
        )
    finally:
        connection.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Parse, validate, and optionally publish HY-TEK meet programs."
    )
    inputs = parser.add_mutually_exclusive_group(required=True)
    inputs.add_argument("--pdf", help="Text-based Meet Program PDF to parse.")
    inputs.add_argument("--csv", help="HY-TEK Meet Manager program CSV export to parse.")
    inputs.add_argument("--input-dir", help="Existing meet-program artifact directory.")
    parser.add_argument("--out-dir", help="Artifact directory required with --pdf or --csv.")
    parser.add_argument("--competition-id", type=int)
    parser.add_argument("--source-url")
    parser.add_argument("--stage-number", type=int)
    parser.add_argument(
        "--pool-role", choices=("main", "competition", "training")
    )
    parser.add_argument("--scheduled-date")
    parser.add_argument("--publish", action="store_true")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--summary-json")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=5432)
    parser.add_argument("--dbname", default="natacion_chile")
    parser.add_argument("--user")
    parser.add_argument("--password")
    parser.add_argument("--schema", default="core")
    args = parser.parse_args()
    if (args.pdf or args.csv) and not args.out_dir:
        parser.error("--out-dir is required with --pdf or --csv")
    if args.publish and not args.competition_id:
        parser.error("--competition-id is required with --publish")
    if args.publish and (not args.user or not args.password):
        parser.error("--user and --password are required with --publish")
    return args


def main() -> None:
    args = parse_args()
    try:
        if args.pdf or args.csv:
            input_dir = Path(args.out_dir)
            parsed = (
                parse_meet_manager_csv(Path(args.csv)) if args.csv
                else parse_pdf(Path(args.pdf))
            )
            if args.competition_id:
                parsed.metadata["competition_id"] = args.competition_id
            if args.source_url:
                parsed.metadata["source_url"] = args.source_url
            if args.stage_number is not None:
                if args.stage_number <= 0:
                    raise MeetProgramError("stage_number must be positive.")
                parsed.metadata["stage_number"] = args.stage_number
            if args.pool_role:
                parsed.metadata["pool_role"] = args.pool_role
            if args.scheduled_date:
                parsed.metadata["scheduled_date"] = args.scheduled_date
            summary = write_artifacts(parsed, input_dir)
        else:
            input_dir = Path(args.input_dir)
            summary = validate_input_dir(input_dir)

        if args.publish:
            publication_id, created = publish_from_artifacts(
                input_dir,
                competition_id=args.competition_id,
                source_url=args.source_url,
                args=args,
            )
            summary.publication_id = publication_id
            summary.publication_created = created

        if args.summary_json:
            write_summary(summary, Path(args.summary_json))
        if args.json:
            print(json.dumps(asdict(summary), ensure_ascii=False, indent=2))
        else:
            print(f"State: {summary.state}")
            print(f"Entries: {summary.counts.get('entries', 0)}")
            for issue in summary.issues:
                print(f"[{issue.severity}] {issue.issue_key}: {issue.message} ({issue.count})")
            if summary.publication_id is not None:
                action = "created" if summary.publication_created else "unchanged"
                print(f"Publication: {summary.publication_id} ({action})")
        if summary.state != "validated":
            raise SystemExit(1)
    except (MeetProgramError, json.JSONDecodeError) as exc:
        raise SystemExit(f"[ERROR] {exc}") from exc


if __name__ == "__main__":
    main()
