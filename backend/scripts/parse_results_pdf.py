#!/usr/bin/env python3
# parser v0.1.5
from __future__ import annotations

import argparse
from difflib import SequenceMatcher
import json
import re
import sys
from dataclasses import dataclass, asdict
import unicodedata
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd

try:
    import pdfplumber
except ImportError as exc:  # pragma: no cover
    raise SystemExit("[ERROR] Falta pdfplumber. Instálalo con: pip install pdfplumber openpyxl") from exc


STATUS_VALUES = {"valid", "dns", "dnf", "dsq", "scratch", "unknown"}
TEXT_STATUSES = {"DNS", "DNF", "DSQ", "SCRATCH", "NT", "NS", "DQ", "VALID", "UNKNOWN"}
TIME_OR_STATUS_PATTERN = (
    r"(?:X)?(?:\d{1,2}:\d{2}:\d{2}(?:[\.,]\d+)?|\d{1,3}:\d{2}(?:[\.,]\d+)?|\d{1,3}(?:[\.,]\d+)?|NT|NS|DNS|DNF|DQ|DSQ)"
)

EVENT_HEADER_RE = re.compile(
    r"^\(?Event\s+(?P<event_number>\d+)\s+(?P<gender>Women|Men|Mixed)\s+(?P<age_group>.+?)\s+(?P<distance_raw>\d+(?:x\d+)?)\s+(?P<course>LC|SC)\s+Meter\s+(?P<stroke>.+?)\)?$",
    re.IGNORECASE,
)

RESULT_LINE_RE = re.compile(
    rf"^(?P<rank>\*?\d+|---)\s+(?P<name>.+?)\s+(?P<age>\d{{1,3}})\s+(?P<team>.+?)\s+(?P<seed>{TIME_OR_STATUS_PATTERN})\s+(?P<final>{TIME_OR_STATUS_PATTERN})(?:\s+(?P<points>\d+(?:[\.,]\s*\d+)?))?$",
    re.IGNORECASE,
)

RELAY_TEAM_RE = re.compile(
    rf"^(?P<rank>\*?\d+|---)\s+(?P<team>.+?)\s+(?:(?P<seed>{TIME_OR_STATUS_PATTERN})\s+)?(?P<final>{TIME_OR_STATUS_PATTERN})(?:\s+(?P<points>\d+(?:[\.,]\s*\d+)?))?$",
    re.IGNORECASE,
)

RELAY_SWIMMER_STUCK_LEG_RE = re.compile(
    r"(?P<age_marker>[WM](?:1[8-9]|[2-9]\d|10\d))(?=[1-4]\))",
    re.IGNORECASE,
)

RELAY_SWIMMER_LEG_MARKER_RE = re.compile(
    r"(?<![A-Za-z0-9])(?P<leg>[1-4])(?:[A-Z])?(?:\)\.?\s*|\s+)",
    re.IGNORECASE,
)

RELAY_SWIMMER_SEGMENT_RE = re.compile(
    r"^(?P<name>.+?)(?:\s+(?P<gender>[WM])(?P<age>\d{1,3})\)?)?$",
    re.IGNORECASE,
)

HEADER_SKIP_PATTERNS = [
    re.compile(r"HY-TEK'?S MEET MANAGER", re.IGNORECASE),
    re.compile(r"^Results\s*$", re.IGNORECASE),
    re.compile(r"^Results\s*-", re.IGNORECASE),
    re.compile(r"^Name\s+Age\s+Team\s+Seed\s+Time\s+Finals\s+Time(?:\s+Points)?$", re.IGNORECASE),
    re.compile(r"^Estadio ", re.IGNORECASE),
    re.compile(r"^Page\s+\d+$", re.IGNORECASE),
    re.compile(r"^.+\s+-\s+\d{1,2}[-/]\d{1,2}[-/]\d{4}$", re.IGNORECASE),
]


@dataclass
class EventContext:
    event_number: int
    gender: str
    age_group: str
    distance_label: str
    distance_m: int
    course_code: str
    stroke: str

    @property
    def event_name(self) -> str:
        return f"{self.gender} {self.age_group} {self.distance_label} {self.course_code} Meter {self.stroke}"

    @property
    def is_relay(self) -> bool:
        return "relay" in self.stroke.lower() or "relay" in self.event_name.lower()


@dataclass
class ParsedResultRow:
    page_number: int
    line_number: int
    event_number: int
    event_name: str
    athlete_name: str
    age_at_event: Optional[int]
    birth_year_estimated: Optional[int]
    club_name: str
    rank_position: Optional[str]
    seed_time_text: Optional[str]
    seed_time_ms: Optional[str]
    result_time_text: Optional[str]
    result_time_ms: Optional[str]
    status: Optional[str]
    points: Optional[str]
    raw_line: str


@dataclass
class ParsedRelayTeamRow:
    page_number: int
    line_number: int
    event_number: int
    event_name: str
    relay_team_name: str
    rank_position: Optional[str]
    seed_time_text: Optional[str]
    seed_time_ms: Optional[str]
    result_time_text: Optional[str]
    result_time_ms: Optional[str]
    status: Optional[str]
    points: Optional[str]
    raw_line: str


@dataclass
class ParsedRelaySwimmerRow:
    page_number: int
    line_number: int
    event_number: int
    event_name: str
    relay_team_name: Optional[str]
    leg_order: int
    swimmer_name: str
    gender: Optional[str]
    age_at_event: Optional[int]
    birth_year_estimated: Optional[int]
    raw_line: str


@dataclass
class ParseStats:
    pages_read: int = 0
    event_headers_found: int = 0
    result_rows_found: int = 0
    relay_team_rows_found: int = 0
    relay_swimmer_rows_found: int = 0
    lines_skipped: int = 0
    lines_unparsed: int = 0


def normalize_string(value):
    if value is None:
        return None
    if isinstance(value, str):
        value = value.strip()
        return value if value != "" else None
    return value


ACCENTED = "ÁÉÍÓÚáéíóúÑñÜü"

def clean_extracted_text(value: str | None) -> str | None:
    if value is None:
        return None

    value = unicodedata.normalize("NFC", str(value))

    # arreglos simples ya detectados
    replacements = {
        "NÑ": "Ñ",
        "nñ": "ñ",
        "Penñ": "Peñ",
        "Munñ": "Muñ",
        "Espanñ": "Españ",
        "Canñ": "Cañ",
        "Ñ u": "Ñu",
        "ñ u": "ñu",
        "Ñ a": "Ña",
        "ñ a": "ña",
        "Ñ o": "Ño",
        "ñ o": "ño",
        "Ñ e": "Ñe",
        "ñ e": "ñe",
        "Ñ i": "Ñi",
        "ñ i": "ñi",
        "Joseí": "José"
    }
    for bad, good in replacements.items():
        value = value.replace(bad, good)

    # Corrige artefactos frecuentes de tildes mal extraídas en nombres propios
    value = re.sub(r"oí(?=[bcdfghjklmnñpqrstvwxyzBCDFGHJKLMNÑPQRSTVWXYZ])", "ó", value)
    value = re.sub(r"aí(?=[bcdfghjklmnñpqrstvwxyzBCDFGHJKLMNÑPQRSTVWXYZ])", "á", value)
    value = re.sub(r"eí(?=[bcdfghjklmnñpqrstvwxyzBCDFGHJKLMNÑPQRSTVWXYZ])", "é", value)

    # Variante con espacio artificial: "Andre ís" -> "Andrés"
    value = re.sub(r"o\s+í(?=[bcdfghjklmnñpqrstvwxyzBCDFGHJKLMNÑPQRSTVWXYZ])", "ó", value)
    value = re.sub(r"a\s+í(?=[bcdfghjklmnñpqrstvwxyzBCDFGHJKLMNÑPQRSTVWXYZ])", "á", value)
    value = re.sub(r"e\s+í(?=[bcdfghjklmnñpqrstvwxyzBCDFGHJKLMNÑPQRSTVWXYZ])", "é", value)

    # corrige duplicación frecuente tipo "Rocíío" -> "Rocío"
    value = re.sub(r"íi", "í", value)
    value = re.sub(r"íí", "í", value)
    value = re.sub(r"ÍI", "Í", value)
    value = re.sub(r"áa", "á", value)
    value = re.sub(r"ée", "é", value)
    value = re.sub(r"óo", "ó", value)
    value = re.sub(r"úu", "ú", value)
    value = re.sub(r"ÁA", "Á", value)
    value = re.sub(r"ÉE", "É", value)
    value = re.sub(r"ÓO", "Ó", value)
    value = re.sub(r"ÚU", "Ú", value)

    # corrige vocal acentuada suelta al final de palabra anterior:
    # "Alumine í" -> "Aluminé"
    value = re.sub(r"([A-Za-zÑñ])e\s+í\b", r"\1é", value)
    value = re.sub(r"([A-Za-zÑñ])o\s+í\b", r"\1ó", value)

    # espacios múltiples
    value = re.sub(r"\s+", " ", value).strip()

    return value if value else None


def normalize_event_gender(value: Optional[str]) -> Optional[str]:
    value = normalize_string(value)
    if value is None:
        return None
    key = value.lower()
    mapping = {
        "women": "women",
        "woman": "women",
        "female": "women",
        "f": "women",
        "men": "men",
        "man": "men",
        "male": "men",
        "m": "men",
        "mixed": "mixed",
        "mix": "mixed",
        "mixto": "mixed",
    }
    return mapping.get(key, key)


def normalize_athlete_gender(value: Optional[str]) -> Optional[str]:
    value = normalize_string(value)
    if value is None:
        return None
    key = value.lower()
    mapping = {
        "women": "female",
        "woman": "female",
        "female": "female",
        "f": "female",
        "w": "female",
        "men": "male",
        "man": "male",
        "male": "male",
        "m": "male",
    }
    return mapping.get(key, key)


def normalize_stroke(value: Optional[str]) -> Optional[str]:
    value = normalize_string(value)
    if value is None:
        return None
    key = value.lower().replace('-', ' ').replace('_', ' ')
    key = re.sub(r"\s+", " ", key)
    mapping = {
        "free": "freestyle",
        "freestyle": "freestyle",
        "back": "backstroke",
        "backstroke": "backstroke",
        "breast": "breaststroke",
        "breaststroke": "breaststroke",
        "fly": "butterfly",
        "butterfly": "butterfly",
        "im": "individual_medley",
        "individual medley": "individual_medley",
        "medley": "individual_medley",
        "medley relay": "medley_relay",
        "freestyle relay": "freestyle_relay",
        "free relay": "freestyle_relay",
    }
    return mapping.get(key, key.replace(' ', '_'))


def derive_competition_year(metadata: Dict[str, Optional[str]], pdf_path: Optional[Path] = None) -> Optional[int]:
    candidates = [metadata.get("competition_name"), metadata.get("results_label")]
    if pdf_path is not None:
        candidates.append(pdf_path.name)
    for candidate in candidates:
        if not candidate:
            continue
        years = re.findall(r"(19\d{2}|20\d{2}|21\d{2})", str(candidate))
        if years:
            return int(years[-1])
    return None


def parse_distance_to_meters(distance_raw: Optional[str]) -> Optional[int]:
    distance_raw = normalize_string(distance_raw)
    if distance_raw is None:
        return None
    m = re.fullmatch(r"(\d+)x(\d+)", distance_raw.lower())
    if m:
        return int(m.group(1)) * int(m.group(2))
    if distance_raw.isdigit():
        return int(distance_raw)
    return None


def info(msg: str) -> None:
    print(f"[INFO] {msg}")


def warn(msg: str) -> None:
    print(f"[WARN] {msg}")


def fail(msg: str) -> None:
    print(f"[ERROR] {msg}", file=sys.stderr)
    raise SystemExit(1)


def normalize_string(x):
    if x is None:
        return None
    if isinstance(x, str):
        x = x.strip()
        return x if x != "" else None
    return x


def normalize_controlled_lower(x):
    x = normalize_string(x)
    return x.lower() if isinstance(x, str) else x


def normalize_swim_time_text(value):
    value = normalize_string(value)
    if value is None:
        return None

    upper = value.upper()
    if upper in TEXT_STATUSES:
        return upper

    is_x = upper.startswith("X")
    raw = value

    base = value[1:].strip() if is_x else value
    base = base.replace(",", ".")

    m = re.fullmatch(r"(\d{1,2}):(\d{2}):(\d{2})(?:\.(\d+))?", base)
    if m:
        hours = int(m.group(1))
        minutes = int(m.group(2))
        seconds = int(m.group(3))
        frac = (m.group(4) or "0")
        centis = int((frac + "00")[:2])
        total_minutes = hours * 60 + minutes
        normalized = f"{total_minutes}:{seconds:02d},{centis:02d}" if total_minutes > 0 else f"{seconds},{centis:02d}"
        return f"X{normalized}" if is_x else normalized

    m = re.fullmatch(r"(\d{1,3}):(\d{2})(?:\.(\d{1,6}))?", base)
    if m:
        minutes = int(m.group(1))
        seconds = int(m.group(2))
        frac = (m.group(3) or "0")
        centis = int((frac + "00")[:2])
        normalized = f"{minutes}:{seconds:02d},{centis:02d}"
        return f"X{normalized}" if is_x else normalized

    m = re.fullmatch(r"(\d{1,3})(?:\.(\d{1,6}))?", base)
    if m:
        seconds = int(m.group(1))
        frac = (m.group(2) or "0")
        centis = int((frac + "00")[:2])
        normalized = f"{seconds},{centis:02d}"
        return f"X{normalized}" if is_x else normalized

    return raw



def derive_result_time_ms(value):
    value = normalize_swim_time_text(value)
    if value is None:
        return None

    upper = value.upper()
    if upper in TEXT_STATUSES:
        return None

    if upper.startswith("X"):
        value = value[1:].strip()

    m = re.fullmatch(r"(\d{1,3}):(\d{2}),(\d{2})", value)
    if m:
        minutes = int(m.group(1))
        seconds = int(m.group(2))
        centis = int(m.group(3))
        return (minutes * 60 + seconds) * 1000 + centis * 10

    m = re.fullmatch(r"(\d{1,3}),(\d{2})", value)
    if m:
        seconds = int(m.group(1))
        centis = int(m.group(2))
        return seconds * 1000 + centis * 10

    return None



def normalize_result_status(status, result_time_text):
    status = normalize_controlled_lower(status)
    if status in STATUS_VALUES:
        return status

    rtt = normalize_string(result_time_text)
    if rtt:
        upper = rtt.upper()
        if upper == "DNS":
            return "dns"
        if upper == "DNF":
            return "dnf"
        if upper in {"DSQ", "DQ"}:
            return "dsq"
        if upper in {"NT", "NS"}:
            return "unknown"
        if upper.startswith("X"):
            return "unknown"
        return "valid"

    return "unknown"



def should_skip_line(line: str) -> bool:
    stripped = line.strip()
    if not stripped:
        return True
    return any(p.search(stripped) for p in HEADER_SKIP_PATTERNS)



def parse_event_header(line: str) -> Optional[EventContext]:
    candidate = line.strip()
    m = EVENT_HEADER_RE.match(candidate)
    if not m:
        return None
    return EventContext(
        event_number=int(m.group("event_number")),
        gender=normalize_event_gender(m.group("gender")),
        age_group=m.group("age_group").strip(),
        distance_label=m.group("distance_raw"),
        distance_m=parse_distance_to_meters(m.group("distance_raw")) or 0,
        course_code=m.group("course").upper(),
        stroke=normalize_stroke(m.group("stroke")),
    )



def parse_result_line(line: str, ctx: EventContext, page_number: int, line_number: int, competition_year: Optional[int]) -> Optional[ParsedResultRow]:
    m = RESULT_LINE_RE.match(line.strip())
    if not m:
        return None

    rank_raw = m.group("rank").strip()
    final_raw = normalize_string(m.group("final"))
    status = normalize_result_status(None, final_raw)
    normalized_final = normalize_swim_time_text(final_raw)
    rank_position: Optional[str] = None if rank_raw == "---" else rank_raw.lstrip("*").lstrip("*")

    if isinstance(normalized_final, str) and normalized_final.upper().startswith("X"):
        rank_position = None

    seed_time_text = normalize_swim_time_text(m.group("seed"))
    seed_time_ms = derive_result_time_ms(seed_time_text)
    result_time_ms = derive_result_time_ms(normalized_final)
    age_at_event = int(m.group("age"))
    birth_year_estimated = (competition_year - age_at_event) if competition_year is not None else None

    points_raw = normalize_string(m.groupdict().get("points"))

    return ParsedResultRow(
        page_number=page_number,
        line_number=line_number,
        event_number=ctx.event_number,
        event_name=clean_extracted_text(ctx.event_name),
        athlete_name=clean_extracted_text(m.group("name")),
        age_at_event=age_at_event,
        birth_year_estimated=birth_year_estimated,
        club_name=clean_extracted_text(m.group("team")),
        rank_position=rank_position,
        seed_time_text=seed_time_text,
        seed_time_ms=str(seed_time_ms) if seed_time_ms is not None else None,
        result_time_text=normalized_final,
        result_time_ms=str(result_time_ms) if result_time_ms is not None else None,
        status=status,
        points=points_raw.replace(" ", "") if isinstance(points_raw, str) else points_raw,
        raw_line=line.strip(),
    )



def parse_relay_team_line(line: str, ctx: EventContext, page_number: int, line_number: int) -> Optional[ParsedRelayTeamRow]:
    m = RELAY_TEAM_RE.match(line.strip())
    if not m:
        return None

    rank_raw = m.group("rank").strip()
    seed_raw = normalize_string(m.group("seed"))
    final_raw = normalize_string(m.group("final"))
    status = normalize_result_status(None, final_raw)
    normalized_final = normalize_swim_time_text(final_raw)
    rank_position: Optional[str] = None if rank_raw == "---" else rank_raw.lstrip("*").lstrip("*")

    if isinstance(normalized_final, str) and normalized_final.upper().startswith("X"):
        rank_position = None

    seed_time_text = normalize_swim_time_text(seed_raw)
    seed_time_ms = derive_result_time_ms(seed_time_text)
    result_time_ms = derive_result_time_ms(normalized_final)

    points_raw = normalize_string(m.groupdict().get("points"))

    return ParsedRelayTeamRow(
        page_number=page_number,
        line_number=line_number,
        event_number=ctx.event_number,
        event_name=clean_extracted_text(ctx.event_name),
        relay_team_name=clean_extracted_text(m.group("team")),
        rank_position=rank_position,
        seed_time_text=seed_time_text,
        seed_time_ms=str(seed_time_ms) if seed_time_ms is not None else None,
        result_time_text=normalized_final,
        result_time_ms=str(result_time_ms) if result_time_ms is not None else None,
        status=status,
        points=points_raw.replace(" ", "") if isinstance(points_raw, str) else points_raw,
        raw_line=line.strip(),
    )



def parse_relay_swimmer_line(line: str, ctx: EventContext, page_number: int, line_number: int, relay_team_name: Optional[str], competition_year: Optional[int]) -> List[ParsedRelaySwimmerRow]:
    stripped = line.strip()
    # pdfplumber a veces pega la edad del nadador anterior con el siguiente tramo: "W394)" -> "W39 4)".
    stripped = RELAY_SWIMMER_STUCK_LEG_RE.sub(r"\g<age_marker> ", stripped)
    # En líneas largas también puede deformar el marcador, por ejemplo "4F)."; segmentar evita fusionar nadadores.
    markers = list(RELAY_SWIMMER_LEG_MARKER_RE.finditer(stripped))
    if not markers:
        return []

    swimmers: List[ParsedRelaySwimmerRow] = []
    for index, marker in enumerate(markers):
        next_start = markers[index + 1].start() if index + 1 < len(markers) else len(stripped)
        segment = stripped[marker.end():next_start].strip()
        m = RELAY_SWIMMER_SEGMENT_RE.match(segment)
        if not m:
            continue
        swimmer_name = clean_extracted_text(m.group("name"))
        if not swimmer_name:
            continue
        gender_raw = (m.group("gender") or ctx.gender or "").upper()
        age_at_event = int(m.group("age")) if m.group("age") else None
        birth_year_estimated = (competition_year - age_at_event) if (competition_year is not None and age_at_event is not None) else None
        swimmers.append(
            ParsedRelaySwimmerRow(
                page_number=page_number,
                line_number=line_number,
                event_number=ctx.event_number,
                event_name=ctx.event_name,
                relay_team_name=relay_team_name,
                leg_order=int(marker.group("leg")),
                swimmer_name=swimmer_name.strip(),
                #swimmer_name=m.group("name").strip(),
                gender=normalize_athlete_gender(gender_raw),
                age_at_event=age_at_event,
                birth_year_estimated=birth_year_estimated,
                raw_line=stripped,
            )
        )
    return swimmers



def normalize_match_text(value: Optional[str]) -> str:
    value = clean_extracted_text(value)
    if value is None:
        return ""
    value = unicodedata.normalize("NFKD", value)
    value = "".join(ch for ch in value if not unicodedata.combining(ch))
    value = value.lower()
    value = re.sub(r"[^a-z0-9]+", " ", value)
    return re.sub(r"\s+", " ", value).strip()



def name_match_score(left: Optional[str], right: Optional[str]) -> float:
    left_norm = normalize_match_text(left)
    right_norm = normalize_match_text(right)
    if not left_norm or not right_norm:
        return 0.0

    ratio = SequenceMatcher(None, left_norm, right_norm).ratio()
    left_tokens = {tok for tok in left_norm.split() if len(tok) > 1 and not any(ch.isdigit() for ch in tok)}
    right_tokens = {tok for tok in right_norm.split() if len(tok) > 1 and not any(ch.isdigit() for ch in tok)}
    token_score = 0.0
    if left_tokens and right_tokens:
        token_score = len(left_tokens & right_tokens) / max(len(left_tokens), len(right_tokens))
    return (ratio * 0.65) + (token_score * 0.35)



def infer_relay_club_name_for_parser(relay_team_name: Optional[str], club_names: List[str]) -> Optional[str]:
    relay_team_name = clean_extracted_text(relay_team_name)
    if relay_team_name is None:
        return None

    relay_norm = normalize_match_text(relay_team_name)
    direct = {normalize_match_text(name): name for name in club_names if clean_extracted_text(name)}
    if relay_norm in direct:
        return direct[relay_norm]

    suffix_match = re.match(r"^(.*?)(?:\s+[A-Z])$", relay_team_name.strip())
    if suffix_match:
        candidate = normalize_match_text(suffix_match.group(1))
        if candidate in direct:
            return direct[candidate]

    candidates = []
    for original in club_names:
        norm = normalize_match_text(original)
        if norm and (relay_norm == norm or relay_norm.startswith(norm + " ")):
            candidates.append((len(norm), original))
    if candidates:
        candidates.sort(reverse=True)
        return candidates[0][1]
    return None



def reconcile_relay_swimmers_with_individuals(parsed_rows: List[ParsedResultRow], relay_team_rows: List[ParsedRelayTeamRow], relay_swimmer_rows: List[ParsedRelaySwimmerRow]) -> None:
    individual_by_club_gender: Dict[Tuple[str, str], List[ParsedResultRow]] = {}
    club_names: List[str] = []
    seen_club_names = set()
    seen_individual_keys = set()

    for row in parsed_rows:
        club_name = clean_extracted_text(row.club_name)
        athlete_gender = None
        event_match = EVENT_HEADER_RE.match(f"Event {row.event_number} {row.event_name}")
        if event_match:
            athlete_gender = normalize_athlete_gender(event_match.group("gender"))
        club_key = normalize_match_text(club_name)
        gender_key = normalize_athlete_gender(athlete_gender)
        if club_name and club_key not in seen_club_names:
            seen_club_names.add(club_key)
            club_names.append(club_name)
        if club_key and gender_key:
            individual_key = (club_key, gender_key, normalize_match_text(row.athlete_name), row.age_at_event)
            if individual_key in seen_individual_keys:
                continue
            seen_individual_keys.add(individual_key)
            individual_by_club_gender.setdefault((club_key, gender_key), []).append(row)

    relay_club_by_team: Dict[str, Optional[str]] = {}
    for row in relay_team_rows:
        relay_club_by_team[normalize_match_text(row.relay_team_name)] = infer_relay_club_name_for_parser(row.relay_team_name, club_names)

    for row in relay_swimmer_rows:
        relay_club = relay_club_by_team.get(normalize_match_text(row.relay_team_name))
        club_key = normalize_match_text(relay_club)
        gender_key = normalize_athlete_gender(row.gender)
        candidates = individual_by_club_gender.get((club_key, gender_key), [])
        scored_candidates: List[Tuple[float, ParsedResultRow]] = []

        for candidate in candidates:
            score = name_match_score(row.swimmer_name, candidate.athlete_name)
            if row.age_at_event is not None and candidate.age_at_event == row.age_at_event:
                score += 0.10
            elif row.age_at_event is not None and score < 0.82:
                score -= 0.10
            scored_candidates.append((score, candidate))

        if not scored_candidates:
            continue

        scored_candidates.sort(key=lambda item: item[0], reverse=True)
        best_score, best = scored_candidates[0]
        second_score = scored_candidates[1][0] if len(scored_candidates) > 1 else 0.0

        if best_score >= 0.82 and (best_score - second_score) >= 0.12:
            row.swimmer_name = best.athlete_name
            row.age_at_event = best.age_at_event
            row.birth_year_estimated = best.birth_year_estimated



def extract_text_lines(pdf_path: Path) -> List[Tuple[int, List[str]]]:
    pages: List[Tuple[int, List[str]]] = []
    with pdfplumber.open(pdf_path) as pdf:
        for page_index, page in enumerate(pdf.pages):
            text = page.extract_text(x_tolerance=1.5, y_tolerance=3) or ""
            lines = [ln.rstrip() for ln in text.splitlines()]
            pages.append((page_index + 1, lines))
    return pages



def parse_pdf(pdf_path: Path):
    pages = extract_text_lines(pdf_path)
    stats = ParseStats(pages_read=len(pages))
    current_event: Optional[EventContext] = None
    rows: List[ParsedResultRow] = []
    relay_team_rows: List[ParsedRelayTeamRow] = []
    relay_swimmer_rows: List[ParsedRelaySwimmerRow] = []
    debug_rows: List[Dict[str, Optional[str]]] = []
    last_relay_team_name: Optional[str] = None

    competition_name: Optional[str] = None
    results_label: Optional[str] = None
    competition_year: Optional[int] = None

    for page_number, lines in pages:
        for idx, raw_line in enumerate(lines, start=1):
            line = raw_line.strip()
            if not line:
                stats.lines_skipped += 1
                continue

            if competition_name is None and ((line.startswith("II Copa") or line.startswith("I Copa")) or "Copa" in line):
                competition_name = line
            if results_label is None and line.lower().startswith("results"):
                results_label = line

            if competition_year is None:
                meta_probe = {"competition_name": competition_name, "results_label": results_label}
                competition_year = derive_competition_year(meta_probe, pdf_path)

            if should_skip_line(line):
                stats.lines_skipped += 1
                continue

            event = parse_event_header(line)
            if event is not None:
                current_event = event
                last_relay_team_name = None
                stats.event_headers_found += 1
                continue

            if line.startswith("(") and line.endswith(")"):
                inner = line[1:-1].strip()
                event = parse_event_header(inner)
                if event is not None:
                    current_event = event
                    last_relay_team_name = None
                    stats.event_headers_found += 1
                    continue

            if current_event is None:
                stats.lines_skipped += 1
                continue

            if current_event.is_relay:
                relay_team = parse_relay_team_line(line, current_event, page_number, idx)
                if relay_team is not None:
                    relay_team_rows.append(relay_team)
                    last_relay_team_name = relay_team.relay_team_name
                    stats.relay_team_rows_found += 1
                    continue

                relay_swimmers = parse_relay_swimmer_line(line, current_event, page_number, idx, last_relay_team_name, competition_year)
                if relay_swimmers:
                    relay_swimmer_rows.extend(relay_swimmers)
                    stats.relay_swimmer_rows_found += len(relay_swimmers)
                    continue

                stats.lines_unparsed += 1
                debug_rows.append(
                    {
                        "page_number": page_number,
                        "line_number": idx,
                        "event_name_context": current_event.event_name,
                        "raw_line": line,
                        "reason": "unparsed_relay_line",
                    }
                )
                continue

            parsed = parse_result_line(line, current_event, page_number, idx, competition_year)
            if parsed is not None:
                rows.append(parsed)
                stats.result_rows_found += 1
                continue

            stats.lines_unparsed += 1
            debug_rows.append(
                {
                    "page_number": page_number,
                    "line_number": idx,
                    "event_name_context": current_event.event_name,
                    "raw_line": line,
                    "reason": "unparsed_inside_event",
                }
            )

    reconcile_relay_swimmers_with_individuals(rows, relay_team_rows, relay_swimmer_rows)

    debug_df = pd.DataFrame(debug_rows, columns=["page_number", "line_number", "event_name_context", "raw_line", "reason"])
    metadata = {
        "pdf_name": pdf_path.name,
        "competition_name": competition_name,
        "results_label": results_label,
        "competition_year": competition_year,
    }
    return rows, relay_team_rows, relay_swimmer_rows, debug_df, stats, metadata



def build_output_frames(parsed_rows: List[ParsedResultRow], relay_team_rows: List[ParsedRelayTeamRow], relay_swimmer_rows: List[ParsedRelaySwimmerRow], competition_id: Optional[int], default_source_id: Optional[int], metadata: Dict[str, Optional[str]]) -> Dict[str, pd.DataFrame]:
    source_id_value = str(default_source_id) if default_source_id is not None else None
    competition_year = metadata.get("competition_year")

    event_records: List[Dict[str, Optional[str]]] = []
    athlete_records: List[Dict[str, Optional[str]]] = []
    result_records: List[Dict[str, Optional[str]]] = []
    club_records: List[Dict[str, Optional[str]]] = []
    relay_team_records: List[Dict[str, Optional[str]]] = []
    relay_swimmer_records: List[Dict[str, Optional[str]]] = []

    seen_clubs = set()
    seen_athletes = set()
    seen_events = set()

    def ensure_event(row_event_number: int, row_event_name: str):
        if row_event_name in seen_events:
            return
        seen_events.add(row_event_name)
        event_match = EVENT_HEADER_RE.match(f"Event {row_event_number} {row_event_name}")
        if not event_match:
            fail(f"No se pudo reconstruir metadata del evento: {row_event_name}")
        event_records.append(
            {
                "competition_id": str(competition_id) if competition_id is not None else None,
                "event_name": row_event_name,
                "stroke": normalize_stroke(event_match.group("stroke")),
                "distance_m": str(parse_distance_to_meters(event_match.group("distance_raw")) or 0),
                "gender": normalize_event_gender(event_match.group("gender")),
                "age_group": event_match.group("age_group").strip(),
                "round_type": "final",
                "source_id": source_id_value,
            }
        )

    for row in parsed_rows:
        club_key = normalize_controlled_lower(row.club_name)
        if club_key and club_key not in seen_clubs:
            seen_clubs.add(club_key)
            club_records.append({"name": row.club_name, "short_name": None, "city": None, "region": None, "source_id": source_id_value})

        ensure_event(row.event_number, row.event_name)

        athlete_key = (normalize_controlled_lower(row.athlete_name), club_key)
        if athlete_key not in seen_athletes:
            seen_athletes.add(athlete_key)
            event_match = EVENT_HEADER_RE.match(f"Event {row.event_number} {row.event_name}")
            athlete_records.append(
                {
                    "full_name": row.athlete_name,
                    "gender": normalize_athlete_gender(event_match.group("gender")) if event_match else None,
                    "club_name": row.club_name,
                    "birth_year": str(row.birth_year_estimated) if row.birth_year_estimated is not None else None,
                    "source_id": source_id_value,
                }
            )

        result_records.append(
            {
                "event_name": row.event_name,
                "athlete_name": row.athlete_name,
                "club_name": row.club_name,
                "rank_position": row.rank_position,
                "age_at_event": str(row.age_at_event) if row.age_at_event is not None else None,
                "birth_year_estimated": str(row.birth_year_estimated) if row.birth_year_estimated is not None else None,
                "seed_time_text": row.seed_time_text,
                "seed_time_ms": row.seed_time_ms,
                "result_time_text": row.result_time_text,
                "result_time_ms": row.result_time_ms,
                "points": row.points,
                "status": row.status,
                "source_id": source_id_value,
            }
        )

    for row in relay_team_rows:
        ensure_event(row.event_number, row.event_name)
        relay_team_records.append(
            {
                "event_name": row.event_name,
                "relay_team_name": row.relay_team_name,
                "rank_position": row.rank_position,
                "seed_time_text": row.seed_time_text,
                "seed_time_ms": row.seed_time_ms,
                "result_time_text": row.result_time_text,
                "result_time_ms": row.result_time_ms,
                "points": row.points,
                "status": row.status,
                "source_id": source_id_value,
                "page_number": row.page_number,
                "line_number": row.line_number,
            }
        )

    for row in relay_swimmer_rows:
        relay_swimmer_records.append(
            {
                "event_name": row.event_name,
                "relay_team_name": row.relay_team_name,
                "leg_order": str(row.leg_order),
                "swimmer_name": row.swimmer_name,
                "gender": row.gender,
                "age_at_event": str(row.age_at_event) if row.age_at_event is not None else None,
                "birth_year_estimated": str(row.birth_year_estimated) if row.birth_year_estimated is not None else None,
                "page_number": row.page_number,
                "line_number": row.line_number,
            }
        )

    frames = {
        "club": pd.DataFrame(club_records, columns=["name", "short_name", "city", "region", "source_id"]),
        "event": pd.DataFrame(event_records, columns=["competition_id", "event_name", "stroke", "distance_m", "gender", "age_group", "round_type", "source_id"]),
        "athlete": pd.DataFrame(athlete_records, columns=["full_name", "gender", "club_name", "birth_year", "source_id"]),
        "result": pd.DataFrame(result_records, columns=["event_name", "athlete_name", "club_name", "rank_position", "age_at_event", "birth_year_estimated", "seed_time_text", "seed_time_ms", "result_time_text", "result_time_ms", "points", "status", "source_id"]),
        "raw_result": pd.DataFrame([asdict(r) for r in parsed_rows]),
        "relay_team": pd.DataFrame(relay_team_records, columns=["event_name", "relay_team_name", "rank_position", "seed_time_text", "seed_time_ms", "result_time_text", "result_time_ms", "points", "status", "source_id", "page_number", "line_number"]),
        "relay_swimmer": pd.DataFrame(relay_swimmer_records, columns=["event_name", "relay_team_name", "leg_order", "swimmer_name", "gender", "age_at_event", "birth_year_estimated", "page_number", "line_number"]),
        "raw_relay_team": pd.DataFrame([asdict(r) for r in relay_team_rows]),
        "raw_relay_swimmer": pd.DataFrame([asdict(r) for r in relay_swimmer_rows]),
    }
    return frames



def save_outputs(frames: Dict[str, pd.DataFrame], debug_df: pd.DataFrame, metadata: Dict[str, Optional[str]], out_dir: Path, excel_name: str) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)

    for key, df in frames.items():
        csv_path = out_dir / f"{key}.csv"
        df.to_csv(csv_path, index=False, encoding="utf-8-sig")

    debug_df.to_csv(out_dir / "debug_unparsed_lines.csv", index=False, encoding="utf-8-sig")

    workbook_path = out_dir / excel_name
    with pd.ExcelWriter(workbook_path, engine="openpyxl") as writer:
        for sheet_name in [
            "club", "event", "athlete", "result", "raw_result",
            "relay_team", "relay_swimmer", "raw_relay_team", "raw_relay_swimmer"
        ]:
            frames[sheet_name].to_excel(writer, sheet_name=sheet_name[:31], index=False)
        debug_df.to_excel(writer, sheet_name="debug_unparsed", index=False)

    with open(out_dir / "metadata.json", "w", encoding="utf-8") as f:
        json.dump(metadata, f, ensure_ascii=False, indent=2)



def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Extrae resultados desde un PDF estilo FCHMN a archivos intermedios CSV/XLSX listos para revisar. v0.1.5 agrega soporte para SC Meter, columna Points, ranks con * y ruido 'Results'."
    )
    parser.add_argument("--pdf", required=True, help="Ruta al PDF de resultados")
    parser.add_argument("--out-dir", required=True, help="Carpeta de salida para CSV/XLSX")
    parser.add_argument("--competition-id", type=int, help="competition_id opcional para poblar la hoja event")
    parser.add_argument("--default-source-id", type=int, help="source_id opcional para poblar hojas de salida")
    parser.add_argument("--excel-name", default="parsed_results_v0_1_5.xlsx", help="Nombre del archivo Excel de salida")
    return parser.parse_args()



def main() -> None:
    args = parse_args()
    pdf_path = Path(args.pdf)
    out_dir = Path(args.out_dir)

    if not pdf_path.exists():
        fail(f"No existe el PDF: {pdf_path}")

    info(f"Leyendo PDF: {pdf_path}")
    parsed_rows, relay_team_rows, relay_swimmer_rows, debug_df, stats, metadata = parse_pdf(pdf_path)

    if not parsed_rows and not relay_team_rows:
        fail("No se extrajeron filas de resultados. Revisa el layout del PDF o el archivo de debug.")

    frames = build_output_frames(parsed_rows, relay_team_rows, relay_swimmer_rows, args.competition_id, args.default_source_id, metadata)
    save_outputs(frames, debug_df, metadata, out_dir, args.excel_name)

    print("\n=== RESUMEN DE EXTRACCIÓN ===")
    print(f"Páginas leídas:           {stats.pages_read}")
    print(f"Encabezados de evento:    {stats.event_headers_found}")
    print(f"Filas individuales:       {stats.result_rows_found}")
    print(f"Equipos de relevo:        {stats.relay_team_rows_found}")
    print(f"Nadadores de relevo:      {stats.relay_swimmer_rows_found}")
    print(f"Líneas omitidas:          {stats.lines_skipped}")
    print(f"Líneas no parseadas:      {stats.lines_unparsed}")
    print("---")
    print(f"club:                     {len(frames['club'])}")
    print(f"event:                    {len(frames['event'])}")
    print(f"athlete:                  {len(frames['athlete'])}")
    print(f"result:                   {len(frames['result'])}")
    print(f"raw_result:               {len(frames['raw_result'])}")
    print(f"relay_team:               {len(frames['relay_team'])}")
    print(f"relay_swimmer:            {len(frames['relay_swimmer'])}")
    print("\n[OK] Extracción terminada.")
    print(f"Salida: {out_dir}")

    if stats.lines_unparsed > 0:
        warn("Hay líneas no parseadas. Revisa debug_unparsed_lines.csv antes de cargar a staging.")


if __name__ == "__main__":
    main()
