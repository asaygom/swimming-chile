from __future__ import annotations

import re
from typing import Any


TEXT_STATUSES = {"DNS", "DNF", "DSQ", "SCRATCH", "NT", "NS", "DQ", "DFS", "VALID", "UNKNOWN"}


def normalize_string(value: Any) -> str | None:
    if value is None:
        return None
    try:
        if value != value:
            return None
    except Exception:
        pass
    value = str(value).strip()
    return value if value else None


def normalize_controlled_lower(value: Any) -> str | None:
    value = normalize_string(value)
    return value.lower() if value is not None else None


def normalize_event_gender(value: Any) -> str | None:
    value = normalize_controlled_lower(value)
    mapping = {
        "women": "women",
        "woman": "women",
        "female": "women",
        "f": "women",
        "mujeres": "women",
        "mujer": "women",
        "men": "men",
        "man": "men",
        "male": "men",
        "m": "men",
        "hombres": "men",
        "hombre": "men",
        "mixed": "mixed",
        "mix": "mixed",
        "mixto": "mixed",
    }
    return mapping.get(value, value)


def normalize_athlete_gender(value: Any) -> str | None:
    value = normalize_controlled_lower(value)
    mapping = {
        "women": "female",
        "woman": "female",
        "female": "female",
        "f": "female",
        "w": "female",
        "mujeres": "female",
        "mujer": "female",
        "men": "male",
        "man": "male",
        "male": "male",
        "m": "male",
        "hombres": "male",
        "hombre": "male",
    }
    return mapping.get(value, value)


def normalize_stroke(value: Any) -> str | None:
    value = normalize_controlled_lower(value)
    if value is None:
        return None
    value = value.replace("-", " ").replace("_", " ")
    value = re.sub(r"\s+", " ", value).strip()
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
        "estilo libre": "freestyle",
        "libre": "freestyle",
        "estilo de espalda": "backstroke",
        "espalda": "backstroke",
        "estilo de pecho": "breaststroke",
        "pecho": "breaststroke",
        "estilo de mariposa": "butterfly",
        "mariposa": "butterfly",
        "ci": "individual_medley",
        "combinado": "individual_medley",
        "combinado individual": "individual_medley",
        "comb relevo": "medley_relay",
        "combinado relevo": "medley_relay",
        "relevo combinado": "medley_relay",
        "libre relevo": "freestyle_relay",
        "relevo libre": "freestyle_relay",
        "estilo libre relevo": "freestyle_relay",
    }
    return mapping.get(value, value.replace(" ", "_"))


def normalize_swim_time_text(value: Any) -> str | None:
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

    match = re.fullmatch(r"(\d{1,2}):(\d{2}):(\d{2})(?:\.(\d+))?", base)
    if match:
        hours = int(match.group(1))
        minutes = int(match.group(2))
        seconds = int(match.group(3))
        frac = match.group(4) or "0"
        centis = int((frac + "00")[:2])
        total_minutes = hours * 60 + minutes
        normalized = f"{total_minutes}:{seconds:02d},{centis:02d}" if total_minutes > 0 else f"{seconds},{centis:02d}"
        return f"X{normalized}" if is_x else normalized

    match = re.fullmatch(r"(\d{1,3}):(\d{2})(?:\.(\d{1,6}))?", base)
    if match:
        minutes = int(match.group(1))
        seconds = int(match.group(2))
        frac = match.group(3) or "0"
        centis = int((frac + "00")[:2])
        normalized = f"{minutes}:{seconds:02d},{centis:02d}"
        return f"X{normalized}" if is_x else normalized

    match = re.fullmatch(r"(\d{1,3})(?:\.(\d{1,6}))?", base)
    if match:
        seconds = int(match.group(1))
        frac = match.group(2) or "0"
        centis = int((frac + "00")[:2])
        normalized = f"{seconds},{centis:02d}"
        return f"X{normalized}" if is_x else normalized

    return raw


def derive_result_time_ms(value: Any) -> int | None:
    value = normalize_swim_time_text(value)
    if value is None:
        return None

    upper = value.upper()
    if upper in TEXT_STATUSES:
        return None

    if upper.startswith("X"):
        value = value[1:].strip()

    match = re.fullmatch(r"(\d{1,3}):(\d{2}),(\d{2})", value)
    if match:
        minutes = int(match.group(1))
        seconds = int(match.group(2))
        centis = int(match.group(3))
        return (minutes * 60 + seconds) * 1000 + centis * 10

    match = re.fullmatch(r"(\d{1,3}),(\d{2})", value)
    if match:
        seconds = int(match.group(1))
        centis = int(match.group(2))
        return seconds * 1000 + centis * 10

    return None
