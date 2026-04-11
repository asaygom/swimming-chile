#!/usr/bin/env python3
# pipeline v0.3.7
from __future__ import annotations

import argparse
import io
import re
import sys
import unicodedata
from dataclasses import dataclass
from difflib import SequenceMatcher
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import pandas as pd

try:
    import psycopg
    HAS_PSYCOPG3 = True
except ImportError:
    psycopg = None
    HAS_PSYCOPG3 = False
    try:
        import psycopg2
        HAS_PSYCOPG2 = True
    except ImportError:
        psycopg2 = None
        HAS_PSYCOPG2 = False

EXPECTED_COLUMNS = {
    "club": ["name", "short_name", "city", "region", "source_id"],
    "event": ["competition_id", "event_name", "stroke", "distance_m", "gender", "age_group", "round_type", "source_id"],
    "athlete": ["full_name", "gender", "club_name", "birth_year", "source_id"],
    "result": ["event_name", "athlete_name", "club_name", "rank_position", "seed_time_text", "seed_time_ms", "result_time_text", "result_time_ms", "age_at_event", "birth_year_estimated", "points", "status", "source_id"],
    "relay_result": ["event_name", "club_name", "relay_team_name", "lane", "heat_number", "rank_position", "seed_time_text", "seed_time_ms", "result_time_text", "result_time_ms", "points", "reaction_time", "record_flag", "status", "source_id", "source_url"],
    "relay_result_member": ["event_name", "club_name", "relay_team_name", "leg_order", "athlete_name", "gender", "age_at_event", "birth_year_estimated"],
}

PARSER_RELAY_INPUT_COLUMNS = {
    "relay_team": ["event_name", "relay_team_name", "rank_position", "seed_time_text", "seed_time_ms", "result_time_text", "result_time_ms", "points", "status", "source_id", "page_number", "line_number"],
    "relay_swimmer": ["event_name", "relay_team_name", "leg_order", "swimmer_name", "gender", "age_at_event", "birth_year_estimated", "page_number", "line_number"],
}

STAGING_TABLES = {
    "club": "stg_club",
    "event": "stg_event",
    "athlete": "stg_athlete",
    "result": "stg_result",
    "relay_result": "stg_relay_result",
    "relay_result_member": "stg_relay_result_member",
}

STATUS_VALUES = {"valid", "dns", "dnf", "dsq", "scratch", "unknown"}
TEXT_STATUSES = {"DNS", "DNF", "DSQ", "SCRATCH", "NT", "NS", "DQ", "VALID", "UNKNOWN"}
DEFAULT_CLUB_ALIAS_CSV = Path(__file__).resolve().parents[1] / "data" / "reference" / "club_alias.csv"


@dataclass
class Config:
    host: str
    port: int
    dbname: str
    user: str
    password: str
    schema: str
    truncate_staging: bool
    truncate_core: bool
    competition_id: Optional[int]
    default_source_id: int


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Carga una competencia desde Excel o CSV hacia PostgreSQL. También puede consumir la carpeta de salida del parser PDF, incluyendo relevos.")
    parser.add_argument("--excel", type=str, help="Ruta al archivo Excel con hojas club, event, athlete y result")
    parser.add_argument("--input-dir", type=str, help="Carpeta con club.csv, event.csv, athlete.csv, result.csv y opcionalmente relay_team.csv + relay_swimmer.csv generados por el parser PDF")
    parser.add_argument("--club-csv", type=str, help="Ruta al CSV de club")
    parser.add_argument("--event-csv", type=str, help="Ruta al CSV de event")
    parser.add_argument("--athlete-csv", type=str, help="Ruta al CSV de athlete")
    parser.add_argument("--result-csv", type=str, help="Ruta al CSV de result")
    parser.add_argument("--relay-team-csv", type=str, help="Ruta al CSV de relay_team del parser PDF")
    parser.add_argument("--relay-swimmer-csv", type=str, help="Ruta al CSV de relay_swimmer del parser PDF")
    parser.add_argument("--host", type=str, default="localhost")
    parser.add_argument("--port", type=int, default=5432)
    parser.add_argument("--dbname", type=str, default="natacion_chile")
    parser.add_argument("--user", type=str, required=True)
    parser.add_argument("--password", type=str, required=True)
    parser.add_argument("--schema", type=str, default="core")
    parser.add_argument("--truncate-staging", action="store_true")
    parser.add_argument("--truncate-core", action="store_true")
    parser.add_argument("--competition-id", type=int, help="competition_id opcional. Si no se indica, se intentará resolver o crear desde metadata.json")
    parser.add_argument("--competition-name", type=str, help="Nombre de competencia opcional para auto-upsert cuando no se indique competition_id")
    parser.add_argument("--competition-source-url", type=str, help="URL fuente opcional para competition cuando se cree automáticamente")
    parser.add_argument("--default-source-id", type=int, default=1)
    parser.add_argument("--club-alias-csv", type=str, default=str(DEFAULT_CLUB_ALIAS_CSV), help="CSV opcional con columnas alias_name, canonical_name para resolver variantes de nombres de clubes")
    return parser.parse_args()


def fail(msg: str) -> None:
    print(f"[ERROR] {msg}", file=sys.stderr)
    sys.exit(1)


def info(msg: str) -> None:
    print(f"[INFO] {msg}")


def fqtn(schema: str, table: str) -> str:
    return f"{schema}.{table}"


def club_match_key_sql(expr: str) -> str:
    accent_chars = "CHR(225)||CHR(233)||CHR(237)||CHR(243)||CHR(250)||CHR(252)||CHR(241)"
    return f"NULLIF(BTRIM(REGEXP_REPLACE(TRANSLATE(LOWER(TRIM({expr})), {accent_chars}, 'aeiouun'), '[^a-z0-9]+', ' ', 'g')), '')"


def club_name_quality_sql(expr: str) -> str:
    accent_chars = "CHR(225)||CHR(233)||CHR(237)||CHR(243)||CHR(250)||CHR(252)||CHR(241)"
    return f"LENGTH(LOWER(TRIM({expr}))) - LENGTH(TRANSLATE(LOWER(TRIM({expr})), {accent_chars}, ''))"


def normalize_string(x):
    if x is None:
        return None
    if pd.isna(x):
        return None
    if isinstance(x, str):
        x = x.strip()
        return x if x != "" else None
    x = str(x).strip()
    return x if x != "" else None


def clean_extracted_text(x):
    x = normalize_string(x)
    if x is None:
        return None
    x = unicodedata.normalize("NFC", str(x))
    x = re.sub(r"\s+", " ", x).strip()
    return x if x else None


def normalize_controlled_lower(x):
    x = normalize_string(x)
    return x.lower() if isinstance(x, str) else x


def normalize_match_text(x):
    x = normalize_string(x)
    if x is None:
        return None
    x = unicodedata.normalize("NFKD", str(x))
    x = "".join(ch for ch in x if not unicodedata.combining(ch))
    x = x.lower()
    x = re.sub(r"[^a-z0-9]+", " ", x)
    return re.sub(r"\s+", " ", x).strip() or None


def normalize_event_gender(x):
    x = normalize_controlled_lower(x)
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
    return mapping.get(x, x)



def normalize_athlete_gender(x):
    x = normalize_controlled_lower(x)
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
    return mapping.get(x, x)



def normalize_stroke(x):
    x = normalize_controlled_lower(x)
    if x is None:
        return None
    x = x.replace('-', ' ').replace('_', ' ')
    x = re.sub(r"\s+", " ", x).strip()
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
    return mapping.get(x, x.replace(' ', '_'))



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
    return "unknown"



def infer_relay_club_name(relay_team_name: Optional[str], club_names: List[str]) -> Optional[str]:
    relay_team_name = normalize_string(relay_team_name)
    if relay_team_name is None:
        return None
    relay_norm = normalize_match_text(relay_team_name)
    if relay_norm is None:
        return None

    direct = {normalize_match_text(name): name for name in club_names if normalize_string(name) is not None}
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



def load_club_aliases(alias_csv: Optional[str]) -> Dict[str, str]:
    if not alias_csv:
        return {}
    alias_path = Path(alias_csv)
    if not alias_path.exists():
        info(f"No se encontró club_alias_csv: {alias_path}. Se continuará sin aliases manuales.")
        return {}

    alias_df = pd.read_csv(alias_path, dtype=str, encoding="utf-8-sig").fillna("")
    required = {"alias_name", "canonical_name"}
    if not required.issubset(set(alias_df.columns)):
        fail(f"{alias_path} debe tener columnas {sorted(required)}")

    aliases: Dict[str, str] = {}
    for _, row in alias_df.iterrows():
        alias_name = clean_extracted_text(row.get("alias_name"))
        canonical_name = clean_extracted_text(row.get("canonical_name"))
        alias_key = normalize_match_text(alias_name)
        if alias_key and canonical_name:
            aliases[alias_key] = canonical_name
            canonical_key = normalize_match_text(canonical_name)
            if canonical_key:
                aliases.setdefault(canonical_key, canonical_name)

    info(f"Aliases de club cargados: {len(aliases)} claves desde {alias_path}")
    return aliases



def resolve_club_alias(value: Optional[str], aliases: Dict[str, str]) -> Optional[str]:
    value = clean_extracted_text(value)
    if value is None:
        return None
    return aliases.get(normalize_match_text(value), value)



def apply_club_aliases(data: Dict[str, pd.DataFrame], aliases: Dict[str, str]) -> None:
    if not aliases:
        return
    for table_key, df in data.items():
        if df.empty:
            continue
        if table_key == "club" and "name" in df.columns:
            df["name"] = df["name"].map(lambda x: resolve_club_alias(x, aliases))
            df.drop_duplicates(subset=["name"], keep="first", inplace=True, ignore_index=True)
        if "club_name" in df.columns:
            df["club_name"] = df["club_name"].map(lambda x: resolve_club_alias(x, aliases))



def club_similarity_key(value: Optional[str]) -> Optional[str]:
    value = normalize_match_text(value)
    if value is None:
        return None
    generic_tokens = {"club", "master", "masters", "swim", "swimming", "team", "natacion", "de", "del", "la", "las", "los"}
    number_words = {"100": "cien"}
    tokens = [number_words.get(token, token) for token in value.split() if token not in generic_tokens]
    return " ".join(tokens) or value



def write_club_alias_candidates(data: Dict[str, pd.DataFrame], out_path: Optional[Path], reference_names: Optional[Iterable[str]] = None) -> None:
    if out_path is None or "club" not in data or data["club"].empty:
        return

    club_names = sorted({name for name in data["club"].get("name", pd.Series(dtype=str)).map(clean_extracted_text).tolist() if name})
    reference_names = sorted({name for name in (reference_names or []) if clean_extracted_text(name)})
    athlete_sets: Dict[str, set] = {}
    if "athlete" in data and not data["athlete"].empty:
        for _, row in data["athlete"].iterrows():
            club_name = clean_extracted_text(row.get("club_name"))
            athlete_key = (
                normalize_match_text(row.get("full_name")),
                normalize_athlete_gender(row.get("gender")),
                normalize_string(row.get("birth_year")),
            )
            if club_name and athlete_key[0]:
                athlete_sets.setdefault(club_name, set()).add(athlete_key)

    records = []
    for i, left in enumerate(club_names):
        left_key = club_similarity_key(left)
        for right in club_names[i + 1:]:
            right_key = club_similarity_key(right)
            if not left_key or not right_key:
                continue
            ratio = SequenceMatcher(None, left_key, right_key).ratio()
            overlap = len(athlete_sets.get(left, set()) & athlete_sets.get(right, set()))
            if ratio >= 0.72 or overlap >= 2:
                records.append(
                    {
                        "club_name_a": left,
                        "club_name_b": right,
                        "candidate_source": "current_input",
                        "similarity_key_a": left_key,
                        "similarity_key_b": right_key,
                        "similarity_ratio": f"{ratio:.3f}",
                        "shared_athletes": str(overlap),
                    }
                )

    existing_pairs = {(record["club_name_a"], record["club_name_b"]) for record in records}
    for left in club_names:
        left_key = club_similarity_key(left)
        left_norm = normalize_match_text(left)
        for right in reference_names:
            right_key = club_similarity_key(right)
            right_norm = normalize_match_text(right)
            if not left_key or not right_key or left_norm == right_norm:
                continue
            ratio = SequenceMatcher(None, left_key, right_key).ratio()
            pair_key = (left, right)
            if ratio >= 0.72 and pair_key not in existing_pairs:
                records.append(
                    {
                        "club_name_a": left,
                        "club_name_b": right,
                        "candidate_source": "alias_reference",
                        "similarity_key_a": left_key,
                        "similarity_key_b": right_key,
                        "similarity_ratio": f"{ratio:.3f}",
                        "shared_athletes": "",
                    }
                )

    if not records:
        return
    out_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(records).to_csv(out_path, index=False, encoding="utf-8-sig")
    info(f"Candidatos de alias de club para revisar: {out_path}")



def normalize_dataframe(df: pd.DataFrame, expected_columns: List[str], table_key: str) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    if table_key == "athlete" and "birth_year" in expected_columns and "birth_year" not in df.columns:
        df["birth_year"] = None
    missing = [c for c in expected_columns if c not in df.columns]
    extra = [c for c in df.columns if c not in expected_columns]
    if missing:
        fail(f"Faltan columnas {missing} en {table_key}. Columnas encontradas: {list(df.columns)}")
    if extra:
        info(f"En {table_key} se ignorarán columnas extra: {extra}")
        df = df[[c for c in df.columns if c in expected_columns]]
    df = df[expected_columns]
    for col in df.columns:
        df[col] = df[col].where(pd.notna(df[col]), None)
        df[col] = df[col].map(normalize_string)

    for text_col in [c for c in ["name", "short_name", "event_name", "club_name", "full_name", "athlete_name", "relay_team_name"] if c in df.columns]:
        df[text_col] = df[text_col].map(clean_extracted_text)

    if table_key == "event":
        df["stroke"] = df["stroke"].map(normalize_stroke)
        df["gender"] = df["gender"].map(normalize_event_gender)
        df["round_type"] = df["round_type"].map(normalize_controlled_lower)

    if table_key == "athlete":
        df["gender"] = df["gender"].map(normalize_athlete_gender)
        if "birth_year" in df.columns:
            df["birth_year"] = df["birth_year"].map(normalize_string)

    if table_key in {"result", "relay_result"}:
        df["seed_time_text"] = df["seed_time_text"].map(normalize_swim_time_text)
        if "points" in df.columns:
            df["points"] = df["points"].map(lambda x: normalize_string(x).replace(",", ".") if isinstance(normalize_string(x), str) else normalize_string(x))
        df["result_time_text"] = df["result_time_text"].map(normalize_swim_time_text)
        normalized_seed_ms = []
        normalized_result_ms = []
        normalized_rank = []
        normalized_status = []
        for stt, sms, tt, ms, rk, st in zip(
            df["seed_time_text"],
            df["seed_time_ms"],
            df["result_time_text"],
            df["result_time_ms"],
            df["rank_position"],
            df["status"],
        ):
            sms_norm = normalize_string(sms)
            if sms_norm is None:
                derived_seed = derive_result_time_ms(stt)
                normalized_seed_ms.append(str(derived_seed) if derived_seed is not None else None)
            else:
                normalized_seed_ms.append(sms_norm)

            ms_norm = normalize_string(ms)
            if ms_norm is None:
                derived_result = derive_result_time_ms(tt)
                normalized_result_ms.append(str(derived_result) if derived_result is not None else None)
            else:
                normalized_result_ms.append(ms_norm)

            if isinstance(tt, str) and tt.upper().startswith("X"):
                normalized_rank.append(None)
                normalized_status.append("unknown")
            else:
                normalized_rank.append(normalize_string(rk))
                normalized_status.append(normalize_result_status(st, tt))

        df["seed_time_ms"] = normalized_seed_ms
        df["result_time_ms"] = normalized_result_ms
        df["rank_position"] = normalized_rank
        df["status"] = normalized_status

    if table_key == "result":
        df["age_at_event"] = df["age_at_event"].map(normalize_string)
        df["birth_year_estimated"] = df["birth_year_estimated"].map(normalize_string)

    if table_key == "relay_result_member":
        df["gender"] = df["gender"].map(normalize_athlete_gender)
        df["leg_order"] = df["leg_order"].map(normalize_string)
        df["age_at_event"] = df["age_at_event"].map(normalize_string)
        df["birth_year_estimated"] = df["birth_year_estimated"].map(normalize_string)
    return df



def transform_parser_relay_outputs(relay_team_df: pd.DataFrame, relay_swimmer_df: pd.DataFrame, club_df: pd.DataFrame, default_source_id: int) -> Dict[str, pd.DataFrame]:
    club_names = [x for x in club_df.get("name", pd.Series(dtype=str)).tolist() if normalize_string(x) is not None]

    relay_team_df = relay_team_df.copy()
    relay_swimmer_df = relay_swimmer_df.copy()

    relay_team_df.columns = [str(c).strip() for c in relay_team_df.columns]
    relay_swimmer_df.columns = [str(c).strip() for c in relay_swimmer_df.columns]

    missing_team = [c for c in PARSER_RELAY_INPUT_COLUMNS["relay_team"] if c not in relay_team_df.columns]
    if missing_team:
        fail(f"Faltan columnas {missing_team} en relay_team.csv")
    missing_swimmer = [c for c in PARSER_RELAY_INPUT_COLUMNS["relay_swimmer"] if c not in relay_swimmer_df.columns]
    if missing_swimmer:
        fail(f"Faltan columnas {missing_swimmer} en relay_swimmer.csv")

    relay_team_df["club_name"] = relay_team_df["relay_team_name"].map(lambda x: infer_relay_club_name(x, club_names))
    relay_team_df["lane"] = None
    relay_team_df["heat_number"] = None
    relay_team_df["points"] = relay_team_df.get("points")
    relay_team_df["reaction_time"] = None
    relay_team_df["record_flag"] = None
    relay_team_df["source_url"] = None
    relay_team_df["source_id"] = relay_team_df["source_id"].fillna(str(default_source_id))
    relay_result_df = relay_team_df[
        ["event_name", "club_name", "relay_team_name", "lane", "heat_number", "rank_position", "seed_time_text", "seed_time_ms", "result_time_text", "result_time_ms", "points", "reaction_time", "record_flag", "status", "source_id", "source_url"]
    ]

    relay_swimmer_df["club_name"] = relay_swimmer_df["relay_team_name"].map(lambda x: infer_relay_club_name(x, club_names))
    relay_swimmer_df["athlete_name"] = relay_swimmer_df["swimmer_name"]
    relay_member_df = relay_swimmer_df[
        ["event_name", "club_name", "relay_team_name", "leg_order", "athlete_name", "gender", "age_at_event", "birth_year_estimated"]
    ]

    return {
        "relay_result": normalize_dataframe(relay_result_df, EXPECTED_COLUMNS["relay_result"], "relay_result"),
        "relay_result_member": normalize_dataframe(relay_member_df, EXPECTED_COLUMNS["relay_result_member"], "relay_result_member"),
    }



def read_inputs(args: argparse.Namespace) -> tuple[Dict[str, pd.DataFrame], Dict[str, Optional[str]]]:
    has_excel = bool(args.excel)
    has_input_dir = bool(args.input_dir)
    has_explicit_csv = any([args.club_csv, args.event_csv, args.athlete_csv, args.result_csv, args.relay_team_csv, args.relay_swimmer_csv])

    modes_used = sum([1 if has_excel else 0, 1 if has_input_dir else 0, 1 if has_explicit_csv else 0])
    if modes_used == 0:
        fail("Debes indicar --excel, --input-dir o los CSV correspondientes.")
    if modes_used > 1:
        fail("Usa solo un modo de entrada por ejecución: --excel, --input-dir o CSV explícitos.")

    if has_excel:
        excel_path = Path(args.excel)
        if not excel_path.exists():
            fail(f"No existe el archivo Excel: {excel_path}")
        info(f"Leyendo Excel: {excel_path}")
        workbook = pd.read_excel(excel_path, sheet_name=None, dtype=str)
        data = {}
        for sheet_name in ["club", "event", "athlete", "result"]:
            expected_columns = EXPECTED_COLUMNS[sheet_name]
            if sheet_name not in workbook:
                fail(f"Falta la hoja '{sheet_name}' en el Excel.")
            data[sheet_name] = normalize_dataframe(workbook[sheet_name], expected_columns, sheet_name)
        data["relay_result"] = pd.DataFrame(columns=EXPECTED_COLUMNS["relay_result"])
        data["relay_result_member"] = pd.DataFrame(columns=EXPECTED_COLUMNS["relay_result_member"])
        return data, {}

    if has_input_dir:
        input_dir = Path(args.input_dir)
        if not input_dir.exists() or not input_dir.is_dir():
            fail(f"No existe la carpeta de entrada: {input_dir}")

        metadata: Dict[str, Optional[str]] = {}
        metadata_path = input_dir / "metadata.json"
        if metadata_path.exists():
            try:
                import json
                metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
            except Exception as exc:
                info(f"No se pudo leer metadata.json: {exc}")
                metadata = {}

        required = {key: input_dir / f"{key}.csv" for key in ["club", "event", "athlete", "result"]}
        missing_csv = [key for key, path in required.items() if not path.exists()]
        if missing_csv:
            fail(f"Faltan CSV en --input-dir para: {missing_csv}")

        data = {}
        for key, csv_path in required.items():
            info(f"Leyendo CSV {key}: {csv_path}")
            df = pd.read_csv(csv_path, dtype=str, encoding="utf-8-sig")
            data[key] = normalize_dataframe(df, EXPECTED_COLUMNS[key], key)

        relay_team_path = input_dir / "relay_team.csv"
        relay_swimmer_path = input_dir / "relay_swimmer.csv"
        if relay_team_path.exists() and relay_swimmer_path.exists():
            info(f"Leyendo CSV relay_team: {relay_team_path}")
            relay_team_df = pd.read_csv(relay_team_path, dtype=str, encoding="utf-8-sig")
            info(f"Leyendo CSV relay_swimmer: {relay_swimmer_path}")
            relay_swimmer_df = pd.read_csv(relay_swimmer_path, dtype=str, encoding="utf-8-sig")
            data.update(transform_parser_relay_outputs(relay_team_df, relay_swimmer_df, data["club"], args.default_source_id))
        else:
            info("No se encontraron relay_team.csv + relay_swimmer.csv. Se continuará sin cargar relevos.")
            data["relay_result"] = pd.DataFrame(columns=EXPECTED_COLUMNS["relay_result"])
            data["relay_result_member"] = pd.DataFrame(columns=EXPECTED_COLUMNS["relay_result_member"])

        ignored_outputs = [
            input_dir / "raw_result.csv",
            input_dir / "raw_relay_team.csv",
            input_dir / "raw_relay_swimmer.csv",
            input_dir / "debug_unparsed_lines.csv",
        ]
        found_ignored = [p.name for p in ignored_outputs if p.exists()]
        if found_ignored:
            info(f"Se detectaron archivos auxiliares del parser PDF que esta versión no cargará: {found_ignored}")
        return data, metadata

    required = {"club": args.club_csv, "event": args.event_csv, "athlete": args.athlete_csv, "result": args.result_csv}
    missing_csv = [k for k, v in required.items() if not v]
    if missing_csv:
        fail(f"Faltan CSV para: {missing_csv}")

    data = {}
    for key, csv_path_str in required.items():
        csv_path = Path(csv_path_str)
        if not csv_path.exists():
            fail(f"No existe el CSV: {csv_path}")
        info(f"Leyendo CSV {key}: {csv_path}")
        df = pd.read_csv(csv_path, dtype=str, encoding="utf-8-sig")
        data[key] = normalize_dataframe(df, EXPECTED_COLUMNS[key], key)

    if args.relay_team_csv and args.relay_swimmer_csv:
        relay_team_path = Path(args.relay_team_csv)
        relay_swimmer_path = Path(args.relay_swimmer_csv)
        if not relay_team_path.exists() or not relay_swimmer_path.exists():
            fail("No existe uno de los CSV de relevos indicados.")
        info(f"Leyendo CSV relay_team: {relay_team_path}")
        relay_team_df = pd.read_csv(relay_team_path, dtype=str, encoding="utf-8-sig")
        info(f"Leyendo CSV relay_swimmer: {relay_swimmer_path}")
        relay_swimmer_df = pd.read_csv(relay_swimmer_path, dtype=str, encoding="utf-8-sig")
        data.update(transform_parser_relay_outputs(relay_team_df, relay_swimmer_df, data["club"], args.default_source_id))
    else:
        data["relay_result"] = pd.DataFrame(columns=EXPECTED_COLUMNS["relay_result"])
        data["relay_result_member"] = pd.DataFrame(columns=EXPECTED_COLUMNS["relay_result_member"])
    return data, {}





def infer_course_type_from_events(event_df: pd.DataFrame) -> str:
    if event_df is None or event_df.empty or "event_name" not in event_df.columns:
        return "unknown"
    names = " | ".join([str(x) for x in event_df["event_name"].dropna().tolist()[:20]])
    if re.search(r"\bSC\s+Meter\b", names, re.IGNORECASE):
        return "scm"
    if re.search(r"\bLC\s+Meter\b", names, re.IGNORECASE):
        return "lcm"
    return "unknown"


def derive_competition_year_from_text(*values: Optional[str]) -> Optional[int]:
    for value in values:
        if not value:
            continue
        years = re.findall(r"(19\d{2}|20\d{2}|21\d{2})", str(value))
        if years:
            return int(years[-1])
    return None


def resolve_competition_id(conn, config: Config, args: argparse.Namespace, data: Dict[str, pd.DataFrame], metadata: Dict[str, Optional[str]]) -> int:
    if config.competition_id is not None:
        return int(config.competition_id)

    competition_name = normalize_string(getattr(args, "competition_name", None)) or normalize_string(metadata.get("competition_name")) or normalize_string(metadata.get("pdf_name"))
    if competition_name is None:
        fail("No se indicó --competition-id y tampoco se pudo derivar competition_name desde metadata.json o --competition-name.")

    season_year = metadata.get("competition_year")
    try:
        season_year_int = int(season_year) if season_year is not None else None
    except Exception:
        season_year_int = None
    if season_year_int is None:
        season_year_int = derive_competition_year_from_text(competition_name, metadata.get("results_label"), metadata.get("pdf_name"))

    course_type = infer_course_type_from_events(data.get("event"))
    source_url = normalize_string(getattr(args, "competition_source_url", None)) or normalize_string(metadata.get("source_url"))
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT id
            FROM {fqtn(config.schema, 'competition')}
            WHERE LOWER(TRIM(name)) = LOWER(TRIM(%s))
              AND (%s IS NULL OR season_year = %s OR season_year IS NULL)
            ORDER BY CASE WHEN season_year = %s THEN 0 ELSE 1 END, id
            LIMIT 1;
        """, (competition_name, season_year_int, season_year_int, season_year_int))
        row = cur.fetchone()
        if row and row[0] is not None:
            competition_id = int(row[0])
            info(f"Se reutilizará competition_id={competition_id} para '{competition_name}'.")
            return competition_id

        cur.execute(f"""
            INSERT INTO {fqtn(config.schema, 'competition')} (name, season_year, course_type, status, source_id, source_url)
            VALUES (%s, %s, %s, 'finished', %s, %s)
            RETURNING id;
        """, (competition_name, season_year_int, course_type, config.default_source_id, source_url))
        competition_id = int(cur.fetchone()[0])
    conn.commit()
    info(f"Se creó competition_id={competition_id} para '{competition_name}'.")
    return competition_id


def athlete_gender_from_event_gender_sql(expr: str) -> str:
    return f"CASE LOWER(TRIM({expr})) WHEN 'women' THEN 'female' WHEN 'men' THEN 'male' ELSE NULL END"


def get_conn(config: Config):
    if HAS_PSYCOPG3:
        return psycopg.connect(host=config.host, port=config.port, dbname=config.dbname, user=config.user, password=config.password)
    if "HAS_PSYCOPG2" in globals() and HAS_PSYCOPG2:
        return psycopg2.connect(host=config.host, port=config.port, dbname=config.dbname, user=config.user, password=config.password)
    fail("No se encontró psycopg ni psycopg2 instalados.")



def execute_sql(cur, statement: str, params: Optional[Iterable] = None) -> None:
    cur.execute(statement, params or [])



def truncate_tables(conn, config: Config) -> None:
    with conn.cursor() as cur:
        if config.truncate_staging:
            info("Vaciando tablas staging...")
            execute_sql(cur, f"TRUNCATE TABLE {fqtn(config.schema, 'stg_relay_result_member')}, {fqtn(config.schema, 'stg_relay_result')}, {fqtn(config.schema, 'stg_result')}, {fqtn(config.schema, 'stg_athlete')}, {fqtn(config.schema, 'stg_event')}, {fqtn(config.schema, 'stg_club')};")
        if config.truncate_core:
            info("Vaciando tablas core de esta carga...")
            execute_sql(cur, f"TRUNCATE TABLE {fqtn(config.schema, 'relay_result_member')}, {fqtn(config.schema, 'relay_result')}, {fqtn(config.schema, 'result')}, {fqtn(config.schema, 'athlete')}, {fqtn(config.schema, 'event')}, {fqtn(config.schema, 'club')} RESTART IDENTITY CASCADE;")
    conn.commit()



def load_df_to_staging(conn, config: Config, table_key: str, df: pd.DataFrame) -> None:
    table_name = fqtn(config.schema, STAGING_TABLES[table_key])
    columns = EXPECTED_COLUMNS[table_key]
    info(f"Cargando {len(df)} filas en {table_name}...")
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False, header=False, na_rep="")
    csv_buffer.seek(0)
    joined_cols = ", ".join(columns)
    if HAS_PSYCOPG3:
        with conn.cursor() as cur:
            copy_sql = f"COPY {table_name} ({joined_cols}) FROM STDIN WITH (FORMAT CSV)"
            with cur.copy(copy_sql) as copy:
                copy.write(csv_buffer.getvalue())
    else:
        with conn.cursor() as cur:
            copy_sql = f"COPY {table_name} ({joined_cols}) FROM STDIN WITH CSV"
            cur.copy_expert(copy_sql, csv_buffer)
    conn.commit()



def load_staging(conn, config: Config, data: Dict[str, pd.DataFrame]) -> None:
    for key in ["club", "event", "athlete", "result", "relay_result", "relay_result_member"]:
        load_df_to_staging(conn, config, key, data[key])



def insert_core_club(cur, schema: str, default_source_id: int) -> None:
    cur.execute(f"""
        UPDATE {fqtn(schema, 'club')} c
        SET name = s.name,
            updated_at = NOW()
        FROM (
            SELECT DISTINCT ON ({club_match_key_sql('s.name')})
                   TRIM(s.name) AS name,
                   {club_match_key_sql('s.name')} AS club_key
            FROM {fqtn(schema, 'stg_club')} s
            WHERE NULLIF(TRIM(s.name), '') IS NOT NULL
            ORDER BY {club_match_key_sql('s.name')}, {club_name_quality_sql('s.name')} DESC, TRIM(s.name)
        ) s
        WHERE {club_match_key_sql('c.name')} = s.club_key
          AND {club_name_quality_sql('s.name')} > {club_name_quality_sql('c.name')};
    """)

    cur.execute(f"""
        INSERT INTO {fqtn(schema, 'club')} (name, short_name, city, region, source_id)
        SELECT s.name, s.short_name, s.city, s.region, s.source_id
        FROM (
            SELECT DISTINCT ON ({club_match_key_sql('s.name')})
                   TRIM(s.name) AS name,
                   NULLIF(TRIM(s.short_name), '') AS short_name,
                   NULLIF(TRIM(s.city), '') AS city,
                   NULLIF(TRIM(s.region), '') AS region,
                   COALESCE(NULLIF(TRIM(s.source_id), '')::BIGINT, %s) AS source_id,
                   {club_match_key_sql('s.name')} AS club_key
            FROM {fqtn(schema, 'stg_club')} s
            WHERE NULLIF(TRIM(s.name), '') IS NOT NULL
            ORDER BY {club_match_key_sql('s.name')}, {club_name_quality_sql('s.name')} DESC, TRIM(s.name)
        ) s
        WHERE NOT EXISTS (
            SELECT 1 FROM {fqtn(schema, 'club')} c
            WHERE {club_match_key_sql('c.name')} = s.club_key
        );
    """, (default_source_id,))



def insert_core_event(cur, schema: str, competition_id: int, default_source_id: int) -> None:
    cur.execute(f"""
        INSERT INTO {fqtn(schema, 'event')} (competition_id, event_name, stroke, distance_m, gender, age_group, round_type, source_id)
        SELECT %s, TRIM(s.event_name), LOWER(NULLIF(TRIM(s.stroke), '')), NULLIF(TRIM(s.distance_m), '')::INTEGER,
               LOWER(NULLIF(TRIM(s.gender), '')), NULLIF(TRIM(s.age_group), ''), LOWER(NULLIF(TRIM(s.round_type), '')),
               COALESCE(NULLIF(TRIM(s.source_id), '')::BIGINT, %s)
        FROM {fqtn(schema, 'stg_event')} s
        WHERE NULLIF(TRIM(s.event_name), '') IS NOT NULL
          AND NOT EXISTS (
              SELECT 1 FROM {fqtn(schema, 'event')} e
              WHERE e.competition_id = %s AND LOWER(TRIM(e.event_name)) = LOWER(TRIM(s.event_name))
          );
    """, (competition_id, default_source_id, competition_id))



def insert_core_athlete(cur, schema: str, default_source_id: int) -> None:
    cur.execute(f"""
        UPDATE {fqtn(schema, 'athlete')} at
        SET birth_year = NULLIF(TRIM(a.birth_year), '')::INTEGER
        FROM {fqtn(schema, 'stg_athlete')} a
        LEFT JOIN {fqtn(schema, 'club')} c ON {club_match_key_sql('a.club_name')} = {club_match_key_sql('c.name')}
        WHERE at.birth_year IS NULL
          AND NULLIF(TRIM(a.birth_year), '') IS NOT NULL
          AND LOWER(TRIM(at.full_name)) = LOWER(TRIM(a.full_name))
          AND (
                LOWER(NULLIF(TRIM(a.gender), '')) IS NULL
                OR at.gender IS NULL
                OR at.gender = LOWER(NULLIF(TRIM(a.gender), ''))
              )
          AND ((at.club_id IS NULL AND c.id IS NULL) OR at.club_id = c.id);
    """)

    cur.execute(f"""
        UPDATE {fqtn(schema, 'athlete')} at
        SET birth_year = NULLIF(TRIM(m.birth_year_estimated), '')::INTEGER
        FROM {fqtn(schema, 'stg_relay_result_member')} m
        LEFT JOIN {fqtn(schema, 'club')} c ON {club_match_key_sql('m.club_name')} = {club_match_key_sql('c.name')}
        WHERE at.birth_year IS NULL
          AND NULLIF(TRIM(m.birth_year_estimated), '') IS NOT NULL
          AND LOWER(TRIM(at.full_name)) = LOWER(TRIM(m.athlete_name))
          AND (
                LOWER(NULLIF(TRIM(m.gender), '')) IS NULL
                OR at.gender IS NULL
                OR at.gender = LOWER(NULLIF(TRIM(m.gender), ''))
              )
          AND ((at.club_id IS NULL AND c.id IS NULL) OR at.club_id = c.id);
    """)

    cur.execute(f"""
        INSERT INTO {fqtn(schema, 'athlete')} (full_name, gender, birth_year, club_id, source_id)
        SELECT DISTINCT TRIM(a.full_name), LOWER(NULLIF(TRIM(a.gender), '')),
               NULLIF(TRIM(a.birth_year), '')::INTEGER, c.id,
               COALESCE(NULLIF(TRIM(a.source_id), '')::BIGINT, %s)
        FROM {fqtn(schema, 'stg_athlete')} a
        LEFT JOIN {fqtn(schema, 'club')} c ON {club_match_key_sql('a.club_name')} = {club_match_key_sql('c.name')}
        WHERE NULLIF(TRIM(a.full_name), '') IS NOT NULL
          AND NOT EXISTS (
              SELECT 1 FROM {fqtn(schema, 'athlete')} at
              WHERE LOWER(TRIM(at.full_name)) = LOWER(TRIM(a.full_name))
                AND (
                    LOWER(NULLIF(TRIM(a.gender), '')) IS NULL
                    OR at.gender IS NULL
                    OR at.gender = LOWER(NULLIF(TRIM(a.gender), ''))
                )
                AND (
                    (
                        NULLIF(TRIM(a.birth_year), '') IS NOT NULL
                        AND (
                            at.birth_year = NULLIF(TRIM(a.birth_year), '')::INTEGER
                            OR (
                                at.birth_year IS NULL
                                AND ((at.club_id IS NULL AND c.id IS NULL) OR at.club_id = c.id)
                            )
                        )
                    )
                    OR (
                        NULLIF(TRIM(a.birth_year), '') IS NULL
                        AND ((at.club_id IS NULL AND c.id IS NULL) OR at.club_id = c.id)
                    )
                )
          );
    """, (default_source_id,))

    cur.execute(f"""
        INSERT INTO {fqtn(schema, 'athlete')} (full_name, gender, birth_year, club_id, source_id)
        SELECT DISTINCT TRIM(m.athlete_name), LOWER(NULLIF(TRIM(m.gender), '')),
               NULLIF(TRIM(m.birth_year_estimated), '')::INTEGER, c.id, %s
        FROM {fqtn(schema, 'stg_relay_result_member')} m
        LEFT JOIN {fqtn(schema, 'club')} c ON {club_match_key_sql('m.club_name')} = {club_match_key_sql('c.name')}
        WHERE NULLIF(TRIM(m.athlete_name), '') IS NOT NULL
          AND NOT EXISTS (
              SELECT 1 FROM {fqtn(schema, 'athlete')} at
              WHERE LOWER(TRIM(at.full_name)) = LOWER(TRIM(m.athlete_name))
                AND (
                    LOWER(NULLIF(TRIM(m.gender), '')) IS NULL
                    OR at.gender IS NULL
                    OR at.gender = LOWER(NULLIF(TRIM(m.gender), ''))
                )
                AND (
                    (
                        NULLIF(TRIM(m.birth_year_estimated), '') IS NOT NULL
                        AND (
                            at.birth_year = NULLIF(TRIM(m.birth_year_estimated), '')::INTEGER
                            OR (
                                at.birth_year IS NULL
                                AND ((at.club_id IS NULL AND c.id IS NULL) OR at.club_id = c.id)
                            )
                        )
                    )
                    OR (
                        NULLIF(TRIM(m.birth_year_estimated), '') IS NULL
                        AND ((at.club_id IS NULL AND c.id IS NULL) OR at.club_id = c.id)
                    )
                )
          );
    """, (default_source_id,))



def insert_core_result(cur, schema: str, competition_id: int, default_source_id: int) -> None:
    cur.execute(f"""
        INSERT INTO {fqtn(schema, 'result')} (
            event_id, athlete_id, club_id, rank_position,
            seed_time_text, seed_time_ms,
            result_time_text, result_time_ms,
            age_at_event, birth_year_estimated, points,
            status, source_id
        )
        SELECT e.id, a.id, c.id,
               NULLIF(TRIM(r.rank_position), '')::INTEGER,
               NULLIF(TRIM(r.seed_time_text), ''),
               NULLIF(TRIM(r.seed_time_ms), '')::BIGINT,
               NULLIF(TRIM(r.result_time_text), ''),
               NULLIF(TRIM(r.result_time_ms), '')::BIGINT,
               NULLIF(TRIM(r.age_at_event), '')::INTEGER,
               NULLIF(TRIM(r.birth_year_estimated), '')::INTEGER,
               NULLIF(TRIM(r.points), '')::NUMERIC(10,2),
               LOWER(NULLIF(TRIM(r.status), '')),
               COALESCE(NULLIF(TRIM(r.source_id), '')::BIGINT, %s)
        FROM {fqtn(schema, 'stg_result')} r
        LEFT JOIN {fqtn(schema, 'club')} c ON {club_match_key_sql('r.club_name')} = {club_match_key_sql('c.name')}
        LEFT JOIN {fqtn(schema, 'event')} e ON LOWER(TRIM(r.event_name)) = LOWER(TRIM(e.event_name)) AND e.competition_id = %s
        LEFT JOIN LATERAL (
            SELECT at.id
            FROM {fqtn(schema, 'athlete')} at
            WHERE LOWER(TRIM(at.full_name)) = LOWER(TRIM(r.athlete_name))
              AND (
                    {athlete_gender_from_event_gender_sql('e.gender')} IS NULL
                    OR at.gender IS NULL
                    OR at.gender = {athlete_gender_from_event_gender_sql('e.gender')}
                  )
            ORDER BY
                CASE
                    WHEN NULLIF(TRIM(r.birth_year_estimated), '') IS NOT NULL
                     AND at.birth_year = NULLIF(TRIM(r.birth_year_estimated), '')::INTEGER THEN 0
                    WHEN at.birth_year IS NULL THEN 1
                    ELSE 2
                END,
                CASE WHEN COALESCE(at.club_id, -1) = COALESCE(c.id, -1) THEN 0 ELSE 1 END,
                at.id
            LIMIT 1
        ) a ON TRUE
        WHERE NULLIF(TRIM(r.event_name), '') IS NOT NULL
          AND NULLIF(TRIM(r.athlete_name), '') IS NOT NULL
          AND e.id IS NOT NULL
          AND a.id IS NOT NULL
          AND NOT EXISTS (
              SELECT 1 FROM {fqtn(schema, 'result')} re
              WHERE re.event_id = e.id AND re.athlete_id = a.id
                AND COALESCE(re.result_time_ms, -1) = COALESCE(NULLIF(TRIM(r.result_time_ms), '')::BIGINT, -1)
                AND COALESCE(re.rank_position, -1) = COALESCE(NULLIF(TRIM(r.rank_position), '')::INTEGER, -1)
                AND COALESCE(re.club_id, -1) = COALESCE(c.id, -1)
                AND COALESCE(re.status, '') = COALESCE(LOWER(NULLIF(TRIM(r.status), '')), '')
          );
    """, (default_source_id, competition_id))



def insert_core_relay_result(cur, schema: str, competition_id: int, default_source_id: int) -> None:
    cur.execute(f"""
        INSERT INTO {fqtn(schema, 'relay_result')} (
            event_id, club_id, relay_team_name, lane, heat_number, rank_position,
            seed_time_text, seed_time_ms,
            result_time_text, result_time_ms,
            points, reaction_time, record_flag, status, source_id, source_url
        )
        SELECT e.id, c.id, TRIM(r.relay_team_name),
               NULLIF(TRIM(r.lane), '')::INTEGER,
               NULLIF(TRIM(r.heat_number), '')::INTEGER,
               NULLIF(TRIM(r.rank_position), '')::INTEGER,
               NULLIF(TRIM(r.seed_time_text), ''),
               NULLIF(TRIM(r.seed_time_ms), '')::BIGINT,
               NULLIF(TRIM(r.result_time_text), ''),
               NULLIF(TRIM(r.result_time_ms), '')::BIGINT,
               NULLIF(TRIM(r.points), '')::NUMERIC(10,2),
               NULLIF(TRIM(r.reaction_time), '')::NUMERIC(6,3),
               NULLIF(TRIM(r.record_flag), ''),
               LOWER(NULLIF(TRIM(r.status), '')),
               COALESCE(NULLIF(TRIM(r.source_id), '')::BIGINT, %s),
               NULLIF(TRIM(r.source_url), '')
        FROM {fqtn(schema, 'stg_relay_result')} r
        LEFT JOIN {fqtn(schema, 'club')} c ON {club_match_key_sql('r.club_name')} = {club_match_key_sql('c.name')}
        LEFT JOIN {fqtn(schema, 'event')} e ON LOWER(TRIM(r.event_name)) = LOWER(TRIM(e.event_name)) AND e.competition_id = %s
        WHERE NULLIF(TRIM(r.event_name), '') IS NOT NULL
          AND NULLIF(TRIM(r.relay_team_name), '') IS NOT NULL
          AND e.id IS NOT NULL
          AND NOT EXISTS (
              SELECT 1 FROM {fqtn(schema, 'relay_result')} rr
              WHERE rr.event_id = e.id
                AND COALESCE(rr.club_id, -1) = COALESCE(c.id, -1)
                AND LOWER(TRIM(rr.relay_team_name)) = LOWER(TRIM(r.relay_team_name))
                AND COALESCE(rr.result_time_ms, -1) = COALESCE(NULLIF(TRIM(r.result_time_ms), '')::BIGINT, -1)
                AND COALESCE(rr.rank_position, -1) = COALESCE(NULLIF(TRIM(r.rank_position), '')::INTEGER, -1)
                AND COALESCE(rr.status, '') = COALESCE(LOWER(NULLIF(TRIM(r.status), '')), '')
          );
    """, (default_source_id, competition_id))



def insert_core_relay_result_member(cur, schema: str, competition_id: int) -> None:
    cur.execute(f"""
        INSERT INTO {fqtn(schema, 'relay_result_member')} (
            relay_result_id, athlete_id, leg_order, athlete_name_raw, gender, age_at_event, birth_year_estimated
        )
        SELECT rr.id,
               a.id,
               NULLIF(TRIM(m.leg_order), '')::INTEGER,
               NULLIF(TRIM(m.athlete_name), ''),
               LOWER(NULLIF(TRIM(m.gender), '')),
               NULLIF(TRIM(m.age_at_event), '')::INTEGER,
               NULLIF(TRIM(m.birth_year_estimated), '')::INTEGER
        FROM {fqtn(schema, 'stg_relay_result_member')} m
        LEFT JOIN {fqtn(schema, 'club')} c ON {club_match_key_sql('m.club_name')} = {club_match_key_sql('c.name')}
        LEFT JOIN {fqtn(schema, 'event')} e ON LOWER(TRIM(m.event_name)) = LOWER(TRIM(e.event_name)) AND e.competition_id = %s
        LEFT JOIN {fqtn(schema, 'relay_result')} rr ON rr.event_id = e.id
             AND LOWER(TRIM(rr.relay_team_name)) = LOWER(TRIM(m.relay_team_name))
             AND ((rr.club_id IS NULL AND c.id IS NULL) OR rr.club_id = c.id)
        LEFT JOIN LATERAL (
            SELECT at.id
            FROM {fqtn(schema, 'athlete')} at
            WHERE LOWER(TRIM(at.full_name)) = LOWER(TRIM(m.athlete_name))
              AND (
                    LOWER(NULLIF(TRIM(m.gender), '')) IS NULL
                    OR at.gender IS NULL
                    OR at.gender = LOWER(NULLIF(TRIM(m.gender), ''))
                  )
            ORDER BY
                CASE
                    WHEN NULLIF(TRIM(m.birth_year_estimated), '') IS NOT NULL
                     AND at.birth_year = NULLIF(TRIM(m.birth_year_estimated), '')::INTEGER THEN 0
                    WHEN at.birth_year IS NULL THEN 1
                    ELSE 2
                END,
                CASE WHEN COALESCE(at.club_id, -1) = COALESCE(c.id, -1) THEN 0 ELSE 1 END,
                at.id
            LIMIT 1
        ) a ON TRUE
        WHERE NULLIF(TRIM(m.event_name), '') IS NOT NULL
          AND NULLIF(TRIM(m.relay_team_name), '') IS NOT NULL
          AND NULLIF(TRIM(m.athlete_name), '') IS NOT NULL
          AND rr.id IS NOT NULL
          AND NULLIF(TRIM(m.leg_order), '') IS NOT NULL
          AND NOT EXISTS (
              SELECT 1 FROM {fqtn(schema, 'relay_result_member')} rrm
              WHERE rrm.relay_result_id = rr.id
                AND rrm.leg_order = NULLIF(TRIM(m.leg_order), '')::INTEGER
          );
    """, (competition_id,))



def load_core(conn, config: Config) -> None:
    info("Insertando datos desde staging hacia core...")
    with conn.cursor() as cur:
        insert_core_club(cur, config.schema, config.default_source_id)
        insert_core_event(cur, config.schema, config.competition_id, config.default_source_id)
        insert_core_athlete(cur, config.schema, config.default_source_id)
        insert_core_result(cur, config.schema, config.competition_id, config.default_source_id)
        insert_core_relay_result(cur, config.schema, config.competition_id, config.default_source_id)
        insert_core_relay_result_member(cur, config.schema, config.competition_id)
    conn.commit()



def fetch_one_value(cur, query: str, params=None) -> int:
    cur.execute(query, params or [])
    row = cur.fetchone()
    return int(row[0]) if row and row[0] is not None else 0



def print_counts(conn, config: Config, staging_data: Dict[str, pd.DataFrame]) -> None:
    with conn.cursor() as cur:
        club_count = fetch_one_value(cur, f"SELECT COUNT(*) FROM {fqtn(config.schema, 'club')};")
        event_count = fetch_one_value(cur, f"SELECT COUNT(*) FROM {fqtn(config.schema, 'event')} WHERE competition_id = %s;", (config.competition_id,))
        athlete_count = fetch_one_value(cur, f"SELECT COUNT(*) FROM {fqtn(config.schema, 'athlete')};")
        result_count = fetch_one_value(cur, f"SELECT COUNT(*) FROM {fqtn(config.schema, 'result')} r JOIN {fqtn(config.schema, 'event')} e ON r.event_id = e.id WHERE e.competition_id = %s;", (config.competition_id,))
        relay_result_count = fetch_one_value(cur, f"SELECT COUNT(*) FROM {fqtn(config.schema, 'relay_result')} rr JOIN {fqtn(config.schema, 'event')} e ON rr.event_id = e.id WHERE e.competition_id = %s;", (config.competition_id,))
        relay_member_count = fetch_one_value(cur, f"SELECT COUNT(*) FROM {fqtn(config.schema, 'relay_result_member')} rrm JOIN {fqtn(config.schema, 'relay_result')} rr ON rrm.relay_result_id = rr.id JOIN {fqtn(config.schema, 'event')} e ON rr.event_id = e.id WHERE e.competition_id = %s;", (config.competition_id,))
    print("\n=== RESUMEN DE CARGA ===")
    print(f"Filas leídas club:               {len(staging_data['club'])}")
    print(f"Filas leídas event:              {len(staging_data['event'])}")
    print(f"Filas leídas athlete:            {len(staging_data['athlete'])}")
    print(f"Filas leídas result:             {len(staging_data['result'])}")
    print(f"Filas leídas relay_result:       {len(staging_data['relay_result'])}")
    print(f"Filas leídas relay_result_member:{len(staging_data['relay_result_member'])}")
    print("---")
    print(f"club total core:                 {club_count}")
    print(f"event comp {config.competition_id}:              {event_count}")
    print(f"athlete total core:              {athlete_count}")
    print(f"result comp {config.competition_id}:             {result_count}")
    print(f"relay_result comp {config.competition_id}:       {relay_result_count}")
    print(f"relay_member comp {config.competition_id}:       {relay_member_count}")



def print_validations(conn, config: Config) -> None:
    validation_queries = {
        "athletes_sin_club_match": f"""
            SELECT COUNT(*) FROM {fqtn(config.schema, 'stg_athlete')} a
            LEFT JOIN {fqtn(config.schema, 'club')} c ON {club_match_key_sql('a.club_name')} = {club_match_key_sql('c.name')}
            WHERE NULLIF(TRIM(a.club_name), '') IS NOT NULL AND c.id IS NULL;
        """,
        "results_sin_event_match": f"""
            SELECT COUNT(*) FROM {fqtn(config.schema, 'stg_result')} r
            LEFT JOIN {fqtn(config.schema, 'event')} e ON LOWER(TRIM(r.event_name)) = LOWER(TRIM(e.event_name)) AND e.competition_id = {config.competition_id}
            WHERE NULLIF(TRIM(r.event_name), '') IS NOT NULL AND e.id IS NULL;
        """,
        "results_sin_athlete_match": f"""
            SELECT COUNT(*) FROM {fqtn(config.schema, 'stg_result')} r
            LEFT JOIN {fqtn(config.schema, 'club')} c ON {club_match_key_sql('r.club_name')} = {club_match_key_sql('c.name')}
            LEFT JOIN {fqtn(config.schema, 'event')} e ON LOWER(TRIM(r.event_name)) = LOWER(TRIM(e.event_name)) AND e.competition_id = {config.competition_id}
            LEFT JOIN LATERAL (
                SELECT at.id
                FROM {fqtn(config.schema, 'athlete')} at
                WHERE LOWER(TRIM(at.full_name)) = LOWER(TRIM(r.athlete_name))
                  AND (
                        {athlete_gender_from_event_gender_sql('e.gender')} IS NULL
                        OR at.gender IS NULL
                        OR at.gender = {athlete_gender_from_event_gender_sql('e.gender')}
                      )
                ORDER BY
                    CASE
                        WHEN NULLIF(TRIM(r.birth_year_estimated), '') IS NOT NULL
                         AND at.birth_year = NULLIF(TRIM(r.birth_year_estimated), '')::INTEGER THEN 0
                        WHEN at.birth_year IS NULL THEN 1
                        ELSE 2
                    END,
                    CASE WHEN COALESCE(at.club_id, -1) = COALESCE(c.id, -1) THEN 0 ELSE 1 END,
                    at.id
                LIMIT 1
            ) a ON TRUE
            WHERE NULLIF(TRIM(r.athlete_name), '') IS NOT NULL AND a.id IS NULL;
        """,
        "results_sin_club_match": f"""
            SELECT COUNT(*) FROM {fqtn(config.schema, 'stg_result')} r
            LEFT JOIN {fqtn(config.schema, 'club')} c ON {club_match_key_sql('r.club_name')} = {club_match_key_sql('c.name')}
            WHERE NULLIF(TRIM(r.club_name), '') IS NOT NULL AND c.id IS NULL;
        """,
        "relay_results_sin_event_match": f"""
            SELECT COUNT(*) FROM {fqtn(config.schema, 'stg_relay_result')} r
            LEFT JOIN {fqtn(config.schema, 'event')} e ON LOWER(TRIM(r.event_name)) = LOWER(TRIM(e.event_name)) AND e.competition_id = {config.competition_id}
            WHERE NULLIF(TRIM(r.event_name), '') IS NOT NULL AND e.id IS NULL;
        """,
        "relay_results_sin_club_match": f"""
            SELECT COUNT(*) FROM {fqtn(config.schema, 'stg_relay_result')} r
            LEFT JOIN {fqtn(config.schema, 'club')} c ON {club_match_key_sql('r.club_name')} = {club_match_key_sql('c.name')}
            WHERE NULLIF(TRIM(r.club_name), '') IS NOT NULL AND c.id IS NULL;
        """,
        "relay_members_sin_relay_match": f"""
            SELECT COUNT(*) FROM {fqtn(config.schema, 'stg_relay_result_member')} m
            LEFT JOIN {fqtn(config.schema, 'club')} c ON {club_match_key_sql('m.club_name')} = {club_match_key_sql('c.name')}
            LEFT JOIN {fqtn(config.schema, 'event')} e ON LOWER(TRIM(m.event_name)) = LOWER(TRIM(e.event_name)) AND e.competition_id = {config.competition_id}
            LEFT JOIN {fqtn(config.schema, 'relay_result')} rr ON rr.event_id = e.id
                 AND LOWER(TRIM(rr.relay_team_name)) = LOWER(TRIM(m.relay_team_name))
                 AND ((rr.club_id IS NULL AND c.id IS NULL) OR rr.club_id = c.id)
            WHERE NULLIF(TRIM(m.event_name), '') IS NOT NULL
              AND NULLIF(TRIM(m.relay_team_name), '') IS NOT NULL
              AND rr.id IS NULL;
        """,
        "relay_members_sin_athlete_match": f"""
            SELECT COUNT(*) FROM {fqtn(config.schema, 'stg_relay_result_member')} m
            LEFT JOIN {fqtn(config.schema, 'club')} c ON {club_match_key_sql('m.club_name')} = {club_match_key_sql('c.name')}
            LEFT JOIN LATERAL (
                SELECT at.id
                FROM {fqtn(config.schema, 'athlete')} at
                WHERE LOWER(TRIM(at.full_name)) = LOWER(TRIM(m.athlete_name))
                  AND (
                        LOWER(NULLIF(TRIM(m.gender), '')) IS NULL
                        OR at.gender IS NULL
                        OR at.gender = LOWER(NULLIF(TRIM(m.gender), ''))
                      )
                ORDER BY
                    CASE
                        WHEN NULLIF(TRIM(m.birth_year_estimated), '') IS NOT NULL
                         AND at.birth_year = NULLIF(TRIM(m.birth_year_estimated), '')::INTEGER THEN 0
                        WHEN at.birth_year IS NULL THEN 1
                        ELSE 2
                    END,
                    CASE WHEN COALESCE(at.club_id, -1) = COALESCE(c.id, -1) THEN 0 ELSE 1 END,
                    at.id
                LIMIT 1
            ) a ON TRUE
            WHERE NULLIF(TRIM(m.athlete_name), '') IS NOT NULL AND a.id IS NULL;
        """,
    }
    print("\n=== VALIDACIONES ===")
    with conn.cursor() as cur:
        for label, query in validation_queries.items():
            count = fetch_one_value(cur, query)
            print(f"{label}: {count}")



def main() -> None:
    args = parse_args()
    config = Config(host=args.host, port=args.port, dbname=args.dbname, user=args.user, password=args.password, schema=args.schema, truncate_staging=args.truncate_staging, truncate_core=args.truncate_core, competition_id=args.competition_id, default_source_id=args.default_source_id)
    data, metadata = read_inputs(args)
    aliases = load_club_aliases(args.club_alias_csv)
    apply_club_aliases(data, aliases)
    candidates_path = (Path(args.input_dir) / "club_alias_candidates.csv") if args.input_dir else None
    write_club_alias_candidates(data, candidates_path, reference_names=aliases.values())
    conn = get_conn(config)
    try:
        config.competition_id = resolve_competition_id(conn, config, args, data, metadata)
        truncate_tables(conn, config)
        load_staging(conn, config, data)
        load_core(conn, config)
        print_counts(conn, config, data)
        print_validations(conn, config)
        print("\n[OK] Pipeline v0.3.7 completado.")
    except Exception as exc:
        conn.rollback()
        fail(f"El pipeline falló: {exc}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
