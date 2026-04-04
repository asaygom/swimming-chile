#!/usr/bin/env python3
from __future__ import annotations

import argparse
import io
import re
import sys
from dataclasses import dataclass
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
    "athlete": ["full_name", "gender", "club_name", "source_id"],
    "result": ["event_name", "athlete_name", "club_name", "rank_position", "result_time_text", "result_time_ms", "status", "source_id"],
}

STAGING_TABLES = {
    "club": "stg_club",
    "event": "stg_event",
    "athlete": "stg_athlete",
    "result": "stg_result",
}

STATUS_VALUES = {"valid", "dns", "dnf", "dsq", "scratch", "unknown"}
TEXT_STATUSES = {"DNS", "DNF", "DSQ", "SCRATCH", "NT", "NS", "DQ", "VALID", "UNKNOWN"}

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
    competition_id: int
    default_source_id: int

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Carga una competencia desde Excel o CSV hacia PostgreSQL. También puede consumir la carpeta de salida del parser PDF.")
    parser.add_argument("--excel", type=str, help="Ruta al archivo Excel con hojas club, event, athlete y result")
    parser.add_argument("--input-dir", type=str, help="Carpeta con club.csv, event.csv, athlete.csv y result.csv generados por el parser PDF")
    parser.add_argument("--club-csv", type=str, help="Ruta al CSV de club")
    parser.add_argument("--event-csv", type=str, help="Ruta al CSV de event")
    parser.add_argument("--athlete-csv", type=str, help="Ruta al CSV de athlete")
    parser.add_argument("--result-csv", type=str, help="Ruta al CSV de result")
    parser.add_argument("--host", type=str, default="localhost")
    parser.add_argument("--port", type=int, default=5432)
    parser.add_argument("--dbname", type=str, default="natacion_chile")
    parser.add_argument("--user", type=str, required=True)
    parser.add_argument("--password", type=str, required=True)
    parser.add_argument("--schema", type=str, default="core")
    parser.add_argument("--truncate-staging", action="store_true")
    parser.add_argument("--truncate-core", action="store_true")
    parser.add_argument("--competition-id", type=int, default=1)
    parser.add_argument("--default-source-id", type=int, default=1)
    return parser.parse_args()

def fail(msg: str) -> None:
    print(f"[ERROR] {msg}", file=sys.stderr)
    sys.exit(1)

def info(msg: str) -> None:
    print(f"[INFO] {msg}")

def fqtn(schema: str, table: str) -> str:
    return f"{schema}.{table}"

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

    # separar prefijo X del tiempo base
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
        if upper == "DNS": return "dns"
        if upper == "DNF": return "dnf"
        if upper in {"DSQ", "DQ"}: return "dsq"
        if upper in {"NT", "NS"}: return "unknown"
    return "unknown"

def normalize_dataframe(df: pd.DataFrame, expected_columns: List[str], table_key: str) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
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
    if table_key == "event":
        df["stroke"] = df["stroke"].map(normalize_stroke)
        df["gender"] = df["gender"].map(normalize_event_gender)
        df["round_type"] = df["round_type"].map(normalize_controlled_lower)
    if table_key == "athlete":
        df["gender"] = df["gender"].map(normalize_athlete_gender)
    if table_key == "result":
        df["result_time_text"] = df["result_time_text"].map(normalize_swim_time_text)

        normalized_ms = []
        normalized_rank = []
        normalized_status = []

        for tt, ms, rk, st in zip(df["result_time_text"], df["result_time_ms"], df["rank_position"], df["status"]):
            ms_norm = normalize_string(ms)
            if ms_norm is None:
                derived = derive_result_time_ms(tt)
                normalized_ms.append(str(derived) if derived is not None else None)
            else:
                normalized_ms.append(ms_norm)

            if isinstance(tt, str) and tt.upper().startswith("X"):
                normalized_rank.append(None)
                normalized_status.append("unknown")   # o "exhibition" si amplías catálogo
            else:
                normalized_rank.append(normalize_string(rk))
                normalized_status.append(normalize_result_status(st, tt))

        df["result_time_ms"] = normalized_ms
        df["rank_position"] = normalized_rank
        df["status"] = normalized_status
    return df

def read_inputs(args: argparse.Namespace) -> Dict[str, pd.DataFrame]:
    has_excel = bool(args.excel)
    has_input_dir = bool(args.input_dir)
    has_explicit_csv = any([args.club_csv, args.event_csv, args.athlete_csv, args.result_csv])

    modes_used = sum([1 if has_excel else 0, 1 if has_input_dir else 0, 1 if has_explicit_csv else 0])
    if modes_used == 0:
        fail("Debes indicar --excel, --input-dir o los cuatro CSV.")
    if modes_used > 1:
        fail("Usa solo un modo de entrada por ejecución: --excel, --input-dir o los cuatro CSV.")

    if has_excel:
        excel_path = Path(args.excel)
        if not excel_path.exists():
            fail(f"No existe el archivo Excel: {excel_path}")
        info(f"Leyendo Excel: {excel_path}")
        workbook = pd.read_excel(excel_path, sheet_name=None, dtype=str)
        data = {}
        for sheet_name, expected_columns in EXPECTED_COLUMNS.items():
            if sheet_name not in workbook:
                fail(f"Falta la hoja '{sheet_name}' en el Excel.")
            data[sheet_name] = normalize_dataframe(workbook[sheet_name], expected_columns, sheet_name)
        return data

    if has_input_dir:
        input_dir = Path(args.input_dir)
        if not input_dir.exists() or not input_dir.is_dir():
            fail(f"No existe la carpeta de entrada: {input_dir}")

        required = {key: input_dir / f"{key}.csv" for key in EXPECTED_COLUMNS}
        missing_csv = [key for key, path in required.items() if not path.exists()]
        if missing_csv:
            fail(f"Faltan CSV en --input-dir para: {missing_csv}")

        ignored_outputs = [
            input_dir / "relay_team.csv",
            input_dir / "relay_swimmer.csv",
            input_dir / "raw_result.csv",
            input_dir / "raw_relay_team.csv",
            input_dir / "raw_relay_swimmer.csv",
            input_dir / "debug_unparsed_lines.csv",
        ]
        found_ignored = [p.name for p in ignored_outputs if p.exists()]
        if found_ignored:
            info(f"Se detectaron archivos auxiliares del parser PDF que esta versión no cargará: {found_ignored}")

        data = {}
        for key, csv_path in required.items():
            info(f"Leyendo CSV {key}: {csv_path}")
            df = pd.read_csv(csv_path, dtype=str, encoding="utf-8-sig")
            data[key] = normalize_dataframe(df, EXPECTED_COLUMNS[key], key)
        return data

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
    return data

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
            execute_sql(cur, f"TRUNCATE TABLE {fqtn(config.schema, 'stg_result')}, {fqtn(config.schema, 'stg_athlete')}, {fqtn(config.schema, 'stg_event')}, {fqtn(config.schema, 'stg_club')};")
        if config.truncate_core:
            info("Vaciando tablas core de esta carga...")
            execute_sql(cur, f"TRUNCATE TABLE {fqtn(config.schema, 'result')}, {fqtn(config.schema, 'athlete')}, {fqtn(config.schema, 'event')}, {fqtn(config.schema, 'club')} RESTART IDENTITY CASCADE;")
    conn.commit()

def load_df_to_staging(conn, config: Config, table_key: str, df: pd.DataFrame) -> None:
    table_name = fqtn(config.schema, STAGING_TABLES[table_key])
    columns = EXPECTED_COLUMNS[table_key]
    info(f"Cargando {len(df)} filas en {table_name}...")
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False, header=False, na_rep="")
    csv_buffer.seek(0)
    if HAS_PSYCOPG3:
        with conn.cursor() as cur:
            copy_sql = f"COPY {table_name} ({", ".join(columns)}) FROM STDIN WITH (FORMAT CSV)"
            with cur.copy(copy_sql) as copy:
                copy.write(csv_buffer.getvalue())
    else:
        with conn.cursor() as cur:
            copy_sql = f"COPY {table_name} ({", ".join(columns)}) FROM STDIN WITH CSV"
            cur.copy_expert(copy_sql, csv_buffer)
    conn.commit()

def load_staging(conn, config: Config, data: Dict[str, pd.DataFrame]) -> None:
    for key in ["club", "event", "athlete", "result"]:
        load_df_to_staging(conn, config, key, data[key])

def insert_core_club(cur, schema: str, default_source_id: int) -> None:
    cur.execute(f"""
        INSERT INTO {fqtn(schema, "club")} (name, short_name, city, region, source_id)
        SELECT DISTINCT TRIM(s.name), NULLIF(TRIM(s.short_name), ''), NULLIF(TRIM(s.city), ''), NULLIF(TRIM(s.region), ''),
               COALESCE(NULLIF(TRIM(s.source_id), '')::BIGINT, %s)
        FROM {fqtn(schema, "stg_club")} s
        WHERE NULLIF(TRIM(s.name), '') IS NOT NULL
          AND NOT EXISTS (
              SELECT 1 FROM {fqtn(schema, "club")} c
              WHERE LOWER(TRIM(c.name)) = LOWER(TRIM(s.name))
          );
    """, (default_source_id,))

def insert_core_event(cur, schema: str, competition_id: int, default_source_id: int) -> None:
    cur.execute(f"""
        INSERT INTO {fqtn(schema, "event")} (competition_id, event_name, stroke, distance_m, gender, age_group, round_type, source_id)
        SELECT %s, TRIM(s.event_name), LOWER(NULLIF(TRIM(s.stroke), '')), NULLIF(TRIM(s.distance_m), '')::INTEGER,
               LOWER(NULLIF(TRIM(s.gender), '')), NULLIF(TRIM(s.age_group), ''), LOWER(NULLIF(TRIM(s.round_type), '')),
               COALESCE(NULLIF(TRIM(s.source_id), '')::BIGINT, %s)
        FROM {fqtn(schema, "stg_event")} s
        WHERE NULLIF(TRIM(s.event_name), '') IS NOT NULL
          AND NOT EXISTS (
              SELECT 1 FROM {fqtn(schema, "event")} e
              WHERE e.competition_id = %s AND LOWER(TRIM(e.event_name)) = LOWER(TRIM(s.event_name))
          );
    """, (competition_id, default_source_id, competition_id))

def insert_core_athlete(cur, schema: str, default_source_id: int) -> None:
    cur.execute(f"""
        INSERT INTO {fqtn(schema, "athlete")} (full_name, gender, club_id, source_id)
        SELECT DISTINCT TRIM(a.full_name), LOWER(NULLIF(TRIM(a.gender), '')), c.id,
               COALESCE(NULLIF(TRIM(a.source_id), '')::BIGINT, %s)
        FROM {fqtn(schema, "stg_athlete")} a
        LEFT JOIN {fqtn(schema, "club")} c ON LOWER(TRIM(a.club_name)) = LOWER(TRIM(c.name))
        WHERE NULLIF(TRIM(a.full_name), '') IS NOT NULL
          AND NOT EXISTS (
              SELECT 1 FROM {fqtn(schema, "athlete")} at
              WHERE LOWER(TRIM(at.full_name)) = LOWER(TRIM(a.full_name))
                AND ((at.club_id IS NULL AND c.id IS NULL) OR at.club_id = c.id)
          );
    """, (default_source_id,))

def insert_core_result(cur, schema: str, competition_id: int, default_source_id: int) -> None:
    cur.execute(f"""
        INSERT INTO {fqtn(schema, "result")} (event_id, athlete_id, club_id, rank_position, result_time_text, result_time_ms, status, source_id)
        SELECT e.id, a.id, c.id, NULLIF(TRIM(r.rank_position), '')::INTEGER, NULLIF(TRIM(r.result_time_text), ''),
               NULLIF(TRIM(r.result_time_ms), '')::BIGINT, LOWER(NULLIF(TRIM(r.status), '')),
               COALESCE(NULLIF(TRIM(r.source_id), '')::BIGINT, %s)
        FROM {fqtn(schema, "stg_result")} r
        LEFT JOIN {fqtn(schema, "club")} c ON LOWER(TRIM(r.club_name)) = LOWER(TRIM(c.name))
        LEFT JOIN {fqtn(schema, "event")} e ON LOWER(TRIM(r.event_name)) = LOWER(TRIM(e.event_name)) AND e.competition_id = %s
        LEFT JOIN {fqtn(schema, "athlete")} a ON LOWER(TRIM(r.athlete_name)) = LOWER(TRIM(a.full_name))
             AND ((a.club_id IS NULL AND c.id IS NULL) OR a.club_id = c.id)
        WHERE NULLIF(TRIM(r.event_name), '') IS NOT NULL
          AND NULLIF(TRIM(r.athlete_name), '') IS NOT NULL
          AND e.id IS NOT NULL
          AND a.id IS NOT NULL
          AND NOT EXISTS (
              SELECT 1 FROM {fqtn(schema, "result")} re
              WHERE re.event_id = e.id AND re.athlete_id = a.id
                AND COALESCE(re.result_time_ms, -1) = COALESCE(NULLIF(TRIM(r.result_time_ms), '')::BIGINT, -1)
                AND COALESCE(re.rank_position, -1) = COALESCE(NULLIF(TRIM(r.rank_position), '')::INTEGER, -1)
                AND COALESCE(re.club_id, -1) = COALESCE(c.id, -1)
                AND COALESCE(re.status, '') = COALESCE(LOWER(NULLIF(TRIM(r.status), '')), '')
          );
    """, (default_source_id, competition_id))

def load_core(conn, config: Config) -> None:
    info("Insertando datos desde staging hacia core...")
    with conn.cursor() as cur:
        insert_core_club(cur, config.schema, config.default_source_id)
        insert_core_event(cur, config.schema, config.competition_id, config.default_source_id)
        insert_core_athlete(cur, config.schema, config.default_source_id)
        insert_core_result(cur, config.schema, config.competition_id, config.default_source_id)
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
    print("\n=== RESUMEN DE CARGA ===")
    print(f"Filas leídas club:    {len(staging_data['club'])}")
    print(f"Filas leídas event:   {len(staging_data['event'])}")
    print(f"Filas leídas athlete: {len(staging_data['athlete'])}")
    print(f"Filas leídas result:  {len(staging_data['result'])}")
    print("---")
    print(f"club total core:      {club_count}")
    print(f"event comp {config.competition_id}:   {event_count}")
    print(f"athlete total core:   {athlete_count}")
    print(f"result comp {config.competition_id}:  {result_count}")

def print_validations(conn, config: Config) -> None:
    validation_queries = {
        "athletes_sin_club_match": f"""
            SELECT COUNT(*) FROM {fqtn(config.schema, "stg_athlete")} a
            LEFT JOIN {fqtn(config.schema, "club")} c ON LOWER(TRIM(a.club_name)) = LOWER(TRIM(c.name))
            WHERE NULLIF(TRIM(a.club_name), '') IS NOT NULL AND c.id IS NULL;
        """,
        "results_sin_event_match": f"""
            SELECT COUNT(*) FROM {fqtn(config.schema, "stg_result")} r
            LEFT JOIN {fqtn(config.schema, "event")} e ON LOWER(TRIM(r.event_name)) = LOWER(TRIM(e.event_name)) AND e.competition_id = {config.competition_id}
            WHERE NULLIF(TRIM(r.event_name), '') IS NOT NULL AND e.id IS NULL;
        """,
        "results_sin_athlete_match": f"""
            SELECT COUNT(*) FROM {fqtn(config.schema, "stg_result")} r
            LEFT JOIN {fqtn(config.schema, "club")} c ON LOWER(TRIM(r.club_name)) = LOWER(TRIM(c.name))
            LEFT JOIN {fqtn(config.schema, "athlete")} a ON LOWER(TRIM(r.athlete_name)) = LOWER(TRIM(a.full_name))
                 AND ((a.club_id IS NULL AND c.id IS NULL) OR a.club_id = c.id)
            WHERE NULLIF(TRIM(r.athlete_name), '') IS NOT NULL AND a.id IS NULL;
        """,
        "results_sin_club_match": f"""
            SELECT COUNT(*) FROM {fqtn(config.schema, "stg_result")} r
            LEFT JOIN {fqtn(config.schema, "club")} c ON LOWER(TRIM(r.club_name)) = LOWER(TRIM(c.name))
            WHERE NULLIF(TRIM(r.club_name), '') IS NOT NULL AND c.id IS NULL;
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
    data = read_inputs(args)
    conn = get_conn(config)
    try:
        truncate_tables(conn, config)
        load_staging(conn, config, data)
        load_core(conn, config)
        print_counts(conn, config, data)
        print_validations(conn, config)
        print("\n[OK] Pipeline v0.3 fixed completado.")
    except Exception as exc:
        conn.rollback()
        fail(f"El pipeline falló: {exc}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()