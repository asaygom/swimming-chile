#!/usr/bin/env python3
"""
run_pipeline_first_competition_v0_3.py

Mejoras v0.3:
- Canoniza result_time_text a formato MM:SS,CC
- Deriva result_time_ms desde result_time_text cuando falta
- Endurece la deduplicación de result con status y club_id
- Usa status más seguro, sin asumir valid por defecto
- Mejora validaciones finales de calidad
"""

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
NON_TIME_TEXT_STATUSES = {"DNS", "DNF", "DSQ", "SCRATCH", "NT", "NS", "DQ", "VALID", "UNKNOWN"}


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
    parser = argparse.ArgumentParser(description="Carga una primera competencia desde Excel o CSV hacia PostgreSQL.")
    parser.add_argument("--excel", type=str, help="Ruta al archivo Excel con hojas club, event, athlete y result")
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

    parser.add_argument("--truncate-staging", action="store_true", help="Vacía staging antes de cargar")
    parser.add_argument("--truncate-core", action="store_true", help="Vacía core.club/event/athlete/result antes de cargar")
    parser.add_argument("--competition-id", type=int, default=1, help="competition_id a usar en core.event")
    parser.add_argument("--default-source-id", type=int, default=1, help="source_id por defecto si llega vacío")
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



def normalize_swim_time_text(value):
    value = normalize_string(value)
    if value is None:
        return None

    upper = value.upper()
    if upper in NON_TIME_TEXT_STATUSES:
        return upper

    value = value.replace("0 days ", "").replace(",", ".").strip()

    m = re.fullmatch(r"(?:(\d{1,2}):(\d{1,2}):(\d{2})|(\d{1,3}):(\d{2})|(\d{1,5}))(?:\.(\d{1,6}))?", value)
    if not m:
        return value

    if m.group(1) is not None:
        hours = int(m.group(1))
        minutes = int(m.group(2))
        seconds = int(m.group(3))
        total_minutes = hours * 60 + minutes
    elif m.group(4) is not None:
        total_minutes = int(m.group(4))
        seconds = int(m.group(5))
    else:
        total_seconds = int(m.group(6))
        total_minutes = total_seconds // 60
        seconds = total_seconds % 60

    frac = (m.group(7) or "").ljust(6, "0")
    hundredths = int(round(int(frac) / 10000)) if frac else 0

    if hundredths == 100:
        hundredths = 0
        seconds += 1
        if seconds == 60:
            seconds = 0
            total_minutes += 1

    return f"{total_minutes:02d}:{seconds:02d},{hundredths:02d}"


def parse_swim_time_to_ms(value):
    normalized = normalize_swim_time_text(value)
    if normalized is None:
        return None

    upper = normalized.upper()
    if upper in NON_TIME_TEXT_STATUSES:
        return None

    m = re.fullmatch(r"(\d+):(\d{2}),(\d{2})", normalized)
    if not m:
        return None

    minutes = int(m.group(1))
    seconds = int(m.group(2))
    hundredths = int(m.group(3))
    return ((minutes * 60) + seconds) * 1000 + hundredths * 10



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
        if upper == "SCRATCH":
            return "scratch"
        if upper in {"NT", "NS", "UNKNOWN"}:
            return "unknown"

        if parse_swim_time_to_ms(rtt) is not None:
            return "valid"

    return "unknown"



def normalize_dataframe(df: pd.DataFrame, expected_columns: List[str], table_key: str) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]

    missing = [c for c in expected_columns if c not in df.columns]
    extra = [c for c in df.columns if c not in expected_columns]
    if missing:
        fail(f"Faltan columnas {missing} en hoja/archivo {table_key}. Columnas encontradas: {list(df.columns)}")
    if extra:
        info(f"En {table_key} se ignorarán columnas extra: {extra}")
        df = df[[c for c in df.columns if c in expected_columns]]

    df = df[expected_columns]

    for col in df.columns:
        df[col] = df[col].where(pd.notna(df[col]), None)
        df[col] = df[col].map(normalize_string)

    if table_key == "event":
        for col in ["stroke", "gender", "round_type"]:
            df[col] = df[col].map(normalize_controlled_lower)

    if table_key == "athlete":
        df["gender"] = df["gender"].map(normalize_controlled_lower)

    if table_key == "result":
        df["result_time_text"] = df["result_time_text"].map(normalize_swim_time_text)

        df["result_time_ms"] = df["result_time_ms"].map(
            lambda x: str(int(float(x))) if normalize_string(x) is not None and str(x).strip() not in {"", "nan", "NaN"} and str(x).strip().replace(".", "", 1).isdigit() else None
        )

        missing_ms_mask = df["result_time_ms"].isna()
        df.loc[missing_ms_mask, "result_time_ms"] = (
            df.loc[missing_ms_mask, "result_time_text"].map(parse_swim_time_to_ms)
        )

        df["result_time_ms"] = df["result_time_ms"].map(lambda x: str(int(x)) if x is not None and str(x) != "" else None)
        df["status"] = [normalize_result_status(st, tt) for st, tt in zip(df["status"], df["result_time_text"])]

        invalid_valid_mask = (df["status"] == "valid") & (df["result_time_ms"].isna())
        if invalid_valid_mask.any():
            info(f"En result se marcarán como unknown {int(invalid_valid_mask.sum())} filas con status valid pero sin tiempo parseable.")
            df.loc[invalid_valid_mask, "status"] = "unknown"

    return df


def read_inputs(args: argparse.Namespace) -> Dict[str, pd.DataFrame]:
    has_excel = bool(args.excel)
    has_csv = any([args.club_csv, args.event_csv, args.athlete_csv, args.result_csv])

    if not has_excel and not has_csv:
        fail("Debes indicar --excel o los cuatro CSV.")

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

    required = {
        "club": args.club_csv,
        "event": args.event_csv,
        "athlete": args.athlete_csv,
        "result": args.result_csv,
    }
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
        return psycopg.connect(
            host=config.host,
            port=config.port,
            dbname=config.dbname,
            user=config.user,
            password=config.password,
        )
    if "HAS_PSYCOPG2" in globals() and HAS_PSYCOPG2:
        return psycopg2.connect(
            host=config.host,
            port=config.port,
            dbname=config.dbname,
            user=config.user,
            password=config.password,
        )
    fail("No se encontró psycopg ni psycopg2 instalados.")


def execute_sql(cur, statement: str, params: Optional[Iterable] = None) -> None:
    cur.execute(statement, params or [])


def truncate_tables(conn, config: Config) -> None:
    with conn.cursor() as cur:
        if config.truncate_staging:
            info("Vaciando tablas staging...")
            execute_sql(
                cur,
                f"TRUNCATE TABLE {fqtn(config.schema, 'stg_result')}, "
                f"{fqtn(config.schema, 'stg_athlete')}, "
                f"{fqtn(config.schema, 'stg_event')}, "
                f"{fqtn(config.schema, 'stg_club')};"
            )

        if config.truncate_core:
            info("Vaciando tablas core de esta carga...")
            execute_sql(
                cur,
                f"TRUNCATE TABLE {fqtn(config.schema, 'result')}, "
                f"{fqtn(config.schema, 'athlete')}, "
                f"{fqtn(config.schema, 'event')}, "
                f"{fqtn(config.schema, 'club')} RESTART IDENTITY CASCADE;"
            )
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
            copy_sql = f"COPY {table_name} ({', '.join(columns)}) FROM STDIN WITH (FORMAT CSV)"
            with cur.copy(copy_sql) as copy:
                copy.write(csv_buffer.getvalue())
    else:
        with conn.cursor() as cur:
            copy_sql = f"COPY {table_name} ({', '.join(columns)}) FROM STDIN WITH CSV"
            cur.copy_expert(copy_sql, csv_buffer)
    conn.commit()


def load_staging(conn, config: Config, data: Dict[str, pd.DataFrame]) -> None:
    for key in ["club", "event", "athlete", "result"]:
        load_df_to_staging(conn, config, key, data[key])


def insert_core_club(cur, schema: str, default_source_id: int) -> None:
    cur.execute(
        f'''
        INSERT INTO {fqtn(schema, 'club')} (
            name,
            short_name,
            city,
            region,
            source_id
        )
        SELECT DISTINCT
            TRIM(s.name) AS name,
            NULLIF(TRIM(s.short_name), '') AS short_name,
            NULLIF(TRIM(s.city), '') AS city,
            NULLIF(TRIM(s.region), '') AS region,
            COALESCE(NULLIF(TRIM(s.source_id), '')::BIGINT, %s) AS source_id
        FROM {fqtn(schema, 'stg_club')} s
        WHERE NULLIF(TRIM(s.name), '') IS NOT NULL
          AND NOT EXISTS (
              SELECT 1
              FROM {fqtn(schema, 'club')} c
              WHERE LOWER(TRIM(c.name)) = LOWER(TRIM(s.name))
          );
        ''',
        (default_source_id,),
    )


def insert_core_event(cur, schema: str, competition_id: int, default_source_id: int) -> None:
    cur.execute(
        f'''
        INSERT INTO {fqtn(schema, 'event')} (
            competition_id,
            event_name,
            stroke,
            distance_m,
            gender,
            age_group,
            round_type,
            source_id
        )
        SELECT
            %s AS competition_id,
            TRIM(s.event_name) AS event_name,
            LOWER(NULLIF(TRIM(s.stroke), '')) AS stroke,
            NULLIF(TRIM(s.distance_m), '')::INTEGER AS distance_m,
            LOWER(NULLIF(TRIM(s.gender), '')) AS gender,
            NULLIF(TRIM(s.age_group), '') AS age_group,
            LOWER(NULLIF(TRIM(s.round_type), '')) AS round_type,
            COALESCE(NULLIF(TRIM(s.source_id), '')::BIGINT, %s) AS source_id
        FROM {fqtn(schema, 'stg_event')} s
        WHERE NULLIF(TRIM(s.event_name), '') IS NOT NULL
          AND NOT EXISTS (
              SELECT 1
              FROM {fqtn(schema, 'event')} e
              WHERE e.competition_id = %s
                AND LOWER(TRIM(e.event_name)) = LOWER(TRIM(s.event_name))
          );
        ''',
        (competition_id, default_source_id, competition_id),
    )


def insert_core_athlete(cur, schema: str, default_source_id: int) -> None:
    cur.execute(
        f'''
        INSERT INTO {fqtn(schema, 'athlete')} (
            full_name,
            gender,
            club_id,
            source_id
        )
        SELECT DISTINCT
            TRIM(a.full_name) AS full_name,
            LOWER(NULLIF(TRIM(a.gender), '')) AS gender,
            c.id AS club_id,
            COALESCE(NULLIF(TRIM(a.source_id), '')::BIGINT, %s) AS source_id
        FROM {fqtn(schema, 'stg_athlete')} a
        LEFT JOIN {fqtn(schema, 'club')} c
            ON LOWER(TRIM(a.club_name)) = LOWER(TRIM(c.name))
        WHERE NULLIF(TRIM(a.full_name), '') IS NOT NULL
          AND NOT EXISTS (
              SELECT 1
              FROM {fqtn(schema, 'athlete')} at
              WHERE LOWER(TRIM(at.full_name)) = LOWER(TRIM(a.full_name))
                AND (
                    (at.club_id IS NULL AND c.id IS NULL)
                    OR at.club_id = c.id
                )
          );
        ''',
        (default_source_id,),
    )



def insert_core_result(cur, schema: str, default_source_id: int) -> None:
    cur.execute(
        f"""
        INSERT INTO {fqtn(schema, 'result')} (
            event_id,
            athlete_id,
            club_id,
            rank_position,
            result_time_text,
            result_time_ms,
            status,
            source_id
        )
        SELECT
            e.id AS event_id,
            a.id AS athlete_id,
            c.id AS club_id,
            NULLIF(TRIM(r.rank_position), '')::INTEGER AS rank_position,
            NULLIF(TRIM(r.result_time_text), '') AS result_time_text,
            NULLIF(TRIM(r.result_time_ms), '')::BIGINT AS result_time_ms,
            LOWER(COALESCE(NULLIF(TRIM(r.status), ''), 'unknown')) AS status,
            COALESCE(NULLIF(TRIM(r.source_id), '')::BIGINT, %s) AS source_id
        FROM {fqtn(schema, 'stg_result')} r
        LEFT JOIN {fqtn(schema, 'event')} e
            ON LOWER(TRIM(r.event_name)) = LOWER(TRIM(e.event_name))
        LEFT JOIN {fqtn(schema, 'athlete')} a
            ON LOWER(TRIM(r.athlete_name)) = LOWER(TRIM(a.full_name))
        LEFT JOIN {fqtn(schema, 'club')} c
            ON LOWER(TRIM(r.club_name)) = LOWER(TRIM(c.name))
        WHERE NULLIF(TRIM(r.event_name), '') IS NOT NULL
          AND NULLIF(TRIM(r.athlete_name), '') IS NOT NULL
          AND e.id IS NOT NULL
          AND a.id IS NOT NULL
          AND NOT EXISTS (
              SELECT 1
              FROM {fqtn(schema, 'result')} re
              WHERE re.event_id = e.id
                AND re.athlete_id = a.id
                AND COALESCE(re.club_id, -1) = COALESCE(c.id, -1)
                AND COALESCE(re.result_time_ms, -1) = COALESCE(NULLIF(TRIM(r.result_time_ms), '')::BIGINT, -1)
                AND COALESCE(re.rank_position, -1) = COALESCE(NULLIF(TRIM(r.rank_position), '')::INTEGER, -1)
                AND LOWER(COALESCE(re.status, 'unknown')) = LOWER(COALESCE(NULLIF(TRIM(r.status), ''), 'unknown'))
          );
        """,
        (default_source_id,),
    )


def load_core(conn, config: Config) -> None:
    info("Insertando datos desde staging hacia core...")
    with conn.cursor() as cur:
        insert_core_club(cur, config.schema, config.default_source_id)
        insert_core_event(cur, config.schema, config.competition_id, config.default_source_id)
        insert_core_athlete(cur, config.schema, config.default_source_id)
        insert_core_result(cur, config.schema, config.default_source_id)
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
        result_count = fetch_one_value(
            cur,
            f'''
            SELECT COUNT(*)
            FROM {fqtn(config.schema, 'result')} r
            JOIN {fqtn(config.schema, 'event')} e ON r.event_id = e.id
            WHERE e.competition_id = %s;
            ''',
            (config.competition_id,),
        )

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
        "athletes_sin_club_match": f'''
            SELECT COUNT(*)
            FROM {fqtn(config.schema, 'stg_athlete')} a
            LEFT JOIN {fqtn(config.schema, 'club')} c
                ON LOWER(TRIM(a.club_name)) = LOWER(TRIM(c.name))
            WHERE NULLIF(TRIM(a.club_name), '') IS NOT NULL
              AND c.id IS NULL;
        ''',
        "results_sin_event_match": f'''
            SELECT COUNT(*)
            FROM {fqtn(config.schema, 'stg_result')} r
            LEFT JOIN {fqtn(config.schema, 'event')} e
                ON LOWER(TRIM(r.event_name)) = LOWER(TRIM(e.event_name))
            WHERE NULLIF(TRIM(r.event_name), '') IS NOT NULL
              AND e.id IS NULL;
        ''',
        "results_sin_athlete_match": f'''
            SELECT COUNT(*)
            FROM {fqtn(config.schema, 'stg_result')} r
            LEFT JOIN {fqtn(config.schema, 'athlete')} a
                ON LOWER(TRIM(r.athlete_name)) = LOWER(TRIM(a.full_name))
            WHERE NULLIF(TRIM(r.athlete_name), '') IS NOT NULL
              AND a.id IS NULL;
        ''',
        "results_sin_club_match": f'''
            SELECT COUNT(*)
            FROM {fqtn(config.schema, 'stg_result')} r
            LEFT JOIN {fqtn(config.schema, 'club')} c
                ON LOWER(TRIM(r.club_name)) = LOWER(TRIM(c.name))
            WHERE NULLIF(TRIM(r.club_name), '') IS NOT NULL
              AND c.id IS NULL;
        ''',
        "results_formato_excel_detectados": f'''
            SELECT COUNT(*)
            FROM {fqtn(config.schema, 'result')}
            WHERE result_time_text ~ '^\d{{2}}:\d{{2}}:\d{{2}}\.\d+';
        ''',
        "results_valid_sin_ms": f'''
            SELECT COUNT(*)
            FROM {fqtn(config.schema, 'result')}
            WHERE status = 'valid'
              AND result_time_text IS NOT NULL
              AND result_time_ms IS NULL;
        ''',
        "results_no_valid_con_ms": f'''
            SELECT COUNT(*)
            FROM {fqtn(config.schema, 'result')}
            WHERE status <> 'valid'
              AND result_time_ms IS NOT NULL;
        ''',
        "results_texto_fuera_formato_canonico": f'''
            SELECT COUNT(*)
            FROM {fqtn(config.schema, 'result')}
            WHERE result_time_text IS NOT NULL
              AND UPPER(result_time_text) NOT IN ('DNS', 'DNF', 'DSQ', 'SCRATCH', 'NT', 'NS', 'DQ', 'VALID', 'UNKNOWN')
              AND result_time_text !~ '^\d+:\d{2},\d{2}$';
        ''',
    }

    print("\n=== VALIDACIONES ===")
    with conn.cursor() as cur:
        for label, query in validation_queries.items():
            count = fetch_one_value(cur, query)
            print(f"{label}: {count}")


def main() -> None:
    args = parse_args()
    config = Config(
        host=args.host,
        port=args.port,
        dbname=args.dbname,
        user=args.user,
        password=args.password,
        schema=args.schema,
        truncate_staging=args.truncate_staging,
        truncate_core=args.truncate_core,
        competition_id=args.competition_id,
        default_source_id=args.default_source_id,
    )

    data = read_inputs(args)
    conn = get_conn(config)

    try:
        truncate_tables(conn, config)
        load_staging(conn, config, data)
        load_core(conn, config)
        print_counts(conn, config, data)
        print_validations(conn, config)
        print("\n[OK] Pipeline v0.3 completado.")
    except Exception as exc:
        conn.rollback()
        fail(f"El pipeline falló: {exc}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
