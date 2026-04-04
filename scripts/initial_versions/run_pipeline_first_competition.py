#!/usr/bin/env python3
"""
run_pipeline_first_competition.py

Pipeline v0.1 para probar funcionamiento con una primera competencia:
1. Lee un Excel o cuatro CSV
2. Carga datos a tablas staging (core.stg_*)
3. Inserta datos en tablas core (club, event, athlete, result) (habiendo creado previamente source, pool, competition)
4. Ejecuta validaciones básicas
5. Muestra un resumen final

Uso con Excel:
python run_pipeline_first_competition.py --excel "/ruta/primera carga.xlsx" --user postgres --password TU_PASSWORD

Uso con CSV:
python run_pipeline_first_competition.py \
  --club-csv "/ruta/stg_club.csv" \
  --event-csv "/ruta/stg_event.csv" \
  --athlete-csv "/ruta/stg_athlete.csv" \
  --result-csv "/ruta/stg_result.csv" \
  --user postgres --password TU_PASSWORD

Opcionales:
  --host localhost
  --port 5432
  --dbname natacion_chile
  --schema core
  --truncate-staging
  --truncate-core
  --competition-id 1
  --default-source-id 1
"""

from __future__ import annotations

import argparse
import io
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
    parser = argparse.ArgumentParser(description="Carga una competencia desde Excel o CSV hacia PostgreSQL.")
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


def normalize_dataframe(df: pd.DataFrame, expected_columns: List[str]) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]

    missing = [c for c in expected_columns if c not in df.columns]
    extra = [c for c in df.columns if c not in expected_columns]
    if missing:
        fail(f"Faltan columnas {missing}. Columnas encontradas: {list(df.columns)}")
    if extra:
        info(f"Se ignorarán columnas extra: {extra}")
        df = df[[c for c in df.columns if c in expected_columns]]

    df = df[expected_columns]

    for col in df.columns:
        df[col] = df[col].where(pd.notna(df[col]), None)
        df[col] = df[col].map(lambda x: x.strip() if isinstance(x, str) else x)

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
            data[sheet_name] = normalize_dataframe(workbook[sheet_name], expected_columns)
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
        data[key] = normalize_dataframe(df, EXPECTED_COLUMNS[key])

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
    if 'HAS_PSYCOPG2' in globals() and HAS_PSYCOPG2:
        return psycopg2.connect(
            host=config.host,
            port=config.port,
            dbname=config.dbname,
            user=config.user,
            password=config.password,
        )
    fail("No se encontró psycopg ni psycopg2 instalados.")


def fqtn(schema: str, table: str) -> str:
    return f"{schema}.{table}"


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

    df = df.astype("object")
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
        f"""
        INSERT INTO {fqtn(schema, 'club')} (
            name,
            short_name,
            city,
            region,
            source_id
        )
        SELECT DISTINCT
            TRIM(name) AS name,
            NULLIF(TRIM(short_name), '') AS short_name,
            NULLIF(TRIM(city), '') AS city,
            NULLIF(TRIM(region), '') AS region,
            COALESCE(NULLIF(TRIM(source_id), '')::BIGINT, %s) AS source_id
        FROM {fqtn(schema, 'stg_club')}
        WHERE NULLIF(TRIM(name), '') IS NOT NULL;
        """,
        (default_source_id,),
    )


def insert_core_event(cur, schema: str, competition_id: int, default_source_id: int) -> None:
    cur.execute(
        f"""
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
            TRIM(event_name) AS event_name,
            LOWER(NULLIF(TRIM(stroke), '')) AS stroke,
            NULLIF(TRIM(distance_m), '')::INTEGER AS distance_m,
            LOWER(NULLIF(TRIM(gender), '')) AS gender,
            NULLIF(TRIM(age_group), '') AS age_group,
            LOWER(NULLIF(TRIM(round_type), '')) AS round_type,
            COALESCE(NULLIF(TRIM(source_id), '')::BIGINT, %s) AS source_id
        FROM {fqtn(schema, 'stg_event')}
        WHERE NULLIF(TRIM(event_name), '') IS NOT NULL;
        """,
        (competition_id, default_source_id),
    )


def insert_core_athlete(cur, schema: str, default_source_id: int) -> None:
    cur.execute(
        f"""
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
        WHERE NULLIF(TRIM(a.full_name), '') IS NOT NULL;
        """,
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
            LOWER(NULLIF(TRIM(r.status), '')) AS status,
            COALESCE(NULLIF(TRIM(r.source_id), '')::BIGINT, %s) AS source_id
        FROM {fqtn(schema, 'stg_result')} r
        LEFT JOIN {fqtn(schema, 'event')} e
            ON LOWER(TRIM(r.event_name)) = LOWER(TRIM(e.event_name))
        LEFT JOIN {fqtn(schema, 'athlete')} a
            ON LOWER(TRIM(r.athlete_name)) = LOWER(TRIM(a.full_name))
        LEFT JOIN {fqtn(schema, 'club')} c
            ON LOWER(TRIM(r.club_name)) = LOWER(TRIM(c.name))
        WHERE NULLIF(TRIM(r.event_name), '') IS NOT NULL
          AND NULLIF(TRIM(r.athlete_name), '') IS NOT NULL;
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


def fetch_one_value(cur, query: str) -> int:
    cur.execute(query)
    row = cur.fetchone()
    return int(row[0]) if row and row[0] is not None else 0


def print_counts(conn, config: Config) -> None:
    with conn.cursor() as cur:
        club_count = fetch_one_value(cur, f"SELECT COUNT(*) FROM {fqtn(config.schema, 'club')};")
        event_count = fetch_one_value(cur, f"SELECT COUNT(*) FROM {fqtn(config.schema, 'event')};")
        athlete_count = fetch_one_value(cur, f"SELECT COUNT(*) FROM {fqtn(config.schema, 'athlete')};")
        result_count = fetch_one_value(cur, f"SELECT COUNT(*) FROM {fqtn(config.schema, 'result')};")

    print("\n=== RESUMEN DE CARGA ===")
    print(f"club:    {club_count}")
    print(f"event:   {event_count}")
    print(f"athlete: {athlete_count}")
    print(f"result:  {result_count}")


def print_validations(conn, config: Config) -> None:
    validation_queries = {
        "athletes_sin_club_match": f"""
            SELECT COUNT(*)
            FROM {fqtn(config.schema, 'stg_athlete')} a
            LEFT JOIN {fqtn(config.schema, 'club')} c
                ON LOWER(TRIM(a.club_name)) = LOWER(TRIM(c.name))
            WHERE NULLIF(TRIM(a.club_name), '') IS NOT NULL
              AND c.id IS NULL;
        """,
        "results_sin_event_match": f"""
            SELECT COUNT(*)
            FROM {fqtn(config.schema, 'stg_result')} r
            LEFT JOIN {fqtn(config.schema, 'event')} e
                ON LOWER(TRIM(r.event_name)) = LOWER(TRIM(e.event_name))
            WHERE NULLIF(TRIM(r.event_name), '') IS NOT NULL
              AND e.id IS NULL;
        """,
        "results_sin_athlete_match": f"""
            SELECT COUNT(*)
            FROM {fqtn(config.schema, 'stg_result')} r
            LEFT JOIN {fqtn(config.schema, 'athlete')} a
                ON LOWER(TRIM(r.athlete_name)) = LOWER(TRIM(a.full_name))
            WHERE NULLIF(TRIM(r.athlete_name), '') IS NOT NULL
              AND a.id IS NULL;
        """,
        "results_sin_club_match": f"""
            SELECT COUNT(*)
            FROM {fqtn(config.schema, 'stg_result')} r
            LEFT JOIN {fqtn(config.schema, 'club')} c
                ON LOWER(TRIM(r.club_name)) = LOWER(TRIM(c.name))
            WHERE NULLIF(TRIM(r.club_name), '') IS NOT NULL
              AND c.id IS NULL;
        """,
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
        print_counts(conn, config)
        print_validations(conn, config)
        print("\n[OK] Pipeline completado.")
    except Exception as exc:
        conn.rollback()
        fail(f"El pipeline falló: {exc}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
