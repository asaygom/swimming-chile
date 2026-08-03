import os
from contextlib import contextmanager
import psycopg
from psycopg.rows import dict_row
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "natacion_chile")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")
DATABASE_URL = os.getenv("DATABASE_URL")


def get_connection_string():
    if DATABASE_URL:
        return DATABASE_URL
    return f"host={DB_HOST} port={DB_PORT} dbname={DB_NAME} user={DB_USER} password={DB_PASSWORD}"


@contextmanager
def get_db_connection(row_factory=dict_row):
    """Los routers usan filas como diccionario. `row_factory` existe para el
    publicador de sembrado, que se comparte con la CLI y lee por posicion."""
    conn = psycopg.connect(get_connection_string(), row_factory=row_factory)
    try:
        yield conn
    finally:
        conn.close()


def is_database_ready() -> bool:
    try:
        with psycopg.connect(
            get_connection_string(),
            connect_timeout=3,
        ) as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1")
                cursor.fetchone()
    except Exception:
        return False
    return True
