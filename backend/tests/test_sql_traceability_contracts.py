from pathlib import Path


BACKEND_DIR = Path(__file__).resolve().parents[1]
SCHEMA_SQL = BACKEND_DIR / "sql" / "schema.sql"
MIGRATION_SQL = BACKEND_DIR / "sql" / "migrations" / "001_traceability_idempotency.sql"


def normalized_sql(path: Path) -> str:
    return " ".join(path.read_text(encoding="utf-8").lower().split())


def test_schema_declares_traceability_tables():
    sql = normalized_sql(SCHEMA_SQL)

    for table_name in ["source_document", "load_run", "validation_issue"]:
        assert f"create table {table_name}" in sql


def test_schema_keeps_idempotency_unique_indexes():
    sql = normalized_sql(SCHEMA_SQL)

    for index_name in [
        "ux_source_document_checksum_sha256",
        "ux_source_document_source_url",
        "ux_event_competition_event_name",
        "ux_result_observed_identity",
        "ux_relay_result_observed_identity",
    ]:
        assert f"create unique index {index_name}" in sql


def test_migration_keeps_phase_2_tables_and_unique_indexes():
    sql = normalized_sql(MIGRATION_SQL)

    for sql_fragment in [
        "create table if not exists source_document",
        "create table if not exists load_run",
        "create table if not exists validation_issue",
        "create unique index if not exists ux_source_document_checksum_sha256",
        "create unique index if not exists ux_result_observed_identity",
        "create unique index if not exists ux_relay_result_observed_identity",
    ]:
        assert sql_fragment in sql
