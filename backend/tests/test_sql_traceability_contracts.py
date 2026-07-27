from pathlib import Path


BACKEND_DIR = Path(__file__).resolve().parents[1]
SCHEMA_SQL = BACKEND_DIR / "sql" / "schema.sql"
MIGRATION_SQL = BACKEND_DIR / "sql" / "migrations" / "001_traceability_idempotency.sql"
COMPETITION_SCOPE_MIGRATION_SQL = BACKEND_DIR / "sql" / "migrations" / "002_competition_scope.sql"
EXPECTED_POINTS_MIGRATION_SQL = BACKEND_DIR / "sql" / "migrations" / "003_expected_points.sql"
ATHLETE_CURRENT_CLUB_MIGRATION_SQL = BACKEND_DIR / "sql" / "migrations" / "004_athlete_current_club_view.sql"
CURRENT_CLUB_POLICY_MIGRATION_SQL = (
    BACKEND_DIR / "sql" / "migrations" / "009_competition_current_club_policy.sql"
)
LOCAL_CLUB_METADATA_MIGRATION_SQL = (
    BACKEND_DIR / "sql" / "migrations" / "010_local_club_metadata.sql"
)


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


def test_schema_declares_competition_scope():
    sql = normalized_sql(SCHEMA_SQL)

    assert "competition_scope text check" in sql
    assert "create index idx_competition_scope on competition(competition_scope)" in sql


def test_schema_declares_expected_points_columns():
    sql = normalized_sql(SCHEMA_SQL)

    assert "create table result" in sql
    assert sql.count("expected_points numeric(10,2)") >= 2


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


def test_competition_scope_migration_adds_column_constraint_and_index():
    sql = normalized_sql(COMPETITION_SCOPE_MIGRATION_SQL)

    for sql_fragment in [
        "alter table competition add column if not exists competition_scope text",
        "add constraint chk_competition_scope check",
        "create index if not exists idx_competition_scope on competition(competition_scope)",
    ]:
        assert sql_fragment in sql


def test_expected_points_migration_adds_result_and_relay_columns():
    sql = normalized_sql(EXPECTED_POINTS_MIGRATION_SQL)

    for sql_fragment in [
        "alter table result add column if not exists expected_points numeric(10,2)",
        "alter table relay_result add column if not exists expected_points numeric(10,2)",
        "update result set expected_points = case rank_position",
        "update relay_result set expected_points = case rank_position",
    ]:
        assert sql_fragment in sql


def test_schema_declares_athlete_current_club_view():
    sql = normalized_sql(SCHEMA_SQL)

    assert "affects_current_club boolean" in sql
    assert "create or replace view athlete_current_club as" in sql
    assert "from result r" in sql
    assert "from relay_result_member rrm" in sql
    assert "row_number() over" in sql
    assert (
        sql.count(
            "coalesce(c.affects_current_club, c.competition_scope = 'fchmn_local')"
        )
        == 2
    )


def test_athlete_current_club_migration_creates_latest_observation_view():
    sql = normalized_sql(ATHLETE_CURRENT_CLUB_MIGRATION_SQL)

    for sql_fragment in [
        "create or replace view athlete_current_club as",
        "union all",
        "from result r",
        "from relay_result_member rrm",
        "order by competition_date desc nulls last",
    ]:
        assert sql_fragment in sql


def test_current_club_policy_migration_is_idempotent_and_filters_both_branches():
    sql = normalized_sql(CURRENT_CLUB_POLICY_MIGRATION_SQL)

    assert (
        "alter table competition add column if not exists affects_current_club boolean"
        in sql
    )
    assert "create or replace view athlete_current_club as" in sql
    assert (
        sql.count(
            "coalesce(c.affects_current_club, c.competition_scope = 'fchmn_local')"
        )
        == 2
    )


def test_schema_declares_nullable_club_country_and_locality_metadata():
    sql = normalized_sql(SCHEMA_SQL)

    assert "country_code text" in sql
    assert "is_local boolean" in sql
    assert "iso 3166-1 alpha-3" in sql


def test_local_club_metadata_migration_is_idempotent_and_repairs_both_result_sources():
    sql = normalized_sql(LOCAL_CLUB_METADATA_MIGRATION_SQL)

    for sql_fragment in [
        "alter table club add column if not exists country_code text",
        "alter table club add column if not exists is_local boolean",
        "from result r",
        "from relay_result rr",
        "c.competition_scope in ('fchmn_local', 'fechida_master')",
        "country_code = coalesce(club.country_code, 'chi')",
        "is_local = coalesce(club.is_local, true)",
        "iso 3166-1 alpha-3",
    ]:
        assert sql_fragment in sql

    assert "union" in sql
    assert "club.country_code is null or club.is_local is null" in sql
