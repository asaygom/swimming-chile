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
MEET_PROGRAM_MIGRATION_SQL = (
    BACKEND_DIR / "sql" / "migrations" / "011_meet_program_publications.sql"
)
MEET_PROGRAM_PARSER_REVISION_SQL = (
    BACKEND_DIR / "sql" / "migrations" / "012_meet_program_parser_revision.sql"
)
MEET_PROGRAM_ESTIMATED_TIMES_SQL = (
    BACKEND_DIR / "sql" / "migrations" / "013_meet_program_estimated_times.sql"
)
MEET_PROGRAM_SEGMENTS_SQL = (
    BACKEND_DIR / "sql" / "migrations" / "014_meet_program_segments.sql"
)
LIVE_ANNOUNCEMENTS_SQL = (
    BACKEND_DIR / "sql" / "migrations" / "016_live_announcements.sql"
)
LIVE_HEAT_HISTORY_SQL = (
    BACKEND_DIR / "sql" / "migrations" / "017_live_heat_movement_history.sql"
)
LIVE_ANNOUNCEMENT_HISTORY_SQL = (
    BACKEND_DIR / "sql" / "migrations" / "018_live_announcement_event_history.sql"
)
LIVE_BRANDING_SQL = (
    BACKEND_DIR / "sql" / "migrations" / "019_competition_live_branding.sql"
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


def test_schema_declares_versioned_meet_program_without_core_identity_links():
    sql = normalized_sql(SCHEMA_SQL)

    for fragment in [
        "create table meet_program_publication",
        "create table meet_program_entry",
        "competition_id bigint not null references competition(id)",
        "source_checksum_sha256 text not null",
        "publication_id bigint not null references meet_program_publication(id)",
        "unique (publication_id, session_number, event_number, heat_number, lane)",
        "create unique index ux_meet_program_one_published_per_segment",
    ]:
        assert fragment in sql

    meet_program_sql = sql.split("create table meet_program_publication", 1)[1].split(
        "-- table: event", 1
    )[0]
    assert "athlete_id" not in meet_program_sql
    assert "club_id" not in meet_program_sql


def test_meet_program_migration_is_numbered_idempotent_and_revision_safe():
    sql = normalized_sql(MEET_PROGRAM_MIGRATION_SQL)

    for fragment in [
        "create table if not exists meet_program_publication",
        "create table if not exists meet_program_entry",
        "unique (competition_id, source_checksum_sha256)",
        "create unique index if not exists ux_meet_program_one_published_per_competition",
        "where status = 'published'",
        "unique (publication_id, session_number, event_number, heat_number, lane)",
    ]:
        assert fragment in sql

    assert "source_url text" in sql
    assert "athlete_id" not in sql
    assert "club_id" not in sql


def test_meet_program_parser_revision_migration_replaces_two_column_identity():
    schema_sql = normalized_sql(SCHEMA_SQL)
    migration_sql = normalized_sql(MEET_PROGRAM_PARSER_REVISION_SQL)

    identity = (
        "constraint uq_meet_program_publication_source_parser "
        "unique (competition_id, source_checksum_sha256, parser_version)"
    )
    assert identity in schema_sql
    assert identity in migration_sql
    assert "from pg_constraint" in migration_sql
    assert "array_agg(attribute_row.attname::text order by key_row.ordinality)" in migration_sql
    assert "drop constraint" in migration_sql
    assert "ux_meet_program_one_published_per_competition" not in migration_sql


def test_meet_program_estimated_times_migration_is_idempotent_and_constrained():
    schema_sql = normalized_sql(SCHEMA_SQL)
    migration_sql = normalized_sql(MEET_PROGRAM_ESTIMATED_TIMES_SQL)

    assert "estimated_start_time text" in schema_sql
    assert "add column if not exists estimated_start_time text" in migration_sql
    assert "chk_meet_program_estimated_start_time" in migration_sql
    assert "^(?:[01][0-9]|2[0-3]):[0-5][0-9]$" in migration_sql


def test_meet_program_segments_scope_publication_and_allow_lane_zero():
    schema_sql = normalized_sql(SCHEMA_SQL)
    migration_sql = normalized_sql(MEET_PROGRAM_SEGMENTS_SQL)

    for fragment in [
        "stage_number integer not null default 1",
        "pool_role text not null default 'main'",
        "scheduled_date date",
        "on meet_program_publication(competition_id, stage_number, pool_role)",
        "check (lane >= 0)",
    ]:
        assert fragment in schema_sql

    for fragment in [
        "add column if not exists stage_number integer not null default 1",
        "add column if not exists pool_role text not null default 'main'",
        "add column if not exists scheduled_date date",
        "ux_meet_program_one_published_per_segment",
        "where status = 'published'",
        "check (lane >= 0)",
    ]:
        assert fragment in migration_sql


def test_live_announcement_schema_declares_competition_admin_foundation():
    schema_sql = normalized_sql(SCHEMA_SQL)
    migration_sql = normalized_sql(LIVE_ANNOUNCEMENTS_SQL)

    for fragment in [
        "create table auth.user_competition_role",
        "role text not null check (role in ('competition_admin'))",
        "unique (user_id, competition_id, role)",
        "create table auth.admin_session",
        "token_hash text not null unique",
        "revoked_at timestamptz",
    ]:
        assert fragment.replace("create table ", "create table if not exists ") in migration_sql
    assert "auth.user_account" not in schema_sql
    assert "live_announcement" not in schema_sql


def test_live_announcement_schema_keeps_tokens_hashed_and_sessions_revocable():
    schema_sql = normalized_sql(SCHEMA_SQL)
    migration_sql = normalized_sql(LIVE_ANNOUNCEMENTS_SQL)

    assert "chk_admin_session_token_hash" in migration_sql
    assert "token_hash ~ '^[0-9a-f]{64}$'" in migration_sql
    assert "revoked_at is null or revoked_at >= created_at" in migration_sql
    assert "token text" not in migration_sql
    assert "ux_admin_session_one_per_user" in migration_sql


def test_live_announcement_schema_is_audited_soft_deleted_and_independently_versioned():
    schema_sql = normalized_sql(SCHEMA_SQL)
    migration_sql = normalized_sql(LIVE_ANNOUNCEMENTS_SQL)

    assert "create table" in migration_sql and "live_announcement" in migration_sql
    assert "display_mode in ('fullscreen', 'ticker')" in migration_sql
    assert "revision bigint not null default 1 check (revision > 0)" in migration_sql
    for actor in ["created_by_user_id", "updated_by_user_id", "activated_by_user_id", "deleted_by_user_id"]:
        assert actor in migration_sql
    assert "deleted_at timestamptz" in migration_sql
    assert "is_active and deleted_at is null" in migration_sql
    assert "ux_live_announcement_one_active_per_competition" in migration_sql
    assert "display_mode <> 'ticker' or length(trim(message)) <= 240" in migration_sql


def test_live_heat_history_is_append_only_scoped_and_fingerprint_audited():
    sql = normalized_sql(LIVE_HEAT_HISTORY_SQL)
    schema_sql = normalized_sql(SCHEMA_SQL)

    for fragment in [
        "create table if not exists core.live_heat_movement",
        "competition_id bigint not null references core.competition(id)",
        "resulting_publication_id bigint not null references core.meet_program_publication(id)",
        "previous_publication_id bigint references core.meet_program_publication(id)",
        "operator_session_fingerprint text not null",
        "resulting_revision bigint not null check (resulting_revision > 0)",
        "occurred_at timestamptz not null default now()",
        "idx_live_heat_movement_competition_occurred",
    ]:
        assert fragment in sql
    assert "cookie" not in sql and "secret" not in sql
    assert "create table live_heat_movement" in schema_sql
    assert "idx_live_heat_movement_competition_occurred" in schema_sql


def test_live_announcement_history_is_migration_only_and_actor_audited():
    sql = normalized_sql(LIVE_ANNOUNCEMENT_HISTORY_SQL)
    schema_sql = normalized_sql(SCHEMA_SQL)

    for fragment in [
        "create table if not exists core.live_announcement_event",
        "competition_id bigint not null references core.competition(id)",
        "announcement_id bigint not null references core.live_announcement(id)",
        "actor_user_id bigint not null references auth.user_account(id)",
        "event_type text not null check",
        "'automatic_deactivate'",
        "message text not null",
        "display_mode text not null",
        "is_active boolean not null",
        "is_deleted boolean not null",
        "occurred_at timestamptz not null default now()",
        "idx_live_announcement_event_competition_occurred",
    ]:
        assert fragment in sql
    assert "live_announcement_event" not in schema_sql


def test_live_branding_is_migration_only_revisioned_and_audited():
    sql = normalized_sql(LIVE_BRANDING_SQL)
    schema_sql = normalized_sql(SCHEMA_SQL)

    for fragment in [
        "create table if not exists core.competition_live_branding",
        "competition_id bigint primary key references core.competition(id)",
        "logo_bytes bytea",
        "mime_type text",
        "width integer",
        "height integer",
        "sha256 text",
        "revision bigint not null default 1 check (revision > 0)",
        "updated_by_user_id bigint not null references auth.user_account(id)",
        "deleted_by_user_id bigint references auth.user_account(id)",
        "deleted_at timestamptz",
    ]:
        assert fragment in sql
    assert "competition_live_branding" not in schema_sql
