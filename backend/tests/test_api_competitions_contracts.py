from pathlib import Path


BACKEND_DIR = Path(__file__).resolve().parents[1]
COMPETITIONS_ROUTER = BACKEND_DIR / "api" / "routers" / "competitions.py"
GOVERNING_BODY_MIGRATION = BACKEND_DIR / "sql" / "migrations" / "005_competition_governing_body.sql"


def normalized_source(path: Path) -> str:
    return " ".join(path.read_text(encoding="utf-8").lower().split())


def test_competitions_api_filters_by_scope_and_governing_body():
    source = normalized_source(COMPETITIONS_ROUTER)

    assert "competition_scope: optional[str]" in source
    assert "governing_body: optional[str]" in source
    assert "and competition_scope = %s" in source
    assert "and governing_body_code = %s" in source
    assert "governing_body_scope_fallbacks" not in source
    assert "governing_body_code is null and competition_scope" not in source
    assert "competition_scope," in source
    assert "governing_body_code" in source
    assert "governing_body_name" in source
    assert "organizer" in source
    assert "coalesce(c.source_url, latest_doc.source_url) as source_url" in source
    assert "join core.source_document sd on sd.id = lr.source_document_id" in source
    assert "r.seed_time_text" in source
    assert "r.seed_time_ms" in source
    assert "rr.seed_time_text" in source
    assert "rr.seed_time_ms" in source


def test_competitions_api_exposes_filter_options_from_database():
    source = normalized_source(COMPETITIONS_ROUTER)

    assert '@router.get("/filter-options")' in COMPETITIONS_ROUTER.read_text(encoding="utf-8")
    assert "timeframe: optional[str]" in source
    assert "start_date >= current_date" in source
    assert "start_date < current_date" in source
    assert "select distinct competition_scope" in source
    assert "select distinct governing_body_code, governing_body_name" in source
    assert "when 'sudamericano_master'" not in source
    assert '"governing_bodies": governing_bodies' in source


def test_competition_stats_exposes_club_medal_table_for_master_events():
    source = normalized_source(COMPETITIONS_ROUTER)

    assert "with eligible_events as (" in source
    assert "e.competition_id = %(competition_id)s" in source
    assert (
        "regexp_replace(lower(coalesce(e.age_group, '')), "
        "'[^a-z0-9]+', '', 'g') like '%%premaster%%'"
    ) in source
    assert "substring(trim(e.age_group) from '^([0-9]+)')::integer < 25" in source
    assert source.count("join eligible_events") == 2
    assert "from core.result r join eligible_events" in source
    assert "from core.relay_result rr join eligible_events" in source
    assert source.count("status = 'valid'") >= 2
    assert source.count("rank_position between 1 and 8") == 2
    assert source.count("club_id is not null") >= 2
    assert "union all" in source
    assert "count(*) filter (where rank_position = 1)::integer as gold_medals" in source
    assert "count(*) filter (where rank_position = 2)::integer as silver_medals" in source
    assert "count(*) filter (where rank_position = 3)::integer as bronze_medals" in source
    assert "count(*) filter (where rank_position between 1 and 3)::integer as total_medals" in source
    assert (
        ") order by gold_medals desc, silver_medals desc, bronze_medals desc, "
        "club_name asc"
    ) in source
    assert "filter (where total_medals > 0)" in source
    assert 'stats["club_medal_table"] = club_tables["club_medal_table"]' in source


def test_competition_stats_exposes_audited_club_points_from_shared_placements():
    source = normalized_source(COMPETITIONS_ROUTER)

    assert "placements as (" in source
    assert source.count("join eligible_events") == 2
    assert source.count("rank_position between 1 and 8") == 2
    assert "'individual' as placement_type" in source
    assert "'relay' as placement_type" in source
    assert "when rank_position = 1 then 9" in source
    assert "when rank_position = 2 then 7" in source
    assert "when rank_position = 8 then 1" in source
    assert "placement_type = 'relay' then base_points * 2" in source
    assert "individual_points" in source
    assert "relay_points" in source
    assert "individual_points + relay_points as total_points" in source
    assert "order by total_points desc, club_name asc" in source
    assert 'stats["club_points_table"]' in source
    assert "r.points" not in source
    assert "rr.points" not in source


def test_governing_body_migration_keeps_source_scope_and_organizer_separate():
    source = normalized_source(GOVERNING_BODY_MIGRATION)

    assert "add column if not exists governing_body_code text" in source
    assert "add column if not exists governing_body_name text" in source
    assert "chk_competition_governing_body_code" in source
    assert "idx_competition_governing_body_code" in source
    assert "where competition_scope = 'fchmn_local'" in source
