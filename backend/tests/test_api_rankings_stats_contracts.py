from pathlib import Path


BACKEND_DIR = Path(__file__).resolve().parents[1]
ROOT_DIR = BACKEND_DIR.parent
RANKINGS_ROUTER = BACKEND_DIR / "api" / "routers" / "rankings.py"
STATS_ROUTER = BACKEND_DIR / "api" / "routers" / "stats.py"
COMPETITIONS_ROUTER = BACKEND_DIR / "api" / "routers" / "competitions.py"
API_MAIN = BACKEND_DIR / "api" / "main.py"
RANKING_SERVICE = ROOT_DIR / "frontend" / "src" / "features" / "rankings" / "api" / "rankingService.ts"
RANKING_SCHEMA = ROOT_DIR / "frontend" / "src" / "lib" / "schemas" / "ranking.ts"
RANKINGS_PAGE = ROOT_DIR / "frontend" / "src" / "features" / "rankings" / "pages" / "RankingsPage.tsx"
APP_ROUTER = ROOT_DIR / "frontend" / "src" / "app" / "router.tsx"


def normalized_source(path: Path) -> str:
    return " ".join(path.read_text(encoding="utf-8").lower().split())


def test_rankings_api_uses_valid_individual_best_time_per_athlete():
    source = normalized_source(RANKINGS_ROUTER)

    assert "@router.get(\"\")" in source
    assert "r.status = 'valid'" in source
    assert "r.result_time_ms is not null" in source
    assert "partition by r.athlete_id" in source
    assert "athlete_best_rank = 1" in source
    assert "comp.start_date >= current_date - interval '1 year'" in source
    assert "extract(year from comp.start_date)::integer = %(year)s" in source
    assert "regexp_replace(r.result_time_text, '^[xx]\\\\s*', '') as time_text" in source
    assert "athlete_search" in source
    assert "from ranked" in source
    assert "from searched" in source
    assert "extract(year from current_date)::integer - a.birth_year" in source
    assert "as event_age_group" in source
    assert "join core.result r" in source
    assert "join core.event e on e.id = r.event_id" in source
    assert "join core.competition comp on comp.id = e.competition_id" in source
    assert "left join core.club club on club.id = r.club_id" in source
    assert "athlete.club_id" not in source


def test_public_rankings_only_include_athletes_linked_to_local_clubs():
    source = normalized_source(RANKINGS_ROUTER)

    assert "def has_membership_schema(cur)" in source
    assert "def has_club_local_flag(cur)" in source
    assert "information_schema.columns" in source
    assert "column_name = 'is_local'" in source
    assert "from club_ops.membership m" in source
    assert "join core.athlete_person_link apl on apl.person_id = m.person_id" in source
    assert "m.status = 'active'" in source
    assert "coalesce(membership_club.is_local, false) = true" in source
    assert "from core.athlete_current_club acc" in source
    assert "coalesce(current_club.is_local, false) = true" in source


def test_ranking_filter_options_only_use_local_athletes():
    source = normalized_source(RANKINGS_ROUTER)

    assert "def get_ranking_filter_options():" in source
    assert "local_athlete_filter = get_local_athlete_filter(cur)" in source
    assert source.count("{local_athlete_filter}") >= 6


def test_rankings_filter_category_by_current_athlete_age_not_event_age_group():
    raw_source = RANKINGS_ROUTER.read_text(encoding="utf-8")
    source = " ".join(raw_source.lower().split())

    assert "current_category_sql" in source
    assert "current_category_min_age_sql" in source
    assert "then 'premaster'" in source
    assert "24 & under" not in source
    assert "({current_category_sql}) = %(age_group)s" in source
    assert "select distinct {current_category_sql} as age_group" in source
    assert "order by category_min_age asc, age_group asc" in source
    assert 'cur.execute(f"""' in raw_source
    assert "filters.append(\"e.age_group = %(age_group)s\")" not in source


def test_ranking_filter_options_keep_distance_and_stroke_related():
    source = normalized_source(RANKINGS_ROUTER)

    assert "select distinct e.distance_m, e.stroke" in source
    assert "event_options = cur.fetchall()" in source
    assert "select distinct e.stroke" in source
    assert '"strokes": strokes' in RANKINGS_ROUTER.read_text(encoding="utf-8")
    assert '"event_options": event_options' in RANKINGS_ROUTER.read_text(encoding="utf-8")
    assert "def get_ranking_filter_options():" in source


def test_club_participation_uses_represented_club_from_result_rows():
    source = normalized_source(STATS_ROUTER)

    assert '@router.get("/clubs/participation")' in source
    assert "r.club_id is not null" in source
    assert "coalesce(r.status, 'unknown') not in ('dns', 'scratch')" in source
    assert "count(distinct r.athlete_id)" in source
    assert "count(distinct comp.id)" in source
    assert "join core.club club on club.id = r.club_id" in source
    assert "athlete.club_id" not in source


def test_club_participation_matrix_counts_unique_athletes_by_competition():
    source = normalized_source(STATS_ROUTER)

    assert '@router.get("/clubs/participation-matrix")' in source
    assert "extract(year from current_date)::integer as current_year" in source
    assert "extract(year from comp.start_date)::integer = %(year)s" in source
    assert "comp.governing_body_code = %(governing_body)s" in source
    assert "count(distinct r.athlete_id)::integer as athletes_count" in source
    assert "sum(athletes_count)::integer as total_athletes" in source
    assert "sum(athletes_count)::integer as athletes_count" in source
    assert '"competitions": competitions' in STATS_ROUTER.read_text(encoding="utf-8")
    assert '"totals": totals' in STATS_ROUTER.read_text(encoding="utf-8")
    assert '"clubs": clubs' in STATS_ROUTER.read_text(encoding="utf-8")
    assert "join core.club club on club.id = r.club_id" in source


def test_club_stats_filter_options_use_participation_data():
    source = normalized_source(STATS_ROUTER)

    assert '@router.get("/clubs/filter-options")' in source
    assert "select distinct extract(year from comp.start_date)::integer as year" in source
    assert "select distinct comp.governing_body_code, comp.governing_body_name" in source
    assert "coalesce(r.status, 'unknown') not in ('dns', 'scratch')" in source


def test_competition_stats_table_uses_profile_stats_by_year_and_governing_body():
    source = normalized_source(STATS_ROUTER)

    assert '@router.get("/competitions")' in source
    assert "extract(year from current_date)::integer as current_year" in source
    assert "extract(year from comp.start_date)::integer = %(year)s" in source
    assert "comp.start_date <= current_date" in source
    assert "and exists" in source
    assert "comp.governing_body_code = %(governing_body)s" in source
    assert "count(distinct athlete_id)::integer as participants_count" in source
    assert "where athlete_gender = 'female'" in source
    assert "where athlete_gender = 'male'" in source
    assert "count(distinct club_id)" in source
    assert "where status = 'dsq'" in source
    assert "where status = 'valid'" in source
    assert "events_count" in source
    assert "order by fc.date desc nulls last, fc.name asc" in source


def test_club_participation_only_aggregates_local_clubs():
    source = normalized_source(STATS_ROUTER)

    assert "def has_club_local_flag(cur)" in source
    assert "information_schema.columns" in source
    assert "column_name = 'is_local'" in source
    assert "coalesce(club.is_local, false) = true" in source


def test_competition_stats_contract_counts_participants_gender_clubs_and_dsq():
    source = normalized_source(COMPETITIONS_ROUTER)

    assert '@router.get("/{competition_id}/stats")' in source
    assert "count(distinct athlete_id)::integer as participants_count" in source
    assert "where athlete_gender = 'female'" in source
    assert "where athlete_gender = 'male'" in source
    assert "count(distinct club_id)" in source
    assert "where status = 'dsq'" in source
    assert "coalesce(r.status, 'unknown') not in ('dns', 'scratch')" in source


def test_stats_and_rankings_routers_are_registered():
    source = normalized_source(API_MAIN)

    assert "rankings" in source
    assert "stats" in source
    assert 'prefix="/api/rankings"' in API_MAIN.read_text(encoding="utf-8")
    assert 'prefix="/api/stats"' in API_MAIN.read_text(encoding="utf-8")


def test_frontend_rankings_use_api_contract_not_fixture():
    service = normalized_source(RANKING_SERVICE)
    schema = normalized_source(RANKING_SCHEMA)
    page = normalized_source(RANKINGS_PAGE)
    app_router = normalized_source(APP_ROUTER)
    layout = normalized_source(ROOT_DIR / "frontend" / "src" / "components" / "layout" / "MainLayout.tsx")

    assert "fixture" not in service
    assert "/api/rankings" in service
    assert "athlete_search" in service
    assert "/api/stats/clubs/participation" in service
    assert "/api/stats/clubs/participation-matrix" in service
    assert "/api/stats/clubs/filter-options" in service
    assert "/api/stats/competitions" in service
    assert "governing_body" in service
    assert "rankingfilteroptionsschema" in schema
    assert "strokes: z.array(strokeschema)" in schema
    assert "event_options: z.array" in schema
    assert "clubparticipationresponseschema" in schema
    assert "clubparticipationmatrixschema" in schema
    assert "clubstatsfilteroptionsschema" in schema
    assert "competitionstatstableschema" in schema
    assert "governing_bodies" in schema
    assert "event_age_group" in schema
    assert "current_age" in schema
    assert "filter((option) => option.stroke === normalizedstroke)" in page
    assert "setstroke(nextstroke)" in page
    assert "type analyticsview = 'swimmers' | 'clubs'" in page
    assert "| 'competitions'" in page
    assert "activeview === 'swimmers'" in page
    assert "activeview === 'clubs'" in page
    assert "enabled: activeview === 'clubs'" in page
    assert "rankingservice.getrankings" in page
    assert "athlete_search: athletesearch" in page
    assert "buscar atleta en este ranking" in page
    assert "mostrando coincidencias" in page
    assert "rankingservice.getclubparticipation" in page
    assert "rankingservice.getclubparticipationmatrix" in page
    assert "rankingservice.getclubstatsfilteroptions" in page
    assert "rankingservice.getcompetitionstatstable" in page
    assert "activeview === 'competitions'" in page
    assert "estadísticas por competencia" in page
    assert "competitionstatsquery.data.data.map" in page
    assert "competition.participants_count" in page
    assert "clubstatsgoverningbody" in page
    assert "governing_body_name" in page
    assert "participación por competencia" in page
    assert "total competencia" in page
    assert "path: 'rankings'" in app_router
    assert "to=\"/rankings\"" in layout
    assert "rankings</span>" in layout
