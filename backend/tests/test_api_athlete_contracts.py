from pathlib import Path


BACKEND_DIR = Path(__file__).resolve().parents[1]
ATHLETES_ROUTER = BACKEND_DIR / "api" / "routers" / "athletes.py"
CLUBS_ROUTER = BACKEND_DIR / "api" / "routers" / "clubs.py"


def normalized_source(path: Path) -> str:
    return " ".join(path.read_text(encoding="utf-8").lower().split())


def test_athletes_api_uses_current_club_view_not_static_athlete_club():
    source = normalized_source(ATHLETES_ROUTER)

    assert "core.athlete_current_club acc" in source
    assert "left join core.club acc_club on acc_club.id = acc.club_id" in source
    assert "acc.club_id = %s" in source
    assert "current_club_name" in source
    assert "left join core.club c on a.club_id = c.id" not in source
    assert "r.seed_time_text" in source
    assert "r.seed_time_ms" in source


def test_athlete_results_expose_historical_represented_club():
    source = normalized_source(ATHLETES_ROUTER)

    assert "left join core.club result_club on result_club.id = r.club_id" in source
    assert "result_club.name as club_name" in source


def test_athlete_profile_exposes_complete_history_without_silent_limit():
    source = normalized_source(ATHLETES_ROUTER)
    profile_source = source.split('@router.get("/{athlete_id}")', maxsplit=1)[1]

    assert "where r.athlete_id = %s" in profile_source
    assert "order by comp.start_date desc, e.distance_m asc" in profile_source
    assert "limit " not in profile_source


def test_athletes_api_uses_shared_token_search():
    source = normalized_source(ATHLETES_ROUTER)

    assert "search_tokens" in source
    assert "build_token_search_clause" in source


def test_athletes_api_filters_public_list_by_local_club_when_available():
    source = normalized_source(ATHLETES_ROUTER)

    assert "has_club_local_flag" in source
    assert "coalesce(acc_club.is_local, false) = true" in source
    assert "membership_club.is_local" in source
    assert "selected_club.is_local" in source


def test_athlete_search_text_normalization_removes_accents_for_search():
    import sys

    if str(BACKEND_DIR) not in sys.path:
        sys.path.insert(0, str(BACKEND_DIR))

    from api.routers.athletes import normalize_search_text

    assert normalize_search_text("Daniel Briceño") == "daniel briceno"


def test_partial_non_contiguous_athlete_name_builds_all_token_conditions():
    import sys

    if str(BACKEND_DIR) not in sys.path:
        sys.path.insert(0, str(BACKEND_DIR))

    from api.search import build_token_search_clause, search_tokens

    tokens = search_tokens("Alexis Sayago")
    clause, params = build_token_search_clause(["a.full_name"], tokens)

    assert tokens == ["alexis", "sayago"]
    assert clause.count("LIKE %s") == 2
    assert " AND " in clause
    assert params == ["%alexis%", "%sayago%"]


def test_clubs_api_counts_current_athletes_from_current_club_view():
    source = normalized_source(CLUBS_ROUTER)

    assert "core.athlete_current_club acc" in source
    assert "coalesce(c.is_local, false) = true" in source
    assert "where acc.club_id = c.id" in source
    assert "c.city" in source
    assert "c.short_name as city" not in source
    assert "from core.athlete a where a.club_id = c.id" not in source


def test_club_profile_exposes_attendance_from_represented_club_results():
    source = normalized_source(CLUBS_ROUTER)

    assert "attendance_matrix" in source
    assert "where r.club_id = %(club_id)s" in source
    assert "where rr.club_id = %(club_id)s" in source
    assert "join core.athlete_current_club acc on acc.athlete_id = a.id" in source
    assert "and acc.club_id = %(club_id)s" in source
    assert "join core.relay_result_member rrm" in source
    assert "not in ('dns', 'scratch')" in source
    assert '"gender": row["gender"]' in source
    assert '"birth_year": row["birth_year"]' in source
    assert '"status": "attended" if row["attended"] else "no_show"' in source
    assert "athlete_current_club" in source


def test_clubs_api_uses_shared_token_search():
    source = normalized_source(CLUBS_ROUTER)

    assert "search_tokens" in source
    assert "build_token_search_clause" in source


def test_club_search_requires_every_token_across_name_fields():
    import sys

    if str(BACKEND_DIR) not in sys.path:
        sys.path.insert(0, str(BACKEND_DIR))

    from api.search import build_token_search_clause, search_tokens

    tokens = search_tokens("Natacion San Bernardo")
    clause, params = build_token_search_clause(
        ["c.name", "COALESCE(c.city, '')", "COALESCE(c.region, '')"], tokens
    )

    assert clause.count("LIKE %s") == 9
    assert clause.count(" AND ") == 2
    assert params == [
        "%natacion%", "%natacion%", "%natacion%",
        "%san%", "%san%", "%san%",
        "%bernardo%", "%bernardo%", "%bernardo%",
    ]
