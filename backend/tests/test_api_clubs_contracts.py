from pathlib import Path


BACKEND_DIR = Path(__file__).resolve().parents[1]
CLUBS_ROUTER = BACKEND_DIR / "api" / "routers" / "clubs.py"


def normalized_source(path: Path) -> str:
    return " ".join(path.read_text(encoding="utf-8").lower().split())


def list_clubs_source() -> str:
    source = normalized_source(CLUBS_ROUTER)
    return source.split('@router.get("/{club_id}")', maxsplit=1)[0]


def test_club_listing_aggregates_roster_counts_once_instead_of_per_club():
    source = list_clubs_source()

    assert "membership_counts as" in source
    assert "current_club_counts as" in source
    assert "group by m.club_id" in source
    assert "group by acc.club_id" in source
    assert "left join membership_counts mc on mc.club_id = c.id" in source
    assert "left join current_club_counts cc on cc.club_id = c.id" in source


def test_club_listing_has_no_correlated_roster_subqueries():
    """`core.athlete_current_club` es una vista con union y window function:
    correlacionada se re-ejecutaba una vez por club."""
    source = list_clubs_source()

    assert "select count(*) from core.athlete_current_club acc where acc.club_id = c.id" not in source
    assert "select count(*) from club_ops.membership m where m.club_id = c.id" not in source
    assert "exists (select 1 from core.athlete_current_club acc where acc.club_id = c.id)" not in source


def test_club_roster_count_prefers_active_memberships_when_any_membership_exists():
    """Un club con membresias pero ninguna activa cuenta cero, no cae al club actual."""
    source = list_clubs_source()

    assert "count(*) filter (where m.status = 'active') as active_members" in source
    assert "when coalesce(mc.any_members, 0) > 0 then coalesce(mc.active_members, 0)" in source
    assert "else coalesce(cc.current_members, 0)" in source


def test_club_inclusion_filter_is_not_the_same_condition_as_the_count():
    """Se incluye por membresia activa O por club actual observado, aunque el
    conteo resultante sea cero."""
    source = list_clubs_source()

    assert "(coalesce(mc.active_members, 0) > 0 or coalesce(cc.current_members, 0) > 0)" in source


def test_club_listing_keeps_local_club_visibility_rule():
    source = list_clubs_source()

    assert "has_club_local_flag" in source
    assert "coalesce(c.is_local, false) = true" in source


def test_club_listing_keeps_shared_token_search_over_name_city_and_region():
    source = list_clubs_source()

    assert "search_tokens" in source
    assert "build_token_search_clause" in source
    assert "[\"c.name\", \"coalesce(c.city, '')\", \"coalesce(c.region, '')\"]" in source


def test_club_listing_still_reports_totals_for_out_of_range_pages():
    """El total se cuenta aparte a proposito: con `count(*) over ()` una pagina
    vacia reportaria cero resultados en vez del total real."""
    source = list_clubs_source()

    assert "select count(*) as total from eligible_club" in source
    assert "count(*) over ()" not in source


def test_club_listing_orders_by_projected_columns():
    source = list_clubs_source()

    assert "order by total_athletes desc, name asc" in source
    assert "order by name asc" in source
