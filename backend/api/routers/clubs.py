import math
from fastapi import APIRouter, HTTPException, Query
from typing import Optional
from ..database import get_db_connection
from ..search import build_token_search_clause, search_tokens

router = APIRouter()


def has_membership_schema(cur) -> bool:
    cur.execute("""
        SELECT
            to_regclass('club_ops.membership') IS NOT NULL
            AND to_regclass('core.athlete_person_link') IS NOT NULL AS available
    """)
    return bool(cur.fetchone()["available"])


def has_club_local_flag(cur) -> bool:
    cur.execute("""
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = 'core'
              AND table_name = 'club'
              AND column_name = 'is_local'
        ) AS available
    """)
    return bool(cur.fetchone()["available"])


@router.get("")
def list_clubs(
    search: Optional[str] = Query(None),
    sort: str = Query("athletes"),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100)
):
    if sort not in {"athletes", "name"}:
        raise HTTPException(status_code=400, detail="sort must be 'athletes' or 'name'")

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            offset = (page - 1) * page_size
            use_membership_schema = has_membership_schema(cur)
            local_club_filter = "COALESCE(c.is_local, FALSE) = TRUE" if has_club_local_flag(cur) else "TRUE"

            # Los conteos de plantel se agregan una sola vez en CTEs en lugar de
            # resolverse con subconsultas correlacionadas por club. Importa porque
            # `core.athlete_current_club` es una vista con UNION ALL y window
            # function: correlacionada, se re-ejecutaba entera una vez por club.
            if use_membership_schema:
                roster_ctes = """
                    WITH membership_counts AS (
                        SELECT m.club_id,
                               COUNT(*) AS any_members,
                               COUNT(*) FILTER (WHERE m.status = 'active') AS active_members
                        FROM club_ops.membership m
                        GROUP BY m.club_id
                    ),
                    current_club_counts AS (
                        SELECT acc.club_id, COUNT(*) AS current_members
                        FROM core.athlete_current_club acc
                        GROUP BY acc.club_id
                    )
                """
                roster_joins = """
                    LEFT JOIN membership_counts mc ON mc.club_id = c.id
                    LEFT JOIN current_club_counts cc ON cc.club_id = c.id
                """
                # Si el club tiene alguna membresia (de cualquier estado) manda el
                # conteo de activas, aunque sea cero; si no, cae al club actual.
                roster_count_sql = """
                    CASE
                        WHEN COALESCE(mc.any_members, 0) > 0 THEN COALESCE(mc.active_members, 0)
                        ELSE COALESCE(cc.current_members, 0)
                    END
                """
                # El filtro de inclusion NO es la misma condicion que el conteo:
                # basta con membresia activa o con club actual observado.
                roster_exists_sql = "(COALESCE(mc.active_members, 0) > 0 OR COALESCE(cc.current_members, 0) > 0)"
            else:
                roster_ctes = """
                    WITH current_club_counts AS (
                        SELECT acc.club_id, COUNT(*) AS current_members
                        FROM core.athlete_current_club acc
                        GROUP BY acc.club_id
                    )
                """
                roster_joins = "LEFT JOIN current_club_counts cc ON cc.club_id = c.id"
                roster_count_sql = "COALESCE(cc.current_members, 0)"
                roster_exists_sql = "COALESCE(cc.current_members, 0) > 0"

            search_filter = ""
            params = []

            if search:
                tokens = search_tokens(search)
                if tokens:
                    search_clause, search_params = build_token_search_clause(
                        ["c.name", "COALESCE(c.city, '')", "COALESCE(c.region, '')"], tokens
                    )
                    search_filter = f" AND {search_clause}"
                    params.extend(search_params)

            eligible_clubs = f"""
                {roster_ctes},
                eligible_club AS (
                    SELECT c.id, c.name, c.city, c.region, c.association_name,
                           {roster_count_sql} AS total_athletes
                    FROM core.club c
                    {roster_joins}
                    WHERE {local_club_filter}
                      AND {roster_exists_sql}
                      {search_filter}
                )
            """

            count_query = eligible_clubs + " SELECT COUNT(*) as total FROM eligible_club"

            if sort == "name":
                order_by = " ORDER BY name ASC"
            else:
                order_by = " ORDER BY total_athletes DESC, name ASC"

            query = eligible_clubs + f"""
                SELECT id, name, city, region, region as country, association_name,
                       total_athletes
                FROM eligible_club
                {order_by}
                LIMIT %s OFFSET %s
            """

            cur.execute(count_query, params)
            total_results = cur.fetchone()['total']

            params.extend([page_size, offset])
            cur.execute(query, params)
            clubs = cur.fetchall()
            
            total_pages = math.ceil(total_results / page_size) if total_results > 0 else 1
            
            return {
                "data": clubs,
                "meta": {
                    "total_results": total_results,
                    "page": page,
                    "page_size": page_size,
                    "total_pages": total_pages
                }
            }

@router.get("/{club_id}")
def get_club(club_id: int):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            use_membership_schema = has_membership_schema(cur)
            local_club_filter = "COALESCE(c.is_local, FALSE) = TRUE" if has_club_local_flag(cur) else "TRUE"

            if use_membership_schema:
                roster_count_sql = """
                    SELECT CASE
                        WHEN EXISTS (SELECT 1 FROM club_ops.membership mx WHERE mx.club_id = c.id)
                        THEN (
                            SELECT COUNT(*)
                            FROM club_ops.membership m
                            WHERE m.club_id = c.id
                              AND m.status = 'active'
                        )
                        ELSE (
                            SELECT COUNT(*)
                            FROM core.athlete_current_club acc
                            WHERE acc.club_id = c.id
                        )
                    END
                """
                current_athletes_cte = """
                    current_athletes AS (
                        SELECT DISTINCT apl.athlete_id
                        FROM club_ops.membership m
                        JOIN core.athlete_person_link apl ON apl.person_id = m.person_id
                        WHERE m.club_id = %(club_id)s
                          AND m.status = 'active'

                        UNION

                        SELECT a.id AS athlete_id
                        FROM core.athlete a
                        JOIN core.athlete_current_club acc
                          ON acc.athlete_id = a.id
                         AND acc.club_id = %(club_id)s
                        WHERE NOT EXISTS (
                              SELECT 1
                              FROM club_ops.membership mx
                              WHERE mx.club_id = %(club_id)s
                          )
                    ),
                """
            else:
                roster_count_sql = "SELECT count(*) FROM core.athlete_current_club acc WHERE acc.club_id = c.id"
                current_athletes_cte = """
                    current_athletes AS (
                        SELECT a.id AS athlete_id
                        FROM core.athlete a
                        JOIN core.athlete_current_club acc
                          ON acc.athlete_id = a.id
                         AND acc.club_id = %(club_id)s
                    ),
                """

            cur.execute(f"""
                SELECT c.id, c.name, c.city, c.region, c.region as country, c.association_name,
                       ({roster_count_sql}) as total_athletes
                FROM core.club c
                WHERE c.id = %s
                  AND {local_club_filter}
            """, (club_id,))
            club = cur.fetchone()
            
            if not club:
                raise HTTPException(status_code=404, detail="Club not found")

            cur.execute("""
                WITH
                """ + current_athletes_cte + """
                attendance AS (
                    SELECT
                        r.athlete_id,
                        a.full_name AS athlete_name,
                        a.gender,
                        a.birth_year,
                        comp.id AS competition_id,
                        comp.name AS competition_name,
                        comp.start_date AS competition_date,
                        COUNT(*) AS entries,
                        BOOL_OR(COALESCE(r.status, 'unknown') NOT IN ('dns', 'scratch')) AS attended
                    FROM core.result r
                    JOIN core.athlete a ON a.id = r.athlete_id
                    JOIN current_athletes ca ON ca.athlete_id = a.id
                    JOIN core.event e ON e.id = r.event_id
                    JOIN core.competition comp ON comp.id = e.competition_id
                    WHERE r.club_id = %(club_id)s
                    GROUP BY r.athlete_id, a.full_name, a.gender, a.birth_year, comp.id, comp.name, comp.start_date

                    UNION ALL

                    SELECT
                        rrm.athlete_id,
                        a.full_name AS athlete_name,
                        a.gender,
                        a.birth_year,
                        comp.id AS competition_id,
                        comp.name AS competition_name,
                        comp.start_date AS competition_date,
                        COUNT(*) AS entries,
                        BOOL_OR(COALESCE(rr.status, 'unknown') NOT IN ('dns', 'scratch')) AS attended
                    FROM core.relay_result rr
                    JOIN core.relay_result_member rrm ON rrm.relay_result_id = rr.id
                    JOIN core.athlete a ON a.id = rrm.athlete_id
                    JOIN current_athletes ca ON ca.athlete_id = a.id
                    JOIN core.event e ON e.id = rr.event_id
                    JOIN core.competition comp ON comp.id = e.competition_id
                    WHERE rr.club_id = %(club_id)s
                      AND rrm.athlete_id IS NOT NULL
                    GROUP BY rrm.athlete_id, a.full_name, a.gender, a.birth_year, comp.id, comp.name, comp.start_date
                )
                SELECT
                    athlete_id,
                    athlete_name,
                    gender,
                    birth_year,
                    competition_id,
                    competition_name,
                    competition_date,
                    SUM(entries)::INTEGER AS entries,
                    BOOL_OR(attended) AS attended
                FROM attendance
                GROUP BY athlete_id, athlete_name, gender, birth_year, competition_id, competition_name, competition_date
                ORDER BY athlete_name ASC, competition_date DESC NULLS LAST, competition_name ASC
            """, {"club_id": club_id})
            attendance_rows = cur.fetchall()

            competitions_by_id = {}
            athletes_by_id = {}
            for row in attendance_rows:
                competition_id = row["competition_id"]
                athlete_id = row["athlete_id"]

                competitions_by_id[competition_id] = {
                    "id": competition_id,
                    "name": row["competition_name"],
                    "date": row["competition_date"],
                }

                athlete = athletes_by_id.setdefault(athlete_id, {
                    "athlete_id": athlete_id,
                    "athlete_name": row["athlete_name"],
                    "gender": row["gender"],
                    "birth_year": row["birth_year"],
                    "competitions": [],
                })
                athlete["competitions"].append({
                    "competition_id": competition_id,
                    "entries": row["entries"],
                    "status": "attended" if row["attended"] else "no_show",
                })

            club["attendance_matrix"] = {
                "competitions": sorted(
                    competitions_by_id.values(),
                    key=lambda item: (item["date"] or "", item["name"]),
                    reverse=True,
                ),
                "athletes": list(athletes_by_id.values()),
            }
            
            return club
