import math
from typing import Optional

from fastapi import APIRouter, Query

from ..database import get_db_connection
from ..search import normalized_search_sql, search_tokens

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


def get_local_athlete_scope(cur) -> tuple[str, str]:
    """Devuelve `(cte, join)` para acotar la consulta a atletas de clubes locales.

    Se resuelve como conjunto en un CTE y no con `EXISTS` correlacionados: la
    rama de club actual lee `core.athlete_current_club`, que es una vista con
    UNION ALL y window function, y correlacionada se re-ejecutaba entera una
    vez por cada fila de resultados.

    Con `cte` vacio la consulta no filtra por club local, igual que antes.
    """
    if not has_club_local_flag(cur):
        return "", ""

    current_club_members = """
        SELECT acc.athlete_id
        FROM core.athlete_current_club acc
        JOIN core.club current_club ON current_club.id = acc.club_id
        WHERE COALESCE(current_club.is_local, FALSE) = TRUE
    """

    if not has_membership_schema(cur):
        local_members = current_club_members
    else:
        local_members = f"""
            SELECT apl.athlete_id
            FROM club_ops.membership m
            JOIN core.athlete_person_link apl ON apl.person_id = m.person_id
            JOIN core.club membership_club ON membership_club.id = m.club_id
            WHERE m.status = 'active'
              AND COALESCE(membership_club.is_local, FALSE) = TRUE

            UNION

            {current_club_members}
        """

    return f"local_athletes AS ({local_members}),", "JOIN local_athletes la ON la.athlete_id = a.id"


CURRENT_CATEGORY_SQL = """
    CASE
        WHEN a.birth_year IS NULL THEN 'Sin categoría'
        WHEN EXTRACT(YEAR FROM CURRENT_DATE)::INTEGER - a.birth_year < 25 THEN 'premaster'
        ELSE (
            ((EXTRACT(YEAR FROM CURRENT_DATE)::INTEGER - a.birth_year - 25) / 5)::INTEGER * 5 + 25
        )::TEXT || '-' || (
            ((EXTRACT(YEAR FROM CURRENT_DATE)::INTEGER - a.birth_year - 25) / 5)::INTEGER * 5 + 29
        )::TEXT
    END
"""

CURRENT_CATEGORY_MIN_AGE_SQL = """
    CASE
        WHEN a.birth_year IS NULL THEN NULL
        WHEN EXTRACT(YEAR FROM CURRENT_DATE)::INTEGER - a.birth_year < 25 THEN 0
        ELSE ((EXTRACT(YEAR FROM CURRENT_DATE)::INTEGER - a.birth_year - 25) / 5)::INTEGER * 5 + 25
    END
"""


@router.get("")
def list_rankings(
    distance_m: Optional[int] = Query(None, gt=0),
    stroke: Optional[str] = Query(None),
    gender: Optional[str] = Query(None),
    age_group: Optional[str] = Query(None),
    course_type: Optional[str] = Query(None),
    year: Optional[int] = Query(None, ge=1900),
    competition_scope: Optional[str] = Query(None),
    athlete_search: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            local_athlete_cte, local_athlete_join = get_local_athlete_scope(cur)
            filters = [
                "r.status = 'valid'",
                "r.result_time_ms IS NOT NULL",
            ]
            params = {}

            if distance_m is not None:
                filters.append("e.distance_m = %(distance_m)s")
                params["distance_m"] = distance_m
            if stroke and stroke != "all":
                filters.append("e.stroke = %(stroke)s")
                params["stroke"] = stroke
            if gender and gender != "all":
                filters.append("e.gender = %(gender)s")
                params["gender"] = gender
            if age_group and age_group != "all":
                filters.append(f"({CURRENT_CATEGORY_SQL}) = %(age_group)s")
                params["age_group"] = age_group
            if course_type and course_type != "all":
                filters.append("comp.course_type = %(course_type)s")
                params["course_type"] = course_type
            if year is not None:
                filters.append("EXTRACT(YEAR FROM comp.start_date)::INTEGER = %(year)s")
                params["year"] = year
            else:
                filters.append("comp.start_date >= CURRENT_DATE - INTERVAL '1 year'")
            if competition_scope and competition_scope != "all":
                filters.append("comp.competition_scope = %(competition_scope)s")
                params["competition_scope"] = competition_scope

            ranked_filters = []
            for index, token in enumerate(search_tokens(athlete_search or "")):
                key = f"athlete_search_{index}"
                ranked_filters.append(f"{normalized_search_sql('athlete_name')} LIKE %({key})s")
                params[key] = f"%{token}%"
            ranked_where_clause = f"WHERE {' AND '.join(ranked_filters)}" if ranked_filters else ""

            where_clause = " AND ".join(filters)
            offset = (page - 1) * page_size
            params.update({"page_size": page_size, "offset": offset})

            base_cte = f"""
                WITH {local_athlete_cte}
                filtered AS (
                    SELECT
                        r.id,
                        r.athlete_id,
                        a.full_name AS athlete_name,
                        r.club_id,
                        club.name AS club_name,
                        REGEXP_REPLACE(r.result_time_text, '^[Xx]\\s*', '') AS time_text,
                        r.result_time_ms AS time_ms,
                        comp.id AS competition_id,
                        comp.name AS competition_name,
                        comp.start_date AS date,
                        e.distance_m,
                        e.stroke,
                        comp.course_type,
                        e.gender,
                        {CURRENT_CATEGORY_SQL} AS age_group,
                        COALESCE(e.age_group, 'Open') AS event_age_group,
                        a.birth_year,
                        EXTRACT(YEAR FROM CURRENT_DATE)::INTEGER - a.birth_year AS current_age,
                        ROW_NUMBER() OVER (
                            PARTITION BY r.athlete_id
                            ORDER BY r.result_time_ms ASC, comp.start_date DESC NULLS LAST, r.id DESC
                        ) AS athlete_best_rank
                    FROM core.result r
                    JOIN core.athlete a ON a.id = r.athlete_id
                    JOIN core.event e ON e.id = r.event_id
                    JOIN core.competition comp ON comp.id = e.competition_id
                    LEFT JOIN core.club club ON club.id = r.club_id
                    {local_athlete_join}
                    WHERE {where_clause}
                ),
                best_by_athlete AS (
                    SELECT *
                    FROM filtered
                    WHERE athlete_best_rank = 1
                ),
                ranked AS (
                    SELECT
                        ROW_NUMBER() OVER (ORDER BY time_ms ASC, date DESC NULLS LAST, id DESC) AS rank,
                        *
                    FROM best_by_athlete
                ),
                searched AS (
                    SELECT *
                    FROM ranked
                    {ranked_where_clause}
                )
            """

            cur.execute(base_cte + "SELECT COUNT(*) AS total FROM searched", params)
            total_results = cur.fetchone()["total"]

            cur.execute(
                base_cte
                + """
                SELECT
                    rank,
                    athlete_name,
                    athlete_id,
                    club_name,
                    time_text,
                    time_ms,
                    competition_id,
                    competition_name,
                    date,
                    distance_m,
                    stroke,
                    course_type,
                    gender,
                    age_group,
                    event_age_group,
                    birth_year,
                    current_age
                FROM searched
                ORDER BY rank ASC
                LIMIT %(page_size)s OFFSET %(offset)s
                """,
                params,
            )
            rankings = cur.fetchall()

            total_pages = math.ceil(total_results / page_size) if total_results > 0 else 1
            return {
                "data": rankings,
                "meta": {
                    "total_results": total_results,
                    "page": page,
                    "page_size": page_size,
                    "total_pages": total_pages,
                },
            }


@router.get("/filter-options")
def get_ranking_filter_options():
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            local_athlete_cte, local_athlete_join = get_local_athlete_scope(cur)

            # Las seis listas salen de un unico recorrido. Antes era una consulta
            # por lista y cada una repetia el join completo result-event-athlete.
            cur.execute(f"""
                WITH {local_athlete_cte}
                eligible AS (
                    SELECT DISTINCT
                        e.distance_m, e.stroke,
                        a.birth_year,
                        comp.start_date,
                        comp.competition_scope
                    FROM core.result r
                    JOIN core.athlete a ON a.id = r.athlete_id
                    JOIN core.event e ON e.id = r.event_id
                    JOIN core.competition comp ON comp.id = e.competition_id
                    {local_athlete_join}
                    WHERE r.status = 'valid'
                      AND r.result_time_ms IS NOT NULL
                )
                SELECT
                    (
                        SELECT COALESCE(json_agg(
                                   json_build_object('distance_m', distance_m, 'stroke', stroke)
                                   ORDER BY distance_m ASC, stroke ASC), '[]'::json)
                        FROM (
                            SELECT DISTINCT distance_m, stroke
                            FROM eligible
                            WHERE distance_m IS NOT NULL
                              AND stroke IS NOT NULL
                              AND stroke NOT LIKE '%_relay'
                        ) s
                    ) AS event_options,
                    (
                        SELECT COALESCE(json_agg(distance_m ORDER BY distance_m ASC), '[]'::json)
                        FROM (
                            SELECT DISTINCT distance_m FROM eligible WHERE distance_m IS NOT NULL
                        ) s
                    ) AS distances,
                    (
                        SELECT COALESCE(json_agg(stroke ORDER BY stroke ASC), '[]'::json)
                        FROM (
                            SELECT DISTINCT stroke
                            FROM eligible
                            WHERE stroke IS NOT NULL
                              AND stroke NOT LIKE '%_relay'
                        ) s
                    ) AS strokes,
                    (
                        SELECT COALESCE(json_agg(age_group
                                   ORDER BY category_min_age ASC, age_group ASC), '[]'::json)
                        FROM (
                            SELECT DISTINCT
                                {CURRENT_CATEGORY_SQL} AS age_group,
                                {CURRENT_CATEGORY_MIN_AGE_SQL} AS category_min_age
                            FROM eligible a
                            WHERE a.birth_year IS NOT NULL
                        ) s
                    ) AS age_groups,
                    (
                        SELECT COALESCE(json_agg(year ORDER BY year DESC), '[]'::json)
                        FROM (
                            SELECT DISTINCT EXTRACT(YEAR FROM start_date)::INTEGER AS year
                            FROM eligible WHERE start_date IS NOT NULL
                        ) s
                    ) AS years,
                    (
                        SELECT COALESCE(json_agg(competition_scope
                                   ORDER BY competition_scope ASC), '[]'::json)
                        FROM (
                            SELECT DISTINCT competition_scope
                            FROM eligible WHERE competition_scope IS NOT NULL
                        ) s
                    ) AS scopes
            """)
            options = cur.fetchone()

            event_options = options["event_options"]
            distances = options["distances"]
            strokes = options["strokes"]
            age_groups = options["age_groups"]
            years = options["years"]
            scopes = options["scopes"]

            return {
                "distances": distances,
                "strokes": strokes,
                "event_options": event_options,
                "age_groups": age_groups,
                "years": years,
                "scopes": scopes,
            }
