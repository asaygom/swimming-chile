import math
import unicodedata
from fastapi import APIRouter, Query, HTTPException
from typing import Optional
from ..database import get_db_connection

router = APIRouter()


def normalize_search_text(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value.casefold())
    return "".join(ch for ch in normalized if not unicodedata.combining(ch))


def athlete_name_sql(expression: str) -> str:
    return f"""
    TRANSLATE(
        LOWER({expression}),
        'áàäâãéèëêíìïîóòöôõúùüûñ',
        'aaaaaeeeeiiiiooooouuuun'
    )
    """


def athlete_natural_name_sql() -> str:
    return """
    CONCAT(
        TRIM(SUBSTRING(a.full_name FROM POSITION(',' IN a.full_name) + 1)),
        ' ',
        TRIM(SUBSTRING(a.full_name FROM 1 FOR POSITION(',' IN a.full_name) - 1))
    )
    """


def athlete_search_sql() -> str:
    # full_name is stored as "Apellido, Nombre"; also match natural order "Nombre Apellido".
    # The normalized checks make "Daniel Briceño" and "Daniel Briceno" equivalent.
    natural_name = athlete_natural_name_sql()
    return f"""
    (
        a.full_name ILIKE %s
        OR {athlete_name_sql('a.full_name')} LIKE %s
        OR (
            POSITION(',' IN a.full_name) > 0
            AND {natural_name} ILIKE %s
        )
        OR (
            POSITION(',' IN a.full_name) > 0
            AND {athlete_name_sql(natural_name)} LIKE %s
        )
    )
    """


@router.get("")
def list_athletes(
    search: Optional[str] = Query(None),
    club_id: Optional[int] = Query(None),
    gender: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100)
):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            offset = (page - 1) * page_size
            
            query = """
                SELECT
                    a.id,
                    a.full_name,
                    a.gender,
                    a.birth_year,
                    acc.club_name,
                    acc.club_id AS current_club_id,
                    acc.club_name AS current_club_name,
                    acc.competition_date AS current_club_observed_at
                FROM core.athlete a
                LEFT JOIN core.athlete_current_club acc ON acc.athlete_id = a.id
                WHERE 1=1
            """
            count_query = """
                SELECT COUNT(*) as total
                FROM core.athlete a
                LEFT JOIN core.athlete_current_club acc ON acc.athlete_id = a.id
                WHERE 1=1
            """
            params = []
            
            if search:
                query += f" AND {athlete_search_sql()}"
                count_query += f" AND {athlete_search_sql()}"
                normalized_search = normalize_search_text(search)
                params.extend([
                    f"%{search}%",
                    f"%{normalized_search}%",
                    f"%{search}%",
                    f"%{normalized_search}%",
                ])
                
            if club_id:
                query += " AND acc.club_id = %s"
                count_query += " AND acc.club_id = %s"
                params.append(club_id)
                
            if gender and gender != 'all':
                query += " AND a.gender = %s"
                count_query += " AND a.gender = %s"
                params.append(gender)
                
            query += " ORDER BY a.full_name LIMIT %s OFFSET %s"
            
            cur.execute(count_query, params)
            total_results = cur.fetchone()['total']
            
            params.extend([page_size, offset])
            cur.execute(query, params)
            athletes = cur.fetchall()
            
            total_pages = math.ceil(total_results / page_size) if total_results > 0 else 1
            
            return {
                "data": athletes,
                "meta": {
                    "total_results": total_results,
                    "page": page,
                    "page_size": page_size,
                    "total_pages": total_pages
                }
            }

@router.get("/{athlete_id}")
def get_athlete(athlete_id: int):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    a.id,
                    a.full_name,
                    a.gender,
                    a.birth_year,
                    acc.club_name,
                    acc.club_id AS current_club_id,
                    acc.club_name AS current_club_name,
                    acc.competition_date AS current_club_observed_at
                FROM core.athlete a
                LEFT JOIN core.athlete_current_club acc ON acc.athlete_id = a.id
                WHERE a.id = %s
            """, (athlete_id,))
            athlete = cur.fetchone()
            
            if not athlete:
                raise HTTPException(status_code=404, detail="Athlete not found")
                
            cur.execute("""
                SELECT 
                    r.id, e.distance_m || 'm ' || e.stroke as event_name, 
                    e.stroke, e.distance_m, comp.course_type, e.age_group,
                    comp.name as competition_name, comp.start_date as competition_date,
                    r.result_time_text, r.result_time_ms, r.status,
                    r.rank_position, r.points
                FROM core.result r
                JOIN core.event e ON r.event_id = e.id
                JOIN core.competition comp ON e.competition_id = comp.id
                WHERE r.athlete_id = %s
                ORDER BY comp.start_date DESC, e.distance_m ASC
                LIMIT 50
            """, (athlete_id,))
            recent_results = cur.fetchall()
            
            athlete["recent_results"] = recent_results
            return athlete
