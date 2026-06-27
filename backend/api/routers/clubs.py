import math
from fastapi import APIRouter, Query
from typing import Optional
from ..database import get_db_connection
from ..search import build_token_search_clause, search_tokens

router = APIRouter()

@router.get("")
def list_clubs(
    search: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100)
):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            offset = (page - 1) * page_size
            
            query = """
                SELECT c.id, c.name, c.city, c.region as country, c.association_name,
                       (SELECT count(*) FROM core.athlete_current_club acc WHERE acc.club_id = c.id) as total_athletes
                FROM core.club c 
                WHERE EXISTS (SELECT 1 FROM core.athlete_current_club acc WHERE acc.club_id = c.id)
            """
            count_query = "SELECT COUNT(*) as total FROM core.club c WHERE EXISTS (SELECT 1 FROM core.athlete_current_club acc WHERE acc.club_id = c.id)"
            params = []
            
            if search:
                tokens = search_tokens(search)
                if tokens:
                    search_clause, search_params = build_token_search_clause(
                        ["c.name", "COALESCE(c.city, '')", "COALESCE(c.region, '')"], tokens
                    )
                    query += f" AND {search_clause}"
                    count_query += f" AND {search_clause}"
                    params.extend(search_params)
                
            query += " ORDER BY c.name LIMIT %s OFFSET %s"
            
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

from fastapi import HTTPException

@router.get("/{club_id}")
def get_club(club_id: int):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT c.id, c.name, c.city, c.region as country, c.association_name,
                       (SELECT count(*) FROM core.athlete_current_club acc WHERE acc.club_id = c.id) as total_athletes
                FROM core.club c
                WHERE c.id = %s
            """, (club_id,))
            club = cur.fetchone()
            
            if not club:
                raise HTTPException(status_code=404, detail="Club not found")

            cur.execute("""
                WITH attendance AS (
                    SELECT
                        r.athlete_id,
                        a.full_name AS athlete_name,
                        comp.id AS competition_id,
                        comp.name AS competition_name,
                        comp.start_date AS competition_date,
                        COUNT(*) AS entries,
                        BOOL_OR(COALESCE(r.status, 'unknown') NOT IN ('dns', 'scratch')) AS attended
                    FROM core.result r
                    JOIN core.athlete a ON a.id = r.athlete_id
                    JOIN core.athlete_current_club acc ON acc.athlete_id = a.id
                    JOIN core.event e ON e.id = r.event_id
                    JOIN core.competition comp ON comp.id = e.competition_id
                    WHERE r.club_id = %(club_id)s
                      AND acc.club_id = %(club_id)s
                    GROUP BY r.athlete_id, a.full_name, comp.id, comp.name, comp.start_date

                    UNION ALL

                    SELECT
                        rrm.athlete_id,
                        a.full_name AS athlete_name,
                        comp.id AS competition_id,
                        comp.name AS competition_name,
                        comp.start_date AS competition_date,
                        COUNT(*) AS entries,
                        BOOL_OR(COALESCE(rr.status, 'unknown') NOT IN ('dns', 'scratch')) AS attended
                    FROM core.relay_result rr
                    JOIN core.relay_result_member rrm ON rrm.relay_result_id = rr.id
                    JOIN core.athlete a ON a.id = rrm.athlete_id
                    JOIN core.athlete_current_club acc ON acc.athlete_id = a.id
                    JOIN core.event e ON e.id = rr.event_id
                    JOIN core.competition comp ON comp.id = e.competition_id
                    WHERE rr.club_id = %(club_id)s
                      AND rrm.athlete_id IS NOT NULL
                      AND acc.club_id = %(club_id)s
                    GROUP BY rrm.athlete_id, a.full_name, comp.id, comp.name, comp.start_date
                )
                SELECT
                    athlete_id,
                    athlete_name,
                    competition_id,
                    competition_name,
                    competition_date,
                    SUM(entries)::INTEGER AS entries,
                    BOOL_OR(attended) AS attended
                FROM attendance
                GROUP BY athlete_id, athlete_name, competition_id, competition_name, competition_date
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
