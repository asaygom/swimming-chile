import math
from typing import Optional

from fastapi import APIRouter, Query

from ..database import get_db_connection

router = APIRouter()


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


@router.get("/clubs/participation")
def list_club_participation(
    year: Optional[int] = Query(None, ge=1900),
    competition_scope: Optional[str] = Query(None),
    governing_body: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            filters = [
                "r.club_id IS NOT NULL",
                "COALESCE(r.status, 'unknown') NOT IN ('dns', 'scratch')",
            ]
            if has_club_local_flag(cur):
                filters.append("COALESCE(club.is_local, FALSE) = TRUE")
            params = {}

            if year is not None:
                filters.append("EXTRACT(YEAR FROM comp.start_date)::INTEGER = %(year)s")
                params["year"] = year
            if competition_scope and competition_scope != "all":
                filters.append("comp.competition_scope = %(competition_scope)s")
                params["competition_scope"] = competition_scope
            if governing_body and governing_body != "all":
                filters.append("comp.governing_body_code = %(governing_body)s")
                params["governing_body"] = governing_body

            where_clause = " AND ".join(filters)
            offset = (page - 1) * page_size
            params.update({"page_size": page_size, "offset": offset})

            base_cte = f"""
                WITH club_participation AS (
                    SELECT
                        club.id AS club_id,
                        club.name AS club_name,
                        COUNT(DISTINCT r.athlete_id)::INTEGER AS unique_athletes,
                        COUNT(DISTINCT comp.id)::INTEGER AS competitions_count,
                        COUNT(*)::INTEGER AS entries_count
                    FROM core.result r
                    JOIN core.event e ON e.id = r.event_id
                    JOIN core.competition comp ON comp.id = e.competition_id
                    JOIN core.club club ON club.id = r.club_id
                    WHERE {where_clause}
                    GROUP BY club.id, club.name
                )
            """

            cur.execute(base_cte + "SELECT COUNT(*) AS total FROM club_participation", params)
            total_results = cur.fetchone()["total"]

            cur.execute(
                base_cte
                + """
                SELECT
                    ROW_NUMBER() OVER (
                        ORDER BY unique_athletes DESC, competitions_count DESC, entries_count DESC, club_name ASC
                    ) AS rank,
                    club_id,
                    club_name,
                    unique_athletes,
                    competitions_count,
                    entries_count
                FROM club_participation
                ORDER BY rank ASC
                LIMIT %(page_size)s OFFSET %(offset)s
                """,
                params,
            )
            clubs = cur.fetchall()

            total_pages = math.ceil(total_results / page_size) if total_results > 0 else 1
            return {
                "data": clubs,
                "meta": {
                    "total_results": total_results,
                    "page": page,
                    "page_size": page_size,
                    "total_pages": total_pages,
                },
            }


@router.get("/clubs/participation-matrix")
def get_club_participation_matrix(
    year: Optional[int] = Query(None, ge=1900),
    governing_body: Optional[str] = Query(None),
):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            selected_year = year
            if selected_year is None:
                cur.execute("SELECT EXTRACT(YEAR FROM CURRENT_DATE)::INTEGER AS current_year")
                selected_year = cur.fetchone()["current_year"]

            filters = [
                "r.club_id IS NOT NULL",
                "COALESCE(r.status, 'unknown') NOT IN ('dns', 'scratch')",
                "EXTRACT(YEAR FROM comp.start_date)::INTEGER = %(year)s",
            ]
            if has_club_local_flag(cur):
                filters.append("COALESCE(club.is_local, FALSE) = TRUE")
            params = {"year": selected_year}

            if governing_body and governing_body != "all":
                filters.append("comp.governing_body_code = %(governing_body)s")
                params["governing_body"] = governing_body

            where_clause = " AND ".join(filters)

            cur.execute(
                f"""
                WITH participation AS (
                    SELECT
                        comp.id AS competition_id,
                        comp.name AS competition_name,
                        comp.start_date AS competition_date,
                        club.id AS club_id,
                        club.name AS club_name,
                        COUNT(DISTINCT r.athlete_id)::INTEGER AS athletes_count
                    FROM core.result r
                    JOIN core.event e ON e.id = r.event_id
                    JOIN core.competition comp ON comp.id = e.competition_id
                    JOIN core.club club ON club.id = r.club_id
                    WHERE {where_clause}
                    GROUP BY comp.id, comp.name, comp.start_date, club.id, club.name
                )
                SELECT DISTINCT
                    competition_id AS id,
                    competition_name AS name,
                    competition_date AS date
                FROM participation
                ORDER BY date ASC NULLS LAST, name ASC
                """,
                params,
            )
            competitions = cur.fetchall()

            cur.execute(
                f"""
                WITH participation AS (
                    SELECT
                        comp.id AS competition_id,
                        club.id AS club_id,
                        club.name AS club_name,
                        COUNT(DISTINCT r.athlete_id)::INTEGER AS athletes_count
                    FROM core.result r
                    JOIN core.event e ON e.id = r.event_id
                    JOIN core.competition comp ON comp.id = e.competition_id
                    JOIN core.club club ON club.id = r.club_id
                    WHERE {where_clause}
                    GROUP BY comp.id, club.id, club.name
                )
                SELECT
                    competition_id,
                    SUM(athletes_count)::INTEGER AS athletes_count
                FROM participation
                GROUP BY competition_id
                """,
                params,
            )
            totals = {
                str(row["competition_id"]): row["athletes_count"]
                for row in cur.fetchall()
            }

            cur.execute(
                f"""
                WITH participation AS (
                    SELECT
                        comp.id AS competition_id,
                        club.id AS club_id,
                        club.name AS club_name,
                        COUNT(DISTINCT r.athlete_id)::INTEGER AS athletes_count
                    FROM core.result r
                    JOIN core.event e ON e.id = r.event_id
                    JOIN core.competition comp ON comp.id = e.competition_id
                    JOIN core.club club ON club.id = r.club_id
                    WHERE {where_clause}
                    GROUP BY comp.id, club.id, club.name
                )
                SELECT
                    club_id,
                    club_name,
                    SUM(athletes_count)::INTEGER AS total_athletes,
                    COUNT(DISTINCT competition_id)::INTEGER AS competitions_count
                FROM participation
                GROUP BY club_id, club_name
                ORDER BY total_athletes DESC, competitions_count DESC, club_name ASC
                """,
                params,
            )
            club_rows = cur.fetchall()

            cur.execute(
                f"""
                WITH participation AS (
                    SELECT
                        comp.id AS competition_id,
                        club.id AS club_id,
                        COUNT(DISTINCT r.athlete_id)::INTEGER AS athletes_count
                    FROM core.result r
                    JOIN core.event e ON e.id = r.event_id
                    JOIN core.competition comp ON comp.id = e.competition_id
                    JOIN core.club club ON club.id = r.club_id
                    WHERE {where_clause}
                    GROUP BY comp.id, club.id
                )
                SELECT club_id, competition_id, athletes_count
                FROM participation
                """,
                params,
            )
            cell_counts = {
                (row["club_id"], row["competition_id"]): row["athletes_count"]
                for row in cur.fetchall()
            }

            clubs = []
            for index, club in enumerate(club_rows, start=1):
                clubs.append({
                    "rank": index,
                    "club_id": club["club_id"],
                    "club_name": club["club_name"],
                    "total_athletes": club["total_athletes"],
                    "competitions_count": club["competitions_count"],
                    "cells": {
                        str(competition["id"]): cell_counts.get((club["club_id"], competition["id"]), 0)
                        for competition in competitions
                    },
                })

            return {
                "year": selected_year,
                "governing_body": governing_body or "all",
                "competitions": competitions,
                "totals": totals,
                "clubs": clubs,
            }


@router.get("/clubs/filter-options")
def get_club_stats_filter_options():
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            filters = [
                "r.club_id IS NOT NULL",
                "COALESCE(r.status, 'unknown') NOT IN ('dns', 'scratch')",
            ]
            if has_club_local_flag(cur):
                filters.append("COALESCE(club.is_local, FALSE) = TRUE")
            where_clause = " AND ".join(filters)

            cur.execute(f"""
                SELECT DISTINCT EXTRACT(YEAR FROM comp.start_date)::INTEGER AS year
                FROM core.result r
                JOIN core.event e ON e.id = r.event_id
                JOIN core.competition comp ON comp.id = e.competition_id
                JOIN core.club club ON club.id = r.club_id
                WHERE {where_clause}
                  AND comp.start_date IS NOT NULL
                ORDER BY year DESC
            """)
            years = [row["year"] for row in cur.fetchall()]

            cur.execute(f"""
                SELECT DISTINCT comp.governing_body_code, comp.governing_body_name
                FROM core.result r
                JOIN core.event e ON e.id = r.event_id
                JOIN core.competition comp ON comp.id = e.competition_id
                JOIN core.club club ON club.id = r.club_id
                WHERE {where_clause}
                  AND comp.governing_body_code IS NOT NULL
                ORDER BY comp.governing_body_name ASC NULLS LAST, comp.governing_body_code ASC
            """)
            governing_bodies = cur.fetchall()

            return {"years": years, "governing_bodies": governing_bodies}


@router.get("/competitions")
def list_competition_stats(
    year: Optional[int] = Query(None, ge=1900),
    governing_body: Optional[str] = Query(None),
):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            selected_year = year
            if selected_year is None:
                cur.execute("SELECT EXTRACT(YEAR FROM CURRENT_DATE)::INTEGER AS current_year")
                selected_year = cur.fetchone()["current_year"]

            filters = [
                "EXTRACT(YEAR FROM comp.start_date)::INTEGER = %(year)s",
                "comp.start_date <= CURRENT_DATE",
            ]
            params = {"year": selected_year}

            if governing_body and governing_body != "all":
                filters.append("comp.governing_body_code = %(governing_body)s")
                params["governing_body"] = governing_body

            where_clause = " AND ".join(filters)

            cur.execute(
                f"""
                WITH filtered_competitions AS (
                    SELECT
                        comp.id,
                        comp.name,
                        comp.start_date AS date,
                        comp.course_type,
                        comp.governing_body_code,
                        comp.governing_body_name
                    FROM core.competition comp
                    WHERE {where_clause}
                      AND EXISTS (
                          SELECT 1
                          FROM core.event e
                          JOIN core.result r ON r.event_id = e.id
                          WHERE e.competition_id = comp.id
                            AND COALESCE(r.status, 'unknown') NOT IN ('dns', 'scratch')
                      )
                ),
                event_counts AS (
                    SELECT
                        e.competition_id,
                        COUNT(*)::INTEGER AS events_count
                    FROM core.event e
                    JOIN filtered_competitions fc ON fc.id = e.competition_id
                    GROUP BY e.competition_id
                ),
                attended_results AS (
                    SELECT
                        fc.id AS competition_id,
                        r.id AS result_id,
                        r.athlete_id,
                        a.gender AS athlete_gender,
                        r.club_id,
                        r.status
                    FROM filtered_competitions fc
                    JOIN core.event e ON e.competition_id = fc.id
                    JOIN core.result r ON r.event_id = e.id
                    JOIN core.athlete a ON a.id = r.athlete_id
                    WHERE COALESCE(r.status, 'unknown') NOT IN ('dns', 'scratch')
                ),
                result_stats AS (
                    SELECT
                        competition_id,
                        COUNT(DISTINCT athlete_id)::INTEGER AS participants_count,
                        COUNT(DISTINCT athlete_id) FILTER (WHERE athlete_gender = 'female')::INTEGER AS women_count,
                        COUNT(DISTINCT athlete_id) FILTER (WHERE athlete_gender = 'male')::INTEGER AS men_count,
                        COUNT(DISTINCT club_id) FILTER (WHERE club_id IS NOT NULL)::INTEGER AS clubs_count,
                        COUNT(*) FILTER (WHERE status = 'dsq')::INTEGER AS dsq_count,
                        COUNT(*) FILTER (WHERE status = 'valid')::INTEGER AS valid_results_count,
                        COUNT(*)::INTEGER AS entries_count
                    FROM attended_results
                    GROUP BY competition_id
                )
                SELECT
                    fc.id,
                    fc.name,
                    fc.date,
                    fc.course_type,
                    fc.governing_body_code,
                    fc.governing_body_name,
                    COALESCE(rs.participants_count, 0)::INTEGER AS participants_count,
                    COALESCE(rs.women_count, 0)::INTEGER AS women_count,
                    COALESCE(rs.men_count, 0)::INTEGER AS men_count,
                    COALESCE(rs.clubs_count, 0)::INTEGER AS clubs_count,
                    COALESCE(ec.events_count, 0)::INTEGER AS events_count,
                    COALESCE(rs.valid_results_count, 0)::INTEGER AS valid_results_count,
                    COALESCE(rs.dsq_count, 0)::INTEGER AS dsq_count,
                    COALESCE(rs.entries_count, 0)::INTEGER AS entries_count
                FROM filtered_competitions fc
                LEFT JOIN result_stats rs ON rs.competition_id = fc.id
                LEFT JOIN event_counts ec ON ec.competition_id = fc.id
                ORDER BY fc.date DESC NULLS LAST, fc.name ASC
                """,
                params,
            )

            return {
                "year": selected_year,
                "governing_body": governing_body or "all",
                "data": cur.fetchall(),
            }
