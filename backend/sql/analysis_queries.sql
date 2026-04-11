-- =====================================================
-- Consultas analiticas base - Natacion Chile
-- Schema: core
--
-- Reemplazar los valores de la seccion "params" en cada
-- consulta antes de ejecutarla.
--
-- Nota de modelo:
-- - result.club_id y relay_result.club_id representan el club
--   contextual de esa competencia/resultado.
-- - athlete.club_id representa la referencia disponible del atleta,
--   pero no debe tratarse como identidad rigida de largo plazo.
-- =====================================================

SET search_path TO core, public;

-- =====================================================
-- 1) Atletas por club, usando participaciones individuales
-- =====================================================

WITH params AS (
    SELECT 'NOMBRE DEL CLUB'::text AS club_name
)
SELECT
    c.id AS club_id,
    c.name AS club_name,
    a.id AS athlete_id,
    a.full_name,
    a.gender,
    COALESCE(a.birth_year, MIN(r.birth_year_estimated)) AS birth_year,
    COUNT(*) AS individual_results,
    COUNT(DISTINCT e.competition_id) AS competitions_count,
    MIN(co.start_date) AS first_competition_date,
    MAX(co.start_date) AS last_competition_date
FROM params p
JOIN club c
  ON lower(trim(c.name)) = lower(trim(p.club_name))
JOIN result r
  ON r.club_id = c.id
JOIN athlete a
  ON a.id = r.athlete_id
JOIN event e
  ON e.id = r.event_id
JOIN competition co
  ON co.id = e.competition_id
GROUP BY c.id, c.name, a.id, a.full_name, a.gender, a.birth_year
ORDER BY a.full_name;

-- =====================================================
-- 2) Atletas por club, usando referencia actual en athlete
-- =====================================================

WITH params AS (
    SELECT 'NOMBRE DEL CLUB'::text AS club_name
)
SELECT
    c.id AS club_id,
    c.name AS club_name,
    a.id AS athlete_id,
    a.full_name,
    a.gender,
    a.birth_year,
    a.created_at
FROM params p
JOIN club c
  ON lower(trim(c.name)) = lower(trim(p.club_name))
JOIN athlete a
  ON a.club_id = c.id
ORDER BY a.full_name;

-- =====================================================
-- 3) Historial de resultados individuales por atleta
-- =====================================================

WITH params AS (
    SELECT 'NOMBRE DEL ATLETA'::text AS athlete_name
)
SELECT
    a.id AS athlete_id,
    a.full_name,
    co.name AS competition_name,
    co.start_date,
    co.course_type,
    c.name AS club_name_at_result,
    e.event_name,
    e.gender AS event_gender,
    e.age_group,
    e.distance_m,
    e.stroke,
    r.rank_position,
    r.seed_time_text,
    r.result_time_text,
    r.result_time_ms,
    r.points,
    r.age_at_event,
    r.birth_year_estimated,
    r.status
FROM params p
JOIN athlete a
  ON lower(trim(a.full_name)) = lower(trim(p.athlete_name))
JOIN result r
  ON r.athlete_id = a.id
JOIN event e
  ON e.id = r.event_id
JOIN competition co
  ON co.id = e.competition_id
LEFT JOIN club c
  ON c.id = r.club_id
ORDER BY co.start_date, co.name, e.event_order, e.event_name;

-- =====================================================
-- 4) Mejores marcas por atleta y prueba
-- =====================================================

WITH params AS (
    SELECT 'NOMBRE DEL ATLETA'::text AS athlete_name
),
ranked_results AS (
    SELECT
        a.id AS athlete_id,
        a.full_name,
        e.distance_m,
        e.stroke,
        e.gender AS event_gender,
        co.course_type,
        r.result_time_text,
        r.result_time_ms,
        co.name AS competition_name,
        co.start_date,
        c.name AS club_name_at_result,
        row_number() OVER (
            PARTITION BY a.id, e.distance_m, e.stroke, e.gender, co.course_type
            ORDER BY r.result_time_ms ASC, co.start_date ASC NULLS LAST
        ) AS rn
    FROM params p
    JOIN athlete a
      ON lower(trim(a.full_name)) = lower(trim(p.athlete_name))
    JOIN result r
      ON r.athlete_id = a.id
    JOIN event e
      ON e.id = r.event_id
    JOIN competition co
      ON co.id = e.competition_id
    LEFT JOIN club c
      ON c.id = r.club_id
    WHERE r.status = 'valid'
      AND r.result_time_ms IS NOT NULL
)
SELECT
    athlete_id,
    full_name,
    distance_m,
    stroke,
    event_gender,
    course_type,
    result_time_text,
    result_time_ms,
    competition_name,
    start_date,
    club_name_at_result
FROM ranked_results
WHERE rn = 1
ORDER BY distance_m, stroke, course_type;

-- =====================================================
-- 5) Ranking de una prueba en una competencia
-- =====================================================

WITH params AS (
    SELECT
        1::bigint AS competition_id,
        50::int AS distance_m,
        'freestyle'::text AS stroke,
        'men'::text AS event_gender,
        NULL::text AS age_group
)
SELECT
    co.name AS competition_name,
    e.event_name,
    e.age_group,
    r.rank_position,
    a.full_name,
    c.name AS club_name,
    r.result_time_text,
    r.result_time_ms,
    r.points,
    r.status
FROM params p
JOIN competition co
  ON co.id = p.competition_id
JOIN event e
  ON e.competition_id = co.id
 AND e.distance_m = p.distance_m
 AND e.stroke = p.stroke
 AND e.gender = p.event_gender
 AND (p.age_group IS NULL OR e.age_group = p.age_group)
JOIN result r
  ON r.event_id = e.id
JOIN athlete a
  ON a.id = r.athlete_id
LEFT JOIN club c
  ON c.id = r.club_id
ORDER BY
    r.rank_position NULLS LAST,
    r.result_time_ms NULLS LAST,
    a.full_name;

-- =====================================================
-- 6) Resumen individual por club y competencia
-- =====================================================

WITH params AS (
    SELECT 1::bigint AS competition_id
)
SELECT
    co.name AS competition_name,
    c.id AS club_id,
    c.name AS club_name,
    COUNT(*) AS individual_results,
    COUNT(DISTINCT r.athlete_id) AS athletes_count,
    SUM(COALESCE(r.points, 0)) AS total_points,
    COUNT(*) FILTER (WHERE r.rank_position = 1) AS first_places,
    COUNT(*) FILTER (WHERE r.rank_position = 2) AS second_places,
    COUNT(*) FILTER (WHERE r.rank_position = 3) AS third_places
FROM params p
JOIN competition co
  ON co.id = p.competition_id
JOIN event e
  ON e.competition_id = co.id
JOIN result r
  ON r.event_id = e.id
LEFT JOIN club c
  ON c.id = r.club_id
GROUP BY co.name, c.id, c.name
ORDER BY total_points DESC, individual_results DESC, club_name;

-- =====================================================
-- 7) Resultados de relevos por club
-- =====================================================

WITH params AS (
    SELECT 'NOMBRE DEL CLUB'::text AS club_name
)
SELECT
    c.id AS club_id,
    c.name AS club_name,
    co.name AS competition_name,
    co.start_date,
    e.event_name,
    e.age_group,
    rr.relay_team_name,
    rr.rank_position,
    rr.seed_time_text,
    rr.result_time_text,
    rr.result_time_ms,
    rr.points,
    rr.status
FROM params p
JOIN club c
  ON lower(trim(c.name)) = lower(trim(p.club_name))
JOIN relay_result rr
  ON rr.club_id = c.id
JOIN event e
  ON e.id = rr.event_id
JOIN competition co
  ON co.id = e.competition_id
ORDER BY co.start_date, co.name, e.event_order, rr.rank_position NULLS LAST;

-- =====================================================
-- 8) Integrantes de relevos por atleta
-- =====================================================

WITH params AS (
    SELECT 'NOMBRE DEL ATLETA'::text AS athlete_name
)
SELECT
    a.id AS athlete_id,
    a.full_name,
    co.name AS competition_name,
    co.start_date,
    c.name AS club_name,
    e.event_name,
    rr.relay_team_name,
    rr.rank_position,
    rr.result_time_text,
    rrm.leg_order,
    rrm.age_at_event,
    rrm.birth_year_estimated
FROM params p
JOIN athlete a
  ON lower(trim(a.full_name)) = lower(trim(p.athlete_name))
JOIN relay_result_member rrm
  ON rrm.athlete_id = a.id
JOIN relay_result rr
  ON rr.id = rrm.relay_result_id
JOIN event e
  ON e.id = rr.event_id
JOIN competition co
  ON co.id = e.competition_id
LEFT JOIN club c
  ON c.id = rr.club_id
ORDER BY co.start_date, co.name, e.event_order, rrm.leg_order;

-- =====================================================
-- 9) Actividad completa de un atleta: individual + relevo
-- =====================================================

WITH params AS (
    SELECT 'NOMBRE DEL ATLETA'::text AS athlete_name
),
individual_results AS (
    SELECT
        'individual'::text AS result_type,
        a.id AS athlete_id,
        a.full_name,
        co.name AS competition_name,
        co.start_date,
        c.name AS club_name,
        e.event_name,
        r.rank_position,
        r.result_time_text,
        r.result_time_ms,
        NULL::integer AS leg_order,
        r.points,
        r.status
    FROM params p
    JOIN athlete a
      ON lower(trim(a.full_name)) = lower(trim(p.athlete_name))
    JOIN result r
      ON r.athlete_id = a.id
    JOIN event e
      ON e.id = r.event_id
    JOIN competition co
      ON co.id = e.competition_id
    LEFT JOIN club c
      ON c.id = r.club_id
),
relay_results AS (
    SELECT
        'relay'::text AS result_type,
        a.id AS athlete_id,
        a.full_name,
        co.name AS competition_name,
        co.start_date,
        c.name AS club_name,
        e.event_name,
        rr.rank_position,
        rr.result_time_text,
        rr.result_time_ms,
        rrm.leg_order,
        rr.points,
        rr.status
    FROM params p
    JOIN athlete a
      ON lower(trim(a.full_name)) = lower(trim(p.athlete_name))
    JOIN relay_result_member rrm
      ON rrm.athlete_id = a.id
    JOIN relay_result rr
      ON rr.id = rrm.relay_result_id
    JOIN event e
      ON e.id = rr.event_id
    JOIN competition co
      ON co.id = e.competition_id
    LEFT JOIN club c
      ON c.id = rr.club_id
)
SELECT *
FROM individual_results
UNION ALL
SELECT *
FROM relay_results
ORDER BY start_date, competition_name, result_type, event_name;

-- =====================================================
-- 10) Clubes con posibles nombres fragmentados todavia
-- =====================================================

SELECT
    lower(regexp_replace(trim(name), '\s+', ' ', 'g')) AS normalized_name,
    COUNT(*) AS club_rows,
    array_agg(id ORDER BY id) AS club_ids,
    array_agg(name ORDER BY id) AS club_names
FROM club
GROUP BY lower(regexp_replace(trim(name), '\s+', ' ', 'g'))
HAVING COUNT(*) > 1
ORDER BY club_rows DESC, normalized_name;
