-- =====================================================
-- Migration 010 - Local club metadata by competition scope
-- Only curated Chilean scopes imply local club metadata.
-- Existing explicit metadata is preserved.
-- =====================================================

SET search_path TO core, public;

ALTER TABLE club
    ADD COLUMN IF NOT EXISTS country_code TEXT;

ALTER TABLE club
    ADD COLUMN IF NOT EXISTS is_local BOOLEAN;

COMMENT ON COLUMN club.country_code IS
    'ISO 3166-1 alpha-3 country code; NULL when unknown';

WITH local_club_observations AS (
    SELECT DISTINCT r.club_id
    FROM result r
    JOIN event e ON e.id = r.event_id
    JOIN competition c ON c.id = e.competition_id
    WHERE r.club_id IS NOT NULL
      AND c.competition_scope IN ('fchmn_local', 'fechida_master')

    UNION

    SELECT DISTINCT rr.club_id
    FROM relay_result rr
    JOIN event e ON e.id = rr.event_id
    JOIN competition c ON c.id = e.competition_id
    WHERE rr.club_id IS NOT NULL
      AND c.competition_scope IN ('fchmn_local', 'fechida_master')
)
UPDATE club AS club
SET country_code = COALESCE(club.country_code, 'CHI'),
    is_local = COALESCE(club.is_local, TRUE),
    updated_at = NOW()
FROM local_club_observations observation
WHERE club.id = observation.club_id
  AND (club.country_code IS NULL OR club.is_local IS NULL);
