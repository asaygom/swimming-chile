-- =====================================================
-- Migration 005 - Competition governing body metadata
-- Adds governing body metadata separately from source and local organizer.
-- source = where the document was obtained; organizer = local host/entity;
-- governing_body = sports federation/circuit authority.
-- =====================================================

SET search_path TO core, public;

ALTER TABLE competition
    ADD COLUMN IF NOT EXISTS governing_body_code TEXT,
    ADD COLUMN IF NOT EXISTS governing_body_name TEXT;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'chk_competition_governing_body_code'
          AND conrelid = 'competition'::regclass
    ) THEN
        ALTER TABLE competition
            ADD CONSTRAINT chk_competition_governing_body_code CHECK (
                governing_body_code IS NULL OR governing_body_code ~ '^[a-z][a-z0-9_]*$'
            );
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_competition_governing_body_code
    ON competition(governing_body_code);

UPDATE competition
SET governing_body_code = 'fchmn',
    governing_body_name = 'FCHMN',
    updated_at = NOW()
WHERE competition_scope = 'fchmn_local'
  AND governing_body_code IS NULL;
