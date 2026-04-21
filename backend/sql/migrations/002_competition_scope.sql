-- =====================================================
-- Migration 002 - Curated competition scope
-- Persists manifest/batch competition_scope on core.competition for filtering.
-- =====================================================

SET search_path TO core, public;

ALTER TABLE competition
    ADD COLUMN IF NOT EXISTS competition_scope TEXT;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'chk_competition_scope'
          AND conrelid = 'competition'::regclass
    ) THEN
        ALTER TABLE competition
            ADD CONSTRAINT chk_competition_scope CHECK (
                competition_scope IS NULL OR competition_scope ~ '^[a-z][a-z0-9_]*$'
            );
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_competition_scope ON competition(competition_scope);
