BEGIN;

SET search_path TO core, public;

ALTER TABLE meet_program_publication
    ADD COLUMN IF NOT EXISTS stage_number INTEGER NOT NULL DEFAULT 1,
    ADD COLUMN IF NOT EXISTS pool_role TEXT NOT NULL DEFAULT 'main',
    ADD COLUMN IF NOT EXISTS scheduled_date DATE;

UPDATE meet_program_publication publication
SET scheduled_date = competition.start_date
FROM competition
WHERE publication.competition_id = competition.id
  AND publication.scheduled_date IS NULL
  AND competition.start_date = competition.end_date;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'meet_program_publication'::regclass
          AND conname = 'chk_meet_program_stage_number'
    ) THEN
        ALTER TABLE meet_program_publication
            ADD CONSTRAINT chk_meet_program_stage_number
            CHECK (stage_number > 0);
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'meet_program_publication'::regclass
          AND conname = 'chk_meet_program_pool_role'
    ) THEN
        ALTER TABLE meet_program_publication
            ADD CONSTRAINT chk_meet_program_pool_role
            CHECK (pool_role IN ('main', 'competition', 'training'));
    END IF;
END
$$;

DROP INDEX IF EXISTS ux_meet_program_one_published_per_competition;
CREATE UNIQUE INDEX IF NOT EXISTS ux_meet_program_one_published_per_segment
    ON meet_program_publication(competition_id, stage_number, pool_role)
    WHERE status = 'published';

ALTER TABLE meet_program_entry
    DROP CONSTRAINT IF EXISTS meet_program_entry_lane_check;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'meet_program_entry'::regclass
          AND conname = 'chk_meet_program_lane_nonnegative'
    ) THEN
        ALTER TABLE meet_program_entry
            ADD CONSTRAINT chk_meet_program_lane_nonnegative
            CHECK (lane >= 0);
    END IF;
END
$$;

COMMIT;
