-- Migration 013 - Preserve estimated Meet Program heat start times.

SET search_path TO core, public;

ALTER TABLE meet_program_entry
    ADD COLUMN IF NOT EXISTS estimated_start_time TEXT;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint AS constraint_row
        JOIN pg_class AS relation_row
          ON relation_row.oid = constraint_row.conrelid
        JOIN pg_namespace AS namespace_row
          ON namespace_row.oid = relation_row.relnamespace
        WHERE namespace_row.nspname = CURRENT_SCHEMA()
          AND relation_row.relname = 'meet_program_entry'
          AND constraint_row.conname = 'chk_meet_program_estimated_start_time'
          AND constraint_row.contype = 'c'
    ) THEN
        ALTER TABLE meet_program_entry
            ADD CONSTRAINT chk_meet_program_estimated_start_time CHECK (
                estimated_start_time IS NULL
                OR estimated_start_time ~ '^(?:[01][0-9]|2[0-3]):[0-5][0-9]$'
            );
    END IF;
END
$$;
