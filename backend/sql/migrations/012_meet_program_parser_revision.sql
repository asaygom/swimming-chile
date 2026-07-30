-- Migration 012 - Republish one Meet Program PDF after parser corrections.

SET search_path TO core, public;

DO $$
DECLARE
    legacy_constraint TEXT;
BEGIN
    FOR legacy_constraint IN
        SELECT constraint_row.conname
        FROM pg_constraint AS constraint_row
        JOIN pg_class AS relation_row
          ON relation_row.oid = constraint_row.conrelid
        JOIN pg_namespace AS namespace_row
          ON namespace_row.oid = relation_row.relnamespace
        WHERE namespace_row.nspname = CURRENT_SCHEMA()
          AND relation_row.relname = 'meet_program_publication'
          AND constraint_row.contype = 'u'
          AND (
              SELECT ARRAY_AGG(attribute_row.attname::TEXT ORDER BY key_row.ordinality)
              FROM UNNEST(constraint_row.conkey)
                   WITH ORDINALITY AS key_row(attnum, ordinality)
              JOIN pg_attribute AS attribute_row
                ON attribute_row.attrelid = constraint_row.conrelid
               AND attribute_row.attnum = key_row.attnum
          ) = ARRAY['competition_id', 'source_checksum_sha256']
    LOOP
        EXECUTE FORMAT(
            'ALTER TABLE meet_program_publication DROP CONSTRAINT %I',
            legacy_constraint
        );
    END LOOP;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint AS constraint_row
        JOIN pg_class AS relation_row
          ON relation_row.oid = constraint_row.conrelid
        JOIN pg_namespace AS namespace_row
          ON namespace_row.oid = relation_row.relnamespace
        WHERE namespace_row.nspname = CURRENT_SCHEMA()
          AND relation_row.relname = 'meet_program_publication'
          AND constraint_row.conname =
              'uq_meet_program_publication_source_parser'
          AND constraint_row.contype = 'u'
    ) THEN
        ALTER TABLE meet_program_publication
            ADD CONSTRAINT uq_meet_program_publication_source_parser
            UNIQUE (competition_id, source_checksum_sha256, parser_version);
    END IF;
END
$$;
