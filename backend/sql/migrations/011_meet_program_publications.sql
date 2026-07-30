-- =====================================================
-- Migration 011 - Versioned public Meet Manager programs
-- Program entries preserve source display values and do not link core identity.
-- =====================================================

SET search_path TO core, public;

CREATE TABLE IF NOT EXISTS meet_program_publication (
    id BIGSERIAL PRIMARY KEY,
    competition_id BIGINT NOT NULL REFERENCES competition(id),
    source_document_id BIGINT NOT NULL REFERENCES source_document(id),
    source_checksum_sha256 TEXT NOT NULL,
    source_url TEXT,
    parser_version TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('pending', 'published', 'superseded')),
    metadata JSONB NOT NULL DEFAULT '{}'::JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    published_at TIMESTAMPTZ,
    superseded_at TIMESTAMPTZ,
    CONSTRAINT chk_meet_program_checksum_sha256 CHECK (
        source_checksum_sha256 ~ '^[0-9a-f]{64}$'
    ),
    UNIQUE (competition_id, source_checksum_sha256)
);

CREATE TABLE IF NOT EXISTS meet_program_entry (
    id BIGSERIAL PRIMARY KEY,
    publication_id BIGINT NOT NULL REFERENCES meet_program_publication(id) ON DELETE CASCADE,
    session_number INTEGER NOT NULL CHECK (session_number > 0),
    session_name TEXT NOT NULL,
    event_number INTEGER NOT NULL CHECK (event_number > 0),
    event_name TEXT NOT NULL,
    heat_number INTEGER NOT NULL CHECK (heat_number > 0),
    heat_total INTEGER CHECK (heat_total IS NULL OR heat_total > 0),
    lane INTEGER NOT NULL CHECK (lane > 0),
    display_name TEXT NOT NULL,
    age INTEGER CHECK (age IS NULL OR age > 0),
    team_name TEXT,
    seed_time_text TEXT,
    seed_time_ms BIGINT CHECK (seed_time_ms IS NULL OR seed_time_ms >= 0),
    entry_type TEXT NOT NULL CHECK (entry_type IN ('individual', 'relay')),
    relay_members JSONB NOT NULL DEFAULT '[]'::JSONB,
    page_number INTEGER CHECK (page_number IS NULL OR page_number > 0),
    column_number INTEGER CHECK (column_number IS NULL OR column_number > 0),
    line_number INTEGER CHECK (line_number IS NULL OR line_number > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (publication_id, session_number, event_number, heat_number, lane)
);

CREATE INDEX IF NOT EXISTS idx_meet_program_publication_competition_id
    ON meet_program_publication(competition_id);

CREATE UNIQUE INDEX IF NOT EXISTS ux_meet_program_one_published_per_competition
    ON meet_program_publication(competition_id)
    WHERE status = 'published';

CREATE INDEX IF NOT EXISTS idx_meet_program_entry_publication_id
    ON meet_program_entry(publication_id);

CREATE INDEX IF NOT EXISTS idx_meet_program_entry_lookup
    ON meet_program_entry(publication_id, session_number, event_number, heat_number);
