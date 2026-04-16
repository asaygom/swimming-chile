-- =====================================================
-- Migration 001 - Traceability and idempotency
-- Apply with search_path set to core, public or run after schema creation.
-- =====================================================

SET search_path TO core, public;

CREATE TABLE IF NOT EXISTS source_document (
    id BIGSERIAL PRIMARY KEY,
    source_id BIGINT REFERENCES source(id),
    document_name TEXT NOT NULL,
    document_type TEXT NOT NULL DEFAULT 'results_pdf',
    source_url TEXT,
    storage_path TEXT,
    checksum_sha256 TEXT,
    parser_version TEXT,
    metadata JSONB,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_source_document_checksum_sha256 CHECK (
        checksum_sha256 IS NULL OR checksum_sha256 ~ '^[0-9a-f]{64}$'
    )
);

CREATE TABLE IF NOT EXISTS load_run (
    id BIGSERIAL PRIMARY KEY,
    source_document_id BIGINT REFERENCES source_document(id),
    competition_id BIGINT REFERENCES competition(id),
    input_dir TEXT,
    parser_version TEXT,
    status TEXT NOT NULL CHECK (status IN ('started', 'completed', 'failed')),
    rows_club INTEGER NOT NULL DEFAULT 0 CHECK (rows_club >= 0),
    rows_event INTEGER NOT NULL DEFAULT 0 CHECK (rows_event >= 0),
    rows_athlete INTEGER NOT NULL DEFAULT 0 CHECK (rows_athlete >= 0),
    rows_result INTEGER NOT NULL DEFAULT 0 CHECK (rows_result >= 0),
    rows_relay_result INTEGER NOT NULL DEFAULT 0 CHECK (rows_relay_result >= 0),
    rows_relay_result_member INTEGER NOT NULL DEFAULT 0 CHECK (rows_relay_result_member >= 0),
    error_message TEXT,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS validation_issue (
    id BIGSERIAL PRIMARY KEY,
    load_run_id BIGINT REFERENCES load_run(id),
    competition_id BIGINT REFERENCES competition(id),
    issue_key TEXT NOT NULL,
    severity TEXT NOT NULL DEFAULT 'warning' CHECK (severity IN ('info', 'warning', 'error')),
    issue_count INTEGER NOT NULL CHECK (issue_count >= 0),
    details JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_source_document_source_id ON source_document(source_id);
CREATE INDEX IF NOT EXISTS idx_source_document_checksum_sha256 ON source_document(checksum_sha256);
CREATE UNIQUE INDEX IF NOT EXISTS ux_source_document_checksum_sha256
    ON source_document(checksum_sha256)
    WHERE checksum_sha256 IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS ux_source_document_source_url
    ON source_document(source_url)
    WHERE source_url IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_load_run_source_document_id ON load_run(source_document_id);
CREATE INDEX IF NOT EXISTS idx_load_run_competition_id ON load_run(competition_id);
CREATE INDEX IF NOT EXISTS idx_load_run_status ON load_run(status);

CREATE INDEX IF NOT EXISTS idx_validation_issue_load_run_id ON validation_issue(load_run_id);
CREATE INDEX IF NOT EXISTS idx_validation_issue_competition_id ON validation_issue(competition_id);
CREATE INDEX IF NOT EXISTS idx_validation_issue_issue_key ON validation_issue(issue_key);

CREATE UNIQUE INDEX IF NOT EXISTS ux_event_competition_event_name ON event(
    competition_id,
    LOWER(TRIM(event_name))
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_result_observed_identity ON result(
    event_id,
    athlete_id,
    COALESCE(club_id, -1),
    COALESCE(rank_position, -1),
    COALESCE(result_time_ms, -1),
    COALESCE(status, '')
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_relay_result_observed_identity ON relay_result(
    event_id,
    COALESCE(club_id, -1),
    LOWER(TRIM(relay_team_name)),
    COALESCE(rank_position, -1),
    COALESCE(result_time_ms, -1),
    COALESCE(status, '')
);
