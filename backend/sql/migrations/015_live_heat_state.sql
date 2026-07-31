BEGIN;

SET search_path TO core, public;

CREATE TABLE IF NOT EXISTS live_heat_state (
    id BIGSERIAL PRIMARY KEY,
    competition_id BIGINT NOT NULL REFERENCES competition(id) ON DELETE CASCADE,
    publication_id BIGINT NOT NULL REFERENCES meet_program_publication(id) ON DELETE CASCADE,
    stage_number INTEGER NOT NULL DEFAULT 1 CHECK (stage_number > 0),
    pool_role TEXT NOT NULL DEFAULT 'main' CHECK (
        pool_role IN ('main', 'competition', 'training')
    ),
    session_number INTEGER NOT NULL CHECK (session_number > 0),
    event_number INTEGER NOT NULL CHECK (event_number > 0),
    heat_number INTEGER NOT NULL CHECK (heat_number > 0),
    status TEXT NOT NULL CHECK (
        status IN ('not_started', 'active', 'paused', 'finished')
    ),
    revision BIGINT NOT NULL DEFAULT 1 CHECK (revision > 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_by_session TEXT NOT NULL,
    UNIQUE (competition_id, stage_number, pool_role)
);

CREATE INDEX IF NOT EXISTS idx_live_heat_state_publication
    ON live_heat_state(publication_id);

COMMIT;
