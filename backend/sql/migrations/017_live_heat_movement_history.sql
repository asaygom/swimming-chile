BEGIN;

CREATE TABLE IF NOT EXISTS core.live_heat_movement (
    id BIGSERIAL PRIMARY KEY,
    competition_id BIGINT NOT NULL REFERENCES core.competition(id) ON DELETE CASCADE,
    previous_publication_id BIGINT REFERENCES core.meet_program_publication(id),
    previous_stage_number INTEGER,
    previous_pool_role TEXT,
    previous_session_number INTEGER,
    previous_event_number INTEGER,
    previous_heat_number INTEGER,
    previous_status TEXT,
    previous_revision BIGINT,
    resulting_publication_id BIGINT NOT NULL REFERENCES core.meet_program_publication(id),
    resulting_stage_number INTEGER NOT NULL CHECK (resulting_stage_number > 0),
    resulting_pool_role TEXT NOT NULL CHECK (
        resulting_pool_role IN ('main', 'competition', 'training')
    ),
    resulting_session_number INTEGER NOT NULL CHECK (resulting_session_number > 0),
    resulting_event_number INTEGER NOT NULL CHECK (resulting_event_number > 0),
    resulting_heat_number INTEGER NOT NULL CHECK (resulting_heat_number > 0),
    resulting_status TEXT NOT NULL CHECK (
        resulting_status IN ('not_started', 'active', 'paused', 'finished')
    ),
    resulting_revision BIGINT NOT NULL CHECK (resulting_revision > 0),
    operator_session_fingerprint TEXT NOT NULL CHECK (
        operator_session_fingerprint ~ '^[0-9a-f]{64}$'
    ),
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (previous_stage_number IS NULL OR previous_stage_number > 0),
    CHECK (previous_session_number IS NULL OR previous_session_number > 0),
    CHECK (previous_event_number IS NULL OR previous_event_number > 0),
    CHECK (previous_heat_number IS NULL OR previous_heat_number > 0),
    CHECK (previous_revision IS NULL OR previous_revision > 0),
    CHECK (previous_pool_role IS NULL OR previous_pool_role IN ('main', 'competition', 'training')),
    CHECK (previous_status IS NULL OR previous_status IN ('not_started', 'active', 'paused', 'finished'))
);

CREATE INDEX IF NOT EXISTS idx_live_heat_movement_competition_occurred
    ON core.live_heat_movement(competition_id, occurred_at DESC, id DESC);

COMMIT;
