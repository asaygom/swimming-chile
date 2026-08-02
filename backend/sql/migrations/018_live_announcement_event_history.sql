BEGIN;

CREATE TABLE IF NOT EXISTS core.live_announcement_event (
    id BIGSERIAL PRIMARY KEY,
    competition_id BIGINT NOT NULL REFERENCES core.competition(id) ON DELETE CASCADE,
    announcement_id BIGINT NOT NULL REFERENCES core.live_announcement(id),
    event_type TEXT NOT NULL CHECK (
        event_type IN (
            'create', 'update', 'activate', 'automatic_deactivate',
            'deactivate', 'delete'
        )
    ),
    revision BIGINT NOT NULL CHECK (revision > 0),
    message TEXT NOT NULL,
    display_mode TEXT NOT NULL CHECK (display_mode IN ('fullscreen', 'ticker')),
    is_active BOOLEAN NOT NULL,
    is_deleted BOOLEAN NOT NULL,
    actor_user_id BIGINT NOT NULL REFERENCES auth.user_account(id),
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_live_announcement_event_competition_occurred
    ON core.live_announcement_event(competition_id, occurred_at DESC, id DESC);

COMMIT;
