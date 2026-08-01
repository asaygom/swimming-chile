BEGIN;

CREATE SCHEMA IF NOT EXISTS auth;

CREATE TABLE IF NOT EXISTS auth.user_competition_role (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL REFERENCES auth.user_account(id) ON DELETE CASCADE,
    competition_id BIGINT NOT NULL REFERENCES core.competition(id) ON DELETE CASCADE,
    role TEXT NOT NULL CHECK (role IN ('competition_admin')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (user_id, competition_id, role)
);

CREATE INDEX IF NOT EXISTS idx_user_competition_role_competition
    ON auth.user_competition_role(competition_id, role);

CREATE TABLE IF NOT EXISTS auth.admin_session (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL REFERENCES auth.user_account(id) ON DELETE CASCADE,
    token_hash TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ NOT NULL,
    revoked_at TIMESTAMPTZ,
    CONSTRAINT chk_admin_session_token_hash CHECK (
        token_hash ~ '^[0-9a-f]{64}$'
    ),
    CONSTRAINT chk_admin_session_expiry CHECK (expires_at > created_at),
    CONSTRAINT chk_admin_session_revocation CHECK (
        revoked_at IS NULL OR revoked_at >= created_at
    )
);

CREATE INDEX IF NOT EXISTS idx_admin_session_user_expires
    ON auth.admin_session(user_id, expires_at);

CREATE UNIQUE INDEX IF NOT EXISTS ux_admin_session_one_per_user
    ON auth.admin_session(user_id);

CREATE TABLE IF NOT EXISTS core.live_announcement (
    id BIGSERIAL PRIMARY KEY,
    competition_id BIGINT NOT NULL REFERENCES core.competition(id) ON DELETE CASCADE,
    message TEXT NOT NULL,
    display_mode TEXT NOT NULL CHECK (display_mode IN ('fullscreen', 'ticker')),
    is_active BOOLEAN NOT NULL DEFAULT FALSE,
    revision BIGINT NOT NULL DEFAULT 1 CHECK (revision > 0),
    created_by_user_id BIGINT NOT NULL REFERENCES auth.user_account(id),
    updated_by_user_id BIGINT NOT NULL REFERENCES auth.user_account(id),
    activated_by_user_id BIGINT REFERENCES auth.user_account(id),
    deleted_by_user_id BIGINT REFERENCES auth.user_account(id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    activated_at TIMESTAMPTZ,
    deleted_at TIMESTAMPTZ,
    CONSTRAINT chk_live_announcement_message CHECK (
        LENGTH(TRIM(message)) BETWEEN 1 AND 1000
    ),
    CONSTRAINT chk_live_announcement_ticker_length CHECK (
        display_mode <> 'ticker' OR LENGTH(TRIM(message)) <= 240
    ),
    CONSTRAINT chk_live_announcement_activation_audit CHECK (
        (activated_at IS NULL) = (activated_by_user_id IS NULL)
    ),
    CONSTRAINT chk_live_announcement_deletion_audit CHECK (
        (deleted_at IS NULL) = (deleted_by_user_id IS NULL)
    ),
    CONSTRAINT chk_live_announcement_active_not_deleted CHECK (
        NOT (is_active AND deleted_at IS NOT NULL)
    )
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_live_announcement_one_active_per_competition
    ON core.live_announcement(competition_id)
    WHERE is_active AND deleted_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_live_announcement_competition_updated
    ON core.live_announcement(competition_id, updated_at DESC);

COMMIT;
