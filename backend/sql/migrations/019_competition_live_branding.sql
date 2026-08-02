BEGIN;

CREATE TABLE IF NOT EXISTS core.competition_live_branding (
    competition_id BIGINT PRIMARY KEY REFERENCES core.competition(id) ON DELETE CASCADE,
    logo_bytes BYTEA,
    mime_type TEXT CHECK (mime_type IS NULL OR mime_type IN ('image/png', 'image/jpeg', 'image/webp')),
    width INTEGER CHECK (width IS NULL OR width BETWEEN 1 AND 4096),
    height INTEGER CHECK (height IS NULL OR height BETWEEN 1 AND 4096),
    sha256 TEXT CHECK (sha256 IS NULL OR sha256 ~ '^[0-9a-f]{64}$'),
    revision BIGINT NOT NULL DEFAULT 1 CHECK (revision > 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_by_user_id BIGINT NOT NULL REFERENCES auth.user_account(id),
    deleted_at TIMESTAMPTZ,
    deleted_by_user_id BIGINT REFERENCES auth.user_account(id),
    CHECK (logo_bytes IS NULL OR OCTET_LENGTH(logo_bytes) <= 2097152),
    CHECK (
        (deleted_at IS NULL AND deleted_by_user_id IS NULL
         AND logo_bytes IS NOT NULL AND mime_type IS NOT NULL
         AND width IS NOT NULL AND height IS NOT NULL AND sha256 IS NOT NULL)
        OR
        (deleted_at IS NOT NULL AND deleted_by_user_id IS NOT NULL
         AND logo_bytes IS NULL AND mime_type IS NULL
         AND width IS NULL AND height IS NULL AND sha256 IS NULL)
    )
);

COMMIT;
