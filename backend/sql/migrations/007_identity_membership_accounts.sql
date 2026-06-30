-- =====================================================
-- Migration 007 - Civil identity, club membership, and user accounts
-- Keeps private civil/contact data outside core.athlete, which remains the
-- public sports identity observed in competition results.
-- =====================================================

CREATE SCHEMA IF NOT EXISTS identity;
CREATE SCHEMA IF NOT EXISTS club_ops;
CREATE SCHEMA IF NOT EXISTS auth;

-- =====================================================
-- TABLE: identity.person
-- Real civil person. Contains private data and must not be exposed by public API.
-- =====================================================

CREATE TABLE IF NOT EXISTS identity.person (
    id BIGSERIAL PRIMARY KEY,
    rut_normalized TEXT UNIQUE,
    date_of_birth DATE,
    first_name TEXT,
    last_name TEXT,
    data_source TEXT,
    notes TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_person_rut_normalized CHECK (
        rut_normalized IS NULL OR rut_normalized ~ '^[0-9]{7,8}[0-9K]$'
    )
);

CREATE INDEX IF NOT EXISTS idx_person_rut_normalized
    ON identity.person(rut_normalized);

-- =====================================================
-- TABLE: identity.contact_point
-- Emails, phones, and future contact channels associated to a civil person.
-- =====================================================

CREATE TABLE IF NOT EXISTS identity.contact_point (
    id BIGSERIAL PRIMARY KEY,
    person_id BIGINT NOT NULL REFERENCES identity.person(id) ON DELETE CASCADE,
    contact_type TEXT NOT NULL,
    contact_value TEXT NOT NULL,
    is_primary BOOLEAN NOT NULL DEFAULT FALSE,
    verified_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_contact_type CHECK (
        contact_type IN ('email', 'phone', 'other')
    ),
    CONSTRAINT chk_contact_value_not_blank CHECK (
        LENGTH(TRIM(contact_value)) > 0
    )
);

CREATE INDEX IF NOT EXISTS idx_contact_point_person_id
    ON identity.contact_point(person_id);

CREATE UNIQUE INDEX IF NOT EXISTS ux_contact_point_person_type_value
    ON identity.contact_point(person_id, contact_type, LOWER(TRIM(contact_value)));

-- =====================================================
-- TABLE: core.athlete_person_link
-- Bridge from observed sports identity to real civil person.
-- =====================================================

CREATE TABLE IF NOT EXISTS core.athlete_person_link (
    athlete_id BIGINT NOT NULL REFERENCES core.athlete(id) ON DELETE CASCADE,
    person_id BIGINT NOT NULL REFERENCES identity.person(id) ON DELETE CASCADE,
    link_source TEXT NOT NULL,
    confidence NUMERIC(5,4),
    verified_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (athlete_id, person_id),
    CONSTRAINT chk_athlete_person_link_source CHECK (
        link_source IN ('manual_club_registry', 'self_claim', 'admin_verified', 'import')
    ),
    CONSTRAINT chk_athlete_person_link_confidence CHECK (
        confidence IS NULL OR (confidence >= 0 AND confidence <= 1)
    )
);

CREATE INDEX IF NOT EXISTS idx_athlete_person_link_person_id
    ON core.athlete_person_link(person_id);

-- =====================================================
-- TABLE: club_ops.membership
-- Club membership/management layer. Membership belongs to a person, not directly
-- to core.athlete, because athletes can have multiple observed sports identities.
-- =====================================================

CREATE TABLE IF NOT EXISTS club_ops.membership (
    id BIGSERIAL PRIMARY KEY,
    club_id BIGINT NOT NULL REFERENCES core.club(id),
    person_id BIGINT NOT NULL REFERENCES identity.person(id) ON DELETE CASCADE,
    status TEXT NOT NULL DEFAULT 'active',
    joined_at DATE,
    left_at DATE,
    member_number TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_membership_status CHECK (
        status IN ('active', 'inactive', 'invited')
    ),
    CONSTRAINT chk_membership_dates CHECK (
        left_at IS NULL OR joined_at IS NULL OR left_at >= joined_at
    )
);

CREATE INDEX IF NOT EXISTS idx_membership_club_id
    ON club_ops.membership(club_id);

CREATE INDEX IF NOT EXISTS idx_membership_person_id
    ON club_ops.membership(person_id);

CREATE UNIQUE INDEX IF NOT EXISTS ux_membership_active_club_person
    ON club_ops.membership(club_id, person_id)
    WHERE status IN ('active', 'invited');

-- =====================================================
-- TABLE: auth.user_account
-- Login account. The account maps to a civil person, not to core.athlete.
-- =====================================================

CREATE TABLE IF NOT EXISTS auth.user_account (
    id BIGSERIAL PRIMARY KEY,
    person_id BIGINT NOT NULL REFERENCES identity.person(id) ON DELETE CASCADE,
    email TEXT NOT NULL,
    password_hash TEXT,
    status TEXT NOT NULL DEFAULT 'pending',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_login_at TIMESTAMPTZ,
    CONSTRAINT chk_user_account_status CHECK (
        status IN ('pending', 'active', 'disabled')
    ),
    CONSTRAINT chk_user_account_email_not_blank CHECK (
        LENGTH(TRIM(email)) > 0
    )
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_user_account_email
    ON auth.user_account(LOWER(TRIM(email)));

CREATE INDEX IF NOT EXISTS idx_user_account_person_id
    ON auth.user_account(person_id);

-- =====================================================
-- TABLE: auth.user_role
-- Role assignments. club_id is nullable for platform-level roles.
-- =====================================================

CREATE TABLE IF NOT EXISTS auth.user_role (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL REFERENCES auth.user_account(id) ON DELETE CASCADE,
    club_id BIGINT REFERENCES core.club(id) ON DELETE CASCADE,
    role TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_user_role CHECK (
        role IN ('athlete', 'club_manager', 'club_admin', 'platform_admin')
    ),
    CONSTRAINT chk_platform_role_scope CHECK (
        (role = 'platform_admin' AND club_id IS NULL)
        OR (role <> 'platform_admin' AND club_id IS NOT NULL)
    )
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_user_role_user_club_role
    ON auth.user_role(user_id, COALESCE(club_id, -1), role);

CREATE INDEX IF NOT EXISTS idx_user_role_club_id
    ON auth.user_role(club_id);
