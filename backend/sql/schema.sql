-- =====================================================
-- Schema v0.1 - Natacion Chile
-- Database: natacion_chile
-- Schema: core
-- Estado actual: incluye resultados individuales, relevos
-- y tablas staging para cargas desde Excel/CSV/PDF parser.
-- =====================================================

SET search_path TO core, public;

-- =====================================================
-- TABLE: source
-- =====================================================

CREATE TABLE source (
    id BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    source_type TEXT NOT NULL,
    base_url TEXT,
    notes TEXT,
    last_checked_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- =====================================================
-- TABLE: club
-- =====================================================

CREATE TABLE club (
    id BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    short_name TEXT,
    city TEXT,
    region TEXT,
    association_name TEXT,
    website TEXT,
    instagram TEXT,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    source_id BIGINT REFERENCES source(id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- =====================================================
-- TABLE: pool
-- =====================================================

CREATE TABLE pool (
    id BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    city TEXT,
    region TEXT,
    address TEXT,
    latitude NUMERIC(9,6),
    longitude NUMERIC(9,6),
    pool_length_m INTEGER CHECK (pool_length_m > 0),
    lanes_count INTEGER CHECK (lanes_count > 0),
    indoor_outdoor TEXT CHECK (indoor_outdoor IN ('indoor', 'outdoor', 'mixed', 'unknown')),
    heated BOOLEAN,
    public_access_type TEXT CHECK (public_access_type IN ('public', 'municipal', 'club', 'private', 'school', 'university', 'unknown')),
    website TEXT,
    contact_info TEXT,
    notes TEXT,
    source_id BIGINT REFERENCES source(id),
    last_verified_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_pool_latitude CHECK (latitude IS NULL OR (latitude BETWEEN -90 AND 90)),
    CONSTRAINT chk_pool_longitude CHECK (longitude IS NULL OR (longitude BETWEEN -180 AND 180))
);

-- =====================================================
-- TABLE: competition
-- =====================================================

CREATE TABLE competition (
    id BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    season_year INTEGER CHECK (season_year IS NULL OR season_year >= 1900),
    start_date DATE,
    end_date DATE,
    city TEXT,
    region TEXT,
    venue_name TEXT,
    pool_id BIGINT REFERENCES pool(id),
    organizer TEXT,
    competition_type TEXT CHECK (
        competition_type IN ('national', 'regional', 'master', 'open', 'school', 'other')
    ),
    course_type TEXT CHECK (
        course_type IN ('scm', 'lcm', 'unknown')
    ),
    status TEXT CHECK (
        status IN ('planned', 'finished', 'cancelled', 'postponed')
    ),
    source_id BIGINT REFERENCES source(id),
    source_url TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_competition_date_range CHECK (
        start_date IS NULL
        OR end_date IS NULL
        OR end_date >= start_date
    )
);

-- =====================================================
-- TABLE: event
-- =====================================================

CREATE TABLE event (
    id BIGSERIAL PRIMARY KEY,
    competition_id BIGINT NOT NULL REFERENCES competition(id),
    event_name TEXT NOT NULL,
    stroke TEXT CHECK (
        stroke IN (
            'freestyle',
            'backstroke',
            'breaststroke',
            'butterfly',
            'individual_medley',
            'medley_relay',
            'freestyle_relay'
        )
    ),
    distance_m INTEGER CHECK (distance_m IS NULL OR distance_m > 0),
    gender TEXT CHECK (
        gender IN ('women', 'men', 'mixed')
    ),
    age_group TEXT,
    round_type TEXT CHECK (
        round_type IN ('heats', 'final', 'timed_final', 'semifinal', 'unknown')
    ),
    event_order INTEGER CHECK (event_order IS NULL OR event_order > 0),
    scheduled_date DATE,
    source_id BIGINT REFERENCES source(id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- =====================================================
-- TABLE: athlete
-- =====================================================

CREATE TABLE athlete (
    id BIGSERIAL PRIMARY KEY,
    full_name TEXT NOT NULL,
    gender TEXT CHECK (
        gender IN ('male', 'female')
    ),
    birth_year INTEGER CHECK (
        birth_year IS NULL OR birth_year >= 1900
    ),
    nationality TEXT,
    club_id BIGINT REFERENCES club(id),
    source_id BIGINT REFERENCES source(id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- =====================================================
-- TABLE: result
-- =====================================================

CREATE TABLE result (
    id BIGSERIAL PRIMARY KEY,
    event_id BIGINT NOT NULL REFERENCES event(id),
    athlete_id BIGINT NOT NULL REFERENCES athlete(id),
    club_id BIGINT REFERENCES club(id),
    lane INTEGER CHECK (lane IS NULL OR lane > 0),
    heat_number INTEGER CHECK (heat_number IS NULL OR heat_number > 0),
    rank_position INTEGER CHECK (rank_position IS NULL OR rank_position > 0),
    result_time_text TEXT,
    result_time_ms BIGINT CHECK (result_time_ms IS NULL OR result_time_ms >= 0),
    seed_time_text TEXT,
    seed_time_ms BIGINT CHECK (seed_time_ms IS NULL OR seed_time_ms >= 0),
    points NUMERIC(10,2),
    age_at_event INTEGER CHECK (age_at_event IS NULL OR age_at_event > 0),
    birth_year_estimated INTEGER CHECK (birth_year_estimated IS NULL OR birth_year_estimated >= 1900),
    record_flag TEXT,
    status TEXT CHECK (
        status IN ('valid', 'dns', 'dnf', 'dsq', 'scratch', 'unknown')
    ),
    source_id BIGINT REFERENCES source(id),
    source_url TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- =====================================================
-- TABLE: relay_result
-- =====================================================

CREATE TABLE relay_result (
    id BIGSERIAL PRIMARY KEY,
    event_id BIGINT NOT NULL REFERENCES event(id),
    club_id BIGINT REFERENCES club(id),
    relay_team_name TEXT NOT NULL,
    lane INTEGER CHECK (lane IS NULL OR lane > 0),
    heat_number INTEGER CHECK (heat_number IS NULL OR heat_number > 0),
    rank_position INTEGER CHECK (rank_position IS NULL OR rank_position > 0),
    result_time_text TEXT,
    result_time_ms BIGINT CHECK (result_time_ms IS NULL OR result_time_ms >= 0),
    seed_time_text TEXT,
    seed_time_ms BIGINT CHECK (seed_time_ms IS NULL OR seed_time_ms >= 0),
    points NUMERIC(10,2),
    reaction_time NUMERIC(6,3) CHECK (reaction_time IS NULL OR reaction_time >= 0),
    record_flag TEXT,
    status TEXT CHECK (
        status IN ('valid', 'dns', 'dnf', 'dsq', 'scratch', 'unknown')
    ),
    source_id BIGINT REFERENCES source(id),
    source_url TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- =====================================================
-- TABLE: relay_result_member
-- =====================================================

CREATE TABLE relay_result_member (
    id BIGSERIAL PRIMARY KEY,
    relay_result_id BIGINT NOT NULL REFERENCES relay_result(id),
    athlete_id BIGINT REFERENCES athlete(id),
    leg_order INTEGER NOT NULL CHECK (leg_order BETWEEN 1 AND 4),
    athlete_name_raw TEXT,
    gender TEXT CHECK (gender IN ('male', 'female')),
    age_at_event INTEGER CHECK (age_at_event IS NULL OR age_at_event > 0),
    birth_year_estimated INTEGER CHECK (birth_year_estimated IS NULL OR birth_year_estimated >= 1900),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (relay_result_id, leg_order)
);

-- =====================================================
-- TABLE: record
-- =====================================================

CREATE TABLE record (
    id BIGSERIAL PRIMARY KEY,
    record_type TEXT NOT NULL,
    stroke TEXT CHECK (
        stroke IN (
            'freestyle',
            'backstroke',
            'breaststroke',
            'butterfly',
            'individual_medley',
            'medley_relay',
            'freestyle_relay'
        )
    ),
    distance_m INTEGER NOT NULL CHECK (distance_m > 0),
    gender TEXT NOT NULL CHECK (
        gender IN ('male', 'female', 'mixed', 'unknown')
    ),
    age_group TEXT,
    course_type TEXT NOT NULL CHECK (
        course_type IN ('scm', 'lcm', 'unknown')
    ),
    result_time_text TEXT NOT NULL,
    result_time_ms BIGINT CHECK (result_time_ms IS NULL OR result_time_ms >= 0),
    athlete_name TEXT,
    club_name TEXT,
    record_date DATE,
    competition_name TEXT,
    city TEXT,
    source_id BIGINT REFERENCES source(id),
    source_url TEXT,
    is_current BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- =====================================================
-- STAGING TABLES
-- =====================================================

CREATE TABLE stg_club (
    name TEXT,
    short_name TEXT,
    city TEXT,
    region TEXT,
    source_id TEXT
);

CREATE TABLE stg_event (
    competition_id TEXT,
    event_name TEXT,
    stroke TEXT,
    distance_m TEXT,
    gender TEXT,
    age_group TEXT,
    round_type TEXT,
    source_id TEXT
);

CREATE TABLE stg_athlete (
    full_name TEXT,
    gender TEXT,
    club_name TEXT,
    source_id TEXT
);

CREATE TABLE stg_result (
    event_name TEXT,
    athlete_name TEXT,
    club_name TEXT,
    rank_position TEXT,
    result_time_text TEXT,
    result_time_ms TEXT,
    age_at_event TEXT,
    birth_year_estimated TEXT,
    seed_time_text TEXT,
    seed_time_ms TEXT,
    status TEXT,
    source_id TEXT
);

CREATE TABLE stg_relay_result (
    event_name TEXT,
    club_name TEXT,
    relay_team_name TEXT,
    lane TEXT,
    heat_number TEXT,
    rank_position TEXT,
    result_time_text TEXT,
    result_time_ms TEXT,
    points TEXT,
    reaction_time TEXT,
    record_flag TEXT,
    status TEXT,
    source_id TEXT,
    source_url TEXT,
    seed_time_text TEXT,
    seed_time_ms TEXT
);

CREATE TABLE stg_relay_result_member (
    event_name TEXT,
    club_name TEXT,
    relay_team_name TEXT,
    leg_order TEXT,
    athlete_name TEXT,
    gender TEXT,
    age_at_event TEXT,
    birth_year_estimated TEXT
);

-- =====================================================
-- INDICES
-- =====================================================

CREATE INDEX idx_club_source_id ON club(source_id);
CREATE INDEX idx_pool_source_id ON pool(source_id);
CREATE INDEX idx_pool_region_city ON pool(region, city);

CREATE INDEX idx_competition_pool_id ON competition(pool_id);
CREATE INDEX idx_competition_source_id ON competition(source_id);
CREATE INDEX idx_competition_start_date ON competition(start_date);
CREATE INDEX idx_competition_season_year ON competition(season_year);

CREATE INDEX idx_event_competition_id ON event(competition_id);
CREATE INDEX idx_event_source_id ON event(source_id);
CREATE INDEX idx_event_scheduled_date ON event(scheduled_date);

CREATE INDEX idx_athlete_club_id ON athlete(club_id);
CREATE INDEX idx_athlete_source_id ON athlete(source_id);
CREATE INDEX idx_athlete_full_name ON athlete(full_name);

CREATE INDEX idx_result_event_id ON result(event_id);
CREATE INDEX idx_result_athlete_id ON result(athlete_id);
CREATE INDEX idx_result_club_id ON result(club_id);
CREATE INDEX idx_result_source_id ON result(source_id);
CREATE INDEX idx_result_rank_position ON result(rank_position);
CREATE INDEX idx_result_status ON result(status);

CREATE INDEX idx_relay_result_event_id ON relay_result(event_id);
CREATE INDEX idx_relay_result_club_id ON relay_result(club_id);
CREATE INDEX idx_relay_result_source_id ON relay_result(source_id);
CREATE INDEX idx_relay_result_rank_position ON relay_result(rank_position);
CREATE INDEX idx_relay_result_status ON relay_result(status);

CREATE INDEX idx_relay_result_member_relay_result_id ON relay_result_member(relay_result_id);
CREATE INDEX idx_relay_result_member_athlete_id ON relay_result_member(athlete_id);

CREATE INDEX idx_record_source_id ON record(source_id);
CREATE INDEX idx_record_is_current ON record(is_current);
CREATE INDEX idx_record_gender_course_type ON record(gender, course_type);
CREATE INDEX idx_record_distance_stroke ON record(distance_m, stroke);