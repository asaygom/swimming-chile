
-- inserts.
INSERT INTO source (name, source_type, base_url)
VALUES ('FECHIDA', 'federation_website', 'https://fechida.cl');

INSERT INTO club (name, short_name, city, region, source_id)
VALUES ('Club Deportivo Ejemplo', 'CDE', 'Santiago', 'Metropolitana', 1);

INSERT INTO pool (
    name,
    city,
    region,
    pool_length_m,
    lanes_count,
    indoor_outdoor,
    public_access_type,
    source_id
)
VALUES (
    'Piscina Olímpica Ejemplo',
    'Santiago',
    'Metropolitana',
    50,
    8,
    'indoor',
    'public',
    1
);

INSERT INTO competition (
    name,
    season_year,
    start_date,
    end_date,
    city,
    region,
    venue_name,
    pool_id,
    organizer,
    competition_type,
    course_type,
    status,
    source_id,
    source_url
)
VALUES (
    'Campeonato Nacional de Verano 2026',
    2026,
    '2026-01-15',
    '2026-01-18',
    'Santiago',
    'Metropolitana',
    'Piscina Olímpica Ejemplo',
    1,
    'FECHIDA',
    'national',
    'lcm',
    'finished',
    1,
    'https://fechida.cl'
);

INSERT INTO event (
    competition_id,
    event_name,
    stroke,
    distance_m,
    gender,
    age_group,
    round_type,
    event_order,
    scheduled_date,
    source_id
)
VALUES (
    1,
    '100 libre masculino absoluto',
    'freestyle',
    100,
    'male',
    'absoluto',
    'final',
    1,
    '2026-01-15',
    1
);

INSERT INTO athlete (
    full_name,
    gender,
    birth_year,
    nationality,
    club_id,
    source_id
)
VALUES (
    'Nadador Ejemplo',
    'male',
    2000,
    'CHI',
    1,
    1
);

INSERT INTO result (
    event_id,
    athlete_id,
    club_id,
    lane,
    heat_number,
    rank_position,
    result_time_text,
    result_time_ms,
    points,
    reaction_time,
    record_flag,
    status,
    source_id,
    source_url
)
VALUES (
    1,
    1,
    1,
    4,
    1,
    1,
    '00:52.34',
    52340,
    10.00,
    0.650,
    NULL,
    'valid',
    1,
    'https://fechida.cl'
);

INSERT INTO record (
    record_type,
    stroke,
    distance_m,
    gender,
    age_group,
    course_type,
    result_time_text,
    result_time_ms,
    athlete_name,
    club_name,
    record_date,
    competition_name,
    city,
    source_id,
    source_url,
    is_current
)
VALUES (
    'national',
    'freestyle',
    100,
    'male',
    'absoluto',
    'lcm',
    '00:49.99',
    49990,
    'Nadador Ejemplo',
    'Club Deportivo Ejemplo',
    '2026-01-15',
    'Campeonato Nacional de Verano 2026',
    'Santiago',
    1,
    'https://fechida.cl',
    TRUE
);