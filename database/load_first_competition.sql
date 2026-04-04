INSERT INTO source (name, source_type, base_url, notes)
VALUES ('FCHMN', 'federation_website_pdf', 'https://fchmn.cl', 'PDF de resultados II Copa Chile 2026 - Resultados oficiales en PDF. Nombre de competencia y recinto corregidos manualmente con validación directa del evento.');

INSERT INTO pool (
    name,
    city,
    region,
    address,
    pool_length_m,
    lanes_count,
    indoor_outdoor,
    source_id
)
VALUES (
    'Club Deportivo Universidad Católica',
    'Santiago',
    'Metropolitana',
    'Avda. Las Flores 13.000 San Carlos de Apoquindo, Las Condes, Santiago-Chile',
    50,
    10,
    'indoor',
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
    'II Copa Cordillera - Etapa Chile',
    2026,
    '2026-03-14',
    '2026-03-14',
    'Santiago',
    'Metropolitana',
    'Club Deportivo Universidad Católica',
    1,
    'FCHMN',
    'master',
    'lcm',
    'finished',
    1,
    'https://fchmn.cl/wp-content/uploads/2026/03/resultados-ii-copa-chile-1.pdf'
);