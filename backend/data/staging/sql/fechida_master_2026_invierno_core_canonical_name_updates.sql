-- Generated from reviewed athlete identity decisions. Do not edit manually.
BEGIN;

CREATE TEMP TABLE reviewed_athlete_canonical_name (
    athlete_id BIGINT PRIMARY KEY,
    expected_names JSONB NOT NULL,
    canonical_full_name TEXT NOT NULL
) ON COMMIT DROP;

INSERT INTO reviewed_athlete_canonical_name (athlete_id, expected_names, canonical_full_name)
VALUES
    (37, '["Blanco, Domingo Francisco"]'::jsonb, 'Blanco, Domingo Francisco Ca'),
    (191, '["Olivares, Pedro"]'::jsonb, 'Olivares, Pedro J'),
    (257, '["Toledo Irribarra, Fredi"]'::jsonb, 'Toledo Irribarra, Fredi Leonardo'),
    (264, '["Torres, Sergio"]'::jsonb, 'Torres, Sergio Antonio'),
    (286, '["Walter Rosales, Raul"]'::jsonb, 'Walter Rosales, Raul Alonso'),
    (385, '["Gonzalez Ibanez, Alvaro"]'::jsonb, 'Gonzalez, Alvaro Nicolas Guil'),
    (436, '["Muñoz, Miguel"]'::jsonb, 'Muñoz, Miguel Angel'),
    (438, '["Navarrete, Luis"]'::jsonb, 'Navarrete, Luis Felipe'),
    (486, '["Salas, Fernando"]'::jsonb, 'Salas, Fernando Arturo'),
    (558, '["Aguilera, Felipe"]'::jsonb, 'Aguilera, Felipe Antonio'),
    (617, '["Castro, Jefferson"]'::jsonb, 'Castro, Jefferson José'),
    (619, '["Caviedes, Pilar"]'::jsonb, 'Caviedes, Maria del Pilar'),
    (649, '["Elgart, Patricio"]'::jsonb, 'Elgart, Patricio Raul'),
    (1550, '["Levrini, Aldo"]'::jsonb, 'Levrini, Aldo Rene'),
    (1661, '["Aguilera, Marisol"]'::jsonb, 'Aguilera, Marisol Lucia'),
    (2130, '["Fabres, Nicolas"]'::jsonb, 'Fabres, Nicolas Alejandro'),
    (2134, '["Fuster, Maria Isabel"]'::jsonb, 'Fuster, Maria Isabel Silvan'),
    (2164, '["Iriarte, Rocio"]'::jsonb, 'Iriarte, Rocio Carolina'),
    (2216, '["Peña, Daniela"]'::jsonb, 'Peña, Daniela Vanessa'),
    (2230, '["Quintanilla, Francisca"]'::jsonb, 'Quintanilla, Francisca Alejandra'),
    (2247, '["Salaverry, Matias"]'::jsonb, 'Salaverry, Matias Ernesto'),
    (2257, '["Sillano, Mauricio"]'::jsonb, 'Sillano, Mauricio Alfredo'),
    (2264, '["Torres Cabrera, Renzo"]'::jsonb, 'Torres Cabrera, Renzo Ricardo'),
    (2274, '["Winter Silva, Juan"]'::jsonb, 'Winter Silva, Juan Enrique'),
    (2282, '["Cera, Mireya"]'::jsonb, 'Cera, Mireya E'),
    (2511, '["Del Rio, Devaky"]'::jsonb, 'Del Rio, Devaky Aruna'),
    (2545, '["Infante, Roberto"]'::jsonb, 'Infante, Roberto José'),
    (2596, '["Puel, Adan"]'::jsonb, 'Puel, Adan Matias'),
    (2829, '["Calderon, Javier"]'::jsonb, 'Calderon, Javier Andres'),
    (2870, '["Montes Rodriguez, Gerardo"]'::jsonb, 'Montes Rodriguez, Gerardo Andres'),
    (2945, '["Aviles, Alonso"]'::jsonb, 'Aviles, Alonso Andres'),
    (3421, '["Guerra Martorell, Manfredo"]'::jsonb, 'Guerra Martorell, Manfredo Andres'),
    (3714, '["Carvajal, Andrea"]'::jsonb, 'Carvajal, Andrea Nicole'),
    (3740, '["Rozas, Carolina"]'::jsonb, 'Rozas, Carolina Diana'),
    (3743, '["Tapia, Luis"]'::jsonb, 'Tapia, Luis Ignacio'),
    (4387, '["Kemble, Hector"]'::jsonb, 'Kemble, Hector Ignacio'),
    (4700, '["Erber, Marlon"]'::jsonb, 'Erber, Marlon Roberto'),
    (4893, '["Opazo, Camila"]'::jsonb, 'Opazo, Camila Alejandra'),
    (4936, '["Tori, Felipe"]'::jsonb, 'Tori, Felipe Andres'),
    (4987, '["Fuentes, Rodrigo"]'::jsonb, 'Fuentes, Rodrigo Ignacio'),
    (4993, '["Hidalgo, Doris"]'::jsonb, 'Hidalgo, Doris Edith'),
    (5009, '["Rebolledo, Paula"]'::jsonb, 'Rebolledo, Paula Maria'),
    (5027, '["Bravo, Lidia"]'::jsonb, 'Bravo, Lidia Carolina'),
    (5117, '["Aguilera, Luis"]'::jsonb, 'Aguilera, Luis Octavio'),
    (5126, '["Camilo, Pedro"]'::jsonb, 'Camilo, Pedro Manuel'),
    (5131, '["Carvajal, Angelica"]'::jsonb, 'Carvajal, Angelica Carolina'),
    (5136, '["Cortez, Rosa"]'::jsonb, 'Cortez, Rosa Ester'),
    (5145, '["Leon, Carolina"]'::jsonb, 'Leon, Carolina Andrea'),
    (5147, '["Melzer, Karin"]'::jsonb, 'Melzer, Karin Erika'),
    (5156, '["Quiroz, Saul"]'::jsonb, 'Quiroz, Saul Andres'),
    (791, '["Rincon, Luis Carlos"]'::jsonb, 'Rincon, Luis Karlos');

DO $$
DECLARE
    missing_or_mismatched INTEGER;
    canonical_name_collision INTEGER;
BEGIN
    SELECT COUNT(*)
    INTO missing_or_mismatched
    FROM reviewed_athlete_canonical_name m
    LEFT JOIN core.athlete a ON a.id = m.athlete_id
    WHERE a.id IS NULL
       OR (
            a.full_name <> m.canonical_full_name
            AND NOT EXISTS (
                SELECT 1
                FROM jsonb_array_elements_text(m.expected_names) expected(name)
                WHERE expected.name = a.full_name
            )
       );

    IF missing_or_mismatched > 0 THEN
        RAISE EXCEPTION 'Reviewed athlete canonical names have missing/mismatched ids: %', missing_or_mismatched;
    END IF;

    SELECT COUNT(*)
    INTO canonical_name_collision
    FROM reviewed_athlete_canonical_name m
    JOIN core.athlete target ON target.id = m.athlete_id
    JOIN core.athlete other
      ON other.id <> target.id
     AND LOWER(TRIM(other.full_name)) = LOWER(TRIM(m.canonical_full_name))
     AND other.gender IS NOT DISTINCT FROM target.gender
     AND other.birth_year IS NOT DISTINCT FROM target.birth_year;

    IF canonical_name_collision > 0 THEN
        RAISE EXCEPTION 'Reviewed athlete canonical names collide with another identity: %', canonical_name_collision;
    END IF;
END $$;

UPDATE core.athlete a
SET full_name = m.canonical_full_name
FROM reviewed_athlete_canonical_name m
WHERE a.id = m.athlete_id
  AND a.full_name IS DISTINCT FROM m.canonical_full_name;

COMMIT;
