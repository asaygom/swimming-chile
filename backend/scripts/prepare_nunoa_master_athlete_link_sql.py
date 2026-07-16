"""Generate portable SQL for reviewed Ñuñoa person-athlete links.

People are resolved in the destination database from stable civil identity
fields. Generated SQL contains PII and must remain in ignored local paths.
"""

from __future__ import annotations

import argparse
import csv
from pathlib import Path


DEFAULT_PREVIEW_DIR = Path("backend/data/staging/nunoa_master_identity_preview")
DEFAULT_CANDIDATES = DEFAULT_PREVIEW_DIR / "athlete_link_candidates.csv"
DEFAULT_PEOPLE = DEFAULT_PREVIEW_DIR / "people_preview.csv"
DEFAULT_SQL_OUTPUT = DEFAULT_PREVIEW_DIR / "load_nunoa_master_athlete_links.sql"
DEFAULT_REPAIR_OUTPUT = DEFAULT_PREVIEW_DIR / "repair_nunoa_master_athlete_links.sql"
DATA_SOURCE = "nunoa_master_2026"


def sql_literal(value: str | None) -> str:
    if value is None or value == "":
        return "NULL"
    return "'" + value.replace("'", "''") + "'"


def load_reviewed_links(candidates_path: Path, people_path: Path) -> list[dict[str, str]]:
    with candidates_path.open(encoding="utf-8-sig", newline="") as handle:
        candidates = list(csv.DictReader(handle, delimiter=";"))
    with people_path.open(encoding="utf-8-sig", newline="") as handle:
        people = {row["row_number"]: row for row in csv.DictReader(handle)}

    selected = [
        row for row in candidates if (row.get("decision") or "").strip().lower() == "link"
    ]
    if not selected:
        raise ValueError("No hay filas con decision=link.")

    reviewed: list[dict[str, str]] = []
    for candidate in selected:
        row_number = (candidate.get("person_row_number") or "").strip()
        person = people.get(row_number)
        if person is None:
            raise ValueError(f"No existe people_preview row_number={row_number}.")
        reviewed.append(
            {
                "person_row_number": row_number,
                # This non-portable ID is retained only to identify a prior bad load.
                "applied_person_id": (candidate.get("person_id") or "").strip(),
                "rut_normalized": (person.get("rut_normalized") or "").strip(),
                "first_name": (person.get("first_name") or "").strip(),
                "last_name": (person.get("last_name") or "").strip(),
                "date_of_birth": (person.get("date_of_birth") or "").strip(),
                "athlete_id": (candidate.get("athlete_id") or "").strip(),
                "confidence": (candidate.get("confidence") or "reviewed").strip(),
                "person_name": (candidate.get("person_name") or "").strip(),
                "athlete_full_name": (candidate.get("athlete_full_name") or "").strip(),
            }
        )

    for field in ("person_row_number", "applied_person_id", "athlete_id", "athlete_full_name"):
        if any(not row[field] for row in reviewed):
            raise ValueError(f"Hay filas link sin {field}.")
    if any(
        not row["rut_normalized"] and not (row["first_name"] and row["last_name"])
        for row in reviewed
    ):
        raise ValueError("Cada fila link debe tener RUT o identidad fallback suficiente.")
    for field in ("person_row_number", "athlete_id"):
        values = [row[field] for row in reviewed]
        if len(values) != len(set(values)):
            raise ValueError(f"Hay valores duplicados para {field}.")
    return reviewed


def render_values(rows: list[dict[str, str]], *, include_applied_id: bool) -> str:
    values: list[str] = []
    for row in rows:
        fields = [row["person_row_number"]]
        if include_applied_id:
            fields.append(row["applied_person_id"])
        fields.extend(
            [
                sql_literal(row["rut_normalized"]),
                sql_literal(row["first_name"]),
                sql_literal(row["last_name"]),
                sql_literal(row["date_of_birth"]),
                row["athlete_id"],
                sql_literal(row["confidence"] or "reviewed"),
                sql_literal(row["person_name"]),
                sql_literal(row["athlete_full_name"]),
            ]
        )
        values.append("    (" + ", ".join(fields) + ")")
    return ",\n".join(values)


def _common_setup(rows: list[dict[str, str]], *, repair: bool) -> str:
    applied_column = "    applied_person_id BIGINT NOT NULL,\n" if repair else ""
    applied_insert = "    applied_person_id,\n" if repair else ""
    return f"""CREATE TEMP TABLE reviewed_nunoa_athlete_link_input (
    source_row_number INTEGER PRIMARY KEY,
{applied_column}    rut_normalized TEXT,
    first_name TEXT,
    last_name TEXT,
    date_of_birth DATE,
    athlete_id BIGINT NOT NULL UNIQUE,
    confidence_label TEXT NOT NULL,
    expected_person_name TEXT NOT NULL,
    expected_athlete_name TEXT NOT NULL
) ON COMMIT DROP;

INSERT INTO reviewed_nunoa_athlete_link_input (
    source_row_number,
{applied_insert}    rut_normalized,
    first_name,
    last_name,
    date_of_birth,
    athlete_id,
    confidence_label,
    expected_person_name,
    expected_athlete_name
)
VALUES
{render_values(rows, include_applied_id=repair)};

CREATE TEMP TABLE nunoa_club_resolved ON COMMIT DROP AS
SELECT c.id AS club_id
FROM core.club c
WHERE LOWER(TRIM(c.name)) IN ('ñuñoa master', 'nunoa master')
   OR LOWER(TRIM(COALESCE(c.short_name, ''))) IN ('ñuñoa master', 'nunoa master');

DO $$
DECLARE club_matches INTEGER;
BEGIN
    SELECT COUNT(*) INTO club_matches FROM nunoa_club_resolved;
    IF club_matches <> 1 THEN
        RAISE EXCEPTION 'Expected exactly one Nunoa Master club, found %', club_matches;
    END IF;
END $$;

CREATE TEMP TABLE reviewed_nunoa_person_match ON COMMIT DROP AS
SELECT i.source_row_number, p.id AS resolved_person_id
FROM reviewed_nunoa_athlete_link_input i
JOIN identity.person p
  ON (
       i.rut_normalized IS NOT NULL
       AND p.rut_normalized = i.rut_normalized
     )
  OR (
       i.rut_normalized IS NULL
       AND p.rut_normalized IS NULL
       AND p.data_source = '{DATA_SOURCE}'
       AND LOWER(TRIM(p.first_name)) = LOWER(TRIM(i.first_name))
       AND LOWER(TRIM(p.last_name)) = LOWER(TRIM(i.last_name))
       AND p.date_of_birth IS NOT DISTINCT FROM i.date_of_birth
     );

DO $$
DECLARE bad_resolutions INTEGER;
BEGIN
    SELECT COUNT(*) INTO bad_resolutions
    FROM (
        SELECT i.source_row_number
        FROM reviewed_nunoa_athlete_link_input i
        LEFT JOIN reviewed_nunoa_person_match pm
          ON pm.source_row_number = i.source_row_number
        GROUP BY i.source_row_number
        HAVING COUNT(pm.resolved_person_id) <> 1
    ) bad;
    IF bad_resolutions > 0 THEN
        RAISE EXCEPTION 'Expected exactly one person per reviewed row; invalid rows: %', bad_resolutions;
    END IF;
END $$;

CREATE TEMP TABLE reviewed_nunoa_athlete_link_resolved ON COMMIT DROP AS
SELECT i.*, pm.resolved_person_id, c.club_id
FROM reviewed_nunoa_athlete_link_input i
JOIN reviewed_nunoa_person_match pm USING (source_row_number)
CROSS JOIN nunoa_club_resolved c;

DO $$
DECLARE
    invalid_athletes INTEGER;
    invalid_memberships INTEGER;
    invalid_current_clubs INTEGER;
BEGIN
    SELECT COUNT(*) INTO invalid_athletes
    FROM reviewed_nunoa_athlete_link_resolved r
    LEFT JOIN core.athlete a ON a.id = r.athlete_id
    WHERE a.id IS NULL
       OR LOWER(TRIM(a.full_name)) <> LOWER(TRIM(r.expected_athlete_name));
    IF invalid_athletes > 0 THEN
        RAISE EXCEPTION 'Missing athletes or expected_athlete_name mismatch: %', invalid_athletes;
    END IF;

    SELECT COUNT(*) INTO invalid_memberships
    FROM reviewed_nunoa_athlete_link_resolved r
    WHERE NOT EXISTS (
        SELECT 1 FROM club_ops.membership m
        WHERE m.person_id = r.resolved_person_id
          AND m.club_id = r.club_id
          AND m.status IN ('active', 'invited')
    );
    IF invalid_memberships > 0 THEN
        RAISE EXCEPTION 'Resolved people are not active/invited Nunoa members: %', invalid_memberships;
    END IF;

    SELECT COUNT(*) INTO invalid_current_clubs
    FROM reviewed_nunoa_athlete_link_resolved r
    WHERE NOT EXISTS (
        SELECT 1 FROM core.athlete_current_club acc
        WHERE acc.athlete_id = r.athlete_id AND acc.club_id = r.club_id
    );
    IF invalid_current_clubs > 0 THEN
        RAISE EXCEPTION 'Reviewed athletes are not currently observed for Nunoa Master: %', invalid_current_clubs;
    END IF;
END $$;
"""


def render_sql(rows: list[dict[str, str]]) -> str:
    return f"""\\encoding UTF8
-- Portable reviewed Ñuñoa links. Contains PII; do not commit.
BEGIN;

{_common_setup(rows, repair=False)}
DO $$
DECLARE person_conflicts INTEGER; athlete_conflicts INTEGER;
BEGIN
    SELECT COUNT(*) INTO person_conflicts
    FROM reviewed_nunoa_athlete_link_resolved r
    JOIN core.athlete_person_link existing
      ON existing.person_id = r.resolved_person_id
     AND existing.athlete_id <> r.athlete_id;
    IF person_conflicts > 0 THEN
        RAISE EXCEPTION 'Some resolved people already links to a different athlete: %', person_conflicts;
    END IF;

    SELECT COUNT(*) INTO athlete_conflicts
    FROM reviewed_nunoa_athlete_link_resolved r
    JOIN core.athlete_person_link existing
      ON existing.athlete_id = r.athlete_id
     AND existing.person_id <> r.resolved_person_id;
    IF athlete_conflicts > 0 THEN
        RAISE EXCEPTION 'Some athletes already links to a different person: %', athlete_conflicts;
    END IF;
END $$;

INSERT INTO core.athlete_person_link (athlete_id, person_id, link_source, confidence, verified_at)
SELECT r.athlete_id, r.resolved_person_id, 'manual_club_registry',
       CASE r.confidence_label WHEN 'high' THEN 1.0000 WHEN 'medium' THEN 0.8500 ELSE 0.7500 END,
       NOW()
FROM reviewed_nunoa_athlete_link_resolved r
WHERE NOT EXISTS (
    SELECT 1 FROM core.athlete_person_link existing
    WHERE existing.athlete_id = r.athlete_id
      AND existing.person_id = r.resolved_person_id
);

COMMIT;
"""


def render_repair_sql(rows: list[dict[str, str]]) -> str:
    expected = len(rows)
    return f"""\\encoding UTF8
-- One-time repair for non-portable Ñuñoa person IDs. Contains PII; do not commit.
BEGIN;

{_common_setup(rows, repair=True)}
DO $$
DECLARE
    expected_wrong_pairs INTEGER;
    correct_pairs_already_present INTEGER;
    same_resolved_people INTEGER;
BEGIN
    SELECT COUNT(*) INTO expected_wrong_pairs
    FROM reviewed_nunoa_athlete_link_resolved r
    JOIN core.athlete_person_link existing
      ON existing.athlete_id = r.athlete_id
     AND existing.person_id = r.applied_person_id
     AND existing.link_source = 'manual_club_registry';
    IF expected_wrong_pairs <> {expected} THEN
        RAISE EXCEPTION 'Expected exactly {expected} erroneous applied pairs, found %', expected_wrong_pairs;
    END IF;

    SELECT COUNT(*) INTO correct_pairs_already_present
    FROM reviewed_nunoa_athlete_link_resolved r
    JOIN core.athlete_person_link existing
      ON existing.athlete_id = r.athlete_id
     AND existing.person_id = r.resolved_person_id;
    IF correct_pairs_already_present <> 0 THEN
        RAISE EXCEPTION 'Correct pairs already exist; refusing repair: %', correct_pairs_already_present;
    END IF;

    SELECT COUNT(*) INTO same_resolved_people
    FROM reviewed_nunoa_athlete_link_resolved
    WHERE applied_person_id = resolved_person_id;
    IF same_resolved_people <> 0 THEN
        RAISE EXCEPTION 'Applied IDs unexpectedly equal resolved IDs; refusing repair: %', same_resolved_people;
    END IF;

END $$;

DELETE FROM core.athlete_person_link existing
USING reviewed_nunoa_athlete_link_resolved r
WHERE existing.person_id = r.applied_person_id
  AND existing.athlete_id = r.athlete_id
  AND existing.link_source = 'manual_club_registry';

DO $$
DECLARE person_conflicts INTEGER; athlete_conflicts INTEGER;
BEGIN
    SELECT COUNT(*) INTO person_conflicts
    FROM reviewed_nunoa_athlete_link_resolved r
    JOIN core.athlete_person_link existing
      ON existing.person_id = r.resolved_person_id
     AND existing.athlete_id <> r.athlete_id;
    IF person_conflicts > 0 THEN
        RAISE EXCEPTION 'Correct people already link to another athlete: %', person_conflicts;
    END IF;

    SELECT COUNT(*) INTO athlete_conflicts
    FROM reviewed_nunoa_athlete_link_resolved r
    JOIN core.athlete_person_link existing
      ON existing.athlete_id = r.athlete_id
     AND existing.person_id NOT IN (r.applied_person_id, r.resolved_person_id);
    IF athlete_conflicts > 0 THEN
        RAISE EXCEPTION 'Athletes have unexpected additional person links: %', athlete_conflicts;
    END IF;
END $$;

INSERT INTO core.athlete_person_link (athlete_id, person_id, link_source, confidence, verified_at)
SELECT r.athlete_id, r.resolved_person_id, 'manual_club_registry',
       CASE r.confidence_label WHEN 'high' THEN 1.0000 WHEN 'medium' THEN 0.8500 ELSE 0.7500 END,
       NOW()
FROM reviewed_nunoa_athlete_link_resolved r;

DO $$
DECLARE wrong_remaining INTEGER; correct_present INTEGER;
BEGIN
    SELECT COUNT(*) INTO wrong_remaining
    FROM reviewed_nunoa_athlete_link_resolved r
    JOIN core.athlete_person_link existing
      ON existing.athlete_id = r.athlete_id AND existing.person_id = r.applied_person_id;
    SELECT COUNT(*) INTO correct_present
    FROM reviewed_nunoa_athlete_link_resolved r
    JOIN core.athlete_person_link existing
      ON existing.athlete_id = r.athlete_id
     AND existing.person_id = r.resolved_person_id
     AND existing.link_source = 'manual_club_registry';
    IF wrong_remaining <> 0 OR correct_present <> {expected} THEN
        RAISE EXCEPTION 'Repair postcondition failed: wrong=%, correct=%', wrong_remaining, correct_present;
    END IF;
END $$;

COMMIT;
"""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Prepare portable reviewed Ñuñoa athlete link SQL.")
    parser.add_argument("--candidates-csv", default=str(DEFAULT_CANDIDATES))
    parser.add_argument("--people-preview-csv")
    parser.add_argument("--sql-output", default=str(DEFAULT_SQL_OUTPUT))
    parser.add_argument("--repair-sql-output", default=str(DEFAULT_REPAIR_OUTPUT))
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    candidates = Path(args.candidates_csv)
    people = Path(args.people_preview_csv) if args.people_preview_csv else candidates.parent / "people_preview.csv"
    rows = load_reviewed_links(candidates, people)
    outputs = (
        (Path(args.sql_output), render_sql(rows)),
        (Path(args.repair_sql_output), render_repair_sql(rows)),
    )
    for output, content in outputs:
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(content, encoding="utf-8")
        print(f"Generated {output} with {len(rows)} reviewed links. Do not commit.")


if __name__ == "__main__":
    main()
