"""Generate guarded SQL to import Ñuñoa Master identity preview data.

The generated SQL contains personal data and must be written only to ignored
local paths. This script does not connect to the database and does not load data.
"""

from __future__ import annotations

import argparse
import csv
from pathlib import Path
from typing import Iterable


DEFAULT_PREVIEW_DIR = Path("backend/data/staging/nunoa_master_identity_preview")
DEFAULT_SQL_OUTPUT = DEFAULT_PREVIEW_DIR / "load_nunoa_master_identity.sql"
DATA_SOURCE = "nunoa_master_2026"


def sql_literal(value: str | None) -> str:
    if value is None or value == "":
        return "NULL"
    return "'" + value.replace("'", "''") + "'"


def load_people(preview_csv: Path) -> list[dict[str, str]]:
    with preview_csv.open(encoding="utf-8-sig", newline="") as handle:
        rows = list(csv.DictReader(handle))
    if not rows:
        raise ValueError(f"No rows found in {preview_csv}")
    blocking = [row for row in rows if (row.get("blocking_issues") or "").strip()]
    if blocking:
        raise ValueError(f"Preview has blocking rows; review before SQL generation: {len(blocking)}")
    return rows


def render_values(rows: Iterable[dict[str, str]]) -> str:
    values: list[str] = []
    for row in rows:
        values.append(
            "    ("
            + ", ".join(
                [
                    row["row_number"],
                    sql_literal(row.get("rut_normalized")),
                    sql_literal(row.get("first_name")),
                    sql_literal(row.get("last_name")),
                    sql_literal(row.get("date_of_birth")),
                    sql_literal(row.get("email")),
                    sql_literal(row.get("gender")),
                    sql_literal(row.get("issues")),
                ]
            )
            + ")"
        )
    return ",\n".join(values)


def render_sql(rows: list[dict[str, str]], *, club_id: int) -> str:
    values = render_values(rows)
    return f"""-- Generated from ignored Ñuñoa Master identity preview.
-- Contains personal data. Do not commit this file.

BEGIN;

CREATE TEMP TABLE nunoa_identity_import (
    row_number INTEGER PRIMARY KEY,
    rut_normalized TEXT,
    first_name TEXT NOT NULL,
    last_name TEXT NOT NULL,
    date_of_birth DATE,
    email TEXT,
    gender TEXT,
    issues TEXT
) ON COMMIT DROP;

INSERT INTO nunoa_identity_import (
    row_number,
    rut_normalized,
    first_name,
    last_name,
    date_of_birth,
    email,
    gender,
    issues
)
VALUES
{values};

DO $$
DECLARE
    missing_club INTEGER;
    duplicated_import_rut INTEGER;
    duplicated_missing_identity INTEGER;
BEGIN
    SELECT COUNT(*)
    INTO missing_club
    FROM core.club
    WHERE id = {club_id};

    IF missing_club <> 1 THEN
        RAISE EXCEPTION 'Expected core.club id {club_id} for Ñuñoa Master, found %', missing_club;
    END IF;

    SELECT COUNT(*)
    INTO duplicated_import_rut
    FROM (
        SELECT rut_normalized
        FROM nunoa_identity_import
        WHERE rut_normalized IS NOT NULL
        GROUP BY rut_normalized
        HAVING COUNT(*) > 1
    ) dup;

    IF duplicated_import_rut > 0 THEN
        RAISE EXCEPTION 'Duplicate RUTs inside import preview: %', duplicated_import_rut;
    END IF;

    SELECT COUNT(*)
    INTO duplicated_missing_identity
    FROM (
        SELECT LOWER(TRIM(first_name)) AS first_name_key,
               LOWER(TRIM(last_name)) AS last_name_key,
               date_of_birth
        FROM nunoa_identity_import
        WHERE rut_normalized IS NULL
        GROUP BY 1, 2, 3
        HAVING COUNT(*) > 1
    ) dup;

    IF duplicated_missing_identity > 0 THEN
        RAISE EXCEPTION 'Duplicate missing-RUT identities inside import preview: %', duplicated_missing_identity;
    END IF;
END $$;

INSERT INTO identity.person (
    rut_normalized,
    date_of_birth,
    first_name,
    last_name,
    data_source,
    notes
)
SELECT
    i.rut_normalized,
    i.date_of_birth,
    i.first_name,
    i.last_name,
    {sql_literal(DATA_SOURCE)},
    CASE
        WHEN i.rut_normalized IS NULL THEN 'Imported as active member with missing RUT'
        ELSE NULL
    END
FROM nunoa_identity_import i
WHERE NOT EXISTS (
    SELECT 1
    FROM identity.person p
    WHERE (
        i.rut_normalized IS NOT NULL
        AND p.rut_normalized = i.rut_normalized
    )
    OR (
        i.rut_normalized IS NULL
        AND p.rut_normalized IS NULL
        AND p.data_source = {sql_literal(DATA_SOURCE)}
        AND LOWER(TRIM(p.first_name)) = LOWER(TRIM(i.first_name))
        AND LOWER(TRIM(p.last_name)) = LOWER(TRIM(i.last_name))
        AND p.date_of_birth IS NOT DISTINCT FROM i.date_of_birth
    )
);

CREATE TEMP TABLE nunoa_identity_resolved AS
SELECT
    i.*,
    p.id AS person_id
FROM nunoa_identity_import i
JOIN identity.person p
  ON (
        i.rut_normalized IS NOT NULL
        AND p.rut_normalized = i.rut_normalized
     )
  OR (
        i.rut_normalized IS NULL
        AND p.rut_normalized IS NULL
        AND p.data_source = {sql_literal(DATA_SOURCE)}
        AND LOWER(TRIM(p.first_name)) = LOWER(TRIM(i.first_name))
        AND LOWER(TRIM(p.last_name)) = LOWER(TRIM(i.last_name))
        AND p.date_of_birth IS NOT DISTINCT FROM i.date_of_birth
     );

DO $$
DECLARE
    unresolved_rows INTEGER;
    duplicate_resolutions INTEGER;
BEGIN
    SELECT COUNT(*)
    INTO unresolved_rows
    FROM nunoa_identity_import i
    LEFT JOIN nunoa_identity_resolved r ON r.row_number = i.row_number
    WHERE r.person_id IS NULL;

    IF unresolved_rows > 0 THEN
        RAISE EXCEPTION 'Unresolved imported people: %', unresolved_rows;
    END IF;

    SELECT COUNT(*)
    INTO duplicate_resolutions
    FROM (
        SELECT row_number
        FROM nunoa_identity_resolved
        GROUP BY row_number
        HAVING COUNT(*) > 1
    ) dup;

    IF duplicate_resolutions > 0 THEN
        RAISE EXCEPTION 'Ambiguous person resolutions: %', duplicate_resolutions;
    END IF;
END $$;

INSERT INTO identity.contact_point (
    person_id,
    contact_type,
    contact_value
)
SELECT
    r.person_id,
    'email',
    r.email
FROM nunoa_identity_resolved r
WHERE r.email IS NOT NULL
  AND NOT EXISTS (
      SELECT 1
      FROM identity.contact_point cp
      WHERE cp.person_id = r.person_id
        AND cp.contact_type = 'email'
        AND LOWER(TRIM(cp.contact_value)) = LOWER(TRIM(r.email))
  );

INSERT INTO club_ops.membership (
    club_id,
    person_id,
    status
)
SELECT
    {club_id},
    r.person_id,
    'active'
FROM nunoa_identity_resolved r
WHERE NOT EXISTS (
    SELECT 1
    FROM club_ops.membership m
    WHERE m.club_id = {club_id}
      AND m.person_id = r.person_id
      AND m.status IN ('active', 'invited')
);

COMMIT;
"""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Prepare Ñuñoa Master identity import SQL.")
    parser.add_argument(
        "--people-preview",
        default=str(DEFAULT_PREVIEW_DIR / "people_preview.csv"),
        help="Path to ignored people_preview.csv generated by preview_nunoa_master_identity_import.py.",
    )
    parser.add_argument(
        "--sql-output",
        default=str(DEFAULT_SQL_OUTPUT),
        help="Ignored local SQL output path. The file will contain PII.",
    )
    parser.add_argument("--club-id", type=int, default=26)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    people_preview = Path(args.people_preview)
    sql_output = Path(args.sql_output)
    rows = load_people(people_preview)
    sql_output.parent.mkdir(parents=True, exist_ok=True)
    sql_output.write_text(render_sql(rows, club_id=args.club_id), encoding="utf-8")
    print(f"Generated {sql_output} with {len(rows)} rows. Contains PII; do not commit.")


if __name__ == "__main__":
    main()
