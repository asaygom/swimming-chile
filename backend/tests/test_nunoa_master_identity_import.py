from __future__ import annotations

import sys
from pathlib import Path


BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from scripts.prepare_nunoa_master_identity_import_sql import render_sql
from scripts.preview_nunoa_master_identity_import import (
    DbState,
    WorkbookMember,
    build_preview,
    parse_args,
)


def sample_row(**overrides: str) -> dict[str, str]:
    row = {
        "row_number": "2",
        "rut_normalized": "123456785",
        "first_name": "Ana María",
        "last_name": "Pérez Soto",
        "date_of_birth": "1980-01-02",
        "email": "club@example.com",
        "gender": "female",
        "issues": "",
    }
    row.update(overrides)
    return row


def test_preview_cli_supports_explicit_offline_mode(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["preview", "--offline"])

    assert parse_args().offline is True


def test_competition_name_mismatch_is_reported_but_does_not_block_identity_load():
    member = WorkbookMember(
        row_number=161,
        rut_raw="12.345.678-5",
        rut_normalized="123456785",
        first_name="Ana María",
        first_surname="Pérez",
        second_surname="Soto",
        last_name="Pérez Soto",
        competition_name="Pérez, Anita",
        date_of_birth="1980-01-02",
        email=None,
        gender="female",
        issues=["competition_name_mismatch"],
    )

    preview = build_preview([member], DbState())

    assert preview["summary"]["auto_ready_people"] == 1
    assert preview["summary"]["requires_review"] == 0
    assert preview["people"][0]["issues"] == "competition_name_mismatch"


def test_sql_only_fills_missing_person_fields_and_never_blanks_existing_values():
    sql = render_sql([sample_row()], club_id=26)

    assert "UPDATE identity.person p" in sql
    assert "rut_normalized = COALESCE(p.rut_normalized, r.rut_normalized)" in sql
    assert "date_of_birth = COALESCE(p.date_of_birth, r.date_of_birth)" in sql
    assert "first_name = COALESCE(NULLIF(TRIM(p.first_name), ''), r.first_name)" in sql
    assert "last_name = COALESCE(NULLIF(TRIM(p.last_name), ''), r.last_name)" in sql
    assert "SET date_of_birth = r.date_of_birth" not in sql


def test_sql_resolves_new_rut_or_birth_date_against_unique_prior_source_identity():
    sql = render_sql([sample_row()], club_id=26)

    assert "p.rut_normalized = i.rut_normalized" in sql
    assert "p.data_source = 'nunoa_master_2026'" in sql
    assert "LOWER(TRIM(p.first_name)) = LOWER(TRIM(i.first_name))" in sql
    assert "LOWER(TRIM(p.last_name)) = LOWER(TRIM(i.last_name))" in sql
    assert "Ambiguous source identity matches" in sql


def test_sql_keeps_shared_email_valid_and_is_structurally_idempotent():
    rows = [
        sample_row(row_number="2", rut_normalized="123456785"),
        sample_row(
            row_number="3",
            rut_normalized="111111111",
            first_name="Beatriz",
            last_name="Rojas Díaz",
        ),
    ]

    sql = render_sql(rows, club_id=26)

    assert sql.count("'club@example.com'") == 2
    assert "cp.person_id = r.person_id" in sql
    assert "LOWER(TRIM(cp.contact_value)) = LOWER(TRIM(r.email))" in sql
    assert "WHERE NOT EXISTS" in sql
    assert "m.status IN ('active', 'invited')" in sql
    assert "CREATE TEMP TABLE nunoa_identity_resolved ON COMMIT DROP AS" in sql
    assert "core.athlete_person_link" not in sql
    assert "DELETE FROM" not in sql
