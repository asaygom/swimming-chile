from __future__ import annotations

import io
import sys
from pathlib import Path
from unittest.mock import patch

import pytest


BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from scripts.prepare_nunoa_master_athlete_link_sql import (
    load_reviewed_links,
    render_repair_sql,
    render_sql,
)


def reviewed_row(**overrides: str) -> dict[str, str]:
    row = {
        "person_row_number": "11",
        "applied_person_id": "157",
        "rut_normalized": "123456785",
        "first_name": "Ana María",
        "last_name": "Pérez Soto",
        "date_of_birth": "1980-01-02",
        "athlete_id": "9001",
        "confidence": "high",
        "person_name": "Pérez Soto, Ana María",
        "athlete_full_name": "Pérez, Ana",
    }
    row.update(overrides)
    return row


def test_loader_joins_review_decisions_to_stable_people_preview():
    candidate_csv = io.StringIO(
        "person_id;person_row_number;person_name;athlete_id;athlete_full_name;confidence;decision\n"
        "157;11;Pérez Soto, Ana María;9001;Pérez, Ana;high;link\n"
    )
    people_csv = io.StringIO(
        "row_number,rut_normalized,first_name,last_name,date_of_birth\n"
        "11,123456785,Ana María,Pérez Soto,1980-01-02\n"
    )
    with patch.object(Path, "open", side_effect=[candidate_csv, people_csv]):
        assert load_reviewed_links(Path("candidates.csv"), Path("people.csv")) == [reviewed_row()]


def test_portable_sql_resolves_people_and_club_by_stable_identity():
    sql = render_sql([reviewed_row()])

    assert "p.rut_normalized = i.rut_normalized" in sql
    assert "p.data_source = 'nunoa_master_2026'" in sql
    assert "LOWER(TRIM(p.first_name)) = LOWER(TRIM(i.first_name))" in sql
    assert "LOWER(TRIM(p.last_name)) = LOWER(TRIM(i.last_name))" in sql
    assert "p.date_of_birth IS NOT DISTINCT FROM i.date_of_birth" in sql
    assert "resolved_person_id" in sql
    assert "club_id = 26" not in sql
    assert "c.short_name" in sql
    assert "m.status IN ('active', 'invited')" in sql


def test_date_of_birth_is_nullable_when_rut_is_available():
    sql = render_sql([reviewed_row(date_of_birth="")])

    assert "date_of_birth DATE," in sql
    assert "date_of_birth DATE NOT NULL" not in sql
    assert "'123456785', 'Ana" in sql


def test_loader_requires_rut_or_sufficient_fallback_identity():
    candidate_csv = io.StringIO(
        "person_id;person_row_number;person_name;athlete_id;athlete_full_name;confidence;decision\n"
        "157;11;Sin identidad;9001;Pérez, Ana;high;link\n"
    )
    people_csv = io.StringIO(
        "row_number,rut_normalized,first_name,last_name,date_of_birth\n"
        "11,,,,\n"
    )
    with patch.object(Path, "open", side_effect=[candidate_csv, people_csv]):
        with pytest.raises(ValueError, match="RUT o identidad fallback suficiente"):
            load_reviewed_links(Path("candidates.csv"), Path("people.csv"))


def test_portable_sql_guards_resolution_names_club_and_conflicts():
    sql = render_sql([reviewed_row()])

    assert "Expected exactly one person per reviewed row" in sql
    assert "Expected exactly one Nunoa Master club" in sql
    assert "expected_athlete_name" in sql
    assert "LOWER(TRIM(a.full_name))" in sql
    assert "core.athlete_current_club" in sql
    assert "already links to a different athlete" in sql
    assert "already links to a different person" in sql


def test_repair_sql_is_limited_to_exact_applied_pairs_and_rejects_unexpected_state():
    rows = [reviewed_row(), reviewed_row(person_row_number="12", applied_person_id="158", rut_normalized="", first_name="Bea", last_name="Rojas", date_of_birth="1990-03-04", athlete_id="9002", athlete_full_name="Rojas, Bea")]

    sql = render_repair_sql(rows)

    assert "applied_person_id" in sql
    assert "expected_wrong_pairs <> 2" in sql
    assert "correct_pairs_already_present <> 0" in sql
    assert "same_resolved_people <> 0" in sql
    assert "link_source = 'manual_club_registry'" in sql
    assert "DELETE FROM core.athlete_person_link existing" in sql
    assert "existing.person_id = r.applied_person_id" in sql
    assert "existing.athlete_id = r.athlete_id" in sql
    delete_position = sql.index("DELETE FROM core.athlete_person_link existing")
    conflict_position = sql.index("Correct people already link to another athlete")
    insert_position = sql.index("INSERT INTO core.athlete_person_link")
    assert delete_position < conflict_position < insert_position
    assert "Repair postcondition failed" in sql
    assert "COMMIT;" in sql
