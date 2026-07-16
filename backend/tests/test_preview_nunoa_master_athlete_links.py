from __future__ import annotations

import json
import sys
from pathlib import Path
from types import SimpleNamespace


BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from scripts import preview_nunoa_master_athlete_links as preview


def test_review_rows_add_explicit_placeholder_for_person_without_candidate():
    people = [
        preview.PersonPreview(
            row_number=10,
            person_id=2,
            rut_normalized="123456785",
            first_name="Ana María",
            last_name="Pérez Soto",
            competition_name="Pérez, Ana",
            date_of_birth="1980-01-02",
            birth_year=1980,
            gender="female",
        ),
        preview.PersonPreview(
            row_number=11,
            person_id=3,
            rut_normalized=None,
            first_name="Beatriz",
            last_name="Rojas Díaz",
            competition_name="Rojas, Beatriz",
            date_of_birth=None,
            birth_year=None,
            gender="female",
        ),
    ]
    candidates = [{"person_id": 2, "athlete_id": 20, "confidence": "high"}]

    rows = preview.build_review_rows(people, candidates)

    assert rows[0] == candidates[0]
    assert rows[1] == {
        "person_id": 3,
        "person_row_number": 11,
        "person_has_rut": "no",
        "person_name": "Rojas Díaz, Beatriz",
        "person_competition_name": "Rojas, Beatriz",
        "person_birth_year": None,
        "person_gender": "female",
        "athlete_id": "",
        "athlete_full_name": "",
        "athlete_birth_year": "",
        "athlete_gender": "",
        "current_club_id": "",
        "current_club_name": "",
        "matched_alias": "",
        "name_score": "",
        "confidence": "none",
        "reasons": "no_candidate",
        "decision": "",
        "review_notes": "",
    }


def test_main_excludes_existing_links_and_reports_post_link_counts(monkeypatch):
    people = [
        preview.PersonPreview(1, 1, None, "Linked", "Person", "", None, None, None),
        preview.PersonPreview(2, 2, None, "Candidate", "Person", "", None, None, None),
        preview.PersonPreview(3, 3, None, "No candidate", "Person", "", None, None, None),
    ]
    athletes = [SimpleNamespace(athlete_id=20), SimpleNamespace(athlete_id=30)]
    candidates = [
        {
            "person_id": 2,
            "athlete_id": 20,
            "confidence": "medium",
        }
    ]
    written: dict[str, str] = {}

    class FakePath:
        def __init__(self, value):
            self.value = str(value)

        def write_text(self, content, **_kwargs):
            written[self.value] = content

    monkeypatch.setattr(
        preview,
        "parse_args",
        lambda: SimpleNamespace(
            club_id=26,
            people_preview="people.csv",
            output_csv="candidates.csv",
            summary_json="summary.json",
        ),
    )
    monkeypatch.setattr(preview, "load_preview_rows", lambda _path: [])
    monkeypatch.setattr(preview, "Path", FakePath)
    monkeypatch.setattr(preview, "resolve_people", lambda _rows: people)
    monkeypatch.setattr(preview, "load_linked_person_ids", lambda _ids: {1})
    monkeypatch.setattr(preview, "load_current_club_athletes", lambda _club_id: athletes)
    monkeypatch.setattr(
        preview,
        "build_candidates",
        lambda unlinked_people, available_athletes: (
            candidates
            if [person.person_id for person in unlinked_people] == [2, 3]
            and available_athletes == athletes
            else []
        ),
    )
    monkeypatch.setattr(preview, "write_csv", lambda _path, _rows: None)

    preview.main()

    summary = json.loads(written["summary.json"])
    assert summary["preview_people"] == 3
    assert summary["already_linked_people"] == 1
    assert summary["unlinked_people"] == 2
    assert summary["available_unlinked_athletes"] == 2
    assert summary["review_rows"] == 2
    assert summary["candidate_rows"] == 1
    assert summary["people_with_candidates"] == 1
    assert summary["people_without_candidates"] == 1


def test_available_athlete_query_excludes_athletes_linked_to_any_person(monkeypatch):
    executed_sql = ""

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def execute(self, sql, _params):
            nonlocal executed_sql
            executed_sql = sql

        def fetchall(self):
            return []

    class Connection:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def cursor(self):
            return Cursor()

    monkeypatch.setattr(preview, "connect", lambda: Connection())

    assert preview.load_current_club_athletes(26) == []
    assert "NOT EXISTS" in executed_sql
    assert "core.athlete_person_link" in executed_sql
