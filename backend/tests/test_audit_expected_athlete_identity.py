import sys
from pathlib import Path

import pandas as pd

BACKEND_DIR = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = BACKEND_DIR / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

import audit_expected_athlete_identity as audit


def test_classify_same_name_group_marks_same_club_delta_one():
    group = pd.DataFrame(
        [
            {"birth_year": "1984.0", "club_key": "efecto peruga"},
            {"birth_year": "1985.0", "club_key": "efecto peruga"},
        ]
    )

    assert audit.classify_same_name_group(group) == "same_club_birth_year_delta_1_review_age_capture"


def test_classify_same_name_group_marks_likely_distinct_people():
    group = pd.DataFrame(
        [
            {"birth_year": "1979.0", "club_key": "club a"},
            {"birth_year": "1993.0", "club_key": "club b"},
        ]
    )

    assert audit.classify_same_name_group(group) == "same_name_different_birth_year_and_club_likely_distinct"


def test_build_same_name_review_rows_groups_by_name_and_gender():
    df = pd.DataFrame(
        [
            {
                "athlete_key": "abbott andres",
                "gender": "male",
                "birth_year": "1984.0",
                "club_key": "efecto peruga",
                "full_name": "Abbott, Andres",
                "club_name": "Efecto Peruga",
                "source_url": "a",
            },
            {
                "athlete_key": "abbott andres",
                "gender": "male",
                "birth_year": "1985.0",
                "club_key": "efecto peruga",
                "full_name": "Abbott, Andres",
                "club_name": "Efecto Peruga",
                "source_url": "b",
            },
            {
                "athlete_key": "abbott andres",
                "gender": "female",
                "birth_year": "1984.0",
                "club_key": "efecto peruga",
                "full_name": "Abbott, Andres",
                "club_name": "Efecto Peruga",
                "source_url": "c",
            },
        ]
    )

    rows = audit.build_same_name_review_rows(df)

    assert len(rows) == 1
    assert rows[0]["athlete_key"] == "abbott andres"
    assert rows[0]["gender"] == "male"
    assert rows[0]["review_category"] == "same_club_birth_year_delta_1_review_age_capture"


def test_preferred_year_from_evidence_requires_source_one_vs_many():
    row = {
        "year_observation_counts": "1984:11 | 1985:3",
        "year_source_counts": "1984:4 | 1985:1",
    }

    assert audit.preferred_year_from_evidence(row) == "1984"

    tie_row = {
        "year_observation_counts": "1984:2 | 1985:2",
        "year_source_counts": "1984:1 | 1985:1",
    }

    assert audit.preferred_year_from_evidence(tie_row) is None


def test_apply_birth_year_corrections_dedupes_expected_core_identity():
    df = pd.DataFrame(
        [
            {
                "expected_row_id": "1",
                "full_name": "Abbott, Andres",
                "gender": "male",
                "birth_year": "1984.0",
                "club_name": "Efecto Peruga",
                "club_key": "efecto peruga",
                "athlete_key": "abbott andres",
                "source_url": "a",
            },
            {
                "expected_row_id": "2",
                "full_name": "Abbott, Andres",
                "gender": "male",
                "birth_year": "1985.0",
                "club_name": "Efecto Peruga",
                "club_key": "efecto peruga",
                "athlete_key": "abbott andres",
                "source_url": "b",
            },
        ]
    )

    corrected, changes = audit.apply_birth_year_corrections(
        df,
        {("abbott andres", "male", "efecto peruga"): "1984"},
    )
    deduped = audit.dedupe_expected_core_athletes(corrected)

    assert len(changes) == 1
    assert changes[0]["old_birth_year"] == "1985"
    assert changes[0]["new_birth_year"] == "1984"
    assert len(deduped) == 1
    assert deduped.iloc[0]["birth_year"] == "1984"
    assert audit.build_same_name_review_rows(deduped) == []


def test_build_missing_birth_year_candidate_rows_requires_single_exact_context():
    df = pd.DataFrame(
        [
            {
                "expected_row_id": "1",
                "full_name": "Arantxa Aranguren",
                "gender": "female",
                "birth_year": "",
                "club_name": "Club A",
                "club_key": "club a",
                "athlete_key": "aranguren arantxa",
                "source_url": "a",
            },
            {
                "expected_row_id": "2",
                "full_name": "Aranguren, Arantxa",
                "gender": "female",
                "birth_year": "1989",
                "club_name": "Club A",
                "club_key": "club a",
                "athlete_key": "aranguren arantxa",
                "source_url": "b",
            },
            {
                "expected_row_id": "3",
                "full_name": "Aranguren, Arantxa",
                "gender": "female",
                "birth_year": "1990",
                "club_name": "Club B",
                "club_key": "club b",
                "athlete_key": "aranguren arantxa",
                "source_url": "c",
            },
        ]
    )

    rows = audit.build_missing_birth_year_candidate_rows(df)

    assert len(rows) == 1
    assert rows[0]["candidate_birth_year"] == "1989"
    assert rows[0]["candidate_reason"] == "same_name_tokens_gender_club_single_known_year"
    assert rows[0]["candidate_full_names"] == "Aranguren, Arantxa"
