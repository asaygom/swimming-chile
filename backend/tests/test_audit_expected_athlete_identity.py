import sys
from pathlib import Path
import uuid

import pandas as pd

BACKEND_DIR = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = BACKEND_DIR / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

import audit_expected_athlete_identity as audit


def _workspace_tmp_dir() -> Path:
    path = BACKEND_DIR / "data" / "raw" / "batch_summaries" / f"test_audit_expected_athlete_identity_{uuid.uuid4().hex}"
    path.mkdir(parents=True)
    return path


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


def test_apply_missing_birth_year_candidates_canonicalizes_name_and_dedupes():
    df = pd.DataFrame(
        [
            {
                "expected_row_id": "1",
                "full_name": "Arantxa Aranguren",
                "gender": "female",
                "birth_year": "",
                "club_name": "Club A",
                "club_key": "club a",
                "athlete_key": "arantxa aranguren",
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
        ]
    )
    candidate_rows = audit.build_missing_birth_year_candidate_rows(df)

    corrected, changes = audit.apply_missing_birth_year_candidates(df, candidate_rows)
    deduped = audit.dedupe_expected_core_athletes(corrected)

    assert len(changes) == 1
    assert changes[0]["old_full_name"] == "Arantxa Aranguren"
    assert changes[0]["new_full_name"] == "Aranguren, Arantxa"
    assert changes[0]["new_birth_year"] == "1989"
    assert len(deduped) == 1
    assert deduped.iloc[0]["full_name"] == "Aranguren, Arantxa"


def test_build_partial_name_candidate_rows_requires_same_year_gender_and_club():
    df = pd.DataFrame(
        [
            {
                "full_name": "Bustos Araya, Gabriela",
                "gender": "female",
                "birth_year": "1980",
                "club_name": "Club A",
                "club_key": "club a",
                "athlete_key": "bustos araya gabriela",
                "source_url": "a",
            },
            {
                "full_name": "Bustos Araya, Maria Gabriela",
                "gender": "female",
                "birth_year": "1980",
                "club_name": "Club A",
                "club_key": "club a",
                "athlete_key": "bustos araya maria gabriela",
                "source_url": "b",
            },
            {
                "full_name": "Bustos Araya, Gabriela",
                "gender": "female",
                "birth_year": "1981",
                "club_name": "Club A",
                "club_key": "club a",
                "athlete_key": "bustos araya gabriela",
                "source_url": "c",
            },
        ]
    )

    rows = audit.build_partial_name_candidate_rows(df)

    assert len(rows) == 1
    assert rows[0]["shorter_full_name"] == "Bustos Araya, Gabriela"
    assert rows[0]["longer_full_name"] == "Bustos Araya, Maria Gabriela"
    assert rows[0]["added_tokens"] == "maria"
    assert rows[0]["same_club"] == "yes"


def test_build_partial_name_candidate_rows_includes_initial_and_cross_club_reviews():
    df = pd.DataFrame(
        [
            {
                "full_name": "Acevedo, Luis A",
                "gender": "male",
                "birth_year": "1969",
                "club_name": "Club A",
                "club_key": "club a",
                "athlete_key": "acevedo luis a",
                "source_url": "a",
            },
            {
                "full_name": "Acevedo, Luis Alberto",
                "gender": "male",
                "birth_year": "1969",
                "club_name": "Club A",
                "club_key": "club a",
                "athlete_key": "acevedo luis alberto",
                "source_url": "b",
            },
            {
                "full_name": "Acevedo, Bernardita",
                "gender": "female",
                "birth_year": "1998",
                "club_name": "Club B",
                "club_key": "club b",
                "athlete_key": "acevedo bernardita",
                "source_url": "c",
            },
            {
                "full_name": "Acevedo Almonte, Bernardita",
                "gender": "female",
                "birth_year": "1998",
                "club_name": "Club C",
                "club_key": "club c",
                "athlete_key": "acevedo almonte bernardita",
                "source_url": "d",
            },
        ]
    )

    rows = audit.build_partial_name_candidate_rows(df)

    by_shorter = {row["shorter_full_name"]: row for row in rows}
    assert by_shorter["Acevedo, Luis A"]["initial_matches"] == "a->alberto"
    assert by_shorter["Acevedo, Luis A"]["same_club"] == "yes"
    assert by_shorter["Acevedo, Bernardita"]["longer_full_name"] == "Acevedo Almonte, Bernardita"
    assert by_shorter["Acevedo, Bernardita"]["same_club"] == "no"


def test_build_expanded_identity_candidate_rows_catches_omitted_names_and_delta_one():
    df = pd.DataFrame(
        [
            {
                "full_name": "Acevedo, Luis",
                "gender": "male",
                "birth_year": "1969",
                "club_name": "Club A",
                "club_key": "club a",
                "athlete_key": "acevedo luis",
                "source_url": "a",
            },
            {
                "full_name": "Acevedo, Luis Alberto",
                "gender": "male",
                "birth_year": "1969",
                "club_name": "Club A",
                "club_key": "club a",
                "athlete_key": "acevedo luis alberto",
                "source_url": "b",
            },
            {
                "full_name": "Abarca Ramirez, Valentina",
                "gender": "female",
                "birth_year": "1991",
                "club_name": "Club B",
                "club_key": "club b",
                "athlete_key": "abarca ramirez valentina",
                "source_url": "c",
            },
            {
                "full_name": "Abarca, Valentina",
                "gender": "female",
                "birth_year": "1991",
                "club_name": "Club B",
                "club_key": "club b",
                "athlete_key": "abarca valentina",
                "source_url": "d",
            },
            {
                "full_name": "Acosta, Andres",
                "gender": "male",
                "birth_year": "1987",
                "club_name": "Club C",
                "club_key": "club c",
                "athlete_key": "acosta andres",
                "source_url": "e",
            },
            {
                "full_name": "Acosta, Andres",
                "gender": "male",
                "birth_year": "1988",
                "club_name": "Club D",
                "club_key": "club d",
                "athlete_key": "acosta andres",
                "source_url": "f",
            },
        ]
    )

    rows = audit.build_expanded_identity_candidate_rows(df)

    by_pair = {(row["left_full_name"], row["right_full_name"]): row for row in rows}
    assert by_pair[("Acevedo, Luis", "Acevedo, Luis Alberto")]["candidate_reason"] == (
        "given_prefix_initial_or_second_name_omitted"
    )
    assert by_pair[("Acevedo, Luis", "Acevedo, Luis Alberto")]["suggested_canonical_full_name"] == (
        "Acevedo, Luis Alberto"
    )
    assert by_pair[("Abarca Ramirez, Valentina", "Abarca, Valentina")]["candidate_reason"] == (
        "surname_prefix_or_second_surname_omitted"
    )
    assert by_pair[("Abarca Ramirez, Valentina", "Abarca, Valentina")]["suggested_canonical_full_name"] == (
        "Abarca Ramirez, Valentina"
    )
    assert by_pair[("Acosta, Andres", "Acosta, Andres")]["review_hint"] == "birth_year_delta_1_review"
    assert by_pair[("Acosta, Andres", "Acosta, Andres")]["left_source_count"] == 1
    assert by_pair[("Acosta, Andres", "Acosta, Andres")]["right_source_count"] == 1
    assert by_pair[("Acosta, Andres", "Acosta, Andres")]["combined_source_count"] == 2


def test_apply_partial_name_decisions_uses_only_curated_merge_rows():
    df = pd.DataFrame(
        [
            {
                "expected_row_id": "1",
                "full_name": "Bustos Araya, Gabriela",
                "gender": "female",
                "birth_year": "1980",
                "club_name": "Club A",
                "club_key": "club a",
                "athlete_key": "bustos araya gabriela",
                "source_url": "a",
            },
            {
                "expected_row_id": "2",
                "full_name": "Bustos Araya, Maria Gabriela",
                "gender": "female",
                "birth_year": "1980",
                "club_name": "Club A",
                "club_key": "club a",
                "athlete_key": "bustos araya maria gabriela",
                "source_url": "b",
            },
            {
                "expected_row_id": "3",
                "full_name": "Machuca, Egidio",
                "gender": "male",
                "birth_year": "1943",
                "club_name": "Club B",
                "club_key": "club b",
                "athlete_key": "machuca egidio",
                "source_url": "c",
            },
        ]
    )
    decisions = [
        {
            "gender": "female",
            "birth_year": "1980",
            "club_key": "club a",
            "shorter_athlete_key": "bustos araya gabriela",
            "canonical_full_name": "Bustos Araya, Maria Gabriela",
            "canonical_athlete_key": "bustos araya maria gabriela",
            "review_hint": "common_given_name_review",
            "notes": "",
        }
    ]

    corrected, changes = audit.apply_partial_name_decisions(df, decisions)
    deduped = audit.dedupe_expected_core_athletes(corrected)

    assert len(changes) == 1
    assert changes[0]["old_full_name"] == "Bustos Araya, Gabriela"
    assert changes[0]["new_full_name"] == "Bustos Araya, Maria Gabriela"
    assert len(deduped) == 2
    assert "Machuca, Egidio" in set(deduped["full_name"])


def test_load_partial_name_decisions_accepts_semicolon_csv():
    tmp_dir = _workspace_tmp_dir()
    try:
        path = tmp_dir / "decisions.csv"
        path.write_text(
            "decision;suggested_canonical_full_name;notes;review_hint;gender;birth_year;club_key;shorter_full_name;longer_full_name\n"
            "merge;Bustos Araya, Maria Gabriela;;common_given_name_review;female;1980;club a;Bustos Araya, Gabriela;Bustos Araya, Maria Gabriela\n"
            "needs_source_review;Machuca, Egidio R;;single_initial_review;male;1943;club b;Machuca, Egidio;Machuca, Egidio R\n",
            encoding="utf-8",
        )

        decisions = audit.load_partial_name_decisions(path)

        assert len(decisions) == 1
        assert decisions[0]["shorter_athlete_key"] == "bustos araya gabriela"
        assert decisions[0]["canonical_athlete_key"] == "bustos araya maria gabriela"
    finally:
        import shutil

        shutil.rmtree(tmp_dir, ignore_errors=True)


def test_load_partial_name_decisions_uses_shorter_club_key_for_cross_club_rows():
    tmp_dir = _workspace_tmp_dir()
    try:
        path = tmp_dir / "decisions.csv"
        path.write_text(
            "decision;suggested_canonical_full_name;gender;birth_year;club_key;shorter_club_key;shorter_full_name;longer_full_name\n"
            "merge;Acevedo Almonte, Bernardita;female;1998;;salmon swim;Acevedo, Bernardita;Acevedo Almonte, Bernardita\n",
            encoding="utf-8",
        )

        decisions = audit.load_partial_name_decisions(path)

        assert len(decisions) == 1
        assert decisions[0]["club_key"] == "salmon swim"
    finally:
        import shutil

        shutil.rmtree(tmp_dir, ignore_errors=True)
