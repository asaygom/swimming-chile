import sys
from pathlib import Path

BACKEND_DIR = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = BACKEND_DIR / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

import audit_club_athlete_year_overlap as audit


def _obs(year, athlete_key, raw_club, canonical_club, source_url="https://example.test/resultados.pdf"):
    return audit.Observation(
        year=str(year),
        athlete_key=athlete_key,
        athlete_label=athlete_key[0],
        raw_club=raw_club,
        canonical_club=canonical_club,
        source_url=source_url,
    )


def test_build_candidate_rows_uses_same_athlete_within_same_year():
    observations = [
        _obs(2024, ("perez ana", "female", "1980"), "Club Deportivo UC", "Club Deportivo UC", "a.pdf"),
        _obs(2024, ("perez ana", "female", "1980"), "Universidad Catolica", "Universidad Catolica", "b.pdf"),
        _obs(2024, ("soto luis", "male", "1975"), "Club Deportivo UC", "Club Deportivo UC", "a.pdf"),
        _obs(2024, ("soto luis", "male", "1975"), "Universidad Catolica", "Universidad Catolica", "b.pdf"),
        _obs(2025, ("perez ana", "female", "1980"), "Otro Club", "Otro Club"),
    ]
    rows, same_competition_conflicts = audit.build_candidate_rows(observations, min_shared_athletes=2)

    assert len(rows) == 1
    assert same_competition_conflicts == 0
    assert rows[0]["club_name_a"] == "Club Deportivo UC"
    assert rows[0]["club_name_b"] == "Universidad Catolica"
    assert rows[0]["shared_athlete_years"] == 2
    assert rows[0]["years"] == "2024"
    assert rows[0]["cross_competition_pairs"] == 1


def test_build_candidate_rows_excludes_same_competition_overlap():
    observations = [
        _obs(2024, ("perez ana", "female", "1980"), "Club Deportivo UC", "Club Deportivo UC", "a.pdf"),
        _obs(2024, ("perez ana", "female", "1980"), "Universidad Catolica", "Universidad Catolica", "a.pdf"),
    ]

    rows, same_competition_conflicts = audit.build_candidate_rows(observations, min_shared_athletes=1)

    assert rows == []
    assert same_competition_conflicts == 1


def test_alias_evidence_keeps_collapsed_variants_visible():
    observations = [
        _obs(2024, ("perez ana", "female", "1980"), "Club Deportivo UC", "Club Deportivo UC", "a.pdf"),
        _obs(2024, ("perez ana", "female", "1980"), "Universidad Catolica", "Club Deportivo UC", "b.pdf"),
    ]

    candidates, _same_competition_conflicts = audit.build_candidate_rows(observations, min_shared_athletes=1)
    evidence = audit.build_alias_evidence_rows(observations)

    assert candidates == []
    assert len(evidence) == 1
    assert evidence[0]["canonical_name"] == "Club Deportivo UC"
    assert evidence[0]["raw_variant_count"] == 2
    assert evidence[0]["variant_pairs_with_cross_competition_same_athlete_year"] == 1
