import shutil
import sys
import uuid
from pathlib import Path

import pandas as pd

BACKEND_DIR = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = BACKEND_DIR / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

import curate_athlete_names as curate


def _workspace_tmp_dir() -> Path:
    path = BACKEND_DIR / "data" / "raw" / "batch_summaries" / f"test_curate_athlete_names_{uuid.uuid4().hex}"
    path.mkdir(parents=True)
    return path


def test_athlete_name_signature_groups_common_ocr_variants():
    assert curate.athlete_name_signature("Pasarin, Claudia") == curate.athlete_name_signature(
        "Pasar\u00ed\u00f3n, Claudia"
    )
    assert curate.athlete_name_signature("Gomez, Francisco") == curate.athlete_name_signature(
        "Go\u00e1mez, Francisco"
    )
    assert curate.athlete_name_signature("Muller, Bettina") == curate.athlete_name_signature(
        "Mu\u00fcller, Bettina"
    )


def test_build_review_rows_prefers_cleaner_canonical_names():
    rows = [
        {"athlete_name": "Pasarin, Claudia", "source_url": "a", "club_name": "Club A", "birth_year": "1964", "gender": "female"},
        {"athlete_name": "Pasar\u00ed\u00e1n, Claudia", "source_url": "b", "club_name": "Club A", "birth_year": "1964", "gender": "female"},
        {"athlete_name": "Pasar\u00ed\u00f3n, Claudia", "source_url": "c", "club_name": "Club A", "birth_year": "1964", "gender": "female"},
        {"athlete_name": "Mu\u00fcller, Bettina", "source_url": "d", "club_name": "Club B", "birth_year": "1964", "gender": "female"},
        {"athlete_name": "Muller, Bettina", "source_url": "e", "club_name": "Club B", "birth_year": "1964", "gender": "female"},
    ]

    review_rows, replacement_map = curate.build_review_rows(rows)

    canonical_by_signature = {row["signature"]: row["canonical_name"] for row in review_rows}
    assert "Pasarin, Claudia" in canonical_by_signature.values()
    assert "Muller, Bettina" in canonical_by_signature.values()
    assert replacement_map[("Pasar\u00ed\u00f3n, Claudia", "1964", "club a", "female")] == "Pasarin, Claudia"
    assert replacement_map[("Mu\u00fcller, Bettina", "1964", "club b", "female")] == "Muller, Bettina"


def test_build_review_rows_requires_birth_year_and_club_context():
    rows = [
        {"athlete_name": "Gomez, Francisco", "source_url": "a", "club_name": "Club A", "birth_year": "1987", "gender": "male"},
        {"athlete_name": "Go\u00e1mez, Francisco", "source_url": "b", "club_name": "Club A", "birth_year": "1987", "gender": "male"},
        {"athlete_name": "Go\u00e9mez, Francisco", "source_url": "c", "club_name": "Club B", "birth_year": "1987", "gender": "male"},
        {"athlete_name": "Go\u00f3mez, Francisco", "source_url": "d", "club_name": "Club A", "birth_year": "1988", "gender": "male"},
        {"athlete_name": "Go\u00famez, Francisco", "source_url": "e", "club_name": "Club A", "birth_year": "", "gender": "male"},
    ]

    _, replacement_map = curate.build_review_rows(rows)

    assert replacement_map == {
        ("Go\u00e1mez, Francisco", "1987", "club a", "male"): "Gomez, Francisco",
    }


def test_build_review_rows_does_not_apply_broad_name_collisions():
    rows = [
        {"athlete_name": "Alfaro, Mauricio", "source_url": "a", "club_name": "Club A", "birth_year": "1990", "gender": "male"},
        {"athlete_name": "Alfaro, Marco", "source_url": "b", "club_name": "Club A", "birth_year": "1990", "gender": "male"},
        {"athlete_name": "Augusto, Gloria", "source_url": "c", "club_name": "Club C", "birth_year": "1971", "gender": "female"},
        {"athlete_name": "Agusto, Gloria", "source_url": "d", "club_name": "Club C", "birth_year": "1971", "gender": "female"},
        {"athlete_name": "Augusto, Gloria", "source_url": "e", "club_name": "Club C", "birth_year": "1971", "gender": "female"},
        {"athlete_name": "Barrios, Sergio", "source_url": "f", "club_name": "Club D", "birth_year": "1998", "gender": "male"},
        {"athlete_name": "Barros, Sergio", "source_url": "g", "club_name": "Club D", "birth_year": "1998", "gender": "male"},
    ]

    _, replacement_map = curate.build_review_rows(rows)

    assert "Alfaro, Mauricio" not in [key[0] for key in replacement_map]
    assert "Augusto, Gloria" not in [key[0] for key in replacement_map]
    assert "Agusto, Gloria" not in [key[0] for key in replacement_map]
    assert "Barrios, Sergio" not in [key[0] for key in replacement_map]


def test_collect_name_rows_reads_parser_tables():
    tmp_dir = _workspace_tmp_dir()
    try:
        input_dir = tmp_dir / "parsed"
        input_dir.mkdir()
        pd.DataFrame(
            [{"full_name": "Cofre\u00e1, Patricio", "club_name": "Club Test", "gender": "male", "birth_year": "1980"}]
        ).to_csv(input_dir / "athlete.csv", index=False)
        pd.DataFrame(
            [{"athlete_name": "Go\u00e1mez, Francisco", "club_name": "Club Test", "birth_year_estimated": "1975"}]
        ).to_csv(input_dir / "result.csv", index=False)
        pd.DataFrame(
            [{"swimmer_name": "Mu\u00fcller, Bettina", "club_name": "Club Test", "gender": "female", "birth_year_estimated": "1982"}]
        ).to_csv(input_dir / "relay_swimmer.csv", index=False)

        rows = curate.collect_name_rows({"source_url": "https://example.test/resultados.pdf"}, input_dir)

        assert [row["table"] for row in rows] == ["athlete", "result", "relay_swimmer"]
        assert rows[0]["athlete_name"] == "Cofre\u00e1, Patricio"
        assert rows[1]["athlete_name"] == "Go\u00e1mez, Francisco"
        assert rows[2]["athlete_name"] == "Mu\u00fcller, Bettina"
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)
