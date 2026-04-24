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
    assert curate.athlete_name_signature("Pasarin, Claudia") == curate.athlete_name_signature("Pasaríón, Claudia")
    assert curate.athlete_name_signature("Gomez, Francisco") == curate.athlete_name_signature("Goámez, Francisco")
    assert curate.athlete_name_signature("Muller, Bettina") == curate.athlete_name_signature("Muüller, Bettina")


def test_build_review_rows_prefers_cleaner_canonical_names():
    rows = [
        {"athlete_name": "Pasarin, Claudia", "source_url": "a", "club_name": "Club A"},
        {"athlete_name": "Pasaríán, Claudia", "source_url": "b", "club_name": "Club A"},
        {"athlete_name": "Pasaríón, Claudia", "source_url": "c", "club_name": "Club A"},
        {"athlete_name": "Muüller, Bettina", "source_url": "d", "club_name": "Club B"},
        {"athlete_name": "Muller, Bettina", "source_url": "e", "club_name": "Club B"},
    ]

    review_rows, replacement_map = curate.build_review_rows(rows)

    canonical_by_signature = {row["signature"]: row["canonical_name"] for row in review_rows}
    assert "Pasarin, Claudia" in canonical_by_signature.values()
    assert "Muller, Bettina" in canonical_by_signature.values()
    assert replacement_map["Pasaríón, Claudia"] == "Pasarin, Claudia"
    assert replacement_map["Muüller, Bettina"] == "Muller, Bettina"


def test_collect_name_rows_reads_parser_tables():
    tmp_dir = _workspace_tmp_dir()
    try:
        input_dir = tmp_dir / "parsed"
        input_dir.mkdir()
        pd.DataFrame(
            [{"full_name": "Cofreá, Patricio", "club_name": "Club Test", "gender": "male", "birth_year": "1980"}]
        ).to_csv(input_dir / "athlete.csv", index=False)
        pd.DataFrame(
            [{"athlete_name": "Goámez, Francisco", "club_name": "Club Test", "birth_year_estimated": "1975"}]
        ).to_csv(input_dir / "result.csv", index=False)
        pd.DataFrame(
            [{"swimmer_name": "Muüller, Bettina", "club_name": "Club Test", "gender": "female", "birth_year_estimated": "1982"}]
        ).to_csv(input_dir / "relay_swimmer.csv", index=False)

        rows = curate.collect_name_rows({"source_url": "https://example.test/resultados.pdf"}, input_dir)

        assert [row["table"] for row in rows] == ["athlete", "result", "relay_swimmer"]
        assert rows[0]["athlete_name"] == "Cofreá, Patricio"
        assert rows[1]["athlete_name"] == "Goámez, Francisco"
        assert rows[2]["athlete_name"] == "Muüller, Bettina"
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)
