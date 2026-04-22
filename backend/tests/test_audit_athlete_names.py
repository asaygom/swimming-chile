import sys
import shutil
import uuid
from pathlib import Path

import pandas as pd

BACKEND_DIR = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = BACKEND_DIR / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

import audit_athlete_names as audit


def _workspace_tmp_dir() -> Path:
    path = BACKEND_DIR / "data" / "raw" / "batch_summaries" / f"test_audit_athlete_names_{uuid.uuid4().hex}"
    path.mkdir(parents=True)
    return path


def test_classify_athlete_name_flags_parser_noise():
    assert "contains_digit" in audit.classify_athlete_name("Rojas, 2")
    assert "contains_club_word" in audit.classify_athlete_name("Club Natacion Master")
    assert "single_letter_ocr_run" in audit.classify_athlete_name("A r a n g u i z, Pedro")
    assert audit.classify_athlete_name("Rojas, Maria") == []


def test_audit_document_includes_raw_line_for_result():
    tmp_dir = _workspace_tmp_dir()
    try:
        input_dir = tmp_dir / "parsed"
        input_dir.mkdir()
        pd.DataFrame(
            [
                {
                    "full_name": "Rojas, 2",
                    "gender": "male",
                    "club_name": "Club Test",
                    "birth_year": "1980",
                    "source_id": "1",
                }
            ]
        ).to_csv(input_dir / "athlete.csv", index=False)
        pd.DataFrame(
            [
                {
                    "event_name": "men 40-44 50 LC Meter freestyle",
                    "athlete_name": "Rojas, 2",
                    "club_name": "Club Test",
                    "birth_year_estimated": "1980",
                }
            ]
        ).to_csv(input_dir / "result.csv", index=False)
        pd.DataFrame(
            [
                {
                    "page_number": "3",
                    "line_number": "14",
                    "event_name": "men 40-44 50 LC Meter freestyle",
                    "athlete_name": "Rojas, 2",
                    "club_name": "Club Test",
                    "raw_line": "1 Rojas, 2 44 Club Test 29,00 28,50",
                }
            ]
        ).to_csv(input_dir / "raw_result.csv", index=False)

        rows, reasons, missing, observed = audit.audit_document(
            {"source_url": "https://example.test/resultados.pdf"},
            input_dir,
        )

        assert missing == []
        assert observed == 2
        assert reasons["contains_digit"] == 2
        result_rows = [row for row in rows if row["table"] == "result"]
        assert result_rows[0]["sample_line_number"] == "14"
        assert "Rojas" in result_rows[0]["sample_raw_line"]
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


def test_load_overrides_resolves_source_url_mapping():
    override_dir = BACKEND_DIR / "data" / "raw" / "batch_summaries" / "scratch"
    mapping = audit.load_overrides([f"https://example.test/a.pdf={override_dir}"])
    assert mapping["https://example.test/a.pdf"] == override_dir


def test_read_csv_if_exists_treats_empty_file_as_empty_frame():
    tmp_dir = _workspace_tmp_dir()
    try:
        csv_path = tmp_dir / "empty.csv"
        csv_path.write_text("", encoding="utf-8")
        assert audit.read_csv_if_exists(csv_path).empty
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)
