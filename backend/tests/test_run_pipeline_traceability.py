import argparse
import sys
from pathlib import Path

import pandas as pd

BACKEND_DIR = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = BACKEND_DIR / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

import run_pipeline_results as pipeline


def test_count_input_rows_for_load_run():
    data = {
        "club": pd.DataFrame([{"name": "Club A"}, {"name": "Club B"}]),
        "event": pd.DataFrame([{"event_name": "Event A"}]),
        "athlete": pd.DataFrame(),
        "result": pd.DataFrame([{"athlete_name": "Uno"}]),
        "relay_result": pd.DataFrame([{"relay_team_name": "Club A"}]),
        "relay_result_member": pd.DataFrame([{"athlete_name": "Dos"}, {"athlete_name": "Tres"}]),
    }

    assert pipeline.count_input_rows(data) == {
        "club": 2,
        "event": 1,
        "athlete": 0,
        "result": 1,
        "relay_result": 1,
        "relay_result_member": 2,
    }


def test_derive_source_document_name_prefers_pdf_metadata():
    args = argparse.Namespace(excel="manual.xlsx", input_dir="backend/data/raw/results_csv/demo")
    metadata = {"pdf_name": "resultados-demo.pdf", "competition_name": "Demo"}

    assert pipeline.derive_source_document_name(args, metadata) == "resultados-demo.pdf"


def test_derive_source_document_name_falls_back_to_input_dir():
    args = argparse.Namespace(excel=None, input_dir="backend/data/raw/results_csv/demo")

    assert pipeline.derive_source_document_name(args, {}) == "demo"


def test_normalize_dataframe_derives_valid_status_from_result_time():
    df = pd.DataFrame(
        [
            {
                "event_name": "Event A",
                "athlete_name": "Nadador Uno",
                "club_name": "Club A",
                "rank_position": "1",
                "seed_time_text": None,
                "seed_time_ms": None,
                "result_time_text": "1:05.30",
                "result_time_ms": None,
                "age_at_event": "35",
                "birth_year_estimated": "1991",
                "points": None,
                "status": None,
                "source_id": "1",
            }
        ]
    )

    normalized = pipeline.normalize_dataframe(df, pipeline.EXPECTED_COLUMNS["result"], "result")

    assert normalized.loc[0, "result_time_text"] == "1:05,30"
    assert normalized.loc[0, "result_time_ms"] == "65300"
    assert normalized.loc[0, "status"] == "valid"


def test_default_club_alias_csv_contains_audited_fchmn_mappings():
    aliases = pipeline.load_club_aliases(str(pipeline.DEFAULT_CLUB_ALIAS_CSV))

    assert pipeline.resolve_club_alias("Orinoco Swim 23", aliases) == "Orinoco Swim"
    assert pipeline.resolve_club_alias("Estadio Español Master-ZZ", aliases) == "Estadio Español"
    assert pipeline.resolve_club_alias("Manateam Swim-AN", aliases) == "Manateam Swim"
    assert pipeline.resolve_club_alias("Natacion Neurodivergentes", aliases) == "Natacion Neurodivergente"
