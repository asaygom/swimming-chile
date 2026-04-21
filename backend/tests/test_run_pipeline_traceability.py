import argparse
import sys
from pathlib import Path

import pandas as pd
import pytest

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


def test_normalize_competition_scope_accepts_snake_case():
    assert pipeline.normalize_competition_scope("fchmn_local") == "fchmn_local"


def test_normalize_competition_scope_rejects_free_text():
    with pytest.raises(SystemExit):
        pipeline.normalize_competition_scope("FCHMN Local")


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


def test_transform_parser_relay_outputs_prefers_relay_team_club_name_with_empty_club_csv():
    relay_team_df = pd.DataFrame(
        [
            {
                "event_name": "Mixed 200 SC Meter Medley Relay",
                "club_name": "Associacao Master",
                "relay_team_name": "Equipe A",
                "rank_position": "1",
                "seed_time_text": None,
                "seed_time_ms": None,
                "result_time_text": "2:05.10",
                "result_time_ms": "125100",
                "points": "18",
                "status": "valid",
                "source_id": "1",
                "page_number": "3",
                "line_number": "42",
            }
        ]
    )
    relay_swimmer_df = pd.DataFrame(
        [
            {
                "event_name": "Mixed 200 SC Meter Medley Relay",
                "relay_team_name": "Equipe A",
                "leg_order": "1",
                "swimmer_name": "Nadador Uno",
                "gender": "male",
                "age_at_event": "35",
                "birth_year_estimated": "1991",
                "page_number": "3",
                "line_number": "43",
            }
        ]
    )
    club_df = pd.DataFrame(columns=pipeline.EXPECTED_COLUMNS["club"])

    transformed = pipeline.transform_parser_relay_outputs(
        relay_team_df,
        relay_swimmer_df,
        club_df,
        default_source_id=1,
    )

    assert transformed["relay_result"].loc[0, "club_name"] == "Associacao Master"
    assert transformed["relay_result_member"].loc[0, "club_name"] == "Associacao Master"
