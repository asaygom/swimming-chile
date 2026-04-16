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
