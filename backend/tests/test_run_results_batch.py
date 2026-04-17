import sys
from argparse import Namespace
from pathlib import Path


BACKEND_DIR = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = BACKEND_DIR / "scripts"
FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures" / "batch_runner"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

import run_results_batch as batch


def test_validate_input_dir_returns_validated_for_minimal_parser_output():
    result = batch.validate_input_dir(FIXTURES_DIR / "valid")

    assert result.state == "validated"
    assert result.counts["event"] == 1
    assert result.counts["result"] == 1
    assert result.issues == []


def test_validate_input_dir_requires_review_when_required_csv_is_missing():
    result = batch.validate_input_dir(FIXTURES_DIR / "missing_result")

    assert result.state == "requires_review"
    assert {issue.issue_key for issue in result.issues} >= {"missing_result_csv", "no_results_found"}


def test_validate_input_dir_requires_review_for_invalid_canon():
    result = batch.validate_input_dir(FIXTURES_DIR / "invalid_canon")

    assert result.state == "requires_review"
    assert any(issue.issue_key == "invalid_event_stroke" for issue in result.issues)


def test_validate_input_dir_requires_review_when_debug_ratio_is_high():
    result = batch.validate_input_dir(FIXTURES_DIR / "high_debug", debug_threshold=0.20)

    assert result.state == "requires_review"
    assert any(issue.issue_key == "debug_unparsed_ratio_exceeded" for issue in result.issues)


def test_build_parse_command_uses_current_python_and_parser_args():
    args = Namespace(
        pdf="backend/data/raw/results_pdf/demo.pdf",
        out_dir="backend/data/raw/results_csv/demo",
        competition_id=42,
        default_source_id=7,
        excel_name="parsed_demo.xlsx",
    )

    command = batch.build_parse_command(args)

    assert command[0] == sys.executable
    assert command[1].endswith("parse_results_pdf.py")
    assert command[2:] == [
        "--pdf",
        str(Path(args.pdf)),
        "--out-dir",
        str(Path(args.out_dir)),
        "--default-source-id",
        "7",
        "--excel-name",
        "parsed_demo.xlsx",
        "--competition-id",
        "42",
    ]
