import sys
from argparse import Namespace
import json
from pathlib import Path

import pytest


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
    assert result.commands == {}


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


def test_build_load_command_uses_pipeline_args():
    args = Namespace(
        host="localhost",
        port=5432,
        dbname="natacion_chile",
        user="postgres",
        password="secret",
        schema="core",
        default_source_id=7,
        competition_id=42,
        truncate_staging=True,
    )

    command = batch.build_load_command(args, Path("backend/data/raw/results_csv/demo"))

    assert command[0] == sys.executable
    assert command[1].endswith("run_pipeline_results.py")
    assert command[2:] == [
        "--input-dir",
        str(Path("backend/data/raw/results_csv/demo")),
        "--host",
        "localhost",
        "--port",
        "5432",
        "--dbname",
        "natacion_chile",
        "--user",
        "postgres",
        "--password",
        "secret",
        "--schema",
        "core",
        "--default-source-id",
        "7",
        "--competition-id",
        "42",
        "--truncate-staging",
    ]


def test_redact_command_hides_password_value():
    command = ["python", "script.py", "--user", "postgres", "--password", "secret", "--schema", "core"]

    assert batch.redact_command(command) == [
        "python",
        "script.py",
        "--user",
        "postgres",
        "--password",
        "***",
        "--schema",
        "core",
    ]


def test_main_does_not_load_when_batch_requires_review(monkeypatch):
    called = {"load": False}

    def fake_run_pipeline(args, input_dir):
        called["load"] = True

    monkeypatch.setattr(batch, "run_pipeline", fake_run_pipeline)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_results_batch.py",
            "--input-dir",
            str(FIXTURES_DIR / "missing_result"),
            "--load",
            "--user",
            "postgres",
            "--password",
            "secret",
        ],
    )

    with pytest.raises(SystemExit) as excinfo:
        batch.main()

    assert excinfo.value.code == 1
    assert called["load"] is False


def test_main_loads_only_after_validated_batch(monkeypatch):
    called = {"load": False}

    def fake_run_pipeline(args, input_dir):
        called["load"] = True

    monkeypatch.setattr(batch, "run_pipeline", fake_run_pipeline)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_results_batch.py",
            "--input-dir",
            str(FIXTURES_DIR / "valid"),
            "--load",
            "--user",
            "postgres",
            "--password",
            "secret",
        ],
    )

    batch.main()

    assert called["load"] is True


def test_main_writes_summary_json_with_redacted_load_command(monkeypatch):
    summary_path = BACKEND_DIR / "data" / "staging" / "csv" / "test_batch_summary.json"
    summary_path.unlink(missing_ok=True)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_results_batch.py",
            "--input-dir",
            str(FIXTURES_DIR / "missing_result"),
            "--load",
            "--user",
            "postgres",
            "--password",
            "secret",
            "--summary-json",
            str(summary_path),
        ],
    )

    with pytest.raises(SystemExit):
        try:
            batch.main()
        finally:
            payload = json.loads(summary_path.read_text(encoding="utf-8"))
            summary_path.unlink(missing_ok=True)

    assert payload["state"] == "requires_review"
    assert payload["commands"]["parse"] is None
    assert "--password" in payload["commands"]["load"]
    assert "secret" not in payload["commands"]["load"]
    assert "***" in payload["commands"]["load"]


def test_process_manifest_continues_across_valid_and_review_documents():
    args = Namespace(
        manifest=str(FIXTURES_DIR / "manifest_input_dirs.jsonl"),
        input_dir=None,
        pdf=None,
        out_dir=None,
        competition_id=None,
        default_source_id=1,
        excel_name="parsed_results.xlsx",
        load=False,
        host="localhost",
        port=5432,
        dbname="natacion_chile",
        user=None,
        password=None,
        schema="core",
        truncate_staging=False,
        debug_threshold=0.20,
    )

    result = batch.process_manifest(args)

    assert result.state == "requires_review"
    assert [document.state for document in result.documents] == ["validated", "requires_review"]


def test_process_manifest_supports_pdf_entries_without_cross_document_contamination(monkeypatch):
    parsed = []

    def fake_run_parser(args):
        parsed.append(
            {
                "pdf": args.pdf,
                "out_dir": args.out_dir,
                "competition_id": args.competition_id,
                "default_source_id": args.default_source_id,
            }
        )
        return Path(args.out_dir)

    monkeypatch.setattr(batch, "run_parser", fake_run_parser)
    args = Namespace(
        manifest=str(FIXTURES_DIR / "manifest_pdfs.jsonl"),
        input_dir=None,
        pdf=None,
        out_dir=None,
        competition_id=None,
        default_source_id=1,
        excel_name="parsed_results.xlsx",
        load=False,
        host="localhost",
        port=5432,
        dbname="natacion_chile",
        user=None,
        password=None,
        schema="core",
        truncate_staging=False,
        debug_threshold=0.20,
    )

    result = batch.process_manifest(args)

    assert result.state == "requires_review"
    assert [document.state for document in result.documents] == ["validated", "requires_review"]
    assert [document.input_dir for document in result.documents] == [
        str(Path("backend/tests/fixtures/batch_runner/valid")),
        str(Path("backend/tests/fixtures/batch_runner/missing_result")),
    ]
    assert parsed == [
        {
            "pdf": "backend/tests/fixtures/batch_runner/pdf_inputs/fixture_a.pdf",
            "out_dir": "backend/tests/fixtures/batch_runner/valid",
            "competition_id": 42,
            "default_source_id": 7,
        },
        {
            "pdf": "backend/tests/fixtures/batch_runner/pdf_inputs/fixture_b.pdf",
            "out_dir": "backend/tests/fixtures/batch_runner/missing_result",
            "competition_id": 43,
            "default_source_id": 1,
        },
    ]
    assert result.documents[0].commands["parse"] is not None
    assert result.documents[1].commands["parse"] is not None


def test_main_writes_manifest_summary_json(monkeypatch):
    summary_path = BACKEND_DIR / "data" / "staging" / "csv" / "test_manifest_summary.json"
    summary_path.unlink(missing_ok=True)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_results_batch.py",
            "--manifest",
            str(FIXTURES_DIR / "manifest_input_dirs.jsonl"),
            "--summary-json",
            str(summary_path),
        ],
    )

    with pytest.raises(SystemExit):
        try:
            batch.main()
        finally:
            payload = json.loads(summary_path.read_text(encoding="utf-8"))
            summary_path.unlink(missing_ok=True)

    assert payload["state"] == "requires_review"
    assert len(payload["documents"]) == 2
    assert [document["state"] for document in payload["documents"]] == ["validated", "requires_review"]
