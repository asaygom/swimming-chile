import json
import subprocess
import sys
from pathlib import Path


BACKEND_DIR = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = BACKEND_DIR / "scripts"
STAGING_DIR = BACKEND_DIR / "data" / "staging" / "csv"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

import run_fchmn_results_validation as validation


def test_run_results_validation_orchestrates_discovery_download_and_batch_validation(monkeypatch):
    calls = []
    manifest_path = STAGING_DIR / "fchmn_results_validation_test-success.jsonl"
    download_summary_path = STAGING_DIR / "fchmn_results_validation_test-success_download.json"
    batch_summary_path = STAGING_DIR / "fchmn_results_validation_test-success_batch.json"
    for path in [manifest_path, download_summary_path, batch_summary_path]:
        path.unlink(missing_ok=True)

    def fake_run(command, check):
        calls.append(command)
        if command[1].endswith("scrape_fchmn.py"):
            Path(command[command.index("--manifest") + 1]).write_text(
                json.dumps(
                    {
                        "source_url": "https://fchmn.cl/resultados.pdf",
                        "pdf": "demo.pdf",
                        "out_dir": "demo",
                        "competition_id": None,
                        "default_source_id": 1,
                    }
                )
                + "\n",
                encoding="utf-8",
            )
        elif command[1].endswith("download_manifest_pdfs.py"):
            Path(command[command.index("--summary-json") + 1]).write_text(
                json.dumps({"state": "downloaded", "state_counts": {"downloaded": 1}, "documents": []}),
                encoding="utf-8",
            )
        elif command[1].endswith("run_results_batch.py"):
            Path(command[command.index("--summary-json") + 1]).write_text(
                json.dumps({"state": "validated", "state_counts": {"validated": 1}, "documents": []}),
                encoding="utf-8",
            )
        return subprocess.CompletedProcess(command, 0)

    monkeypatch.setattr(validation.subprocess, "run", fake_run)
    args = validation.parse_args_from_list(
        [
            "--url",
            "https://fchmn.cl/resultados/",
            "--run-id",
            "test-success",
            "--manifest-dir",
            str(STAGING_DIR),
            "--summary-dir",
            str(STAGING_DIR),
            "--pdf-dir",
            str(STAGING_DIR / "pdf"),
            "--out-dir-root",
            str(STAGING_DIR / "csv"),
        ]
    )

    try:
        result = validation.run_results_validation(args)
    finally:
        for path in [manifest_path, download_summary_path, batch_summary_path]:
            path.unlink(missing_ok=True)

    assert result.state == "validated"
    assert result.download_state_counts == {"downloaded": 1}
    assert result.batch_state_counts == {"validated": 1}
    assert [Path(call[1]).name for call in calls] == [
        "scrape_fchmn.py",
        "download_manifest_pdfs.py",
        "run_results_batch.py",
    ]


def test_run_results_validation_stops_before_batch_when_download_fails(monkeypatch):
    calls = []
    manifest_path = STAGING_DIR / "fchmn_results_validation_test-failure.jsonl"
    download_summary_path = STAGING_DIR / "fchmn_results_validation_test-failure_download.json"
    batch_summary_path = STAGING_DIR / "fchmn_results_validation_test-failure_batch.json"
    for path in [manifest_path, download_summary_path, batch_summary_path]:
        path.unlink(missing_ok=True)

    def fake_run(command, check):
        calls.append(command)
        if command[1].endswith("scrape_fchmn.py"):
            Path(command[command.index("--manifest") + 1]).write_text("", encoding="utf-8")
            return subprocess.CompletedProcess(command, 0)
        if command[1].endswith("download_manifest_pdfs.py"):
            Path(command[command.index("--summary-json") + 1]).write_text(
                json.dumps({"state": "failed", "state_counts": {"failed": 1}, "documents": []}),
                encoding="utf-8",
            )
            return subprocess.CompletedProcess(command, 1)
        raise AssertionError("batch should not run after download failure")

    monkeypatch.setattr(validation.subprocess, "run", fake_run)
    args = validation.parse_args_from_list(
        [
            "--run-id",
            "test-failure",
            "--manifest-dir",
            str(STAGING_DIR),
            "--summary-dir",
            str(STAGING_DIR),
        ]
    )

    try:
        result = validation.run_results_validation(args)
    finally:
        for path in [manifest_path, download_summary_path, batch_summary_path]:
            path.unlink(missing_ok=True)

    assert result.state == "failed"
    assert result.download_state_counts == {"failed": 1}
    assert [Path(call[1]).name for call in calls] == ["scrape_fchmn.py", "download_manifest_pdfs.py"]
