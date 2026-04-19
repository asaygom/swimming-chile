import json
import sys
from argparse import Namespace
from pathlib import Path


BACKEND_DIR = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = BACKEND_DIR / "scripts"
FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures" / "batch_runner"
STAGING_DIR = BACKEND_DIR / "data" / "staging" / "csv"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

import download_manifest_pdfs as downloader
import run_results_batch as batch


def test_manifest_download_then_batch_validation_chain(monkeypatch):
    manifest_path = STAGING_DIR / "test_results_chain_manifest.jsonl"
    pdf_path = STAGING_DIR / "test_results_chain.pdf"
    parser_seen = []
    pdf_bytes = b"%PDF-1.4\nchain fixture\n"
    manifest_path.write_text(
        json.dumps(
            {
                "source_url": "https://fchmn.cl/wp-content/uploads/2026/03/resultados-demo.pdf",
                "pdf": str(pdf_path),
                "out_dir": str(FIXTURES_DIR / "valid"),
                "competition_id": 42,
                "default_source_id": 7,
            }
        )
        + "\n",
        encoding="utf-8",
    )
    pdf_path.unlink(missing_ok=True)

    def fake_run_parser(args):
        assert Path(args.pdf).exists()
        parser_seen.append(
            {
                "pdf": args.pdf,
                "out_dir": args.out_dir,
                "competition_id": args.competition_id,
                "default_source_id": args.default_source_id,
            }
        )
        return Path(args.out_dir)

    monkeypatch.setattr(batch, "run_parser", fake_run_parser)

    try:
        download_result = downloader.process_manifest(
            manifest_path,
            timeout_seconds=5,
            fetcher=lambda url, timeout: pdf_bytes,
        )
        batch_args = Namespace(
            manifest=str(manifest_path),
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
        batch_result = batch.process_manifest(batch_args)
    finally:
        manifest_path.unlink(missing_ok=True)
        pdf_path.unlink(missing_ok=True)

    assert download_result.state == "downloaded"
    assert download_result.documents[0].pdf_sha256 == downloader.sha256_bytes(pdf_bytes)
    assert batch_result.state == "validated"
    assert [document.state for document in batch_result.documents] == ["validated"]
    assert parser_seen == [
        {
            "pdf": str(pdf_path),
            "out_dir": str(FIXTURES_DIR / "valid"),
            "competition_id": 42,
            "default_source_id": 7,
        }
    ]
