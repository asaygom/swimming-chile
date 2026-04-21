import json
import os
import shutil
import sys
from pathlib import Path


BACKEND_DIR = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = BACKEND_DIR / "scripts"
STAGING_DIR = BACKEND_DIR / "data" / "staging" / "csv"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

import audit_fchmn_artifacts as auditor


def write_jsonl(path: Path, entries: list[dict]) -> None:
    path.write_text("\n".join(json.dumps(entry) for entry in entries) + "\n", encoding="utf-8")


def write_summary(path: Path, documents: list[dict], mtime: int) -> None:
    path.write_text(json.dumps({"state": "mixed", "documents": documents}), encoding="utf-8")
    os.utime(path, (mtime, mtime))


def touch_pdf(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"%PDF-1.4\nfixture\n")


def write_complete_parse_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)
    for filename in auditor.REQUIRED_CSV_FILES:
        path.joinpath(filename).write_text("", encoding="utf-8")


def manifest_entry(root: Path, slug: str, source_url: str) -> dict:
    return {
        "source_url": source_url,
        "pdf": str(root / "pdfs" / f"{slug}.pdf"),
        "out_dir": str(root / "csv" / slug),
    }


def test_build_audit_classifies_artifact_gaps_and_candidates():
    test_root = STAGING_DIR / "test_audit_fchmn_artifacts"
    shutil.rmtree(test_root, ignore_errors=True)
    manifest_dir = test_root / "manifests"
    summary_dir = test_root / "summaries"
    manifest_dir.mkdir(parents=True)
    summary_dir.mkdir(parents=True)
    manifest_path = manifest_dir / "historical.jsonl"

    try:
        entries = [
            manifest_entry(test_root, "missing-download", "https://fchmn.cl/resultados-missing-download.pdf"),
            manifest_entry(test_root, "missing-parse", "https://fchmn.cl/resultados-missing-parse.pdf"),
            manifest_entry(test_root, "missing-validation", "https://fchmn.cl/resultados-missing-validation.pdf"),
            manifest_entry(test_root, "failed", "https://fchmn.cl/resultados-failed.pdf"),
            manifest_entry(test_root, "review", "https://fchmn.cl/resultados-review.pdf"),
            manifest_entry(test_root, "local", "https://fchmn.cl/resultados-copa-local.pdf"),
            manifest_entry(test_root, "non-local", "https://fchmn.cl/resultados-copa-argentina.pdf"),
        ]
        write_jsonl(manifest_path, entries)

        for slug in ["missing-parse", "missing-validation", "failed", "review", "local", "non-local"]:
            touch_pdf(test_root / "pdfs" / f"{slug}.pdf")
        for slug in ["missing-validation", "review", "local", "non-local"]:
            write_complete_parse_dir(test_root / "csv" / slug)

        write_summary(
            summary_dir / "old_batch.json",
            [
                {
                    "state": "failed",
                    "source_url": "https://fchmn.cl/resultados-copa-argentina.pdf",
                    "input_dir": str(test_root / "csv" / "non-local"),
                    "issues": [{"message": "parser old failure"}],
                }
            ],
            mtime=1,
        )
        write_summary(
            summary_dir / "new_batch.json",
            [
                {
                    "state": "failed",
                    "source_url": "https://fchmn.cl/resultados-failed.pdf",
                    "input_dir": str(test_root / "csv" / "failed"),
                    "issues": [{"message": "parser failed"}],
                },
                {
                    "state": "requires_review",
                    "source_url": "https://fchmn.cl/resultados-review.pdf",
                    "input_dir": str(test_root / "csv" / "review"),
                    "issues": [{"message": "stroke fuera de canon"}],
                    "metadata": {"competition_name": "Copa Review"},
                },
                {
                    "state": "validated",
                    "source_url": "https://fchmn.cl/resultados-copa-local.pdf",
                    "input_dir": str(test_root / "csv" / "local"),
                    "metadata": {"competition_name": "Copa Local"},
                },
                {
                    "state": "validated",
                    "source_url": "https://fchmn.cl/resultados-copa-argentina.pdf",
                    "input_dir": str(test_root / "csv" / "non-local"),
                    "metadata": {"competition_name": "Copa Argentina"},
                },
            ],
            mtime=2,
        )

        result = auditor.build_audit(manifest_dir, summary_dir, test_root, focus_manifest=manifest_path)

        assert result.state == "audited"
        assert result.total_documents == 7
        assert result.category_counts == {
            "failed": 1,
            "missing_download": 1,
            "missing_parse": 1,
            "missing_validation": 1,
            "requires_review": 1,
            "validated_local_candidate": 1,
            "validated_non_local_candidate": 1,
        }

        documents = {document.source_url: document for document in result.documents}
        assert documents["https://fchmn.cl/resultados-copa-argentina.pdf"].category == "validated_non_local_candidate"
        assert documents["https://fchmn.cl/resultados-copa-argentina.pdf"].latest_validation_summary == "new_batch.json"
        assert documents["https://fchmn.cl/resultados-review.pdf"].issues == ["stroke fuera de canon"]
    finally:
        shutil.rmtree(test_root, ignore_errors=True)


def test_summary_kind_treats_load_as_validation_evidence():
    assert auditor.summary_kind(Path("fchmn_home_load.json")) == "batch"
    assert auditor.summary_kind(Path("fchmn_home_download.json")) == "download"
