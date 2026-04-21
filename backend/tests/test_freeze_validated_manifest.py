import json
import sys
from pathlib import Path

import pytest


BACKEND_DIR = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = BACKEND_DIR / "scripts"
STAGING_DIR = BACKEND_DIR / "data" / "staging" / "csv"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

import freeze_validated_manifest as freezer


def write_summary(path: Path) -> None:
    path.write_text(
        json.dumps(
            {
                "state": "requires_review",
                "documents": [
                    {
                        "state": "validated",
                        "input_dir": "backend/data/raw/results_csv/local_a",
                        "source_url": "https://fchmn.cl/local-a.pdf",
                    },
                    {
                        "state": "validated",
                        "input_dir": "backend/data/raw/results_csv/international_b",
                        "source_url": "https://fchmn.cl/international-b.pdf",
                    },
                    {
                        "state": "requires_review",
                        "input_dir": "backend/data/raw/results_csv/review_c",
                        "source_url": "https://fchmn.cl/review-c.pdf",
                    },
                ],
            }
        ),
        encoding="utf-8",
    )


def write_summary_with_url(path: Path, source_url: str) -> None:
    path.write_text(
        json.dumps(
            {
                "state": "validated",
                "documents": [
                    {
                        "state": "validated",
                        "input_dir": f"backend/data/raw/results_csv/{path.stem}",
                        "source_url": source_url,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )


def test_freeze_validated_manifest_requires_explicit_curation():
    summary_path = STAGING_DIR / "test_freeze_requires_curation_summary.json"
    manifest_path = STAGING_DIR / "test_freeze_requires_curation_manifest.jsonl"
    write_summary(summary_path)

    try:
        with pytest.raises(SystemExit) as excinfo:
            freezer.freeze_validated_manifest(
                summary_path,
                manifest_path,
                "fchmn_local",
                default_source_id=1,
                allowed_source_urls=set(),
            )
    finally:
        summary_path.unlink(missing_ok=True)
        manifest_path.unlink(missing_ok=True)

    assert "allow-source-url-file" in str(excinfo.value)


def test_freeze_validated_manifest_includes_only_allowed_validated_documents():
    summary_path = STAGING_DIR / "test_freeze_summary.json"
    manifest_path = STAGING_DIR / "test_freeze_manifest.jsonl"
    write_summary(summary_path)

    try:
        result = freezer.freeze_validated_manifest(
            summary_path,
            manifest_path,
            "fchmn_local",
            default_source_id=7,
            allowed_source_urls={"https://fchmn.cl/local-a.pdf"},
        )
        entries = [json.loads(line) for line in manifest_path.read_text(encoding="utf-8").splitlines()]
    finally:
        summary_path.unlink(missing_ok=True)
        manifest_path.unlink(missing_ok=True)

    assert result.state == "frozen"
    assert result.included_documents == 1
    assert result.skipped_documents == 2
    assert entries == [
        {
            "input_dir": "backend/data/raw/results_csv/local_a",
            "default_source_id": 7,
            "competition_scope": "fchmn_local",
            "source_url": "https://fchmn.cl/local-a.pdf",
        }
    ]


def test_freeze_validated_manifest_can_explicitly_include_all_validated_documents():
    summary_path = STAGING_DIR / "test_freeze_all_summary.json"
    manifest_path = STAGING_DIR / "test_freeze_all_manifest.jsonl"
    write_summary(summary_path)

    try:
        result = freezer.freeze_validated_manifest(
            summary_path,
            manifest_path,
            "fchmn_local",
            default_source_id=1,
            allowed_source_urls=set(),
            allow_all_validated=True,
        )
        entries = [json.loads(line) for line in manifest_path.read_text(encoding="utf-8").splitlines()]
    finally:
        summary_path.unlink(missing_ok=True)
        manifest_path.unlink(missing_ok=True)

    assert result.state == "frozen"
    assert result.included_documents == 2
    assert [entry["source_url"] for entry in entries] == [
        "https://fchmn.cl/local-a.pdf",
        "https://fchmn.cl/international-b.pdf",
    ]


def test_freeze_validated_manifests_consolidates_multiple_summaries():
    summary_a_path = STAGING_DIR / "test_freeze_multi_a_summary.json"
    summary_b_path = STAGING_DIR / "test_freeze_multi_b_summary.json"
    manifest_path = STAGING_DIR / "test_freeze_multi_manifest.jsonl"
    write_summary_with_url(summary_a_path, "https://fchmn.cl/local-a.pdf")
    write_summary_with_url(summary_b_path, "https://fchmn.cl/local-b.pdf")

    try:
        result = freezer.freeze_validated_manifests(
            [summary_a_path, summary_b_path],
            manifest_path,
            "fchmn_local",
            default_source_id=1,
            allowed_source_urls={"https://fchmn.cl/local-a.pdf", "https://fchmn.cl/local-b.pdf"},
        )
        entries = [json.loads(line) for line in manifest_path.read_text(encoding="utf-8").splitlines()]
    finally:
        summary_a_path.unlink(missing_ok=True)
        summary_b_path.unlink(missing_ok=True)
        manifest_path.unlink(missing_ok=True)

    assert result.state == "frozen"
    assert result.included_documents == 2
    assert result.batch_summary_paths == [str(summary_a_path), str(summary_b_path)]
    assert [entry["source_url"] for entry in entries] == [
        "https://fchmn.cl/local-a.pdf",
        "https://fchmn.cl/local-b.pdf",
    ]


def test_read_allowed_source_urls_ignores_blank_lines_comments_and_bom():
    allow_path = STAGING_DIR / "test_freeze_allowed_urls.txt"
    allow_path.write_text("\ufeff\n# curated\nhttps://fchmn.cl/local-a.pdf\n\n", encoding="utf-8")

    try:
        allowed = freezer.read_allowed_source_urls(allow_path)
    finally:
        allow_path.unlink(missing_ok=True)

    assert allowed == {"https://fchmn.cl/local-a.pdf"}
