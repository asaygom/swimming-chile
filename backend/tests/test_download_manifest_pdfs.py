import json
import sys
from pathlib import Path


BACKEND_DIR = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = BACKEND_DIR / "scripts"
STAGING_DIR = BACKEND_DIR / "data" / "staging" / "csv"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

import download_manifest_pdfs as downloader


def test_process_manifest_downloads_pdf_and_reports_checksum():
    manifest_path = STAGING_DIR / "test_download_manifest.jsonl"
    pdf_path = STAGING_DIR / "test_downloaded.pdf"
    content = b"%PDF-1.4\nfixture\n"
    manifest_path.write_text(
        json.dumps({"source_url": "https://fchmn.cl/resultados.pdf", "pdf": str(pdf_path)}) + "\n",
        encoding="utf-8",
    )
    pdf_path.unlink(missing_ok=True)

    try:
        result = downloader.process_manifest(
            manifest_path,
            timeout_seconds=5,
            fetcher=lambda url, timeout: content,
        )
    finally:
        manifest_path.unlink(missing_ok=True)

    try:
        assert result.state == "downloaded"
        assert result.documents[0].state == "downloaded"
        assert result.documents[0].bytes == len(content)
        assert result.documents[0].pdf_sha256 == downloader.sha256_bytes(content)
        assert pdf_path.read_bytes() == content
    finally:
        pdf_path.unlink(missing_ok=True)


def test_process_manifest_skips_existing_pdf_without_overwrite():
    manifest_path = STAGING_DIR / "test_download_skip_manifest.jsonl"
    pdf_path = STAGING_DIR / "test_existing.pdf"
    existing = b"%PDF-1.4\nexisting\n"
    manifest_path.write_text(
        json.dumps({"source_url": "https://fchmn.cl/resultados.pdf", "pdf": str(pdf_path)}) + "\n",
        encoding="utf-8",
    )
    pdf_path.write_bytes(existing)

    try:
        result = downloader.process_manifest(
            manifest_path,
            timeout_seconds=5,
            fetcher=lambda url, timeout: b"new content",
        )
    finally:
        manifest_path.unlink(missing_ok=True)

    try:
        assert result.state == "skipped"
        assert result.documents[0].state == "skipped"
        assert result.documents[0].pdf_sha256 == downloader.sha256_bytes(existing)
        assert pdf_path.read_bytes() == existing
    finally:
        pdf_path.unlink(missing_ok=True)


def test_process_manifest_marks_entries_without_source_url_as_failed():
    manifest_path = STAGING_DIR / "test_download_failed_manifest.jsonl"
    manifest_path.write_text(
        json.dumps({"pdf": str(STAGING_DIR / "missing-source.pdf")}) + "\n",
        encoding="utf-8",
    )

    try:
        result = downloader.process_manifest(manifest_path, timeout_seconds=5)
    finally:
        manifest_path.unlink(missing_ok=True)

    assert result.state == "failed"
    assert result.documents[0].state == "failed"
    assert result.documents[0].message == "Falta source_url."


def test_read_manifest_accepts_utf8_bom():
    manifest_path = STAGING_DIR / "test_download_bom_manifest.jsonl"
    manifest_path.write_text(
        "\ufeff" + json.dumps({"source_url": "https://fchmn.cl/resultados.pdf", "pdf": "demo.pdf"}) + "\n",
        encoding="utf-8",
    )

    try:
        entries = downloader.read_manifest_entries(manifest_path)
    finally:
        manifest_path.unlink(missing_ok=True)

    assert entries == [{"source_url": "https://fchmn.cl/resultados.pdf", "pdf": "demo.pdf"}]


def test_process_manifest_continues_when_one_download_fails():
    manifest_path = STAGING_DIR / "test_download_partial_manifest.jsonl"
    good_pdf_path = STAGING_DIR / "test_partial_good.pdf"
    bad_pdf_path = STAGING_DIR / "test_partial_bad.pdf"
    manifest_path.write_text(
        "\n".join(
            [
                json.dumps({"source_url": "https://fchmn.cl/good.pdf", "pdf": str(good_pdf_path)}),
                json.dumps({"source_url": "https://fchmn.cl/bad.pdf", "pdf": str(bad_pdf_path)}),
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    good_pdf_path.unlink(missing_ok=True)
    bad_pdf_path.unlink(missing_ok=True)

    def fake_fetcher(url, timeout):
        if url.endswith("bad.pdf"):
            raise RuntimeError("download failed")
        return b"%PDF-1.4\ngood\n"

    try:
        result = downloader.process_manifest(manifest_path, timeout_seconds=5, fetcher=fake_fetcher)
    finally:
        manifest_path.unlink(missing_ok=True)

    try:
        assert result.state == "failed"
        assert [document.state for document in result.documents] == ["downloaded", "failed"]
        assert good_pdf_path.exists()
        assert not bad_pdf_path.exists()
        assert result.documents[1].message == "download failed"
    finally:
        good_pdf_path.unlink(missing_ok=True)
        bad_pdf_path.unlink(missing_ok=True)
