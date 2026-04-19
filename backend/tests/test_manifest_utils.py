import json
import sys
from pathlib import Path

import pytest


BACKEND_DIR = Path(__file__).resolve().parents[1]
STAGING_DIR = BACKEND_DIR / "data" / "staging" / "csv"
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from natacion_chile.manifest import count_jsonl_manifest_entries, read_jsonl_manifest_entries


def test_read_jsonl_manifest_entries_ignores_blank_lines_comments_and_bom():
    manifest_path = STAGING_DIR / "test_manifest_utils.jsonl"
    manifest_path.write_text(
        "\ufeff\n# comment\n" + json.dumps({"input_dir": "demo"}) + "\n\n",
        encoding="utf-8",
    )

    try:
        entries = read_jsonl_manifest_entries(manifest_path)
        count = count_jsonl_manifest_entries(manifest_path)
    finally:
        manifest_path.unlink(missing_ok=True)

    assert entries == [{"input_dir": "demo"}]
    assert count == 1


def test_read_jsonl_manifest_entries_rejects_non_object_lines():
    manifest_path = STAGING_DIR / "test_manifest_utils_invalid.jsonl"
    manifest_path.write_text("[1, 2, 3]\n", encoding="utf-8")

    try:
        with pytest.raises(SystemExit) as excinfo:
            read_jsonl_manifest_entries(manifest_path)
    finally:
        manifest_path.unlink(missing_ok=True)

    assert "debe ser un objeto JSON" in str(excinfo.value)
