from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def read_jsonl_manifest_entries(manifest_path: Path) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    with manifest_path.open("r", encoding="utf-8-sig") as handle:
        for line_number, raw_line in enumerate(handle, start=1):
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            try:
                entry = json.loads(line)
            except json.JSONDecodeError as exc:
                raise SystemExit(f"[ERROR] Manifest JSONL invalido en linea {line_number}: {exc}") from exc
            if not isinstance(entry, dict):
                raise SystemExit(f"[ERROR] Manifest linea {line_number} debe ser un objeto JSON.")
            entries.append(entry)
    return entries


def count_jsonl_manifest_entries(manifest_path: Path) -> int:
    if not manifest_path.exists():
        return 0
    return len(read_jsonl_manifest_entries(manifest_path))
