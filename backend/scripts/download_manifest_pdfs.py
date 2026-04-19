#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Callable
from urllib.request import urlopen


BACKEND_DIR = Path(__file__).resolve().parents[1]
PROJECT_DIR = BACKEND_DIR.parent


@dataclass
class DownloadResult:
    state: str
    source_url: str | None
    pdf: str | None
    bytes: int = 0
    pdf_sha256: str | None = None
    previous_pdf_sha256: str | None = None
    message: str | None = None


@dataclass
class DownloadManifestResult:
    state: str
    manifest_path: str
    state_counts: dict[str, int]
    documents: list[DownloadResult]


def read_manifest_entries(manifest_path: Path) -> list[dict[str, Any]]:
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


def fetch_url_bytes(url: str, timeout_seconds: int) -> bytes:
    with urlopen(url, timeout=timeout_seconds) as response:
        return response.read()


def sha256_bytes(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def resolve_manifest_path(value: Any) -> Path | None:
    if not value:
        return None
    path = Path(str(value))
    return path if path.is_absolute() else PROJECT_DIR / path


def write_bytes_atomic(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f"{path.name}.download")
    tmp_path.write_bytes(content)
    tmp_path.replace(path)


def download_one(
    entry: dict[str, Any],
    timeout_seconds: int,
    overwrite: bool = False,
    fetcher: Callable[[str, int], bytes] = fetch_url_bytes,
) -> DownloadResult:
    source_url = entry.get("source_url")
    pdf = entry.get("pdf") or entry.get("pdf_path")

    if not source_url:
        return DownloadResult("failed", None, str(pdf) if pdf else None, message="Falta source_url.")
    if not pdf:
        return DownloadResult("failed", str(source_url), None, message="Falta pdf o pdf_path.")

    pdf_path = resolve_manifest_path(pdf)
    assert pdf_path is not None
    previous_sha256 = sha256_bytes(pdf_path.read_bytes()) if pdf_path.exists() else None
    if pdf_path.exists() and not overwrite:
        return DownloadResult(
            "skipped",
            str(source_url),
            str(pdf_path),
            bytes=pdf_path.stat().st_size,
            pdf_sha256=previous_sha256,
            message="PDF existente; usa --overwrite para reemplazarlo.",
        )

    try:
        content = fetcher(str(source_url), timeout_seconds)
        write_bytes_atomic(pdf_path, content)
    except Exception as exc:
        return DownloadResult("failed", str(source_url), str(pdf_path), message=str(exc))
    new_sha256 = sha256_bytes(content)
    state = "downloaded"
    message = None
    if previous_sha256 and previous_sha256 != new_sha256:
        state = "updated"
        message = "PDF reemplazado; checksum cambio."
    elif previous_sha256 == new_sha256:
        state = "unchanged"
        message = "PDF reemplazado; checksum sin cambios."
    return DownloadResult(
        state,
        str(source_url),
        str(pdf_path),
        bytes=len(content),
        pdf_sha256=new_sha256,
        previous_pdf_sha256=previous_sha256,
        message=message,
    )


def summarize_state(documents: list[DownloadResult]) -> str:
    states = {document.state for document in documents}
    if "failed" in states:
        return "failed"
    if "updated" in states:
        return "updated"
    if "downloaded" in states:
        return "downloaded"
    if "unchanged" in states:
        return "unchanged"
    return "skipped"


def count_states(documents: list[DownloadResult]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for document in documents:
        counts[document.state] = counts.get(document.state, 0) + 1
    return counts


def process_manifest(
    manifest_path: Path,
    timeout_seconds: int,
    overwrite: bool = False,
    fetcher: Callable[[str, int], bytes] = fetch_url_bytes,
) -> DownloadManifestResult:
    if not manifest_path.exists() or not manifest_path.is_file():
        raise SystemExit(f"[ERROR] No existe el manifest: {manifest_path}")

    documents = [
        download_one(entry, timeout_seconds=timeout_seconds, overwrite=overwrite, fetcher=fetcher)
        for entry in read_manifest_entries(manifest_path)
    ]
    return DownloadManifestResult(summarize_state(documents), str(manifest_path), count_states(documents), documents)


def write_summary_json(result: DownloadManifestResult, summary_path: Path) -> None:
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(json.dumps(asdict(result), ensure_ascii=False, indent=2), encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Descarga PDFs declarados en un manifest JSONL sin parsear, validar ni cargar a core."
    )
    parser.add_argument("--manifest", required=True, help="Manifest JSONL con source_url y pdf/pdf_path.")
    parser.add_argument("--summary-json", help="Ruta donde escribir resumen auditable JSON.")
    parser.add_argument("--overwrite", action="store_true", help="Reemplaza PDFs existentes.")
    parser.add_argument("--timeout-seconds", type=int, default=30)
    parser.add_argument("--json", action="store_true", help="Imprime el resumen como JSON.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    result = process_manifest(Path(args.manifest), args.timeout_seconds, overwrite=args.overwrite)
    if args.summary_json:
        write_summary_json(result, Path(args.summary_json))

    if args.json:
        print(json.dumps(asdict(result), ensure_ascii=False, indent=2))
    else:
        print(f"Estado descarga: {result.state}")
        print(f"Manifest: {result.manifest_path}")
        print(f"Documentos: {len(result.documents)}")
        for index, document in enumerate(result.documents, start=1):
            print(f"  {index}. {document.state} - {document.pdf}")

    if result.state == "failed":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
