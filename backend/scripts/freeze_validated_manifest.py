#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any


@dataclass
class FreezeManifestResult:
    state: str
    batch_summary_path: str
    manifest_path: str
    included_documents: int
    skipped_documents: int
    competition_scope: str


def read_json(path: Path) -> dict[str, Any]:
    if not path.exists() or not path.is_file():
        raise SystemExit(f"[ERROR] No existe el summary: {path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise SystemExit("[ERROR] El summary debe ser un objeto JSON.")
    return payload


def read_allowed_source_urls(path: Path | None) -> set[str]:
    if path is None:
        return set()
    if not path.exists() or not path.is_file():
        raise SystemExit(f"[ERROR] No existe la lista de source_url: {path}")
    urls: set[str] = set()
    for raw_line in path.read_text(encoding="utf-8-sig").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        urls.add(line)
    return urls


def build_manifest_entry(document: dict[str, Any], competition_scope: str, default_source_id: int) -> dict[str, Any]:
    input_dir = document.get("input_dir")
    if not input_dir:
        raise SystemExit("[ERROR] Un documento validated no tiene input_dir.")

    entry: dict[str, Any] = {
        "input_dir": input_dir,
        "default_source_id": default_source_id,
        "competition_scope": competition_scope,
    }
    if document.get("source_url"):
        entry["source_url"] = document["source_url"]
    return entry


def freeze_validated_manifest(
    batch_summary_path: Path,
    manifest_path: Path,
    competition_scope: str,
    default_source_id: int,
    allowed_source_urls: set[str],
    allow_all_validated: bool = False,
) -> FreezeManifestResult:
    if not competition_scope:
        raise SystemExit("[ERROR] --competition-scope es requerido.")
    if not allowed_source_urls and not allow_all_validated:
        raise SystemExit("[ERROR] Entrega --allow-source-url-file o usa --allow-all-validated de forma explicita.")

    summary = read_json(batch_summary_path)
    documents = summary.get("documents")
    if not isinstance(documents, list):
        raise SystemExit("[ERROR] El summary no contiene documents como lista.")

    entries: list[dict[str, Any]] = []
    skipped = 0
    for document in documents:
        if not isinstance(document, dict):
            skipped += 1
            continue
        if document.get("state") != "validated":
            skipped += 1
            continue
        source_url = document.get("source_url")
        if not allow_all_validated and source_url not in allowed_source_urls:
            skipped += 1
            continue
        entries.append(build_manifest_entry(document, competition_scope, default_source_id))

    if not entries:
        return FreezeManifestResult(
            "failed",
            str(batch_summary_path),
            str(manifest_path),
            included_documents=0,
            skipped_documents=skipped,
            competition_scope=competition_scope,
        )

    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    lines = [json.dumps(entry, ensure_ascii=False) for entry in entries]
    manifest_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return FreezeManifestResult(
        "frozen",
        str(batch_summary_path),
        str(manifest_path),
        included_documents=len(entries),
        skipped_documents=skipped,
        competition_scope=competition_scope,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Genera un manifest JSONL congelado solo con documentos validated y scope curado."
    )
    parser.add_argument("--batch-summary", required=True, help="Summary JSON generado por run_results_batch.py.")
    parser.add_argument("--manifest", required=True, help="Manifest JSONL congelado a escribir.")
    parser.add_argument("--competition-scope", required=True, help="Scope curado que se agregara a cada documento incluido.")
    parser.add_argument("--default-source-id", type=int, default=1)
    parser.add_argument(
        "--allow-source-url-file",
        help="Archivo UTF-8 con una source_url permitida por linea. Lineas vacias y comentarios se ignoran.",
    )
    parser.add_argument(
        "--allow-all-validated",
        action="store_true",
        help="Incluye todos los documentos validated del summary. Usar solo despues de curacion manual.",
    )
    parser.add_argument("--json", action="store_true", help="Imprime resumen como JSON.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    result = freeze_validated_manifest(
        Path(args.batch_summary),
        Path(args.manifest),
        args.competition_scope,
        args.default_source_id,
        read_allowed_source_urls(Path(args.allow_source_url_file) if args.allow_source_url_file else None),
        allow_all_validated=args.allow_all_validated,
    )

    if args.json:
        print(json.dumps(asdict(result), ensure_ascii=False, indent=2))
    else:
        print(f"Estado manifest congelado: {result.state}")
        print(f"Manifest: {result.manifest_path}")
        print(f"Documentos incluidos: {result.included_documents}")
        print(f"Documentos omitidos: {result.skipped_documents}")

    if result.state == "failed":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
