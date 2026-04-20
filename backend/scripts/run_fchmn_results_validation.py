#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any


BACKEND_DIR = Path(__file__).resolve().parents[1]
SCRAPER_SCRIPT = BACKEND_DIR / "scripts" / "scrape_fchmn.py"
DOWNLOADER_SCRIPT = BACKEND_DIR / "scripts" / "download_manifest_pdfs.py"
BATCH_SCRIPT = BACKEND_DIR / "scripts" / "run_results_batch.py"
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from natacion_chile.manifest import count_jsonl_manifest_entries


@dataclass
class FchmnResultsValidationResult:
    state: str
    run_id: str
    source_url: str
    manifest_path: str
    download_summary_path: str
    batch_summary_path: str
    discovered_documents: int
    return_codes: dict[str, int]
    download_state_counts: dict[str, int]
    batch_state_counts: dict[str, int]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Orquesta discovery -> download -> batch validation de resultados FCHMN sin cargar a core."
    )
    parser.add_argument("--url", default="https://fchmn.cl/resultados/", help="Pagina FCHMN desde donde descubrir PDFs.")
    parser.add_argument("--run-id", help="Identificador estable para nombres de manifest y summaries.")
    parser.add_argument("--limit", type=int, default=5, help="Maximo de PDFs a descubrir.")
    parser.add_argument("--crawl-pages", type=int, default=1, help="Cantidad maxima de paginas WordPress a recorrer desde --url.")
    parser.add_argument("--year", type=int, help="Año para agrupar PDFs/CSVs; si falta, se infiere desde cada URL.")
    parser.add_argument("--competition-id", type=int)
    parser.add_argument("--default-source-id", type=int, default=1)
    parser.add_argument("--manifest-dir", default=str(BACKEND_DIR / "data" / "raw" / "manifests"))
    parser.add_argument("--summary-dir", default=str(BACKEND_DIR / "data" / "raw" / "batch_summaries"))
    parser.add_argument("--pdf-dir", default=str(BACKEND_DIR / "data" / "raw" / "results_pdf" / "fchmn_auto"))
    parser.add_argument("--out-dir-root", default=str(BACKEND_DIR / "data" / "raw" / "results_csv" / "fchmn_auto"))
    parser.add_argument("--overwrite-download", action="store_true", help="Pasa --overwrite al downloader.")
    parser.add_argument("--timeout-seconds", type=int, default=30)
    parser.add_argument("--json", action="store_true", help="Imprime resumen como JSON.")
    return parser


def parse_args_from_list(argv: list[str]) -> argparse.Namespace:
    return build_parser().parse_args(argv)


def parse_args() -> argparse.Namespace:
    parser = build_parser()
    return parser.parse_args()


def default_run_id() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def build_output_paths(args: argparse.Namespace) -> tuple[str, Path, Path, Path]:
    run_id = args.run_id or default_run_id()
    manifest_path = Path(args.manifest_dir) / f"fchmn_results_validation_{run_id}.jsonl"
    download_summary_path = Path(args.summary_dir) / f"fchmn_results_validation_{run_id}_download.json"
    batch_summary_path = Path(args.summary_dir) / f"fchmn_results_validation_{run_id}_batch.json"
    return run_id, manifest_path, download_summary_path, batch_summary_path


def build_scrape_command(args: argparse.Namespace, manifest_path: Path) -> list[str]:
    command = [
        sys.executable,
        str(SCRAPER_SCRIPT),
        "--url",
        args.url,
        "--manifest",
        str(manifest_path),
        "--pdf-dir",
        args.pdf_dir,
        "--out-dir-root",
        args.out_dir_root,
        "--limit",
        str(args.limit),
        "--crawl-pages",
        str(args.crawl_pages),
        "--default-source-id",
        str(args.default_source_id),
        "--json",
    ]
    if args.year is not None:
        command.extend(["--year", str(args.year)])
    if args.competition_id is not None:
        command.extend(["--competition-id", str(args.competition_id)])
    return command


def build_download_command(args: argparse.Namespace, manifest_path: Path, summary_path: Path) -> list[str]:
    command = [
        sys.executable,
        str(DOWNLOADER_SCRIPT),
        "--manifest",
        str(manifest_path),
        "--summary-json",
        str(summary_path),
        "--timeout-seconds",
        str(args.timeout_seconds),
        "--json",
    ]
    if args.overwrite_download:
        command.append("--overwrite")
    return command


def build_batch_command(manifest_path: Path, summary_path: Path) -> list[str]:
    return [
        sys.executable,
        str(BATCH_SCRIPT),
        "--manifest",
        str(manifest_path),
        "--summary-json",
        str(summary_path),
        "--json",
    ]


def read_json_if_exists(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def count_manifest_entries(manifest_path: Path) -> int:
    return count_jsonl_manifest_entries(manifest_path)


def run_results_validation(args: argparse.Namespace) -> FchmnResultsValidationResult:
    run_id, manifest_path, download_summary_path, batch_summary_path = build_output_paths(args)
    return_codes: dict[str, int] = {}
    discovered_documents = 0

    scrape_result = subprocess.run(build_scrape_command(args, manifest_path), check=False)
    return_codes["scrape"] = scrape_result.returncode
    if scrape_result.returncode != 0:
        return FchmnResultsValidationResult(
            "failed",
            run_id,
            args.url,
            str(manifest_path),
            str(download_summary_path),
            str(batch_summary_path),
            discovered_documents,
            return_codes,
            {},
            {},
        )
    discovered_documents = count_manifest_entries(manifest_path)
    if discovered_documents == 0:
        return FchmnResultsValidationResult(
            "failed",
            run_id,
            args.url,
            str(manifest_path),
            str(download_summary_path),
            str(batch_summary_path),
            discovered_documents,
            return_codes,
            {},
            {},
        )

    download_result = subprocess.run(build_download_command(args, manifest_path, download_summary_path), check=False)
    return_codes["download"] = download_result.returncode
    download_summary = read_json_if_exists(download_summary_path)
    if download_result.returncode != 0:
        return FchmnResultsValidationResult(
            download_summary.get("state", "failed"),
            run_id,
            args.url,
            str(manifest_path),
            str(download_summary_path),
            str(batch_summary_path),
            discovered_documents,
            return_codes,
            download_summary.get("state_counts", {}),
            {},
        )

    batch_result = subprocess.run(build_batch_command(manifest_path, batch_summary_path), check=False)
    return_codes["batch"] = batch_result.returncode
    batch_summary = read_json_if_exists(batch_summary_path)
    return FchmnResultsValidationResult(
        batch_summary.get("state", "failed" if batch_result.returncode else "validated"),
        run_id,
        args.url,
        str(manifest_path),
        str(download_summary_path),
        str(batch_summary_path),
        discovered_documents,
        return_codes,
        download_summary.get("state_counts", {}),
        batch_summary.get("state_counts", {}),
    )


def main() -> None:
    args = parse_args()
    result = run_results_validation(args)
    if args.json:
        print(json.dumps(asdict(result), ensure_ascii=False, indent=2))
    else:
        print(f"Estado validacion FCHMN: {result.state}")
        print(f"Run id: {result.run_id}")
        print(f"Manifest: {result.manifest_path}")
        print(f"Download summary: {result.download_summary_path}")
        print(f"Batch summary: {result.batch_summary_path}")
        print(f"Documentos descubiertos: {result.discovered_documents}")
        print(f"Estados descarga: {result.download_state_counts}")
        print(f"Estados batch: {result.batch_state_counts}")
    if result.state != "validated":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
