#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import subprocess
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))


REQUIRED_PARSER_OUTPUTS = {
    "club": ["name", "short_name", "city", "region", "source_id"],
    "event": ["competition_id", "event_name", "stroke", "distance_m", "gender", "age_group", "round_type", "source_id"],
    "athlete": ["full_name", "gender", "club_name", "birth_year", "source_id"],
    "result": ["event_name", "athlete_name", "club_name", "rank_position", "seed_time_text", "seed_time_ms", "result_time_text", "result_time_ms", "age_at_event", "birth_year_estimated", "points", "status", "source_id"],
}

OPTIONAL_RELAY_OUTPUTS = {
    "relay_team": ["event_name", "relay_team_name", "rank_position", "seed_time_text", "seed_time_ms", "result_time_text", "result_time_ms", "points", "status", "source_id", "page_number", "line_number"],
    "relay_swimmer": ["event_name", "relay_team_name", "leg_order", "swimmer_name", "gender", "age_at_event", "birth_year_estimated", "page_number", "line_number"],
}

EVENT_GENDERS = {"women", "men", "mixed"}
ATHLETE_GENDERS = {"female", "male"}
STROKES = {
    "freestyle",
    "backstroke",
    "breaststroke",
    "butterfly",
    "individual_medley",
    "medley_relay",
    "freestyle_relay",
}
STATUSES = {"valid", "dns", "dnf", "dsq", "scratch", "unknown"}

DEFAULT_DEBUG_THRESHOLD = 0.20
PARSER_SCRIPT = BACKEND_DIR / "scripts" / "parse_results_pdf.py"
PIPELINE_SCRIPT = BACKEND_DIR / "scripts" / "run_pipeline_results.py"
PROJECT_DIR = BACKEND_DIR.parent


@dataclass
class BatchIssue:
    severity: str
    issue_key: str
    message: str
    count: int = 1


@dataclass
class BatchValidationResult:
    state: str
    input_dir: str
    source_url: str | None
    counts: dict[str, int]
    issues: list[BatchIssue]
    metadata: dict[str, Any]
    commands: dict[str, list[str] | None]


@dataclass
class BatchManifestResult:
    state: str
    manifest_path: str
    documents: list[BatchValidationResult]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Ejecuta parseo opcional, valida salidas y carga a core solo con --load."
    )
    input_group = parser.add_mutually_exclusive_group(required=True)
    input_group.add_argument("--input-dir", help="Carpeta generada por parse_results_pdf.py")
    input_group.add_argument("--pdf", help="PDF de resultados a parsear antes de validar")
    input_group.add_argument("--manifest", help="Manifest JSONL con documentos a procesar uno a uno")
    parser.add_argument("--out-dir", help="Carpeta de salida requerida cuando se usa --pdf")
    parser.add_argument("--competition-id", type=int, help="competition_id que se pasara al parser")
    parser.add_argument("--source-url", help="URL original del documento para trazabilidad cuando exista.")
    parser.add_argument("--default-source-id", type=int, default=1, help="source_id por defecto que se pasara al parser")
    parser.add_argument("--excel-name", default="parsed_results.xlsx", help="Nombre del Excel consolidado que generara el parser")
    parser.add_argument("--load", action="store_true", help="Ejecuta run_pipeline_results.py solo si el batch queda validated.")
    parser.add_argument("--host", type=str, default="localhost", help="Host PostgreSQL para --load.")
    parser.add_argument("--port", type=int, default=5432, help="Puerto PostgreSQL para --load.")
    parser.add_argument("--dbname", type=str, default="natacion_chile", help="Base PostgreSQL para --load.")
    parser.add_argument("--user", type=str, help="Usuario PostgreSQL requerido para --load.")
    parser.add_argument("--password", type=str, help="Password PostgreSQL requerido para --load.")
    parser.add_argument("--schema", type=str, default="core", help="Schema PostgreSQL para --load.")
    parser.add_argument("--truncate-staging", action="store_true", help="Trunca staging durante --load.")
    parser.add_argument(
        "--debug-threshold",
        type=float,
        default=DEFAULT_DEBUG_THRESHOLD,
        help="Umbral maximo de debug_unparsed_lines sobre filas parseadas antes de requerir revision.",
    )
    parser.add_argument("--json", action="store_true", help="Imprime el resumen como JSON.")
    parser.add_argument("--summary-json", help="Ruta donde escribir un resumen auditable JSON de la corrida.")
    args = parser.parse_args()
    if args.pdf and not args.out_dir:
        parser.error("--out-dir es requerido cuando se usa --pdf")
    if args.load and (not args.user or not args.password):
        parser.error("--user y --password son requeridos cuando se usa --load")
    return args


def build_parse_command(args: argparse.Namespace) -> list[str]:
    command = [
        sys.executable,
        str(PARSER_SCRIPT),
        "--pdf",
        str(Path(args.pdf)),
        "--out-dir",
        str(Path(args.out_dir)),
        "--default-source-id",
        str(args.default_source_id),
        "--excel-name",
        args.excel_name,
    ]
    if args.competition_id is not None:
        command.extend(["--competition-id", str(args.competition_id)])
    return command


def run_parser(args: argparse.Namespace) -> Path:
    pdf_path = Path(args.pdf)
    out_dir = Path(args.out_dir)
    if not pdf_path.exists() or not pdf_path.is_file():
        raise SystemExit(f"[ERROR] No existe el PDF: {pdf_path}")

    command = build_parse_command(args)
    subprocess.run(command, check=True)
    return out_dir


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


def build_manifest_item_args(base_args: argparse.Namespace, entry: dict[str, Any]) -> argparse.Namespace:
    item = argparse.Namespace(**vars(base_args))
    item.manifest = None
    item.input_dir = resolve_manifest_path(entry.get("input_dir"))
    item.pdf = resolve_manifest_path(entry.get("pdf") or entry.get("pdf_path"))
    item.out_dir = resolve_manifest_path(entry.get("out_dir"))
    item.competition_id = entry.get("competition_id", base_args.competition_id)
    item.source_url = entry.get("source_url", getattr(base_args, "source_url", None))
    item.default_source_id = entry.get("default_source_id", base_args.default_source_id)
    item.excel_name = entry.get("excel_name", base_args.excel_name)

    has_input_dir = bool(item.input_dir)
    has_pdf = bool(item.pdf)
    if has_input_dir == has_pdf:
        raise SystemExit("[ERROR] Cada entrada del manifest debe tener exactamente uno de input_dir o pdf.")
    if has_pdf and not item.out_dir:
        raise SystemExit("[ERROR] Cada entrada con pdf debe incluir out_dir.")
    return item


def resolve_manifest_path(value: Any) -> str | None:
    if not value:
        return None
    path = Path(str(value))
    if path.is_absolute():
        return str(path)
    return str(PROJECT_DIR / path)


def build_load_command(args: argparse.Namespace, input_dir: Path) -> list[str]:
    command = [
        sys.executable,
        str(PIPELINE_SCRIPT),
        "--input-dir",
        str(input_dir),
        "--host",
        args.host,
        "--port",
        str(args.port),
        "--dbname",
        args.dbname,
        "--user",
        args.user,
        "--password",
        args.password,
        "--schema",
        args.schema,
        "--default-source-id",
        str(args.default_source_id),
    ]
    if args.competition_id is not None:
        command.extend(["--competition-id", str(args.competition_id)])
    if getattr(args, "source_url", None):
        command.extend(["--competition-source-url", str(args.source_url)])
    if args.truncate_staging:
        command.append("--truncate-staging")
    return command


def redact_command(command: list[str]) -> list[str]:
    redacted = list(command)
    for index, token in enumerate(redacted[:-1]):
        if token == "--password":
            redacted[index + 1] = "***"
    return redacted


def run_pipeline(args: argparse.Namespace, input_dir: Path) -> None:
    command = build_load_command(args, input_dir)
    subprocess.run(command, check=True)


def read_csv_rows(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        return list(reader.fieldnames or []), list(reader)


def read_metadata(input_dir: Path, issues: list[BatchIssue]) -> dict[str, Any]:
    metadata_path = input_dir / "metadata.json"
    if not metadata_path.exists():
        issues.append(BatchIssue("error", "missing_metadata", "Falta metadata.json."))
        return {}
    try:
        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        issues.append(BatchIssue("error", "invalid_metadata_json", f"metadata.json no es JSON valido: {exc}."))
        return {}

    if metadata.get("pdf_name") and not metadata.get("pdf_sha256"):
        issues.append(BatchIssue("error", "missing_pdf_sha256", "metadata.json tiene pdf_name pero no pdf_sha256."))
    if metadata.get("pdf_sha256") and not isinstance(metadata["pdf_sha256"], str):
        issues.append(BatchIssue("error", "invalid_pdf_sha256", "pdf_sha256 debe ser texto hexadecimal."))
    if isinstance(metadata.get("pdf_sha256"), str) and len(metadata["pdf_sha256"]) != 64:
        issues.append(BatchIssue("error", "invalid_pdf_sha256", "pdf_sha256 debe tener 64 caracteres."))
    if metadata.get("pdf_name") and not metadata.get("parser_version"):
        issues.append(BatchIssue("error", "missing_parser_version", "metadata.json tiene pdf_name pero no parser_version."))
    return metadata


def add_missing_file_issues(input_dir: Path, issues: list[BatchIssue]) -> None:
    for key in REQUIRED_PARSER_OUTPUTS:
        if not (input_dir / f"{key}.csv").exists():
            issues.append(BatchIssue("error", f"missing_{key}_csv", f"Falta {key}.csv."))

    relay_team_exists = (input_dir / "relay_team.csv").exists()
    relay_swimmer_exists = (input_dir / "relay_swimmer.csv").exists()
    if relay_team_exists != relay_swimmer_exists:
        issues.append(
            BatchIssue(
                "error",
                "incomplete_relay_outputs",
                "Los relevos requieren relay_team.csv y relay_swimmer.csv juntos.",
            )
        )


def validate_columns(key: str, actual: list[str], expected: list[str], issues: list[BatchIssue]) -> None:
    missing = [col for col in expected if col not in actual]
    if missing:
        issues.append(BatchIssue("error", f"missing_{key}_columns", f"Faltan columnas en {key}.csv: {missing}.", len(missing)))


def count_invalid_values(rows: list[dict[str, str]], column: str, allowed: set[str]) -> int:
    invalid = 0
    for row in rows:
        value = (row.get(column) or "").strip()
        if value and value not in allowed:
            invalid += 1
    return invalid


def validate_canons(data: dict[str, list[dict[str, str]]], issues: list[BatchIssue]) -> None:
    checks = [
        ("event", "gender", EVENT_GENDERS),
        ("event", "stroke", STROKES),
        ("athlete", "gender", ATHLETE_GENDERS),
        ("result", "status", STATUSES),
        ("relay_team", "status", STATUSES),
        ("relay_swimmer", "gender", ATHLETE_GENDERS),
    ]
    for key, column, allowed in checks:
        rows = data.get(key, [])
        if not rows:
            continue
        invalid_count = count_invalid_values(rows, column, allowed)
        if invalid_count:
            issues.append(
                BatchIssue(
                    "error",
                    f"invalid_{key}_{column}",
                    f"{key}.csv tiene valores fuera de canon en {column}.",
                    invalid_count,
                )
            )


def validate_required_identities(data: dict[str, list[dict[str, str]]], issues: list[BatchIssue]) -> None:
    result_missing = sum(
        1
        for row in data.get("result", [])
        if not (row.get("event_name") or "").strip() or not (row.get("athlete_name") or "").strip()
    )
    if result_missing:
        issues.append(BatchIssue("error", "result_missing_identity", "Hay resultados sin event_name o athlete_name.", result_missing))

    relay_missing = sum(
        1
        for row in data.get("relay_team", [])
        if not (row.get("event_name") or "").strip() or not (row.get("relay_team_name") or "").strip()
    )
    if relay_missing:
        issues.append(BatchIssue("error", "relay_missing_identity", "Hay relevos sin event_name o relay_team_name.", relay_missing))


def validate_debug_ratio(input_dir: Path, parsed_rows: int, threshold: float, counts: dict[str, int], issues: list[BatchIssue]) -> None:
    debug_path = input_dir / "debug_unparsed_lines.csv"
    if not debug_path.exists():
        issues.append(BatchIssue("warning", "missing_debug_unparsed_lines", "Falta debug_unparsed_lines.csv."))
        return

    _, debug_rows = read_csv_rows(debug_path)
    counts["debug_unparsed_lines"] = len(debug_rows)
    if parsed_rows <= 0:
        return
    ratio = len(debug_rows) / parsed_rows
    if ratio > threshold:
        issues.append(
            BatchIssue(
                "error",
                "debug_unparsed_ratio_exceeded",
                f"debug_unparsed_lines.csv supera el umbral: {ratio:.3f} > {threshold:.3f}.",
                len(debug_rows),
            )
        )


def validate_input_dir(input_dir: Path, debug_threshold: float = DEFAULT_DEBUG_THRESHOLD, source_url: str | None = None) -> BatchValidationResult:
    issues: list[BatchIssue] = []
    counts: dict[str, int] = {}
    data: dict[str, list[dict[str, str]]] = {}

    if not input_dir.exists() or not input_dir.is_dir():
        return BatchValidationResult(
            state="failed",
            input_dir=str(input_dir),
            source_url=source_url,
            counts=counts,
            issues=[BatchIssue("error", "input_dir_not_found", f"No existe la carpeta: {input_dir}.")],
            metadata={},
            commands={},
        )

    metadata = read_metadata(input_dir, issues)
    add_missing_file_issues(input_dir, issues)

    for key, expected_columns in {**REQUIRED_PARSER_OUTPUTS, **OPTIONAL_RELAY_OUTPUTS}.items():
        path = input_dir / f"{key}.csv"
        if not path.exists():
            continue
        columns, rows = read_csv_rows(path)
        validate_columns(key, columns, expected_columns, issues)
        data[key] = rows
        counts[key] = len(rows)

    if counts.get("event", 0) == 0:
        issues.append(BatchIssue("error", "no_events_found", "El parser no encontro eventos."))

    parsed_result_rows = counts.get("result", 0) + counts.get("relay_team", 0)
    if parsed_result_rows == 0:
        issues.append(BatchIssue("error", "no_results_found", "El parser no encontro resultados individuales ni relevos."))

    validate_canons(data, issues)
    validate_required_identities(data, issues)
    validate_debug_ratio(input_dir, parsed_result_rows, debug_threshold, counts, issues)

    state = "requires_review" if any(issue.severity == "error" for issue in issues) else "validated"
    return BatchValidationResult(state=state, input_dir=str(input_dir), source_url=source_url, counts=counts, issues=issues, metadata=metadata, commands={})


def print_text_summary(result: BatchValidationResult) -> None:
    print(f"Estado batch: {result.state}")
    print(f"Input dir: {result.input_dir}")
    if result.source_url:
        print(f"Source URL: {result.source_url}")
    print("Conteos:")
    for key in sorted(result.counts):
        print(f"  {key}: {result.counts[key]}")
    if not result.issues:
        print("Issues: ninguno")
        return
    print("Issues:")
    for issue in result.issues:
        print(f"  [{issue.severity}] {issue.issue_key}: {issue.message} ({issue.count})")


def write_summary_json(result: BatchValidationResult, summary_path: Path) -> None:
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(json.dumps(asdict(result), ensure_ascii=False, indent=2), encoding="utf-8")


def write_manifest_summary_json(result: BatchManifestResult, summary_path: Path) -> None:
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(json.dumps(asdict(result), ensure_ascii=False, indent=2), encoding="utf-8")


def process_one(args: argparse.Namespace) -> BatchValidationResult:
    parse_command = build_parse_command(args) if args.pdf else None
    input_dir = run_parser(args) if args.pdf else Path(args.input_dir)
    result = validate_input_dir(input_dir, args.debug_threshold, getattr(args, "source_url", None))
    result.commands["parse"] = parse_command
    result.commands["load"] = redact_command(build_load_command(args, input_dir)) if args.load else None
    if args.load and result.state == "validated":
        run_pipeline(args, input_dir)
        result.state = "loaded"
    return result


def summarize_manifest_state(documents: list[BatchValidationResult], load_enabled: bool) -> str:
    states = {document.state for document in documents}
    if "failed" in states:
        return "failed"
    if "requires_review" in states:
        return "requires_review"
    if load_enabled and states == {"loaded"}:
        return "loaded"
    return "validated"


def process_manifest(args: argparse.Namespace) -> BatchManifestResult:
    manifest_path = Path(args.manifest)
    if not manifest_path.exists() or not manifest_path.is_file():
        raise SystemExit(f"[ERROR] No existe el manifest: {manifest_path}")

    documents: list[BatchValidationResult] = []
    for entry in read_manifest_entries(manifest_path):
        item_args = build_manifest_item_args(args, entry)
        documents.append(process_one(item_args))

    state = summarize_manifest_state(documents, args.load)
    return BatchManifestResult(state=state, manifest_path=str(manifest_path), documents=documents)


def print_manifest_summary(result: BatchManifestResult) -> None:
    print(f"Estado manifest: {result.state}")
    print(f"Manifest: {result.manifest_path}")
    print(f"Documentos: {len(result.documents)}")
    for index, document in enumerate(result.documents, start=1):
        print(f"  {index}. {document.state} - {document.input_dir}")


def main() -> None:
    args = parse_args()
    if args.manifest:
        manifest_result = process_manifest(args)
        if args.summary_json:
            write_manifest_summary_json(manifest_result, Path(args.summary_json))
        if args.json:
            print(json.dumps(asdict(manifest_result), ensure_ascii=False, indent=2))
        else:
            print_manifest_summary(manifest_result)
        if manifest_result.state in {"failed", "requires_review"}:
            raise SystemExit(1)
        return

    result = process_one(args)
    if args.summary_json:
        write_summary_json(result, Path(args.summary_json))
    if args.json:
        payload = asdict(result)
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print_text_summary(result)

    if result.state in {"failed", "requires_review"}:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
