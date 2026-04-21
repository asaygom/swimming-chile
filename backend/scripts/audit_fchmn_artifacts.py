#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any


BACKEND_DIR = Path(__file__).resolve().parents[1]
REQUIRED_CSV_FILES = {
    "club.csv",
    "event.csv",
    "athlete.csv",
    "result.csv",
    "relay_team.csv",
    "relay_swimmer.csv",
}


@dataclass
class AuditDocument:
    source_url: str
    name: str
    category: str
    pdf_present: bool
    parse_present: bool
    latest_download_state: str | None = None
    latest_download_summary: str | None = None
    latest_validation_state: str | None = None
    latest_validation_summary: str | None = None
    competition_name: str | None = None
    issues: list[str] = field(default_factory=list)


@dataclass
class AuditResult:
    state: str
    total_documents: int
    category_counts: dict[str, int]
    focus_manifest_path: str | None
    documents: list[AuditDocument]


def project_path(path: str | Path, project_root: Path) -> Path:
    candidate = Path(path)
    if candidate.is_absolute():
        return candidate
    return project_root / candidate


def normalized_path(path: str | Path | None, project_root: Path) -> str | None:
    if not path:
        return None
    return str(project_path(path, project_root).resolve()).casefold()


def document_name(source_url: str | None, fallback: str) -> str:
    if source_url:
        return Path(source_url.split("?", 1)[0]).name
    return Path(fallback).name


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8-sig") as handle:
        for line_number, raw_line in enumerate(handle, start=1):
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            try:
                entry = json.loads(line)
            except json.JSONDecodeError as exc:
                raise SystemExit(f"[ERROR] JSONL invalido en {path}:{line_number}: {exc}") from exc
            if not isinstance(entry, dict):
                raise SystemExit(f"[ERROR] Entrada JSONL debe ser objeto en {path}:{line_number}")
            entries.append(entry)
    return entries


def read_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8-sig"))
    if not isinstance(payload, dict):
        raise SystemExit(f"[ERROR] Summary JSON debe ser objeto: {path}")
    return payload


def entry_key(entry: dict[str, Any], project_root: Path) -> str | None:
    if entry.get("source_url"):
        return str(entry["source_url"])
    return (
        normalized_path(entry.get("pdf") or entry.get("pdf_path"), project_root)
        or normalized_path(entry.get("input_dir") or entry.get("out_dir"), project_root)
    )


def is_complete_parse_dir(path: Path) -> bool:
    if not path.exists() or not path.is_dir():
        return False
    files = {child.name for child in path.iterdir() if child.is_file()}
    return REQUIRED_CSV_FILES <= files


def issue_messages(document: dict[str, Any]) -> list[str]:
    messages: list[str] = []
    for issue in document.get("issues") or []:
        if isinstance(issue, dict):
            messages.append(str(issue.get("message") or issue.get("code") or issue))
        else:
            messages.append(str(issue))
    return messages


def summary_kind(path: Path) -> str:
    name = path.name
    if (
        name.endswith("_batch.json")
        or name.endswith("_load.json")
        or name.startswith("regression")
        or "recheck" in name
        or "probe" in name
    ):
        return "batch"
    return "download"


def is_non_local_hint(record: dict[str, Any]) -> bool:
    text = " ".join(
        [
            record.get("source_url") or "",
            *record.get("names", set()),
            *record.get("competition_names", set()),
        ]
    ).casefold()
    return any(
        hint in text
        for hint in [
            "sudamericano",
            "sudamericana",
            "recife",
            "argentina",
            "panamericano",
            "aguas abiertas",
        ]
    )


def is_local_hint(record: dict[str, Any]) -> bool:
    if is_non_local_hint(record):
        return False
    if "fchmn_home_validated_for_load_20260419.jsonl" in record.get("manifests", set()):
        return True
    text = " ".join(
        [
            record.get("source_url") or "",
            *record.get("names", set()),
            *record.get("competition_names", set()),
        ]
    ).casefold()
    return any(hint in text for hint in ["fchmn", "copa", "torneo", "nacional", "master", "coppa"])


def classify_record(record: dict[str, Any], project_root: Path) -> str:
    pdf_present = any(
        normalized and project_path(normalized, project_root).exists()
        for normalized in record.get("pdf_paths_raw", set())
    )
    parse_present = any(is_complete_parse_dir(project_path(raw, project_root)) for raw in record.get("dir_paths_raw", set()))
    latest_validation = record.get("latest_validation") or {}
    validation_state = latest_validation.get("state")

    if not pdf_present:
        return "missing_download"
    if validation_state == "failed":
        return "failed"
    if not parse_present:
        return "missing_parse"
    if not validation_state:
        return "missing_validation"
    if validation_state == "requires_review":
        return "requires_review"
    if validation_state in {"validated", "loaded"}:
        if is_non_local_hint(record):
            return "validated_non_local_candidate"
        if is_local_hint(record):
            return "validated_local_candidate"
        return "validated_local_candidate"
    return "missing_validation"


def build_audit(
    manifest_dir: Path,
    summary_dir: Path,
    project_root: Path,
    focus_manifest: Path | None = None,
) -> AuditResult:
    records: dict[str, dict[str, Any]] = defaultdict(
        lambda: {
            "source_url": "",
            "pdf_paths_raw": set(),
            "dir_paths_raw": set(),
            "manifests": set(),
            "names": set(),
            "competition_names": set(),
            "latest_download": None,
            "latest_validation": None,
        }
    )
    name_to_source_url: dict[str, str] = {}
    dir_to_source_url: dict[str, str] = {}

    manifest_paths = sorted(manifest_dir.glob("*.jsonl"))
    for manifest_path in manifest_paths:
        for entry in read_jsonl(manifest_path):
            key = entry_key(entry, project_root)
            if not key:
                continue
            record = records[key]
            source_url = entry.get("source_url") or ""
            record["source_url"] = record["source_url"] or source_url
            record["manifests"].add(manifest_path.name)
            for field_name in ("pdf", "pdf_path"):
                if entry.get(field_name):
                    record["pdf_paths_raw"].add(str(project_path(entry[field_name], project_root)))
            for field_name in ("out_dir", "input_dir"):
                if entry.get(field_name):
                    raw_path = str(project_path(entry[field_name], project_root))
                    record["dir_paths_raw"].add(raw_path)
                    if source_url:
                        dir_to_source_url[normalized_path(raw_path, project_root) or raw_path.casefold()] = source_url
            if source_url:
                record["names"].add(document_name(source_url, key))
                name_to_source_url[document_name(source_url, key).casefold()] = source_url
                for raw_pdf in record["pdf_paths_raw"]:
                    name_to_source_url[Path(raw_pdf).name.casefold()] = source_url

    summary_paths = sorted(summary_dir.glob("*.json"), key=lambda path: (path.stat().st_mtime, path.name))
    for order, summary_path in enumerate(summary_paths, start=1):
        summary = read_json(summary_path)
        kind = summary_kind(summary_path)
        for document in summary.get("documents") or []:
            if not isinstance(document, dict):
                continue
            metadata = document.get("metadata") or {}
            inferred_source_url = document.get("source_url") or ""
            input_dir_key = normalized_path(document.get("input_dir"), project_root)
            pdf_name = Path(str(document.get("pdf") or "")).name.casefold()
            metadata_pdf_name = str(metadata.get("pdf_name") or "").casefold()
            if not inferred_source_url and input_dir_key in dir_to_source_url:
                inferred_source_url = dir_to_source_url[input_dir_key]
            if not inferred_source_url and pdf_name in name_to_source_url:
                inferred_source_url = name_to_source_url[pdf_name]
            if not inferred_source_url and metadata_pdf_name in name_to_source_url:
                inferred_source_url = name_to_source_url[metadata_pdf_name]

            key = inferred_source_url or entry_key(document, project_root)
            if not key:
                continue
            record = records[key]
            record["source_url"] = record["source_url"] or inferred_source_url
            for field_name in ("pdf", "pdf_path"):
                if document.get(field_name):
                    record["pdf_paths_raw"].add(str(project_path(document[field_name], project_root)))
            for field_name in ("out_dir", "input_dir"):
                if document.get(field_name):
                    record["dir_paths_raw"].add(str(project_path(document[field_name], project_root)))
            if metadata.get("pdf_name"):
                record["names"].add(str(metadata["pdf_name"]))
            elif record["source_url"]:
                record["names"].add(document_name(record["source_url"], key))
            if metadata.get("competition_name"):
                record["competition_names"].add(str(metadata["competition_name"]))

            event = {
                "order": order,
                "summary": summary_path.name,
                "state": document.get("state"),
                "issues": issue_messages(document),
            }
            if kind == "download":
                record["latest_download"] = event
            else:
                record["latest_validation"] = event

    focus_urls: set[str] | None = None
    if focus_manifest is not None:
        focus_urls = {entry["source_url"] for entry in read_jsonl(focus_manifest) if entry.get("source_url")}

    documents: list[AuditDocument] = []
    for key, record in records.items():
        source_url = record["source_url"]
        if focus_urls is not None and source_url not in focus_urls:
            continue
        category = classify_record(record, project_root)
        latest_download = record.get("latest_download") or {}
        latest_validation = record.get("latest_validation") or {}
        pdf_present = category != "missing_download"
        parse_present = any(is_complete_parse_dir(project_path(raw, project_root)) for raw in record.get("dir_paths_raw", set()))
        documents.append(
            AuditDocument(
                source_url=source_url,
                name=next(iter(sorted(record["names"])), None) or document_name(source_url, key),
                category=category,
                pdf_present=pdf_present,
                parse_present=parse_present,
                latest_download_state=latest_download.get("state"),
                latest_download_summary=latest_download.get("summary"),
                latest_validation_state=latest_validation.get("state"),
                latest_validation_summary=latest_validation.get("summary"),
                competition_name=next(iter(sorted(record["competition_names"])), None),
                issues=latest_validation.get("issues", []),
            )
        )

    documents.sort(key=lambda document: (document.category, document.source_url or document.name))
    counts = Counter(document.category for document in documents)
    return AuditResult(
        state="audited",
        total_documents=len(documents),
        category_counts=dict(sorted(counts.items())),
        focus_manifest_path=str(focus_manifest) if focus_manifest else None,
        documents=documents,
    )


def print_table(result: AuditResult) -> None:
    print(f"Estado: {result.state}")
    print(f"Documentos: {result.total_documents}")
    print("Brechas:")
    for category, count in result.category_counts.items():
        print(f"- {category}: {count}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Audita manifests, summaries, PDFs y CSVs FCHMN sin descargar, parsear ni cargar a core."
    )
    parser.add_argument("--manifest-dir", default=str(BACKEND_DIR / "data" / "raw" / "manifests"))
    parser.add_argument("--summary-dir", default=str(BACKEND_DIR / "data" / "raw" / "batch_summaries"))
    parser.add_argument("--project-root", default=str(BACKEND_DIR.parent))
    parser.add_argument("--focus-manifest", help="Limita la auditoria a los source_url de un manifest JSONL.")
    parser.add_argument("--summary-json", help="Escribe el resultado completo como JSON.")
    parser.add_argument("--json", action="store_true", help="Imprime el resultado completo como JSON.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    project_root = Path(args.project_root)
    result = build_audit(
        Path(args.manifest_dir),
        Path(args.summary_dir),
        project_root,
        Path(args.focus_manifest) if args.focus_manifest else None,
    )
    payload = asdict(result)

    if args.summary_json:
        summary_path = Path(args.summary_json)
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        summary_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print_table(result)


if __name__ == "__main__":
    main()
