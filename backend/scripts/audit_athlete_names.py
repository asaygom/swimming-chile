"""Audit suspicious athlete names in parsed result CSVs.

This script is intentionally read-only: it consumes parsed CSV folders from a
manifest and writes review artifacts. It does not parse PDFs and does not load
data to core.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
from collections import Counter, defaultdict
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import pandas as pd
from pandas.errors import EmptyDataError

from run_pipeline_results import clean_extracted_text, normalize_match_text


PROJECT_ROOT = Path(__file__).resolve().parents[2]
STATUS_TOKENS = {"DQ", "DNS", "DNF", "SCR", "NS", "NT"}
CLUB_WORDS = {
    "club",
    "natacion",
    "swim",
    "team",
    "master",
    "masters",
    "deportivo",
    "deportiva",
    "universidad",
}
PDF_NOISE_WORDS = {"page", "pagina", "evento", "event", "resultados", "results", "finals"}
TIME_RE = re.compile(r"\b\d{1,2}[:.,]\d{2}(?:[:.,]\d{2})?\b")
SINGLE_LETTER_RUN_RE = re.compile(r"(?:^|\s)(?:[A-Za-zÁÉÍÓÚÜÑáéíóúüñ]\s+){3,}[A-Za-zÁÉÍÓÚÜÑáéíóúüñ](?:\s|$)")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Audit suspicious athlete names from parsed manifest CSV folders."
    )
    parser.add_argument("--manifest", required=True, help="Manifest JSONL with parsed input_dir entries.")
    parser.add_argument("--summary-json", required=True, help="Output JSON summary path.")
    parser.add_argument("--review-csv", required=True, help="Output CSV with suspicious athlete names.")
    parser.add_argument(
        "--override-input-dir",
        action="append",
        default=[],
        metavar="SOURCE_URL=INPUT_DIR",
        help="Override one manifest input_dir for a source_url, useful for scratch parser outputs.",
    )
    parser.add_argument("--json", action="store_true", help="Print JSON summary to stdout.")
    return parser.parse_args()


def resolve_path(value: str) -> Path:
    path = Path(value)
    if path.is_absolute():
        return path
    return PROJECT_ROOT / path


def load_manifest(path: Path) -> List[dict]:
    documents: List[dict] = []
    with path.open("r", encoding="utf-8-sig") as fh:
        for line in fh:
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                continue
            documents.append(json.loads(stripped))
    return documents


def load_overrides(values: Sequence[str]) -> Dict[str, Path]:
    overrides: Dict[str, Path] = {}
    for value in values:
        if "=" not in value:
            raise ValueError(f"override-input-dir debe usar SOURCE_URL=INPUT_DIR: {value}")
        source_url, input_dir = value.split("=", 1)
        source_url = (clean_extracted_text(source_url) or "").strip()
        if not source_url:
            raise ValueError(f"override-input-dir sin SOURCE_URL: {value}")
        overrides[source_url] = resolve_path(input_dir)
    return overrides


def classify_athlete_name(name: Optional[str]) -> List[str]:
    cleaned = clean_extracted_text(name)
    if not cleaned:
        return ["empty_name"]

    reasons: List[str] = []
    normalized = normalize_match_text(cleaned) or ""
    tokens = normalized.split()

    if re.search(r"\d", cleaned):
        reasons.append("contains_digit")
    if TIME_RE.search(cleaned):
        reasons.append("contains_time_token")
    if any(token.upper() in STATUS_TOKENS for token in re.findall(r"[A-Za-z]+", cleaned)):
        reasons.append("contains_status_token")
    if len(tokens) < 2:
        reasons.append("too_few_name_tokens")
    if SINGLE_LETTER_RUN_RE.search(cleaned):
        reasons.append("single_letter_ocr_run")
    if any(token in CLUB_WORDS for token in tokens):
        reasons.append("contains_club_word")
    if any(token in PDF_NOISE_WORDS for token in tokens):
        reasons.append("contains_pdf_noise")
    if re.search(r"[|_{}\[\]<>]", cleaned):
        reasons.append("contains_layout_punctuation")
    if cleaned.count("(") != cleaned.count(")") or cleaned.count("[") != cleaned.count("]"):
        reasons.append("unbalanced_brackets")

    return reasons


def read_csv_if_exists(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path, dtype=str, encoding="utf-8-sig").fillna("")
    except EmptyDataError:
        return pd.DataFrame()


def first_raw_lookup(
    df: pd.DataFrame,
    name_column: str,
    extra_key_columns: Sequence[str],
) -> Dict[Tuple[str, ...], dict]:
    lookup: Dict[Tuple[str, ...], dict] = {}
    if df.empty or name_column not in df.columns:
        return lookup
    for _, row in df.iterrows():
        key_values = [clean_extracted_text(row.get(name_column)) or ""]
        for column in extra_key_columns:
            key_values.append(clean_extracted_text(row.get(column)) or "")
        key = tuple(key_values)
        if key not in lookup:
            lookup[key] = row.to_dict()
    return lookup


def add_review_record(
    records: Dict[Tuple[str, str, str, str, str], dict],
    reason_counts: Counter,
    *,
    source_url: str,
    input_dir: Path,
    table_name: str,
    athlete_name: str,
    reasons: Sequence[str],
    club_name: str = "",
    event_name: str = "",
    gender: str = "",
    birth_year: str = "",
    raw_row: Optional[dict] = None,
) -> None:
    reason_text = "|".join(reasons)
    key = (source_url, table_name, athlete_name, club_name, reason_text)
    if key not in records:
        raw_row = raw_row or {}
        records[key] = {
            "source_url": source_url,
            "input_dir": str(input_dir),
            "table": table_name,
            "athlete_name": athlete_name,
            "club_name": club_name,
            "event_name": event_name,
            "gender": gender,
            "birth_year": birth_year,
            "reasons": reason_text,
            "observation_count": 0,
            "sample_page_number": raw_row.get("page_number", ""),
            "sample_line_number": raw_row.get("line_number", ""),
            "sample_raw_line": raw_row.get("raw_line", ""),
        }
    records[key]["observation_count"] += 1
    reason_counts.update(reasons)


def audit_document(document: dict, input_dir: Path) -> Tuple[List[dict], Counter, List[str], int]:
    source_url = clean_extracted_text(document.get("source_url")) or ""
    missing: List[str] = []
    records: Dict[Tuple[str, str, str, str, str], dict] = {}
    reason_counts: Counter = Counter()
    observed_names = 0

    athlete_df = read_csv_if_exists(input_dir / "athlete.csv")
    result_df = read_csv_if_exists(input_dir / "result.csv")
    raw_result_df = read_csv_if_exists(input_dir / "raw_result.csv")
    relay_df = read_csv_if_exists(input_dir / "relay_swimmer.csv")
    raw_relay_df = read_csv_if_exists(input_dir / "raw_relay_swimmer.csv")

    if athlete_df.empty:
        missing.append(str(input_dir / "athlete.csv"))
    if result_df.empty:
        missing.append(str(input_dir / "result.csv"))

    raw_result_lookup = first_raw_lookup(raw_result_df, "athlete_name", ["club_name", "event_name"])
    raw_relay_lookup = first_raw_lookup(raw_relay_df, "swimmer_name", ["relay_team_name", "event_name"])

    if not athlete_df.empty and "full_name" in athlete_df.columns:
        for _, row in athlete_df.iterrows():
            athlete_name = clean_extracted_text(row.get("full_name")) or ""
            observed_names += 1
            reasons = classify_athlete_name(athlete_name)
            if reasons:
                club_name = clean_extracted_text(row.get("club_name")) or ""
                add_review_record(
                    records,
                    reason_counts,
                    source_url=source_url,
                    input_dir=input_dir,
                    table_name="athlete",
                    athlete_name=athlete_name,
                    club_name=club_name,
                    gender=clean_extracted_text(row.get("gender")) or "",
                    birth_year=clean_extracted_text(row.get("birth_year")) or "",
                    reasons=reasons,
                )

    if not result_df.empty and "athlete_name" in result_df.columns:
        for _, row in result_df.iterrows():
            athlete_name = clean_extracted_text(row.get("athlete_name")) or ""
            observed_names += 1
            reasons = classify_athlete_name(athlete_name)
            if reasons:
                club_name = clean_extracted_text(row.get("club_name")) or ""
                event_name = clean_extracted_text(row.get("event_name")) or ""
                raw_row = raw_result_lookup.get((athlete_name, club_name, event_name), {})
                add_review_record(
                    records,
                    reason_counts,
                    source_url=source_url,
                    input_dir=input_dir,
                    table_name="result",
                    athlete_name=athlete_name,
                    club_name=club_name,
                    event_name=event_name,
                    birth_year=clean_extracted_text(row.get("birth_year_estimated")) or "",
                    reasons=reasons,
                    raw_row=raw_row,
                )

    if not relay_df.empty and "swimmer_name" in relay_df.columns:
        for _, row in relay_df.iterrows():
            athlete_name = clean_extracted_text(row.get("swimmer_name")) or ""
            observed_names += 1
            reasons = classify_athlete_name(athlete_name)
            if reasons:
                relay_team_name = clean_extracted_text(row.get("relay_team_name")) or ""
                event_name = clean_extracted_text(row.get("event_name")) or ""
                raw_row = raw_relay_lookup.get((athlete_name, relay_team_name, event_name), {})
                add_review_record(
                    records,
                    reason_counts,
                    source_url=source_url,
                    input_dir=input_dir,
                    table_name="relay_swimmer",
                    athlete_name=athlete_name,
                    club_name=relay_team_name,
                    event_name=event_name,
                    gender=clean_extracted_text(row.get("gender")) or "",
                    birth_year=clean_extracted_text(row.get("birth_year_estimated")) or "",
                    reasons=reasons,
                    raw_row=raw_row,
                )

    return list(records.values()), reason_counts, missing, observed_names


def write_csv(path: Path, rows: Sequence[dict], fieldnames: Sequence[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def top_source_counts(rows: Iterable[dict], limit: int = 10) -> List[dict]:
    counts = Counter(row["source_url"] for row in rows)
    return [{"source_url": source_url, "suspicious_rows": count} for source_url, count in counts.most_common(limit)]


def main() -> int:
    args = parse_args()
    manifest_path = resolve_path(args.manifest)
    summary_path = resolve_path(args.summary_json)
    review_path = resolve_path(args.review_csv)
    overrides = load_overrides(args.override_input_dir)

    documents = load_manifest(manifest_path)
    all_rows: List[dict] = []
    reason_counts: Counter = Counter()
    missing_csv: List[str] = []
    observed_names = 0
    override_hits = 0

    for document in documents:
        source_url = clean_extracted_text(document.get("source_url")) or ""
        input_dir_value = document.get("input_dir") or document.get("out_dir")
        if source_url in overrides:
            input_dir = overrides[source_url]
            override_hits += 1
        elif input_dir_value:
            input_dir = resolve_path(str(input_dir_value))
        else:
            missing_csv.append(f"missing_input_dir:{source_url}")
            continue

        rows, doc_reasons, missing, doc_observed = audit_document(document, input_dir)
        all_rows.extend(rows)
        reason_counts.update(doc_reasons)
        missing_csv.extend(missing)
        observed_names += doc_observed

    all_rows.sort(key=lambda row: (row["source_url"], row["table"], row["athlete_name"], row["reasons"]))
    fieldnames = [
        "source_url",
        "input_dir",
        "table",
        "athlete_name",
        "club_name",
        "event_name",
        "gender",
        "birth_year",
        "reasons",
        "observation_count",
        "sample_page_number",
        "sample_line_number",
        "sample_raw_line",
    ]
    write_csv(review_path, all_rows, fieldnames)

    summary = {
        "state": "audited",
        "manifest_documents": len(documents),
        "override_input_dir_hits": override_hits,
        "observed_name_rows": observed_names,
        "suspicious_review_rows": len(all_rows),
        "reason_counts": dict(sorted(reason_counts.items())),
        "missing_csv": missing_csv,
        "top_sources_by_suspicious_rows": top_source_counts(all_rows),
        "review_csv": str(review_path),
    }
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    if args.json:
        print(json.dumps(summary, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
