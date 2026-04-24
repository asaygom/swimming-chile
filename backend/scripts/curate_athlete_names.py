"""Curate athlete-name variants after parsing and before load.

This script is intentionally separate from the parser and loader. It consumes
parsed CSV folders from a manifest, groups likely OCR variants, and writes
auditable replacement proposals without loading data to core.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import unicodedata
from collections import Counter, defaultdict
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from audit_athlete_names import load_manifest, load_overrides, read_csv_if_exists, resolve_path
from run_pipeline_results import clean_extracted_text, normalize_match_text


NAME_TOKEN_RE = re.compile(r"[A-Za-zÁÉÍÓÚÜÑáéíóúüñ]+")
FRAGMENTED_NAME_RE = re.compile(r"(?:^|\s)(?:[A-Za-zÁÉÍÓÚÜÑáéíóúüñ]\s+){2,}[A-Za-zÁÉÍÓÚÜÑáéíóúüñ](?:\s|$)")
NOISY_VOWEL_RUN_RE = re.compile(r"[aeiou]{2,}")
REPEATED_VOWEL_RUN_RE = re.compile(r"([aeiou])\1+")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Curate athlete-name variants from parsed manifest CSV folders."
    )
    parser.add_argument("--manifest", required=True, help="Manifest JSONL with parsed input_dir entries.")
    parser.add_argument("--summary-json", required=True, help="Output JSON summary path.")
    parser.add_argument(
        "--review-csv",
        required=True,
        help="Output CSV with grouped athlete-name replacement proposals.",
    )
    parser.add_argument(
        "--override-input-dir",
        action="append",
        default=[],
        metavar="SOURCE_URL=INPUT_DIR",
        help="Override one manifest input_dir for a source_url, useful for scratch parser outputs.",
    )
    parser.add_argument("--json", action="store_true", help="Print JSON summary to stdout.")
    return parser.parse_args()


def strip_accents(value: Optional[str]) -> Optional[str]:
    cleaned = clean_extracted_text(value)
    if not cleaned:
        return None
    normalized = unicodedata.normalize("NFKD", cleaned)
    return "".join(ch for ch in normalized if not unicodedata.combining(ch))


def flatten_visible_name(value: Optional[str]) -> Optional[str]:
    cleaned = strip_accents(value)
    if not cleaned:
        return None
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned or None


def token_signature(token: str) -> str:
    normalized = normalize_match_text(token) or ""
    normalized = normalized.replace(" ", "")
    if not normalized:
        return ""
    collapsed = re.sub(r"([aeiou])\1+", r"\1", normalized)
    consonants = re.sub(r"[aeiou]", "", collapsed)
    core = consonants or collapsed
    return f"{collapsed[0]}|{core}|{collapsed[-1]}"


def athlete_name_signature(name: Optional[str]) -> Optional[str]:
    cleaned = clean_extracted_text(name)
    if not cleaned:
        return None
    sides = [side.strip() for side in cleaned.split(",")]
    signature_sides: List[str] = []
    for side in sides:
        tokens = NAME_TOKEN_RE.findall(side)
        if not tokens:
            continue
        token_signatures = [token_signature(token) for token in tokens if token_signature(token)]
        if token_signatures:
            signature_sides.append(".".join(token_signatures))
    if not signature_sides:
        return None
    return ",".join(signature_sides)


def athlete_name_noise_score(name: Optional[str]) -> int:
    cleaned = clean_extracted_text(name) or ""
    if not cleaned:
        return 100

    score = 0
    if "," not in cleaned:
        score += 3
    if re.search(r"\d", cleaned):
        score += 10
    if FRAGMENTED_NAME_RE.search(cleaned):
        score += 6
    if "ñ ñ" in cleaned.lower():
        score += 6

    flat = flatten_visible_name(cleaned) or ""
    for token in NAME_TOKEN_RE.findall(flat):
        token_lower = token.lower()
        if NOISY_VOWEL_RUN_RE.search(token_lower):
            score += 2
        if len(re.findall(r"[ÁÉÍÓÚáéíóú]", token)) > 1:
            score += 2
    score += len(re.findall(r"[ÁÉÍÓÚáéíóúÜüÑñ]", cleaned))
    return score


def collect_name_rows(document: dict, input_dir: Path) -> List[dict]:
    source_url = clean_extracted_text(document.get("source_url")) or ""
    rows: List[dict] = []
    table_specs = (
        ("athlete", "athlete.csv", "full_name", "club_name", "gender", "birth_year"),
        ("result", "result.csv", "athlete_name", "club_name", None, "birth_year_estimated"),
        ("relay_swimmer", "relay_swimmer.csv", "swimmer_name", "club_name", "gender", "birth_year_estimated"),
    )

    for table_name, filename, name_column, club_column, gender_column, birth_year_column in table_specs:
        df = read_csv_if_exists(input_dir / filename)
        if df.empty or name_column not in df.columns:
            continue
        for _, row in df.iterrows():
            athlete_name = clean_extracted_text(row.get(name_column))
            if not athlete_name:
                continue
            rows.append(
                {
                    "source_url": source_url,
                    "input_dir": str(input_dir),
                    "table": table_name,
                    "athlete_name": athlete_name,
                    "club_name": clean_extracted_text(row.get(club_column)) or "",
                    "gender": clean_extracted_text(row.get(gender_column)) or "" if gender_column else "",
                    "birth_year": clean_extracted_text(row.get(birth_year_column)) or "",
                }
            )
    return rows


def normalize_birth_year(value: Optional[str]) -> str:
    cleaned = clean_extracted_text(value)
    if not cleaned:
        return ""
    match = re.match(r"^(\d{4})(?:\.0)?$", cleaned)
    return match.group(1) if match else cleaned


def curation_group_key(row: dict) -> Optional[Tuple[str, str, str, str]]:
    signature = athlete_name_signature(row.get("athlete_name"))
    birth_year = normalize_birth_year(row.get("birth_year"))
    club_key = normalize_match_text(row.get("club_name")) or ""
    gender = normalize_match_text(row.get("gender")) or ""
    if not signature or not birth_year or not club_key:
        return None
    return signature, birth_year, club_key, gender


def choose_canonical_name(group_rows: Sequence[dict]) -> str:
    counts = Counter(row["athlete_name"] for row in group_rows)
    candidates = sorted(
        counts.keys(),
        key=lambda name: (
            -counts[name],
            athlete_name_noise_score(name),
            sum(1 for ch in name if ord(ch) > 127),
            len(flatten_visible_name(name) or name),
            flatten_visible_name(name) or name,
        ),
    )
    chosen = candidates[0]
    chosen_flat = flatten_visible_name(chosen)
    return chosen_flat or chosen


def _name_tokens(value: Optional[str]) -> List[str]:
    flat = flatten_visible_name(value)
    if not flat:
        return []
    return [token.lower() for token in NAME_TOKEN_RE.findall(flat)]


def _single_vowel_deletion_distance(original: str, canonical: str) -> Optional[int]:
    """Return deleted vowel count when canonical is original minus only vowels."""
    if original == canonical:
        return 0
    i = 0
    j = 0
    deletions = 0
    while i < len(original) and j < len(canonical):
        if original[i] == canonical[j]:
            i += 1
            j += 1
            continue
        if original[i] in "aeiou":
            deletions += 1
            i += 1
            continue
        return None
    while i < len(original):
        if original[i] not in "aeiou":
            return None
        deletions += 1
        i += 1
    if j != len(canonical):
        return None
    return deletions


def is_safe_ocr_replacement(original_name: str, canonical_name: str, counts: Counter) -> bool:
    original_tokens = _name_tokens(original_name)
    canonical_tokens = _name_tokens(canonical_name)
    if not original_tokens or len(original_tokens) != len(canonical_tokens):
        return False
    if not any(ord(ch) > 127 for ch in original_name) and not any(
        REPEATED_VOWEL_RUN_RE.search(token) for token in original_tokens
    ):
        return False

    changed_tokens = 0
    for original_token, canonical_token in zip(original_tokens, canonical_tokens):
        if original_token == canonical_token:
            continue
        if token_signature(original_token) != token_signature(canonical_token):
            return False
        deleted_vowels = _single_vowel_deletion_distance(original_token, canonical_token)
        if deleted_vowels is None or deleted_vowels > 1:
            return False
        changed_tokens += 1

    if changed_tokens == 0:
        return False

    original_count = counts[original_name]
    canonical_count = counts.get(canonical_name, 0)
    return canonical_count >= original_count or athlete_name_noise_score(original_name) > athlete_name_noise_score(canonical_name)


def build_review_rows(rows: Sequence[dict]) -> Tuple[List[dict], Dict[Tuple[str, str, str, str], str]]:
    grouped: Dict[Tuple[str, str, str, str], List[dict]] = defaultdict(list)
    for row in rows:
        group_key = curation_group_key(row)
        if group_key:
            grouped[group_key].append(row)

    review_rows: List[dict] = []
    replacement_map: Dict[Tuple[str, str, str, str], str] = {}
    for group_key, group_rows in grouped.items():
        signature, birth_year, club_key, gender = group_key
        distinct_names = sorted({row["athlete_name"] for row in group_rows})
        if len(distinct_names) < 2:
            continue

        canonical_name = choose_canonical_name(group_rows)
        counts = Counter(row["athlete_name"] for row in group_rows)
        source_urls = sorted({row["source_url"] for row in group_rows if row["source_url"]})
        club_names = sorted({row["club_name"] for row in group_rows if row["club_name"]})

        for original_name in distinct_names:
            flattened_original = flatten_visible_name(original_name) or original_name
            replacement_needed = flattened_original != canonical_name and is_safe_ocr_replacement(
                original_name,
                canonical_name,
                counts,
            )
            if replacement_needed:
                replacement_map[(original_name, birth_year, club_key, gender)] = canonical_name
            review_rows.append(
                {
                    "signature": signature,
                    "birth_year": birth_year,
                    "club_key": club_key,
                    "gender": gender,
                    "canonical_name": canonical_name,
                    "original_name": original_name,
                    "original_name_flat": flattened_original,
                    "needs_replacement": "yes" if replacement_needed else "no",
                    "occurrence_count": counts[original_name],
                    "group_size": len(distinct_names),
                    "source_urls": " | ".join(source_urls),
                    "club_names": " | ".join(club_names[:10]),
                }
            )

    review_rows.sort(key=lambda row: (row["canonical_name"], row["signature"], row["original_name"]))
    return review_rows, replacement_map


def write_csv(path: Path, rows: Sequence[dict], fieldnames: Sequence[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    args = parse_args()
    manifest_path = resolve_path(args.manifest)
    summary_path = resolve_path(args.summary_json)
    review_path = resolve_path(args.review_csv)
    overrides = load_overrides(args.override_input_dir)

    documents = load_manifest(manifest_path)
    all_rows: List[dict] = []
    override_hits = 0
    missing_input_dirs: List[str] = []
    for document in documents:
        source_url = clean_extracted_text(document.get("source_url")) or ""
        input_dir_value = document.get("input_dir") or document.get("out_dir")
        if source_url in overrides:
            input_dir = overrides[source_url]
            override_hits += 1
        elif input_dir_value:
            input_dir = resolve_path(str(input_dir_value))
        else:
            missing_input_dirs.append(source_url)
            continue
        all_rows.extend(collect_name_rows(document, input_dir))

    review_rows, replacement_map = build_review_rows(all_rows)
    fieldnames = [
        "signature",
        "birth_year",
        "club_key",
        "gender",
        "canonical_name",
        "original_name",
        "original_name_flat",
        "needs_replacement",
        "occurrence_count",
        "group_size",
        "source_urls",
        "club_names",
    ]
    write_csv(review_path, review_rows, fieldnames)

    summary = {
        "state": "curated",
        "manifest_documents": len(documents),
        "override_input_dir_hits": override_hits,
        "observed_name_rows": len(all_rows),
        "variant_groups": len({row["signature"] for row in review_rows}),
        "replacement_rows": sum(1 for row in review_rows if row["needs_replacement"] == "yes"),
        "unique_replacements": len(replacement_map),
        "missing_input_dirs": missing_input_dirs,
        "review_csv": str(review_path),
    }
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    if args.json:
        print(json.dumps(summary, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
