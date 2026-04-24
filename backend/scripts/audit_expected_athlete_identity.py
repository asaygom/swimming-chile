"""Audit same-name athlete groups in an expected core.athlete preview.

This is a read-only post-curation diagnostic. It does not decide merges: it
separates likely distinct same-name people from cases worth reviewing, such as
same club with birth_year delta +/-1.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import unicodedata
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Audit same-name groups from an expected core.athlete CSV."
    )
    parser.add_argument("--input-csv", required=True, help="Expected core.athlete preview CSV.")
    parser.add_argument("--review-csv", required=True, help="Output CSV grouped by same athlete_key + gender.")
    parser.add_argument("--summary-json", required=True, help="Output JSON summary.")
    parser.add_argument(
        "--birth-year-evidence-csv",
        help="Optional CSV with same-club delta-1 birth_year evidence.",
    )
    parser.add_argument(
        "--corrected-output-csv",
        help="Optional output expected core.athlete CSV after conservative birth_year corrections.",
    )
    parser.add_argument(
        "--birth-year-corrections-csv",
        help="Optional output CSV listing applied birth_year corrections.",
    )
    parser.add_argument(
        "--missing-birth-year-candidates-csv",
        help="Optional output CSV listing no-birth_year rows with one exact contextual candidate.",
    )
    parser.add_argument("--json", action="store_true", help="Print JSON summary to stdout.")
    return parser.parse_args()


def resolve_path(value: str) -> Path:
    path = Path(value)
    if path.is_absolute():
        return path
    return PROJECT_ROOT / path


def parse_birth_year(value: object) -> Optional[int]:
    text = "" if value is None else str(value).strip()
    if not text or text.lower() == "nan":
        return None
    match = re.match(r"^(\d{4})(?:\.0)?$", text)
    if not match:
        return None
    return int(match.group(1))


def birth_year_text(value: object) -> str:
    year = parse_birth_year(value)
    return str(year) if year is not None else ""


def parse_year_counts(value: object) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    text = "" if value is None else str(value)
    for part in text.split(" | "):
        if ":" not in part:
            continue
        year, count = part.rsplit(":", 1)
        year = year.strip()
        try:
            counts[year] = int(count.strip())
        except ValueError:
            continue
    return counts


def normalize_token_text(value: object) -> str:
    text = "" if value is None else str(value).strip()
    if not text:
        return ""
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = text.lower()
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def name_token_key(value: object) -> str:
    tokens = [token for token in normalize_token_text(value).split() if token]
    return " ".join(sorted(tokens))


def preferred_year_from_evidence(row: dict) -> Optional[str]:
    """Return a safe preferred year when source support is one-vs-many.

    Source count is the guardrail because event observations can be inflated by
    a swimmer racing many events in one meet.
    """
    source_counts = parse_year_counts(row.get("year_source_counts"))
    observation_counts = parse_year_counts(row.get("year_observation_counts"))
    if len(source_counts) != 2 or len(observation_counts) != 2:
        return None
    if any(count == 0 for count in source_counts.values()):
        return None
    if any(count == 0 for count in observation_counts.values()):
        return None

    source_items = sorted(source_counts.items(), key=lambda item: item[1])
    if source_items[0][1] != 1 or source_items[1][1] <= 1:
        return None

    preferred_year = source_items[1][0]
    observation_preferred = max(observation_counts.items(), key=lambda item: item[1])[0]
    if preferred_year != observation_preferred:
        return None
    return preferred_year


def classify_same_name_group(group: pd.DataFrame) -> str:
    years = sorted(
        {year for year in (parse_birth_year(value) for value in group["birth_year"]) if year is not None}
    )
    clubs = sorted({str(value).strip() for value in group["club_key"] if str(value).strip()})

    if len(group) < 2:
        return "single"
    if len(years) == 1 and len(clubs) == 1:
        return "strong_duplicate_same_birth_year_same_club"
    if len(years) <= 1 and len(clubs) > 1:
        return "same_birth_year_different_club_review_club_change_or_club_alias"
    if len(years) > 1 and len(clubs) == 1:
        max_delta = max(years) - min(years)
        if max_delta <= 1:
            return "same_club_birth_year_delta_1_review_age_capture"
        return "same_club_birth_year_delta_gt1_probably_distinct_or_age_issue"
    if len(years) > 1 and len(clubs) > 1:
        max_delta = max(years) - min(years)
        if max_delta <= 1:
            return "different_club_birth_year_delta_1_weak_review"
        return "same_name_different_birth_year_and_club_likely_distinct"
    return "same_name_review"


def build_same_name_review_rows(df: pd.DataFrame) -> List[dict]:
    rows: List[dict] = []
    for (athlete_key, gender), group in df.groupby(["athlete_key", "gender"], dropna=False):
        if not athlete_key or len(group) < 2:
            continue
        group = group.sort_values(["birth_year", "club_key", "full_name", "source_url"])
        years = [year for year in (parse_birth_year(value) for value in group["birth_year"]) if year is not None]
        rows.append(
            {
                "review_category": classify_same_name_group(group),
                "athlete_key": athlete_key,
                "gender": gender,
                "row_count": len(group),
                "distinct_birth_years": len(set(years)),
                "min_birth_year": min(years) if years else "",
                "max_birth_year": max(years) if years else "",
                "max_birth_year_delta": max(years) - min(years) if len(years) >= 2 else 0,
                "distinct_clubs": len({value for value in group["club_key"] if value}),
                "birth_years": " | ".join(group["birth_year"].astype(str).tolist()),
                "full_names": " | ".join(group["full_name"].astype(str).tolist()),
                "club_names": " | ".join(group["club_name"].astype(str).tolist()),
                "source_urls": " | ".join(group["source_url"].astype(str).tolist()),
            }
        )

    category_order = {
        "strong_duplicate_same_birth_year_same_club": 0,
        "same_club_birth_year_delta_1_review_age_capture": 1,
        "same_birth_year_different_club_review_club_change_or_club_alias": 2,
        "different_club_birth_year_delta_1_weak_review": 3,
        "same_club_birth_year_delta_gt1_probably_distinct_or_age_issue": 4,
        "same_name_different_birth_year_and_club_likely_distinct": 5,
    }
    rows.sort(key=lambda row: (category_order.get(row["review_category"], 99), -row["row_count"], row["athlete_key"]))
    return rows


def load_birth_year_evidence(path: Path) -> Dict[Tuple[str, str, str], str]:
    corrections: Dict[Tuple[str, str, str], str] = {}
    if not path.exists():
        raise FileNotFoundError(path)
    with path.open(newline="", encoding="utf-8-sig") as fh:
        for row in csv.DictReader(fh):
            preferred_year = preferred_year_from_evidence(row)
            if not preferred_year:
                continue
            key = (
                str(row.get("athlete_key") or "").strip(),
                str(row.get("gender") or "").strip(),
                str(row.get("club_key") or "").strip(),
            )
            if all(key):
                corrections[key] = preferred_year
    return corrections


def apply_birth_year_corrections(
    df: pd.DataFrame,
    correction_map: Dict[Tuple[str, str, str], str],
) -> Tuple[pd.DataFrame, List[dict]]:
    corrected = df.copy()
    changes: List[dict] = []
    for index, row in corrected.iterrows():
        key = (
            str(row.get("athlete_key") or "").strip(),
            str(row.get("gender") or "").strip(),
            str(row.get("club_key") or "").strip(),
        )
        preferred_year = correction_map.get(key)
        old_year = birth_year_text(row.get("birth_year"))
        if not preferred_year or not old_year or old_year == preferred_year:
            corrected.at[index, "birth_year"] = old_year
            continue
        changes.append(
            {
                "expected_row_id": row.get("expected_row_id", ""),
                "full_name": row.get("full_name", ""),
                "gender": row.get("gender", ""),
                "club_name": row.get("club_name", ""),
                "club_key": row.get("club_key", ""),
                "athlete_key": row.get("athlete_key", ""),
                "old_birth_year": old_year,
                "new_birth_year": preferred_year,
                "source_url": row.get("source_url", ""),
            }
        )
        corrected.at[index, "birth_year"] = preferred_year
    return corrected, changes


def dedupe_expected_core_athletes(df: pd.DataFrame) -> pd.DataFrame:
    output = df.copy()
    output["birth_year"] = output["birth_year"].map(birth_year_text)
    output = output.drop_duplicates(
        subset=["athlete_key", "gender", "birth_year", "club_key"],
        keep="first",
        ignore_index=True,
    )
    if "expected_row_id" in output.columns:
        output["expected_row_id"] = [str(index) for index in range(1, len(output) + 1)]
    return output


def build_missing_birth_year_candidate_rows(df: pd.DataFrame) -> List[dict]:
    known_matches: Dict[Tuple[str, str, str], List[dict]] = {}
    for _, row in df.iterrows():
        year = birth_year_text(row.get("birth_year"))
        if not year:
            continue
        key = (
            name_token_key(row.get("full_name")),
            str(row.get("gender") or "").strip(),
            str(row.get("club_key") or "").strip(),
        )
        if all(key):
            known_matches.setdefault(key, []).append(
                {
                    "birth_year": year,
                    "full_name": row.get("full_name", ""),
                }
            )

    rows: List[dict] = []
    for _, row in df.iterrows():
        if birth_year_text(row.get("birth_year")):
            continue
        key = (
            name_token_key(row.get("full_name")),
            str(row.get("gender") or "").strip(),
            str(row.get("club_key") or "").strip(),
        )
        matches = known_matches.get(key, [])
        years = sorted({match["birth_year"] for match in matches})
        if len(years) != 1:
            continue
        candidate_names = sorted({str(match["full_name"]) for match in matches if match["full_name"]})
        rows.append(
            {
                "expected_row_id": row.get("expected_row_id", ""),
                "full_name": row.get("full_name", ""),
                "gender": row.get("gender", ""),
                "club_name": row.get("club_name", ""),
                "club_key": row.get("club_key", ""),
                "athlete_key": row.get("athlete_key", ""),
                "candidate_birth_year": years[0],
                "candidate_reason": "same_name_tokens_gender_club_single_known_year",
                "candidate_full_names": " | ".join(candidate_names),
                "candidate_rows": len(matches),
                "source_url": row.get("source_url", ""),
            }
        )
    rows.sort(key=lambda row: (row["athlete_key"], row["gender"], row["club_key"], row["full_name"]))
    return rows


def write_csv(path: Path, rows: Sequence[dict]) -> None:
    fieldnames = [
        "review_category",
        "athlete_key",
        "gender",
        "row_count",
        "distinct_birth_years",
        "min_birth_year",
        "max_birth_year",
        "max_birth_year_delta",
        "distinct_clubs",
        "birth_years",
        "full_names",
        "club_names",
        "source_urls",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def write_dict_csv(path: Path, rows: Sequence[dict], fieldnames: Sequence[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    args = parse_args()
    input_path = resolve_path(args.input_csv)
    review_path = resolve_path(args.review_csv)
    summary_path = resolve_path(args.summary_json)

    df = pd.read_csv(input_path, dtype=str, encoding="utf-8-sig").fillna("")
    source_row_count = len(df)

    birth_year_correction_count = 0
    corrected_row_count = None
    rows_without_birth_year_after_correction = None
    missing_birth_year_candidate_count = None
    if args.birth_year_evidence_csv:
        evidence_path = resolve_path(args.birth_year_evidence_csv)
        correction_map = load_birth_year_evidence(evidence_path)
        corrected_df, correction_rows = apply_birth_year_corrections(df, correction_map)
        birth_year_correction_count = len(correction_rows)
        corrected_df = dedupe_expected_core_athletes(corrected_df)
        corrected_row_count = len(corrected_df)
        rows_without_birth_year_after_correction = sum(
            1 for value in corrected_df["birth_year"].tolist() if not birth_year_text(value)
        )

        if args.corrected_output_csv:
            output_path = resolve_path(args.corrected_output_csv)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            corrected_df.to_csv(output_path, index=False, encoding="utf-8-sig")
        if args.birth_year_corrections_csv:
            write_dict_csv(
                resolve_path(args.birth_year_corrections_csv),
                correction_rows,
                [
                    "expected_row_id",
                    "full_name",
                    "gender",
                    "club_name",
                    "club_key",
                    "athlete_key",
                    "old_birth_year",
                    "new_birth_year",
                    "source_url",
                ],
            )
        df_for_review = corrected_df
        df_for_missing_birth_year = corrected_df
    else:
        df_for_review = df
        df_for_missing_birth_year = df

    if args.missing_birth_year_candidates_csv:
        missing_birth_year_rows = build_missing_birth_year_candidate_rows(df_for_missing_birth_year)
        write_dict_csv(
            resolve_path(args.missing_birth_year_candidates_csv),
            missing_birth_year_rows,
            [
                "expected_row_id",
                "full_name",
                "gender",
                "club_name",
                "club_key",
                "athlete_key",
                "candidate_birth_year",
                "candidate_reason",
                "candidate_full_names",
                "candidate_rows",
                "source_url",
            ],
        )
        missing_birth_year_candidate_count = len(missing_birth_year_rows)

    rows = build_same_name_review_rows(df_for_review)
    write_csv(review_path, rows)
    category_counts = {}
    for row in rows:
        category_counts[row["review_category"]] = category_counts.get(row["review_category"], 0) + 1
    summary = {
        "source_expected_core_athlete_csv": str(input_path),
        "review_csv": str(review_path),
        "source_row_count": source_row_count,
        "same_name_group_count": len(rows),
        "category_counts": dict(sorted(category_counts.items())),
    }
    if corrected_row_count is not None:
        summary.update(
            {
                "birth_year_correction_count": birth_year_correction_count,
                "corrected_row_count": corrected_row_count,
                "corrected_row_delta": corrected_row_count - source_row_count,
                "rows_without_birth_year_after_correction": rows_without_birth_year_after_correction,
            }
        )
    if missing_birth_year_candidate_count is not None:
        summary["missing_birth_year_candidate_count"] = missing_birth_year_candidate_count
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    if args.json:
        print(json.dumps(summary, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
