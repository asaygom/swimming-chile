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
    parser.add_argument(
        "--apply-missing-birth-year-candidates",
        action="store_true",
        help="Apply unique missing-birth_year candidates to the corrected output and review.",
    )
    parser.add_argument(
        "--missing-birth-year-consolidation-csv",
        help="Optional output CSV listing applied missing-birth_year consolidations.",
    )
    parser.add_argument(
        "--partial-name-candidates-csv",
        help="Optional output CSV listing same-context partial/extended name candidates.",
    )
    parser.add_argument(
        "--expanded-identity-candidates-csv",
        help="Optional output CSV listing broader reviewed identity candidates before load.",
    )
    parser.add_argument(
        "--partial-name-decisions-csv",
        help="Optional reviewed CSV. Only rows with decision=merge are applied.",
    )
    parser.add_argument(
        "--partial-name-consolidation-csv",
        help="Optional output CSV listing applied partial-name consolidations.",
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


def name_token_set(value: object) -> set:
    return set(name_token_key(value).split())


def ordered_name_key(value: object) -> str:
    return normalize_token_text(value)


def split_ordered_name(value: object) -> Tuple[str, str]:
    text = "" if value is None else str(value)
    if "," in text:
        surname, given = text.split(",", 1)
        return normalize_token_text(surname), normalize_token_text(given)
    tokens = normalize_token_text(text).split()
    if not tokens:
        return "", ""
    return tokens[-1], " ".join(tokens[:-1])


def _match_tokens(shorter_tokens: Sequence[str], longer_tokens: Sequence[str]) -> Tuple[bool, List[str], List[str]]:
    unmatched_longer = list(longer_tokens)
    unmatched_shorter: List[str] = []
    initial_matches: List[str] = []
    for token in shorter_tokens:
        if token in unmatched_longer:
            unmatched_longer.remove(token)
            continue
        unmatched_shorter.append(token)
    for token in unmatched_shorter:
        if len(token) == 1:
            match = next((candidate for candidate in unmatched_longer if candidate.startswith(token)), None)
            if match:
                unmatched_longer.remove(match)
                initial_matches.append(f"{token}->{match}")
                continue
        return False, [], []
    return True, unmatched_longer, initial_matches


def partial_name_match(left_name: object, right_name: object) -> Optional[dict]:
    left_tokens = name_token_key(left_name).split()
    right_tokens = name_token_key(right_name).split()
    if len(left_tokens) < 2 or len(right_tokens) < 2 or left_tokens == right_tokens:
        return None

    candidates = []
    if len(left_tokens) <= len(right_tokens):
        matched, added, initial_matches = _match_tokens(left_tokens, right_tokens)
        if matched:
            candidates.append(("left", "right", added, initial_matches))
    if len(right_tokens) <= len(left_tokens):
        matched, added, initial_matches = _match_tokens(right_tokens, left_tokens)
        if matched:
            candidates.append(("right", "left", added, initial_matches))
    if not candidates:
        return None

    shorter_side, longer_side, added_tokens, initial_matches = sorted(
        candidates,
        key=lambda item: (len(item[2]), len(item[3])),
    )[0]
    if not added_tokens and not initial_matches:
        return None
    if len(added_tokens) > 2 and not initial_matches:
        return None
    return {
        "shorter_side": shorter_side,
        "longer_side": longer_side,
        "added_tokens": added_tokens,
        "initial_matches": initial_matches,
    }


def edit_distance_at_most_one(left: str, right: str) -> bool:
    if left == right:
        return True
    if abs(len(left) - len(right)) > 1:
        return False
    if len(left) > len(right):
        left, right = right, left
    mismatches = 0
    index_left = 0
    index_right = 0
    while index_left < len(left) and index_right < len(right):
        if left[index_left] == right[index_right]:
            index_left += 1
            index_right += 1
            continue
        mismatches += 1
        if mismatches > 1:
            return False
        if len(left) == len(right):
            index_left += 1
        index_right += 1
    return True


def ordered_prefix_tokens(shorter: str, longer: str) -> bool:
    shorter_tokens = [token for token in normalize_token_text(shorter).split() if token]
    longer_tokens = [token for token in normalize_token_text(longer).split() if token]
    if not shorter_tokens or len(shorter_tokens) > len(longer_tokens):
        return False
    return longer_tokens[: len(shorter_tokens)] == shorter_tokens


def compatible_first_given(left: str, right: str) -> bool:
    left_tokens = [token for token in normalize_token_text(left).split() if token]
    right_tokens = [token for token in normalize_token_text(right).split() if token]
    if not left_tokens or not right_tokens:
        return False
    if left_tokens[0] == right_tokens[0]:
        return True
    return edit_distance_at_most_one(left_tokens[0], right_tokens[0])


def source_url_set(value: object) -> set[str]:
    text = "" if value is None else str(value)
    return {part.strip() for part in text.split(" | ") if part.strip()}


def expanded_identity_match(left_name: object, right_name: object, same_birth_year: bool, birth_year_delta: int) -> Optional[dict]:
    left_surname, left_given = split_ordered_name(left_name)
    right_surname, right_given = split_ordered_name(right_name)
    if not left_surname or not left_given or not right_surname or not right_given:
        return None
    reasons: List[str] = []
    if same_birth_year and compatible_first_given(left_given, right_given):
        if ordered_prefix_tokens(left_surname, right_surname) and left_surname != right_surname:
            reasons.append("surname_prefix_or_second_surname_omitted")
        elif ordered_prefix_tokens(right_surname, left_surname) and left_surname != right_surname:
            reasons.append("surname_prefix_or_second_surname_omitted")
    if same_birth_year and left_surname == right_surname and left_given != right_given:
        if ordered_prefix_tokens(left_given, right_given) or ordered_prefix_tokens(right_given, left_given):
            reasons.append("given_prefix_initial_or_second_name_omitted")
        else:
            matched_left, _, initial_left = _match_tokens(left_given.split(), right_given.split())
            matched_right, _, initial_right = _match_tokens(right_given.split(), left_given.split())
            if (matched_left and initial_left) or (matched_right and initial_right):
                reasons.append("given_prefix_initial_or_second_name_omitted")
    if (
        birth_year_delta == 1
        and (left_surname == right_surname or edit_distance_at_most_one(left_surname, right_surname))
        and compatible_first_given(left_given, right_given)
    ):
        reasons.append("birth_year_delta_1_name_compatible")
    if not reasons:
        return None
    return {"candidate_reason": ";".join(sorted(set(reasons)))}


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


def _candidate_change_by_row_id(candidate_rows: Sequence[dict]) -> Dict[str, dict]:
    changes: Dict[str, dict] = {}
    for row in candidate_rows:
        names = [name.strip() for name in str(row.get("candidate_full_names") or "").split(" | ") if name.strip()]
        unique_names = sorted(set(names))
        if len(unique_names) != 1:
            continue
        row_id = str(row.get("expected_row_id") or "").strip()
        candidate_birth_year = birth_year_text(row.get("candidate_birth_year"))
        if not row_id or not candidate_birth_year:
            continue
        changes[row_id] = {
            "canonical_full_name": unique_names[0],
            "canonical_athlete_key": ordered_name_key(unique_names[0]),
            "candidate_birth_year": candidate_birth_year,
            "candidate_reason": row.get("candidate_reason", ""),
            "candidate_rows": row.get("candidate_rows", ""),
            "source_url": row.get("source_url", ""),
        }
    return changes


def apply_missing_birth_year_candidates(
    df: pd.DataFrame,
    candidate_rows: Sequence[dict],
) -> Tuple[pd.DataFrame, List[dict]]:
    candidate_by_row_id = _candidate_change_by_row_id(candidate_rows)
    corrected = df.copy()
    changes: List[dict] = []
    for index, row in corrected.iterrows():
        if birth_year_text(row.get("birth_year")):
            continue
        row_id = str(row.get("expected_row_id") or "").strip()
        candidate = candidate_by_row_id.get(row_id)
        if not candidate:
            continue
        changes.append(
            {
                "expected_row_id": row_id,
                "old_full_name": row.get("full_name", ""),
                "new_full_name": candidate["canonical_full_name"],
                "gender": row.get("gender", ""),
                "club_name": row.get("club_name", ""),
                "club_key": row.get("club_key", ""),
                "old_athlete_key": row.get("athlete_key", ""),
                "new_athlete_key": candidate["canonical_athlete_key"],
                "old_birth_year": "",
                "new_birth_year": candidate["candidate_birth_year"],
                "candidate_reason": candidate["candidate_reason"],
                "candidate_rows": candidate["candidate_rows"],
                "source_url": row.get("source_url", ""),
            }
        )
        corrected.at[index, "full_name"] = candidate["canonical_full_name"]
        corrected.at[index, "athlete_key"] = candidate["canonical_athlete_key"]
        corrected.at[index, "birth_year"] = candidate["candidate_birth_year"]
    return corrected, changes


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


def build_partial_name_candidate_rows(df: pd.DataFrame) -> List[dict]:
    rows: List[dict] = []
    context_columns = ["gender", "birth_year"]
    normalized = df.copy()
    normalized["birth_year"] = normalized["birth_year"].map(birth_year_text)
    normalized = normalized[normalized["birth_year"] != ""]

    for (gender, birth_year), group in normalized.groupby(context_columns, dropna=False):
        if not gender or not birth_year or len(group) < 2:
            continue
        names = []
        for _, row in group.iterrows():
            tokens = name_token_key(row.get("full_name")).split()
            if len(tokens) < 2:
                continue
            names.append(
                {
                    "full_name": row.get("full_name", ""),
                    "athlete_key": row.get("athlete_key", ""),
                    "club_key": row.get("club_key", ""),
                    "tokens": tokens,
                    "club_name": row.get("club_name", ""),
                    "source_url": row.get("source_url", ""),
                }
            )

        emitted_pairs = set()
        for index, left in enumerate(names):
            for right in names[index + 1 :]:
                match = partial_name_match(left["full_name"], right["full_name"])
                if not match:
                    continue
                if match["shorter_side"] == "left":
                    shorter = left
                    longer = right
                else:
                    shorter = right
                    longer = left
                added_tokens = sorted(match["added_tokens"])
                initial_matches = sorted(match["initial_matches"])
                same_club = shorter["club_key"] == longer["club_key"]
                pair_key = tuple(
                    sorted(
                        [
                            shorter["athlete_key"] + "|" + shorter["club_key"],
                            longer["athlete_key"] + "|" + longer["club_key"],
                        ]
                    )
                )
                if pair_key in emitted_pairs:
                    continue
                emitted_pairs.add(pair_key)
                if initial_matches:
                    reason = "same_gender_birth_year_initial_compatible"
                elif same_club:
                    reason = "same_gender_birth_year_club_token_subset"
                else:
                    reason = "same_gender_birth_year_cross_club_token_subset"
                rows.append(
                    {
                        "candidate_reason": reason,
                        "gender": gender,
                        "birth_year": birth_year,
                        "same_club": "yes" if same_club else "no",
                        "shorter_club_key": shorter["club_key"],
                        "longer_club_key": longer["club_key"],
                        "club_key": shorter["club_key"] if same_club else "",
                        "club_name": shorter["club_name"] or longer["club_name"],
                        "shorter_club_name": shorter["club_name"],
                        "longer_club_name": longer["club_name"],
                        "shorter_full_name": shorter["full_name"],
                        "longer_full_name": longer["full_name"],
                        "shorter_athlete_key": shorter["athlete_key"],
                        "longer_athlete_key": longer["athlete_key"],
                        "added_tokens": " ".join(added_tokens),
                        "initial_matches": " | ".join(initial_matches),
                        "source_urls": " | ".join(
                            sorted({url for url in [shorter["source_url"], longer["source_url"]] if url})
                        ),
                    }
                )

    rows.sort(
        key=lambda row: (
            row["birth_year"],
            row["same_club"],
            row["shorter_club_key"],
            row["shorter_athlete_key"],
            row["longer_athlete_key"],
        )
    )
    return rows


def preferred_expanded_canonical_name(left: dict, right: dict, candidate_reason: str) -> str:
    left_name = str(left.get("full_name") or "")
    right_name = str(right.get("full_name") or "")
    if "surname_prefix_or_second_surname_omitted" in candidate_reason:
        left_surname, _ = split_ordered_name(left_name)
        right_surname, _ = split_ordered_name(right_name)
        return left_name if len(left_surname.split()) >= len(right_surname.split()) else right_name
    if "given_prefix_initial_or_second_name_omitted" in candidate_reason:
        _, left_given = split_ordered_name(left_name)
        _, right_given = split_ordered_name(right_name)
        return left_name if len(left_given.split()) >= len(right_given.split()) else right_name
    return left_name


def build_expanded_identity_candidate_rows(df: pd.DataFrame) -> List[dict]:
    rows: List[dict] = []
    normalized = df.copy()
    normalized["birth_year"] = normalized["birth_year"].map(birth_year_text)
    normalized = normalized[normalized["birth_year"] != ""]
    grouped: Dict[str, List[dict]] = {}
    for _, row in normalized.iterrows():
        gender = str(row.get("gender") or "").strip()
        if not gender:
            continue
        grouped.setdefault(gender, []).append(row.to_dict())

    emitted_pairs = set()
    for gender, group in grouped.items():
        for index, left in enumerate(group):
            left_year = parse_birth_year(left.get("birth_year"))
            for right in group[index + 1 :]:
                right_year = parse_birth_year(right.get("birth_year"))
                if left_year is None or right_year is None:
                    continue
                birth_year_delta = abs(left_year - right_year)
                if birth_year_delta > 1:
                    continue
                match = expanded_identity_match(
                    left.get("full_name"),
                    right.get("full_name"),
                    same_birth_year=birth_year_delta == 0,
                    birth_year_delta=birth_year_delta,
                )
                if not match:
                    continue
                left_sources = source_url_set(left.get("source_url"))
                right_sources = source_url_set(right.get("source_url"))
                pair_key = tuple(sorted([str(left.get("athlete_key") or ""), str(right.get("athlete_key") or "")]))
                if pair_key in emitted_pairs:
                    continue
                emitted_pairs.add(pair_key)
                same_club = str(left.get("club_key") or "") == str(right.get("club_key") or "")
                candidate_reason = match["candidate_reason"]
                rows.append(
                    {
                        "decision": "",
                        "suggested_canonical_full_name": preferred_expanded_canonical_name(
                            left, right, candidate_reason
                        ),
                        "review_hint": (
                            "same_club_high_confidence"
                            if same_club and birth_year_delta == 0
                            else "birth_year_delta_1_review"
                            if birth_year_delta == 1
                            else "cross_club_review"
                        ),
                        "candidate_reason": candidate_reason,
                        "gender": gender,
                        "birth_year": left_year if birth_year_delta == 0 else "",
                        "left_birth_year": left_year,
                        "right_birth_year": right_year,
                        "birth_year_delta": birth_year_delta,
                        "same_club": "yes" if same_club else "no",
                        "left_full_name": left.get("full_name", ""),
                        "left_club": left.get("club_name", ""),
                        "left_athlete_key": left.get("athlete_key", ""),
                        "right_full_name": right.get("full_name", ""),
                        "right_club": right.get("club_name", ""),
                        "right_athlete_key": right.get("athlete_key", ""),
                        "left_source_count": len(left_sources),
                        "right_source_count": len(right_sources),
                        "combined_source_count": len(left_sources | right_sources),
                        "left_source_urls": " | ".join(sorted(left_sources)),
                        "right_source_urls": " | ".join(sorted(right_sources)),
                        "source_urls": " | ".join(sorted(left_sources | right_sources)),
                    }
                )
    rows.sort(
        key=lambda row: (
            row["review_hint"],
            row["birth_year_delta"],
            row["left_full_name"],
            row["right_full_name"],
        )
    )
    return rows


def read_dict_rows(path: Path) -> List[dict]:
    text = path.read_text(encoding="utf-8-sig")
    sample = text[:2048]
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=",;")
    except csv.Error:
        first_line = text.splitlines()[0] if text.splitlines() else ""
        delimiter = ";" if first_line.count(";") > first_line.count(",") else ","
        dialect = csv.excel()
        dialect.delimiter = delimiter
    return list(csv.DictReader(text.splitlines(), dialect=dialect))


def load_partial_name_decisions(path: Path) -> List[dict]:
    if not path.exists():
        raise FileNotFoundError(path)
    decisions: List[dict] = []
    for row in read_dict_rows(path):
        decision = str(row.get("decision") or "").strip().lower()
        if decision != "merge":
            continue
        gender = str(row.get("gender") or "").strip()
        birth_year = birth_year_text(row.get("birth_year"))
        club_key = str(row.get("club_key") or row.get("shorter_club_key") or "").strip()
        shorter_key = ordered_name_key(row.get("shorter_full_name") or row.get("shorter_athlete_key"))
        canonical_name = str(row.get("suggested_canonical_full_name") or row.get("longer_full_name") or "").strip()
        canonical_key = ordered_name_key(canonical_name)
        if not all([gender, birth_year, club_key, shorter_key, canonical_name, canonical_key]):
            continue
        decisions.append(
            {
                "gender": gender,
                "birth_year": birth_year,
                "club_key": club_key,
                "shorter_athlete_key": shorter_key,
                "canonical_full_name": canonical_name,
                "canonical_athlete_key": canonical_key,
                "source_decision": decision,
                "notes": row.get("notes", ""),
                "review_hint": row.get("review_hint", ""),
                "source_urls": row.get("source_urls", ""),
            }
        )
    return decisions


def _resolve_partial_decision_target(
    key: Tuple[str, str, str, str],
    decision_map: Dict[Tuple[str, str, str, str], dict],
) -> dict:
    seen = set()
    current_key = key
    current = decision_map[current_key]
    while True:
        target_key = (
            current["gender"],
            current["birth_year"],
            current["club_key"],
            current["canonical_athlete_key"],
        )
        if target_key in seen or target_key not in decision_map:
            return current
        seen.add(target_key)
        current = decision_map[target_key]


def apply_partial_name_decisions(
    df: pd.DataFrame,
    decisions: Sequence[dict],
) -> Tuple[pd.DataFrame, List[dict]]:
    decision_map: Dict[Tuple[str, str, str, str], dict] = {}
    for decision in decisions:
        key = (
            decision["gender"],
            decision["birth_year"],
            decision["club_key"],
            decision["shorter_athlete_key"],
        )
        decision_map[key] = decision

    corrected = df.copy()
    changes: List[dict] = []
    for index, row in corrected.iterrows():
        birth_year = birth_year_text(row.get("birth_year"))
        key = (
            str(row.get("gender") or "").strip(),
            birth_year,
            str(row.get("club_key") or "").strip(),
            ordered_name_key(row.get("full_name")),
        )
        if key not in decision_map:
            continue
        target = _resolve_partial_decision_target(key, decision_map)
        old_full_name = row.get("full_name", "")
        old_athlete_key = row.get("athlete_key", "")
        if ordered_name_key(old_full_name) == target["canonical_athlete_key"]:
            continue
        changes.append(
            {
                "expected_row_id": row.get("expected_row_id", ""),
                "old_full_name": old_full_name,
                "new_full_name": target["canonical_full_name"],
                "gender": row.get("gender", ""),
                "birth_year": birth_year,
                "club_name": row.get("club_name", ""),
                "club_key": row.get("club_key", ""),
                "old_athlete_key": old_athlete_key,
                "new_athlete_key": target["canonical_athlete_key"],
                "review_hint": target.get("review_hint", ""),
                "notes": target.get("notes", ""),
                "source_url": row.get("source_url", ""),
            }
        )
        corrected.at[index, "full_name"] = target["canonical_full_name"]
        corrected.at[index, "athlete_key"] = target["canonical_athlete_key"]
        corrected.at[index, "birth_year"] = birth_year
    return corrected, changes


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
    final_row_count = None
    rows_without_birth_year_after_correction = None
    missing_birth_year_candidate_count = None
    missing_birth_year_consolidation_count = None
    partial_name_candidate_count = None
    expanded_identity_candidate_count = None
    partial_name_decision_count = None
    partial_name_consolidation_count = None
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

    missing_birth_year_rows: List[dict] = []
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

    if args.apply_missing_birth_year_candidates:
        if not missing_birth_year_rows:
            missing_birth_year_rows = build_missing_birth_year_candidate_rows(df_for_missing_birth_year)
            missing_birth_year_candidate_count = len(missing_birth_year_rows)
        consolidated_df, consolidation_rows = apply_missing_birth_year_candidates(
            df_for_missing_birth_year,
            missing_birth_year_rows,
        )
        missing_birth_year_consolidation_count = len(consolidation_rows)
        consolidated_df = dedupe_expected_core_athletes(consolidated_df)
        final_row_count = len(consolidated_df)
        df_for_review = consolidated_df
        df_for_missing_birth_year = consolidated_df
        if args.corrected_output_csv:
            output_path = resolve_path(args.corrected_output_csv)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            consolidated_df.to_csv(output_path, index=False, encoding="utf-8-sig")
        if args.missing_birth_year_consolidation_csv:
            write_dict_csv(
                resolve_path(args.missing_birth_year_consolidation_csv),
                consolidation_rows,
                [
                    "expected_row_id",
                    "old_full_name",
                    "new_full_name",
                    "gender",
                    "club_name",
                    "club_key",
                    "old_athlete_key",
                    "new_athlete_key",
                    "old_birth_year",
                    "new_birth_year",
                    "candidate_reason",
                    "candidate_rows",
                    "source_url",
                ],
            )

    if args.partial_name_decisions_csv:
        partial_decisions = load_partial_name_decisions(resolve_path(args.partial_name_decisions_csv))
        partial_name_decision_count = len(partial_decisions)
        partial_df, partial_rows = apply_partial_name_decisions(df_for_review, partial_decisions)
        partial_name_consolidation_count = len(partial_rows)
        partial_df = dedupe_expected_core_athletes(partial_df)
        final_row_count = len(partial_df)
        df_for_review = partial_df
        df_for_missing_birth_year = partial_df
        if args.corrected_output_csv:
            output_path = resolve_path(args.corrected_output_csv)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            partial_df.to_csv(output_path, index=False, encoding="utf-8-sig")
        if args.partial_name_consolidation_csv:
            write_dict_csv(
                resolve_path(args.partial_name_consolidation_csv),
                partial_rows,
                [
                    "expected_row_id",
                    "old_full_name",
                    "new_full_name",
                    "gender",
                    "birth_year",
                    "club_name",
                    "club_key",
                    "old_athlete_key",
                    "new_athlete_key",
                    "review_hint",
                    "notes",
                    "source_url",
                ],
            )

    rows = build_same_name_review_rows(df_for_review)
    write_csv(review_path, rows)

    if args.partial_name_candidates_csv:
        partial_name_rows = build_partial_name_candidate_rows(df_for_review)
        write_dict_csv(
            resolve_path(args.partial_name_candidates_csv),
            partial_name_rows,
            [
                "candidate_reason",
                "gender",
                "birth_year",
                "same_club",
                "shorter_club_name",
                "longer_club_name",
                "shorter_club_key",
                "longer_club_key",
                "club_key",
                "club_name",
                "shorter_full_name",
                "longer_full_name",
                "shorter_athlete_key",
                "longer_athlete_key",
                "added_tokens",
                "initial_matches",
                "source_urls",
            ],
        )
        partial_name_candidate_count = len(partial_name_rows)

    if args.expanded_identity_candidates_csv:
        expanded_identity_rows = build_expanded_identity_candidate_rows(df_for_review)
        write_dict_csv(
            resolve_path(args.expanded_identity_candidates_csv),
            expanded_identity_rows,
            [
                "decision",
                "suggested_canonical_full_name",
                "review_hint",
                "candidate_reason",
                "gender",
                "birth_year",
                "left_birth_year",
                "right_birth_year",
                "birth_year_delta",
                "same_club",
                "left_full_name",
                "left_club",
                "left_athlete_key",
                "right_full_name",
                "right_club",
                "right_athlete_key",
                "left_source_count",
                "right_source_count",
                "combined_source_count",
                "left_source_urls",
                "right_source_urls",
                "source_urls",
            ],
        )
        expanded_identity_candidate_count = len(expanded_identity_rows)

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
    if final_row_count is not None:
        summary.update(
            {
                "final_row_count": final_row_count,
                "final_row_delta": final_row_count - source_row_count,
                "rows_without_birth_year_final": sum(
                    1 for value in df_for_review["birth_year"].tolist() if not birth_year_text(value)
                ),
            }
        )
    if missing_birth_year_candidate_count is not None:
        summary["missing_birth_year_candidate_count"] = missing_birth_year_candidate_count
    if missing_birth_year_consolidation_count is not None:
        summary["missing_birth_year_consolidation_count"] = missing_birth_year_consolidation_count
    if partial_name_candidate_count is not None:
        summary["partial_name_candidate_count"] = partial_name_candidate_count
    if expanded_identity_candidate_count is not None:
        summary["expanded_identity_candidate_count"] = expanded_identity_candidate_count
    if partial_name_decision_count is not None:
        summary["partial_name_decision_merge_count"] = partial_name_decision_count
    if partial_name_consolidation_count is not None:
        summary["partial_name_consolidation_count"] = partial_name_consolidation_count
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    if args.json:
        print(json.dumps(summary, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
