#!/usr/bin/env python
"""Audit possible club aliases using athlete-year evidence.

This script is intentionally read-only: it consumes parsed CSV folders from a
manifest and writes review artifacts. It does not parse PDFs and does not load
data to core.
"""

from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict
from dataclasses import dataclass
from itertools import combinations
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Set, Tuple

import pandas as pd

from run_pipeline_results import (
    clean_extracted_text,
    load_club_aliases,
    normalize_athlete_gender,
    normalize_match_text,
    resolve_club_alias,
)


AthleteKey = Tuple[str, str, str]


@dataclass(frozen=True)
class Observation:
    year: str
    athlete_key: AthleteKey
    athlete_label: str
    raw_club: str
    canonical_club: str
    source_url: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Audit club duplicate candidates by shared athletes within the same competition year."
    )
    parser.add_argument("--manifest", required=True, help="Manifest JSONL with parsed input_dir entries.")
    parser.add_argument(
        "--club-alias-csv",
        default=str(Path(__file__).resolve().parents[1] / "data" / "reference" / "club_alias.csv"),
        help="CSV with alias_name, canonical_name columns.",
    )
    parser.add_argument("--summary-json", required=True, help="Output JSON summary path.")
    parser.add_argument("--candidate-csv", required=True, help="Output CSV with pending club-pair candidates.")
    parser.add_argument("--alias-evidence-csv", required=True, help="Output CSV validating current alias groups.")
    parser.add_argument("--min-shared-athletes", type=int, default=2)
    parser.add_argument("--json", action="store_true", help="Print JSON summary to stdout.")
    return parser.parse_args()


def load_manifest(path: Path) -> List[dict]:
    documents: List[dict] = []
    with path.open("r", encoding="utf-8") as fh:
        for line_number, line in enumerate(fh, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                documents.append(json.loads(line))
            except json.JSONDecodeError as exc:
                raise SystemExit(f"{path}:{line_number}: invalid JSONL entry: {exc}") from exc
    return documents


def read_competition_year(input_dir: Path) -> Optional[str]:
    metadata_path = input_dir / "metadata.json"
    if not metadata_path.exists():
        return None
    with metadata_path.open("r", encoding="utf-8") as fh:
        metadata = json.load(fh)
    year = metadata.get("competition_year")
    if year is None:
        start_date = clean_extracted_text(metadata.get("competition_start_date"))
        if start_date and len(start_date) >= 4:
            year = start_date[:4]
    return str(year) if year else None


def athlete_key(row: pd.Series) -> Optional[AthleteKey]:
    name = normalize_match_text(row.get("full_name"))
    gender = normalize_athlete_gender(row.get("gender")) or ""
    birth_year = clean_extracted_text(row.get("birth_year")) or ""
    if not name:
        return None
    return (name, gender, birth_year)


def read_observations(documents: Sequence[dict], aliases: Dict[str, str]) -> Tuple[List[Observation], List[str]]:
    observations: List[Observation] = []
    missing_athlete_csv: List[str] = []

    for document in documents:
        input_dir_value = document.get("input_dir") or document.get("out_dir")
        if not input_dir_value:
            continue
        input_dir = Path(input_dir_value)
        athlete_csv = input_dir / "athlete.csv"
        source_url = clean_extracted_text(document.get("source_url")) or ""
        if not athlete_csv.exists():
            missing_athlete_csv.append(str(input_dir))
            continue
        year = read_competition_year(input_dir)
        if not year:
            continue

        df = pd.read_csv(athlete_csv, dtype=str, encoding="utf-8-sig").fillna("")
        for _, row in df.iterrows():
            raw_club = clean_extracted_text(row.get("club_name"))
            key = athlete_key(row)
            if not raw_club or not key:
                continue
            canonical = resolve_club_alias(raw_club, aliases) or raw_club
            observations.append(
                Observation(
                    year=year,
                    athlete_key=key,
                    athlete_label=clean_extracted_text(row.get("full_name")) or key[0],
                    raw_club=raw_club,
                    canonical_club=canonical,
                    source_url=source_url,
                )
            )

    return observations, missing_athlete_csv


def sample(values: Iterable[str], limit: int = 8) -> str:
    cleaned = sorted({value for value in values if value})
    return " | ".join(cleaned[:limit])


def build_candidate_rows(observations: Sequence[Observation], min_shared_athletes: int) -> Tuple[List[dict], int]:
    by_athlete_year: Dict[Tuple[str, AthleteKey], List[Observation]] = defaultdict(list)
    for obs in observations:
        by_athlete_year[(obs.year, obs.athlete_key)].append(obs)

    pair_evidence: Dict[Tuple[str, str], dict] = {}
    same_competition_conflicts = 0
    for (year, athlete), athlete_observations in by_athlete_year.items():
        by_club: Dict[str, List[Observation]] = defaultdict(list)
        for obs in athlete_observations:
            by_club[obs.canonical_club].append(obs)
        if len(by_club) < 2:
            continue
        for left, right in combinations(sorted(by_club), 2):
            left_observations = by_club[left]
            right_observations = by_club[right]
            left_urls = {obs.source_url for obs in left_observations}
            right_urls = {obs.source_url for obs in right_observations}
            shared_urls = left_urls & right_urls
            cross_urls = {
                f"{left_url} <> {right_url}"
                for left_url in left_urls
                for right_url in right_urls
                if left_url != right_url
            }
            if shared_urls:
                same_competition_conflicts += 1
            if not cross_urls:
                continue

            key = (left, right)
            record = pair_evidence.setdefault(
                key,
                {
                    "club_name_a": left,
                    "club_name_b": right,
                    "years": set(),
                    "shared_athletes": set(),
                    "cross_competition_pairs": set(),
                    "same_competition_conflict_urls": set(),
                    "raw_variants_a": set(),
                    "raw_variants_b": set(),
                    "source_urls": set(),
                },
            )
            record["years"].add(year)
            record["shared_athletes"].add(f"{athlete[0]} ({athlete[1] or 'unknown'}, {athlete[2] or 'unknown'})")
            record["cross_competition_pairs"].update(cross_urls)
            record["same_competition_conflict_urls"].update(shared_urls)
            record["raw_variants_a"].update(obs.raw_club for obs in left_observations)
            record["raw_variants_b"].update(obs.raw_club for obs in right_observations)
            record["source_urls"].update(obs.source_url for obs in left_observations + right_observations)

    rows = []
    for record in pair_evidence.values():
        shared_count = len(record["shared_athletes"])
        if shared_count < min_shared_athletes:
            continue
        rows.append(
            {
                "club_name_a": record["club_name_a"],
                "club_name_b": record["club_name_b"],
                "shared_athlete_years": shared_count,
                "years": sample(record["years"]),
                "cross_competition_pairs": len(record["cross_competition_pairs"]),
                "same_competition_conflicts": len(record["same_competition_conflict_urls"]),
                "shared_athletes_sample": sample(record["shared_athletes"]),
                "raw_variants_a": sample(record["raw_variants_a"]),
                "raw_variants_b": sample(record["raw_variants_b"]),
                "source_urls": sample(record["source_urls"], limit=20),
            }
        )
    return (
        sorted(rows, key=lambda row: (-row["shared_athlete_years"], row["club_name_a"], row["club_name_b"])),
        same_competition_conflicts,
    )


def build_alias_evidence_rows(observations: Sequence[Observation]) -> List[dict]:
    by_canonical: Dict[str, List[Observation]] = defaultdict(list)
    for obs in observations:
        by_canonical[obs.canonical_club].append(obs)

    rows = []
    for canonical, club_observations in sorted(by_canonical.items()):
        raw_variants = {obs.raw_club for obs in club_observations}
        if len(raw_variants) < 2:
            continue

        athlete_by_variant: Dict[str, Set[Tuple[str, AthleteKey, str]]] = defaultdict(set)
        urls_by_variant: Dict[str, Set[str]] = defaultdict(set)
        for obs in club_observations:
            athlete_by_variant[obs.raw_club].add((obs.year, obs.athlete_key, obs.source_url))
            urls_by_variant[obs.raw_club].add(obs.source_url)

        shared_pairs = 0
        same_competition_pairs = 0
        shared_athletes: Set[str] = set()
        for left, right in combinations(sorted(raw_variants), 2):
            left_observations = athlete_by_variant[left]
            right_observations = athlete_by_variant[right]
            overlap = {
                (left_year, left_athlete, left_url, right_url)
                for left_year, left_athlete, left_url in left_observations
                for right_year, right_athlete, right_url in right_observations
                if left_year == right_year and left_athlete == right_athlete and left_url != right_url
            }
            same_competition_overlap = {
                (left_year, left_athlete, left_url)
                for left_year, left_athlete, left_url in left_observations
                for right_year, right_athlete, right_url in right_observations
                if left_year == right_year and left_athlete == right_athlete and left_url == right_url
            }
            if overlap:
                shared_pairs += 1
                shared_athletes.update(f"{athlete[0]} ({year})" for year, athlete, _left_url, _right_url in overlap)
            if same_competition_overlap:
                same_competition_pairs += 1

        rows.append(
            {
                "canonical_name": canonical,
                "raw_variant_count": len(raw_variants),
                "raw_variants": sample(raw_variants, limit=30),
                "variant_pairs_with_cross_competition_same_athlete_year": shared_pairs,
                "variant_pairs_with_same_competition_conflicts": same_competition_pairs,
                "shared_athletes_sample": sample(shared_athletes),
                "source_urls": sample((url for urls in urls_by_variant.values() for url in urls), limit=30),
            }
        )

    return sorted(
        rows,
        key=lambda row: (
            -row["variant_pairs_with_cross_competition_same_athlete_year"],
            row["canonical_name"],
        ),
    )


def write_csv(path: Path, rows: Sequence[dict], fieldnames: Sequence[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    args = parse_args()
    manifest_path = Path(args.manifest)
    summary_path = Path(args.summary_json)
    candidate_path = Path(args.candidate_csv)
    alias_evidence_path = Path(args.alias_evidence_csv)

    aliases = load_club_aliases(args.club_alias_csv)
    documents = load_manifest(manifest_path)
    observations, missing_athlete_csv = read_observations(documents, aliases)
    candidate_rows, same_competition_conflicts = build_candidate_rows(observations, args.min_shared_athletes)
    alias_evidence_rows = build_alias_evidence_rows(observations)

    write_csv(
        candidate_path,
        candidate_rows,
        [
            "club_name_a",
            "club_name_b",
            "shared_athlete_years",
            "years",
            "cross_competition_pairs",
            "same_competition_conflicts",
            "shared_athletes_sample",
            "raw_variants_a",
            "raw_variants_b",
            "source_urls",
        ],
    )
    write_csv(
        alias_evidence_path,
        alias_evidence_rows,
        [
            "canonical_name",
            "raw_variant_count",
            "raw_variants",
            "variant_pairs_with_cross_competition_same_athlete_year",
            "variant_pairs_with_same_competition_conflicts",
            "shared_athletes_sample",
            "source_urls",
        ],
    )

    summary = {
        "state": "audited",
        "manifest_documents": len(documents),
        "athlete_observations": len(observations),
        "candidate_pairs": len(candidate_rows),
        "same_competition_conflicts_excluded": same_competition_conflicts,
        "alias_groups_with_multiple_raw_variants": len(alias_evidence_rows),
        "min_shared_athletes": args.min_shared_athletes,
        "missing_athlete_csv_documents": missing_athlete_csv,
        "candidate_csv": str(candidate_path),
        "alias_evidence_csv": str(alias_evidence_path),
    }
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    if args.json:
        print(json.dumps(summary, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
