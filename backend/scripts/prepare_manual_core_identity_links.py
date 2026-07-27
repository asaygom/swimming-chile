"""Prepare reviewed manual source-to-Core athlete links.

This stage converts human notes into a structured, DB-validated decision CSV.
The free-text notes are never consumed by the curation or load stages.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
from collections import Counter
from pathlib import Path
from typing import Optional, Sequence

from audit_expected_athlete_identity import (
    load_club_alias_maps,
    load_core_athletes,
    normalize_token_text,
    ordered_name_key,
    resolve_path,
    write_semicolon_dict_csv,
)


CORE_ID_RE = re.compile(r"\bID\s*[:#]?\s*(\d+)\b", re.IGNORECASE)
MANUAL_LINK_COLUMNS = [
    "decision",
    "validation_status",
    "target_athlete_id",
    "source_full_name",
    "source_athlete_key",
    "source_gender",
    "source_birth_year",
    "source_club_names",
    "source_club_keys",
    "source_tables",
    "source_urls",
    "core_full_name",
    "core_gender",
    "core_birth_year",
    "core_base_club_name",
    "core_current_club_name",
    "core_historical_club_names",
    "club_context_match",
    "name_matches_core",
    "gender_matches_core",
    "birth_year_matches_core",
    "suggested_canonical_full_name",
    "canonical_birth_year",
    "gender",
    "birth_year",
    "left_full_name",
    "left_birth_year",
    "right_full_name",
    "right_birth_year",
    "review_notes",
]
CANONICAL_UPDATE_COLUMNS = [
    "target_athlete_id",
    "current_core_names",
    "proposed_canonical_full_name",
    "source_full_names",
    "decision_source",
]
CANONICAL_COLLISION_COLUMNS = [
    "target_athlete_id",
    "target_current_core_name",
    "proposed_canonical_full_name",
    "target_gender",
    "target_birth_year",
    "collision_athlete_id",
    "collision_core_full_name",
    "collision_base_club_name",
    "collision_current_club_name",
    "required_action",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Transforma notes revisadas en links de identidad estructurados y validados contra Core."
    )
    parser.add_argument("--notes-review-csv", required=True)
    parser.add_argument("--core-candidates-review-csv", required=True)
    parser.add_argument("--structured-decisions-csv", required=True)
    parser.add_argument("--canonical-updates-review-csv", required=True)
    parser.add_argument("--canonical-collisions-review-csv", required=True)
    parser.add_argument("--summary-json", required=True)
    parser.add_argument("--club-alias-csv")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", default="5432")
    parser.add_argument("--dbname", default="natacion_chile")
    parser.add_argument("--user", default="postgres")
    parser.add_argument("--password")
    parser.add_argument("--schema", default="core")
    parser.add_argument("--json", action="store_true")
    return parser.parse_args()


def read_dict_rows(path: Path) -> list[dict]:
    raw = path.read_bytes()
    text = None
    for encoding in ("utf-8-sig", "cp1252"):
        try:
            text = raw.decode(encoding)
            break
        except UnicodeDecodeError:
            continue
    if text is None:
        raise ValueError(f"No se pudo decodificar {path}.")
    if not text.strip():
        return []
    first_line = text.splitlines()[0]
    delimiter = ";" if first_line.count(";") > first_line.count(",") else ","
    return list(csv.DictReader(text.splitlines(), delimiter=delimiter))


def extract_target_id(notes: object) -> Optional[int]:
    text = str(notes or "").strip()
    if not text:
        return None
    matches = CORE_ID_RE.findall(text)
    if len(matches) != 1:
        raise ValueError(f"La nota marcada debe contener exactamente un ID de Core: {text!r}")
    return int(matches[0])


def _match_label(left: object, right: object, *, normalized: bool = False) -> str:
    left_text = str(left or "").strip()
    right_text = str(right or "").strip()
    if not left_text or not right_text:
        return "unknown"
    if normalized:
        return "yes" if normalize_token_text(left_text) == normalize_token_text(right_text) else "no"
    return "yes" if left_text == right_text else "no"


def _club_key_set(value: object) -> set[str]:
    return {part.strip() for part in str(value or "").split(" | ") if part.strip()}


def build_manual_link_rows(review_rows: Sequence[dict], core_by_id: dict[int, dict]) -> list[dict]:
    links: list[dict] = []
    for line_number, source in enumerate(review_rows, start=2):
        try:
            target_id = extract_target_id(source.get("notes"))
        except ValueError as exc:
            raise ValueError(f"Fila {line_number}: {exc}") from exc
        if target_id is None:
            continue
        core = core_by_id.get(target_id)
        if core is None:
            raise ValueError(f"Fila {line_number}: athlete_id={target_id} no existe en Core.")

        source_name = str(source.get("source_full_name") or "").strip()
        source_gender = str(source.get("gender") or "").strip()
        source_birth_year = str(source.get("birth_year") or "").strip()
        core_name = str(core.get("full_name") or "").strip()
        core_gender = str(core.get("gender") or "").strip()
        core_birth_year = str(core.get("birth_year") or "").strip()
        if not source_name or not source_birth_year:
            raise ValueError(f"Fila {line_number}: identidad fuente incompleta.")
        if not core_name or not core_birth_year:
            raise ValueError(f"Fila {line_number}: athlete_id={target_id} no tiene nombre/año canónico completo.")
        if source_gender and core_gender and source_gender != core_gender:
            raise ValueError(
                f"Fila {line_number}: género fuente {source_gender!r} contradice Core {core_gender!r} "
                f"para athlete_id={target_id}."
            )

        source_club_keys = str(source.get("source_club_keys") or "").strip()
        core_context_keys = (
            _club_key_set(core.get("club_key"))
            | _club_key_set(core.get("current_club_key"))
            | _club_key_set(core.get("historical_club_keys"))
        )
        context_match = bool(_club_key_set(source_club_keys) & core_context_keys)
        canonical_gender = core_gender or source_gender
        links.append(
            {
                "decision": "merge",
                "validation_status": "validated_core_id",
                "target_athlete_id": str(target_id),
                "source_full_name": source_name,
                "source_athlete_key": str(source.get("source_athlete_key") or ordered_name_key(source_name)),
                "source_gender": source_gender,
                "source_birth_year": source_birth_year,
                "source_club_names": str(source.get("source_club_names") or "").strip(),
                "source_club_keys": source_club_keys,
                "source_tables": str(source.get("source_tables") or "").strip(),
                "source_urls": str(source.get("source_urls") or "").strip(),
                "core_full_name": core_name,
                "core_gender": core_gender,
                "core_birth_year": core_birth_year,
                "core_base_club_name": str(core.get("club_name") or "").strip(),
                "core_current_club_name": str(core.get("current_club_name") or "").strip(),
                "core_historical_club_names": str(core.get("historical_club_names") or "").strip(),
                "club_context_match": "yes" if context_match else "no",
                "name_matches_core": _match_label(source_name, core_name, normalized=True),
                "gender_matches_core": _match_label(source_gender, core_gender),
                "birth_year_matches_core": _match_label(source_birth_year, core_birth_year),
                "suggested_canonical_full_name": core_name,
                "canonical_birth_year": core_birth_year,
                # Compatibility fields consumed by curate_athlete_names.py.
                "gender": canonical_gender,
                "birth_year": source_birth_year,
                "left_full_name": source_name,
                "left_birth_year": source_birth_year,
                "right_full_name": core_name,
                "right_birth_year": core_birth_year,
                "review_notes": str(source.get("notes") or "").strip(),
            }
        )
    return links


def validate_core_candidate_rows(rows: Sequence[dict], core_by_id: dict[int, dict]) -> Counter:
    counts: Counter = Counter()
    for line_number, row in enumerate(rows, start=2):
        decision = str(row.get("decision") or "").strip().lower()
        if decision not in {"merge", "no_merge"}:
            raise ValueError(f"Fila {line_number}: decisión inválida o vacía: {decision!r}.")
        raw_id = str(row.get("core_athlete_id") or "").strip()
        try:
            target_id = int(raw_id)
        except ValueError as exc:
            raise ValueError(f"Fila {line_number}: core_athlete_id inválido: {raw_id!r}.") from exc
        core = core_by_id.get(target_id)
        if core is None:
            raise ValueError(f"Fila {line_number}: athlete_id={target_id} no existe en Core.")
        reviewed_core_name = str(row.get("core_full_name") or "").strip()
        current_core_name = str(core.get("full_name") or "").strip()
        if reviewed_core_name != current_core_name:
            raise ValueError(
                f"Fila {line_number}: el nombre Core cambió para athlete_id={target_id}: "
                f"{reviewed_core_name!r} != {current_core_name!r}."
            )
        counts[decision] += 1
    return counts


def build_canonical_update_rows(rows: Sequence[dict], core_by_id: dict[int, dict]) -> list[dict]:
    validate_core_candidate_rows(rows, core_by_id)
    updates: dict[int, dict] = {}
    for row in rows:
        if str(row.get("decision") or "").strip().lower() != "merge":
            continue
        target_id = int(str(row.get("core_athlete_id") or "").strip())
        current_name = str(core_by_id[target_id].get("full_name") or "").strip()
        proposed_name = str(
            row.get("suggested_canonical_full_name") or row.get("core_full_name") or ""
        ).strip()
        if not proposed_name or proposed_name == current_name:
            continue
        source_name = str(row.get("source_full_name") or "").strip()
        previous = updates.get(target_id)
        if previous and previous["proposed_canonical_full_name"] != proposed_name:
            raise ValueError(f"Decisiones contradictorias de nombre canónico para athlete_id={target_id}.")
        if previous:
            names = {part for part in previous["source_full_names"].split(" | ") if part}
            if source_name:
                names.add(source_name)
            previous["source_full_names"] = " | ".join(sorted(names))
            continue
        updates[target_id] = {
            "target_athlete_id": str(target_id),
            "current_core_names": current_name,
            "proposed_canonical_full_name": proposed_name,
            "source_full_names": source_name,
            "decision_source": "core_identity_candidates_review",
        }
    return [updates[target_id] for target_id in sorted(updates)]


def build_canonical_collision_rows(updates: Sequence[dict], core_rows: Sequence[dict]) -> list[dict]:
    core_by_id = {int(row["core_athlete_id"]): row for row in core_rows}
    collisions: list[dict] = []
    for update in updates:
        target_id = int(update["target_athlete_id"])
        target = core_by_id[target_id]
        proposed = str(update["proposed_canonical_full_name"]).strip()
        for other in core_rows:
            other_id = int(other["core_athlete_id"])
            if other_id == target_id:
                continue
            same_name = str(other.get("full_name") or "").strip().casefold() == proposed.casefold()
            same_gender = (other.get("gender") or None) == (target.get("gender") or None)
            same_birth_year = str(other.get("birth_year") or "") == str(target.get("birth_year") or "")
            if not (same_name and same_gender and same_birth_year):
                continue
            collisions.append(
                {
                    "target_athlete_id": str(target_id),
                    "target_current_core_name": str(target.get("full_name") or "").strip(),
                    "proposed_canonical_full_name": proposed,
                    "target_gender": str(target.get("gender") or "").strip(),
                    "target_birth_year": str(target.get("birth_year") or "").strip(),
                    "collision_athlete_id": str(other_id),
                    "collision_core_full_name": str(other.get("full_name") or "").strip(),
                    "collision_base_club_name": str(other.get("club_name") or "").strip(),
                    "collision_current_club_name": str(other.get("current_club_name") or "").strip(),
                    "required_action": "review_core_identity_merge_before_name_update",
                }
            )
    return sorted(
        collisions,
        key=lambda row: (int(row["target_athlete_id"]), int(row["collision_athlete_id"])),
    )


def main() -> int:
    args = parse_args()
    notes_path = resolve_path(args.notes_review_csv)
    candidates_path = resolve_path(args.core_candidates_review_csv)
    structured_path = resolve_path(args.structured_decisions_csv)
    canonical_path = resolve_path(args.canonical_updates_review_csv)
    collision_path = resolve_path(args.canonical_collisions_review_csv)
    summary_path = resolve_path(args.summary_json)

    club_aliases = {}
    if args.club_alias_csv:
        club_aliases, _ = load_club_alias_maps(resolve_path(args.club_alias_csv))
    core_rows = load_core_athletes(args, club_aliases)
    core_by_id = {int(row["core_athlete_id"]): row for row in core_rows}
    note_rows = read_dict_rows(notes_path)
    candidate_rows = read_dict_rows(candidates_path)

    manual_links = build_manual_link_rows(note_rows, core_by_id)
    decision_counts = validate_core_candidate_rows(candidate_rows, core_by_id)
    canonical_updates = build_canonical_update_rows(candidate_rows, core_by_id)
    canonical_collisions = build_canonical_collision_rows(canonical_updates, core_rows)

    write_semicolon_dict_csv(structured_path, manual_links, MANUAL_LINK_COLUMNS)
    write_semicolon_dict_csv(canonical_path, canonical_updates, CANONICAL_UPDATE_COLUMNS)
    write_semicolon_dict_csv(collision_path, canonical_collisions, CANONICAL_COLLISION_COLUMNS)
    summary = {
        "state": "validated",
        "notes_review_csv": str(notes_path),
        "core_candidates_review_csv": str(candidates_path),
        "structured_decisions_csv": str(structured_path),
        "canonical_updates_review_csv": str(canonical_path),
        "canonical_collisions_review_csv": str(collision_path),
        "reviewed_note_row_count": sum(bool(str(row.get("notes") or "").strip()) for row in note_rows),
        "manual_core_link_count": len(manual_links),
        "manual_name_mismatch_count": sum(row["name_matches_core"] == "no" for row in manual_links),
        "manual_birth_year_mismatch_count": sum(row["birth_year_matches_core"] == "no" for row in manual_links),
        "manual_club_context_mismatch_count": sum(row["club_context_match"] == "no" for row in manual_links),
        "core_candidate_decision_counts": dict(sorted(decision_counts.items())),
        "canonical_name_update_count": len(canonical_updates),
        "canonical_name_collision_count": len(canonical_collisions),
    }
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    if args.json:
        print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
