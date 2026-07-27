import sys
from pathlib import Path

import pytest


BACKEND_DIR = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = BACKEND_DIR / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

import prepare_manual_core_identity_links as prepare


def test_extract_target_id_requires_exactly_one_marked_id():
    assert prepare.extract_target_id("existe en BD, ID 698. parser solapó la edad") == 698
    assert prepare.extract_target_id("  ") is None

    with pytest.raises(ValueError, match="exactamente un ID"):
        prepare.extract_target_id("comparar ID 698 con ID 699")

    with pytest.raises(ValueError, match="exactamente un ID"):
        prepare.extract_target_id("existe en BD")


def test_build_manual_links_uses_core_canonical_name_year_and_context():
    review_rows = [
        {
            "source_full_name": "Hernandez, Williams Antonio",
            "source_athlete_key": "hernandez williams antonio",
            "gender": "male",
            "birth_year": "1983",
            "source_club_names": "Atlantis",
            "source_club_keys": "atlantis",
            "source_tables": "athlete | result",
            "source_urls": "stage-6",
            "notes": "existe en BD, ID 698. parser no extrajo bien la edad",
        }
    ]
    core_by_id = {
        698: {
            "core_athlete_id": 698,
            "full_name": "Hernandez Soto, Williams Antonio",
            "gender": "male",
            "birth_year": "1982",
            "club_name": "Atlantis",
            "club_key": "atlantis",
            "current_club_name": "Atlantis",
            "current_club_key": "atlantis",
            "historical_club_names": "Atlantis | Club Antiguo",
            "historical_club_keys": "atlantis | club antiguo",
        }
    }

    rows = prepare.build_manual_link_rows(review_rows, core_by_id)

    assert rows == [
        {
            "decision": "merge",
            "validation_status": "validated_core_id",
            "target_athlete_id": "698",
            "source_full_name": "Hernandez, Williams Antonio",
            "source_athlete_key": "hernandez williams antonio",
            "source_gender": "male",
            "source_birth_year": "1983",
            "source_club_names": "Atlantis",
            "source_club_keys": "atlantis",
            "source_tables": "athlete | result",
            "source_urls": "stage-6",
            "core_full_name": "Hernandez Soto, Williams Antonio",
            "core_gender": "male",
            "core_birth_year": "1982",
            "core_base_club_name": "Atlantis",
            "core_current_club_name": "Atlantis",
            "core_historical_club_names": "Atlantis | Club Antiguo",
            "club_context_match": "yes",
            "name_matches_core": "no",
            "gender_matches_core": "yes",
            "birth_year_matches_core": "no",
            "suggested_canonical_full_name": "Hernandez Soto, Williams Antonio",
            "canonical_birth_year": "1982",
            "gender": "male",
            "birth_year": "1983",
            "left_full_name": "Hernandez, Williams Antonio",
            "left_birth_year": "1983",
            "right_full_name": "Hernandez Soto, Williams Antonio",
            "right_birth_year": "1982",
            "review_notes": "existe en BD, ID 698. parser no extrajo bien la edad",
        }
    ]


def test_build_manual_links_rejects_unknown_core_id():
    with pytest.raises(ValueError, match="no existe en Core"):
        prepare.build_manual_link_rows(
            [
                {
                    "source_full_name": "Persona, Nueva",
                    "birth_year": "1990",
                    "gender": "female",
                    "notes": "existe en BD, ID 999",
                }
            ],
            {},
        )


def test_build_canonical_update_rows_respects_merge_and_no_merge():
    core_by_id = {
        264: {"core_athlete_id": 264, "full_name": "Torres, Sergio", "gender": "male", "birth_year": "1994"},
        265: {"core_athlete_id": 265, "full_name": "Torres Lozada, Sergio", "gender": "male", "birth_year": "1994"},
        5117: {"core_athlete_id": 5117, "full_name": "Aguilera, Luis", "gender": "male", "birth_year": "1984"},
    }
    reviewed_candidates = [
        {
            "decision": "merge",
            "core_athlete_id": "5117",
            "core_full_name": "Aguilera, Luis",
            "suggested_canonical_full_name": "Aguilera, Luis Octavio",
            "source_full_name": "Aguilera, Luis Octavio",
        },
        {
            "decision": "merge",
            "core_athlete_id": "264",
            "core_full_name": "Torres, Sergio",
            "suggested_canonical_full_name": "Torres, Sergio",
            "source_full_name": "Torres, Sergio",
        },
        {
            "decision": "no_merge",
            "core_athlete_id": "265",
            "core_full_name": "Torres Lozada, Sergio",
            "suggested_canonical_full_name": "Torres, Sergio",
            "source_full_name": "Torres, Sergio",
        },
    ]

    rows = prepare.build_canonical_update_rows(reviewed_candidates, core_by_id)

    assert rows == [
        {
            "target_athlete_id": "5117",
            "current_core_names": "Aguilera, Luis",
            "proposed_canonical_full_name": "Aguilera, Luis Octavio",
            "source_full_names": "Aguilera, Luis Octavio",
            "decision_source": "core_identity_candidates_review",
        }
    ]


def test_build_canonical_update_rows_rejects_stale_core_name():
    with pytest.raises(ValueError, match="nombre Core cambió"):
        prepare.build_canonical_update_rows(
            [
                {
                    "decision": "merge",
                    "core_athlete_id": "10",
                    "core_full_name": "Nombre, Antiguo",
                    "suggested_canonical_full_name": "Nombre Completo, Correcto",
                }
            ],
            {10: {"core_athlete_id": 10, "full_name": "Nombre, Actual"}},
        )


def test_build_canonical_collision_rows_exposes_existing_exact_identity():
    updates = [
        {
            "target_athlete_id": "495",
            "current_core_names": "Sanchez, Marcela",
            "proposed_canonical_full_name": "Acori Sanchez, Patricia Marcela",
            "source_full_names": "Acori Sanchez, Patricia Marcela",
            "decision_source": "core_identity_candidates_review",
        }
    ]
    core_rows = [
        {
            "core_athlete_id": 495,
            "full_name": "Sanchez, Marcela",
            "gender": "female",
            "birth_year": "1973",
            "club_name": "Club A",
            "current_club_name": "Club A",
        },
        {
            "core_athlete_id": 3701,
            "full_name": "Acori Sanchez, Patricia Marcela",
            "gender": "female",
            "birth_year": "1973",
            "club_name": "Club B",
            "current_club_name": "Club B",
        },
    ]

    rows = prepare.build_canonical_collision_rows(updates, core_rows)

    assert rows == [
        {
            "target_athlete_id": "495",
            "target_current_core_name": "Sanchez, Marcela",
            "proposed_canonical_full_name": "Acori Sanchez, Patricia Marcela",
            "target_gender": "female",
            "target_birth_year": "1973",
            "collision_athlete_id": "3701",
            "collision_core_full_name": "Acori Sanchez, Patricia Marcela",
            "collision_base_club_name": "Club B",
            "collision_current_club_name": "Club B",
            "required_action": "review_core_identity_merge_before_name_update",
        }
    ]
