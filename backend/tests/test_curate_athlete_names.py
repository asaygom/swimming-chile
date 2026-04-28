import shutil
import sys
import uuid
from pathlib import Path

import pandas as pd

BACKEND_DIR = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = BACKEND_DIR / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

import curate_athlete_names as curate


def _workspace_tmp_dir() -> Path:
    path = BACKEND_DIR / "data" / "raw" / "batch_summaries" / f"test_curate_athlete_names_{uuid.uuid4().hex}"
    path.mkdir(parents=True)
    return path


def test_athlete_name_signature_groups_common_ocr_variants():
    assert curate.athlete_name_signature("Pasarin, Claudia") == curate.athlete_name_signature(
        "Pasar\u00ed\u00f3n, Claudia"
    )
    assert curate.athlete_name_signature("Gomez, Francisco") == curate.athlete_name_signature(
        "Go\u00e1mez, Francisco"
    )
    assert curate.athlete_name_signature("Muller, Bettina") == curate.athlete_name_signature(
        "Mu\u00fcller, Bettina"
    )


def test_build_review_rows_prefers_cleaner_canonical_names():
    rows = [
        {"athlete_name": "Pasarin, Claudia", "source_url": "a", "club_name": "Club A", "birth_year": "1964", "gender": "female"},
        {"athlete_name": "Pasar\u00ed\u00e1n, Claudia", "source_url": "b", "club_name": "Club A", "birth_year": "1964", "gender": "female"},
        {"athlete_name": "Pasar\u00ed\u00f3n, Claudia", "source_url": "c", "club_name": "Club A", "birth_year": "1964", "gender": "female"},
        {"athlete_name": "Mu\u00fcller, Bettina", "source_url": "d", "club_name": "Club B", "birth_year": "1964", "gender": "female"},
        {"athlete_name": "Muller, Bettina", "source_url": "e", "club_name": "Club B", "birth_year": "1964", "gender": "female"},
    ]

    review_rows, replacement_map = curate.build_review_rows(rows)

    canonical_by_signature = {row["signature"]: row["canonical_name"] for row in review_rows}
    assert "Pasarin, Claudia" in canonical_by_signature.values()
    assert "Muller, Bettina" in canonical_by_signature.values()
    assert replacement_map[("Pasar\u00ed\u00f3n, Claudia", "1964", "club a", "female")] == "Pasarin, Claudia"
    assert replacement_map[("Mu\u00fcller, Bettina", "1964", "club b", "female")] == "Muller, Bettina"


def test_build_review_rows_requires_birth_year_and_club_context():
    rows = [
        {"athlete_name": "Gomez, Francisco", "source_url": "a", "club_name": "Club A", "birth_year": "1987", "gender": "male"},
        {"athlete_name": "Go\u00e1mez, Francisco", "source_url": "b", "club_name": "Club A", "birth_year": "1987", "gender": "male"},
        {"athlete_name": "Go\u00e9mez, Francisco", "source_url": "c", "club_name": "Club B", "birth_year": "1987", "gender": "male"},
        {"athlete_name": "Go\u00f3mez, Francisco", "source_url": "d", "club_name": "Club A", "birth_year": "1988", "gender": "male"},
        {"athlete_name": "Go\u00famez, Francisco", "source_url": "e", "club_name": "Club A", "birth_year": "", "gender": "male"},
    ]

    _, replacement_map = curate.build_review_rows(rows)

    assert replacement_map == {
        ("Go\u00e1mez, Francisco", "1987", "club a", "male"): "Gomez, Francisco",
    }


def test_build_review_rows_does_not_apply_broad_name_collisions():
    rows = [
        {"athlete_name": "Alfaro, Mauricio", "source_url": "a", "club_name": "Club A", "birth_year": "1990", "gender": "male"},
        {"athlete_name": "Alfaro, Marco", "source_url": "b", "club_name": "Club A", "birth_year": "1990", "gender": "male"},
        {"athlete_name": "Augusto, Gloria", "source_url": "c", "club_name": "Club C", "birth_year": "1971", "gender": "female"},
        {"athlete_name": "Agusto, Gloria", "source_url": "d", "club_name": "Club C", "birth_year": "1971", "gender": "female"},
        {"athlete_name": "Augusto, Gloria", "source_url": "e", "club_name": "Club C", "birth_year": "1971", "gender": "female"},
        {"athlete_name": "Barrios, Sergio", "source_url": "f", "club_name": "Club D", "birth_year": "1998", "gender": "male"},
        {"athlete_name": "Barros, Sergio", "source_url": "g", "club_name": "Club D", "birth_year": "1998", "gender": "male"},
    ]

    _, replacement_map = curate.build_review_rows(rows)

    assert "Alfaro, Mauricio" not in [key[0] for key in replacement_map]
    assert "Augusto, Gloria" not in [key[0] for key in replacement_map]
    assert "Agusto, Gloria" not in [key[0] for key in replacement_map]
    assert "Barrios, Sergio" not in [key[0] for key in replacement_map]


def test_collect_name_rows_reads_parser_tables():
    tmp_dir = _workspace_tmp_dir()
    try:
        input_dir = tmp_dir / "parsed"
        input_dir.mkdir()
        pd.DataFrame(
            [{"full_name": "Cofre\u00e1, Patricio", "club_name": "Club Test", "gender": "male", "birth_year": "1980"}]
        ).to_csv(input_dir / "athlete.csv", index=False)
        pd.DataFrame(
            [{"athlete_name": "Go\u00e1mez, Francisco", "club_name": "Club Test", "birth_year_estimated": "1975"}]
        ).to_csv(input_dir / "result.csv", index=False)
        pd.DataFrame(
            [{"swimmer_name": "Mu\u00fcller, Bettina", "club_name": "Club Test", "gender": "female", "birth_year_estimated": "1982"}]
        ).to_csv(input_dir / "relay_swimmer.csv", index=False)

        rows = curate.collect_name_rows({"source_url": "https://example.test/resultados.pdf"}, input_dir)

        assert [row["table"] for row in rows] == ["athlete", "result", "relay_swimmer"]
        assert rows[0]["athlete_name"] == "Cofre\u00e1, Patricio"
        assert rows[1]["athlete_name"] == "Go\u00e1mez, Francisco"
        assert rows[2]["athlete_name"] == "Mu\u00fcller, Bettina"
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


def test_apply_athlete_curations_to_df_updates_names_and_birth_years():
    df = pd.DataFrame(
        [
            {
                "full_name": "Go\u00e1mez, Francisco",
                "club_name": "Club Test",
                "gender": "male",
                "birth_year": "1975",
            },
            {
                "full_name": "Acevedo, Luis A",
                "club_name": "Natacion Master Recoleta",
                "gender": "male",
                "birth_year": "1969",
            },
            {
                "full_name": "Arantxa Aranguren",
                "club_name": "Lozada Swim Team",
                "gender": "female",
                "birth_year": "",
            },
            {
                "full_name": "Abbott, Andres",
                "club_name": "Efecto Peruga",
                "gender": "male",
                "birth_year": "1985",
            },
        ]
    )
    rules = {
        "ocr_name_rules": [
            {
                "old_key": "goamez francisco",
                "new_name": "Gomez, Francisco",
                "new_key": "gomez francisco",
                "birth_year": "1975",
                "club_key": "club test",
                "gender": "male",
            }
        ],
        "birth_year_rules": {("abbott andres", "male", "efecto peruga"): "1984"},
        "missing_birth_year_rules": [
            {
                "old_key": "arantxa aranguren",
                "new_name": "Aranguren, Arantxa",
                "new_key": "aranguren arantxa",
                "birth_year": "1994",
                "club_key": "lozada swim team",
                "gender": "female",
            }
        ],
        "partial_name_rules": [
            {
                "old_key": "acevedo luis a",
                "new_name": "Acevedo, Luis Alberto",
                "new_key": "acevedo luis alberto",
                "birth_year": "1969",
                "club_key": "natacion master recoleta",
                "gender": "male",
            }
        ],
    }

    curated, counts = curate.apply_athlete_curations_to_df(df, "athlete", rules)

    assert curated.loc[0, "full_name"] == "Gomez, Francisco"
    assert curated.loc[1, "full_name"] == "Acevedo, Luis Alberto"
    assert curated.loc[2, "full_name"] == "Aranguren, Arantxa"
    assert curated.loc[2, "birth_year"] == "1994"
    assert curated.loc[3, "birth_year"] == "1984"
    assert counts == {
        "known_ocr_name_residue_repairs": 1,
        "birth_year_corrections": 1,
        "missing_birth_year_consolidations": 1,
        "partial_name_consolidations": 1,
    }


def test_partial_name_rules_chain_and_apply_by_identity_when_unambiguous():
    rules = {
        "ocr_name_rules": [],
        "birth_year_rules": {},
        "missing_birth_year_rules": [],
        "partial_name_rules": curate.resolve_partial_name_rule_chains(
            [
                {
                    "old_key": "acevedo luis",
                    "new_name": "Acevedo, Luis A",
                    "new_key": "acevedo luis a",
                    "birth_year": "1969",
                    "club_key": "natacion master recoleta",
                    "gender": "male",
                },
                {
                    "old_key": "acevedo luis a",
                    "new_name": "Acevedo, Luis Alberto",
                    "new_key": "acevedo luis alberto",
                    "birth_year": "1969",
                    "club_key": "natacion master recoleta",
                    "gender": "male",
                },
            ]
        ),
        "comma_order_rules": [],
    }
    rules["partial_name_identity_rules"] = curate.build_partial_name_identity_rules(rules["partial_name_rules"])
    df = pd.DataFrame(
        [
            {
                "full_name": "Acevedo, Luis",
                "club_name": "Natacion Recoleta",
                "birth_year": "1969",
                "gender": "male",
            },
            {
                "full_name": "Acevedo, Luis A",
                "club_name": "Master Recoleta",
                "birth_year": "1969",
                "gender": "male",
            },
        ]
    )

    curated, counts = curate.apply_athlete_curations_to_df(df, "athlete", rules)

    assert curated["full_name"].tolist() == ["Acevedo, Luis Alberto", "Acevedo, Luis Alberto"]
    assert counts == {"partial_name_identity_consolidations": 2}


def test_repair_known_ocr_name_residue_materializes_known_patterns():
    assert curate.repair_known_ocr_name_residue("A\u00c1lvarez, Alex") == "Alvarez, Alex"
    assert curate.repair_known_ocr_name_residue("Cofre\u00e1, Patricio") == "Cofre, Patricio"
    assert curate.repair_known_ocr_name_residue("Brice\u00f1 \u00f1o, Da\u00f1iel") == "Brice\u00f1o, Da\u00f1iel"
    assert curate.repair_known_ocr_name_residue("Ya\u00f1 \u00f1ez, Roberto") == "Ya\u00f1ez, Roberto"
    assert curate.repair_known_ocr_name_residue("Mar\u00ed\u00e1 Jos\u00e9, Bocaz") == "Maria Jos\u00e9, Bocaz"
    assert curate.repair_known_ocr_name_residue("Olivares O\u00c1 rdenes, Cristi\u00e1n") == "Olivares Ordenes, Cristi\u00e1n"


def test_canonicalize_space_ordered_name_preserves_surname_particles():
    assert curate.canonicalize_space_ordered_name("Eduardo Nuñez") == "Nuñez, Eduardo"
    assert (
        curate.canonicalize_space_ordered_name("Maria Antonieta de La Maza")
        == "de La Maza, Maria Antonieta"
    )
    assert curate.canonicalize_space_ordered_name("Rojas, Jorge") == "Rojas, Jorge"


def test_apply_athlete_curations_to_df_canonicalizes_space_ordered_names():
    df = pd.DataFrame(
        [
            {
                "full_name": "Jennifer Gomez",
                "club_name": "Delfines",
                "birth_year": "",
                "gender": "female",
            }
        ]
    )
    rules = {
        "ocr_name_rules": [],
        "birth_year_rules": {},
        "missing_birth_year_rules": [],
        "partial_name_rules": [],
    }

    curated, counts = curate.apply_athlete_curations_to_df(df, "athlete", rules)

    assert curated.loc[0, "full_name"] == "Gomez, Jennifer"
    assert counts == {"space_order_name_canonicalizations": 1}


def test_apply_athlete_curations_to_df_does_not_flip_space_ordered_names_with_birth_year():
    df = pd.DataFrame(
        [
            {
                "full_name": "Herrera Adriana",
                "club_name": "Turquesa",
                "birth_year": "1974",
                "gender": "female",
            }
        ]
    )
    rules = {
        "ocr_name_rules": [],
        "birth_year_rules": {},
        "missing_birth_year_rules": [],
        "partial_name_rules": [],
    }

    curated, counts = curate.apply_athlete_curations_to_df(df, "athlete", rules)

    assert curated.loc[0, "full_name"] == "Herrera Adriana"
    assert counts == {}


def test_apply_athlete_curations_to_df_corrects_likely_comma_order_from_corpus_rule():
    df = pd.DataFrame(
        [
            {
                "full_name": "Adriana, Herrera",
                "club_name": "Turquesa",
                "birth_year": "1974",
                "gender": "female",
            }
        ]
    )
    rules = {
        "ocr_name_rules": [],
        "birth_year_rules": {},
        "missing_birth_year_rules": [],
        "partial_name_rules": [],
        "comma_order_rules": [
            {
                "old_key": "adriana herrera",
                "new_name": "Herrera, Adriana",
                "new_key": "herrera adriana",
                "birth_year": "1974",
                "club_key": "turquesa",
                "gender": "female",
            }
        ],
    }

    curated, counts = curate.apply_athlete_curations_to_df(df, "athlete", rules)

    assert curated.loc[0, "full_name"] == "Herrera, Adriana"
    assert counts == {"comma_order_corrections": 1}


def test_apply_athlete_curations_to_df_corrects_comma_order_without_club_when_unambiguous():
    rules = {
        "ocr_name_rules": [],
        "birth_year_rules": {},
        "missing_birth_year_rules": [],
        "partial_name_rules": [],
        "comma_order_rules": [
            {
                "old_key": "natalia silva",
                "new_name": "Silva, Natalia",
                "new_key": "silva natalia",
                "birth_year": "1985",
                "club_key": "agua plena san rafael",
                "gender": "female",
            }
        ],
    }
    rules["comma_order_identity_rules"] = curate.build_comma_order_identity_rules(rules["comma_order_rules"])
    df = pd.DataFrame(
        [
            {
                "event_name": "women 120-159 4x50 SC Meter medley_relay",
                "relay_team_name": "Agua Plena San Rafael A",
                "leg_order": "2",
                "swimmer_name": "Natalia, Silva",
                "gender": "female",
                "birth_year_estimated": "1985",
            }
        ]
    )

    curated, counts = curate.apply_athlete_curations_to_df(df, "relay_swimmer", rules)

    assert curated.loc[0, "swimmer_name"] == "Silva, Natalia"
    assert counts == {"comma_order_corrections": 1}


def test_drop_result_rows_with_athlete_gender_conflict():
    athlete_df = pd.DataFrame(
        [
            {
                "full_name": "Henriquez, Soledad",
                "gender": "female",
                "club_name": "MSBDO",
                "birth_year": "1984",
            },
            {
                "full_name": "Rivera, Pedro Pablo",
                "gender": "male",
                "club_name": "SIMAS",
                "birth_year": "1980",
            },
            {
                "full_name": "Perez, Paulina",
                "gender": "male",
                "club_name": "RECOL",
                "birth_year": "1970",
            },
        ]
    )
    result_df = pd.DataFrame(
        [
            {
                "event_name": "men 35-39 50 LC Meter breaststroke",
                "athlete_name": "Henriquez, Soledad",
                "club_name": "MSBDO",
                "birth_year_estimated": "1984",
                "result_time_ms": "252110",
            },
            {
                "event_name": "men 40-44 50 LC Meter breaststroke",
                "athlete_name": "Rivera, Pedro Pablo",
                "club_name": "SIMAS",
                "birth_year_estimated": "1980",
                "result_time_ms": "32000",
            },
            {
                "event_name": "men 50-54 200 LC Meter backstroke",
                "athlete_name": "Perez Pacheco, Paulina",
                "club_name": "RECOL",
                "birth_year_estimated": "1970",
                "result_time_ms": "48450",
            },
            {
                "event_name": "women 50-54 50 LC Meter butterfly",
                "athlete_name": "Perez Pacheco, Paulina",
                "club_name": "RECOL",
                "birth_year_estimated": "1970",
                "result_time_ms": "61160",
            },
        ]
    )

    filtered, dropped = curate.drop_result_rows_with_athlete_gender_conflict(result_df, athlete_df)

    assert dropped == 2
    assert filtered["athlete_name"].tolist() == ["Rivera, Pedro Pablo", "Perez Pacheco, Paulina"]
    assert filtered["event_name"].tolist() == [
        "men 40-44 50 LC Meter breaststroke",
        "women 50-54 50 LC Meter butterfly",
    ]


def test_apply_athlete_curations_to_df_repairs_relay_swimmer_without_club_column():
    df = pd.DataFrame(
        [
            {
                "event_name": "mixed 200 LC Meter freestyle_relay",
                "relay_team_name": "Club A",
                "swimmer_name": "A\u00c1lvarez, Alonso",
                "birth_year_estimated": "1987",
                "gender": "male",
            }
        ]
    )
    rules = {
        "ocr_name_rules": [],
        "birth_year_rules": {},
        "missing_birth_year_rules": [],
        "partial_name_rules": [],
    }

    curated, counts = curate.apply_athlete_curations_to_df(df, "relay_swimmer", rules)

    assert curated.loc[0, "swimmer_name"] == "Alvarez, Alonso"
    assert counts == {"known_ocr_name_residue_repairs": 1}


def test_apply_athlete_curations_to_df_applies_identity_rule_to_relay_swimmer_without_club_column():
    df = pd.DataFrame(
        [
            {
                "event_name": "mixed 200 LC Meter freestyle_relay",
                "relay_team_name": "Master Recoleta A",
                "swimmer_name": "Acevedo, Luis A",
                "birth_year_estimated": "1969",
                "gender": "male",
            }
        ]
    )
    rules = {
        "ocr_name_rules": [],
        "birth_year_rules": {},
        "missing_birth_year_rules": [],
        "partial_name_rules": [],
        "partial_name_identity_rules": [
            {
                "old_key": "acevedo luis a",
                "new_name": "Acevedo, Luis Alberto",
                "new_key": "acevedo luis alberto",
                "birth_year": "1969",
                "club_key": "",
                "gender": "male",
            }
        ],
        "comma_order_rules": [],
    }

    curated, counts = curate.apply_athlete_curations_to_df(df, "relay_swimmer", rules)

    assert curated.loc[0, "swimmer_name"] == "Acevedo, Luis Alberto"
    assert counts == {"partial_name_identity_consolidations": 1}


def test_apply_athlete_curations_to_df_applies_identity_rule_to_missing_birth_year():
    df = pd.DataFrame(
        [
            {
                "full_name": "Acevedo, Luis",
                "club_name": "NRECO",
                "birth_year": "",
                "gender": "male",
            }
        ]
    )
    rules = {
        "ocr_name_rules": [],
        "birth_year_rules": {},
        "missing_birth_year_rules": [],
        "partial_name_rules": [],
        "partial_name_identity_rules": [
            {
                "old_key": "acevedo luis",
                "new_name": "Acevedo, Luis Alberto",
                "new_key": "acevedo luis alberto",
                "birth_year": "1969",
                "club_key": "",
                "gender": "male",
            }
        ],
        "comma_order_rules": [],
    }

    curated, counts = curate.apply_athlete_curations_to_df(df, "athlete", rules)

    assert curated.loc[0, "full_name"] == "Acevedo, Luis Alberto"
    assert curated.loc[0, "birth_year"] == "1969"
    assert counts == {"partial_name_missing_birth_year_consolidations": 1}


def test_apply_athlete_curations_to_df_applies_identity_after_space_order_canonicalization():
    df = pd.DataFrame(
        [
            {
                "full_name": "Luis Acevedo",
                "club_name": "NRECO",
                "birth_year": "",
                "gender": "male",
            }
        ]
    )
    rules = {
        "ocr_name_rules": [],
        "birth_year_rules": {},
        "missing_birth_year_rules": [],
        "partial_name_rules": [],
        "partial_name_identity_rules": [
            {
                "old_key": "acevedo luis",
                "new_name": "Acevedo, Luis Alberto",
                "new_key": "acevedo luis alberto",
                "birth_year": "1969",
                "club_key": "",
                "gender": "male",
            }
        ],
        "comma_order_rules": [],
    }

    curated, counts = curate.apply_athlete_curations_to_df(df, "athlete", rules)

    assert curated.loc[0, "full_name"] == "Acevedo, Luis Alberto"
    assert curated.loc[0, "birth_year"] == "1969"
    assert counts == {
        "space_order_name_canonicalizations": 1,
        "partial_name_missing_birth_year_consolidations": 1,
    }


def test_apply_athlete_curations_to_df_repairs_rule_outputs():
    df = pd.DataFrame(
        [
            {
                "full_name": "Barahona, Manuel",
                "club_name": "Pe\u00f1alolen Master",
                "gender": "male",
                "birth_year": "1972",
            }
        ]
    )
    rules = {
        "ocr_name_rules": [
            {
                "old_key": "barahona manuel",
                "new_name": "Barahona Ligu\u00fce\u00f1o, Manuel",
                "new_key": "barahona ligueno manuel",
                "birth_year": "1972",
                "club_key": "penalolen master",
                "gender": "male",
            }
        ],
        "birth_year_rules": {},
        "missing_birth_year_rules": [],
        "partial_name_rules": [],
    }

    curated, counts = curate.apply_athlete_curations_to_df(df, "athlete", rules)

    assert curated.loc[0, "full_name"] == "Barahona Ligue\u00f1o, Manuel"
    assert counts == {"ocr_name_replacements": 1, "known_ocr_name_residue_repairs": 1}


def test_materialize_document_inputs_writes_curated_copy_and_manifest_document():
    tmp_dir = _workspace_tmp_dir()
    try:
        input_dir = tmp_dir / "results_csv" / "fchmn_auto" / "2024" / "sample"
        input_dir.mkdir(parents=True)
        pd.DataFrame(
            [
                {
                    "full_name": "Acevedo, Luis A",
                    "gender": "male",
                    "club_name": "Natacion Master Recoleta",
                    "birth_year": "1969",
                    "source_id": "1",
                }
            ]
        ).to_csv(input_dir / "athlete.csv", index=False)
        pd.DataFrame(
            [
                {
                    "event_name": "50 Free",
                    "athlete_name": "Acevedo, Luis A",
                    "club_name": "Natacion Master Recoleta",
                    "rank_position": "1",
                    "seed_time_text": "",
                    "seed_time_ms": "",
                    "result_time_text": "30.00",
                    "result_time_ms": "30000",
                    "age_at_event": "55",
                    "birth_year_estimated": "1969",
                    "points": "",
                    "status": "valid",
                    "source_id": "1",
                }
            ]
        ).to_csv(input_dir / "result.csv", index=False)
        pd.DataFrame([{"name": "Natacion Master Recoleta"}]).to_csv(input_dir / "club.csv", index=False)
        pd.DataFrame([{"event_name": "50 Free"}]).to_csv(input_dir / "event.csv", index=False)
        (input_dir / "metadata.json").write_text('{"parser_version":"test"}\n', encoding="utf-8")

        rules = {
            "ocr_name_rules": [],
            "birth_year_rules": {},
            "missing_birth_year_rules": [],
            "partial_name_rules": [
                {
                    "old_key": "acevedo luis a",
                    "new_name": "Acevedo, Luis Alberto",
                    "new_key": "acevedo luis alberto",
                    "birth_year": "1969",
                    "club_key": "natacion master recoleta",
                    "gender": "male",
                }
            ],
        }
        document = {"source_url": "https://example.test/a.pdf", "input_dir": str(input_dir)}

        output_document, counts = curate.materialize_document_inputs(
            document,
            input_dir,
            tmp_dir / "curated",
            rules,
        )

        output_dir = Path(output_document["input_dir"])
        athlete_df = pd.read_csv(output_dir / "athlete.csv", dtype=str)
        result_df = pd.read_csv(output_dir / "result.csv", dtype=str)
        metadata = (output_dir / "metadata.json").read_text(encoding="utf-8")

        assert athlete_df.loc[0, "full_name"] == "Acevedo, Luis Alberto"
        assert result_df.loc[0, "athlete_name"] == "Acevedo, Luis Alberto"
        assert counts["athlete_partial_name_consolidations"] == 1
        assert counts["result_partial_name_consolidations"] == 1
        assert '"athlete_materialized": true' in metadata
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


def test_load_materialization_rules_accepts_multiple_partial_decision_files():
    tmp_dir = _workspace_tmp_dir()
    try:
        first = tmp_dir / "first.csv"
        second = tmp_dir / "second.csv"
        first.write_text(
            "decision;suggested_canonical_full_name;gender;birth_year;club_key;shorter_full_name;longer_full_name\n"
            "merge;Acevedo, Luis Alberto;male;1969;club a;Acevedo, Luis A;Acevedo, Luis Alberto\n",
            encoding="utf-8",
        )
        second.write_text(
            "decision;suggested_canonical_full_name;gender;birth_year;club_key;shorter_full_name;longer_full_name\n"
            "merge;Garay Carrasco, Victor;male;1963;club b;Garay C, Victor;Garay Carrasco, Victor\n",
            encoding="utf-8",
        )

        class Args:
            birth_year_evidence_csv = None
            missing_birth_year_consolidation_csv = None
            partial_name_decisions_csv = [str(first), str(second)]

        rules = curate.load_materialization_rules(Args(), {})

        assert [row["new_name"] for row in rules["partial_name_rules"]] == [
            "Acevedo, Luis Alberto",
            "Garay Carrasco, Victor",
        ]
        assert rules["ocr_name_rules"] == []
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)
