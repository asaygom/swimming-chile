import hashlib
import json
import sys
from pathlib import Path

BACKEND_DIR = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = BACKEND_DIR / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

import parse_results_pdf as parser


FIXTURE_PATH = Path(__file__).resolve().parent / "fixtures" / "parser_golden_lines.json"


def load_fixture(name):
    return json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))[name]


def individual_context():
    return parser.EventContext(
        event_number=1,
        gender="men",
        age_group="35-39",
        distance_label="100",
        distance_m=100,
        course_code="LC",
        stroke="freestyle",
    )


def relay_context():
    return parser.EventContext(
        event_number=2,
        gender="men",
        age_group="160-199",
        distance_label="4x50",
        distance_m=200,
        course_code="SC",
        stroke="freestyle_relay",
    )


def test_normalize_event_gender_to_competition_canon():
    assert parser.normalize_event_gender("Women") == "women"
    assert parser.normalize_event_gender("Hombres") == "men"
    assert parser.normalize_event_gender("Mixto") == "mixed"


def test_normalize_athlete_gender_to_person_canon():
    assert parser.normalize_athlete_gender("W") == "female"
    assert parser.normalize_athlete_gender("Mujer") == "female"
    assert parser.normalize_athlete_gender("M") == "male"


def test_normalize_stroke_to_domain_canon():
    assert parser.normalize_stroke("Libre") == "freestyle"
    assert parser.normalize_stroke("Espalda") == "backstroke"
    assert parser.normalize_stroke("Pecho") == "breaststroke"
    assert parser.normalize_stroke("Mariposa") == "butterfly"
    assert parser.normalize_stroke("Combinado") == "individual_medley"
    assert parser.normalize_stroke("Relevo Libre") == "freestyle_relay"
    assert parser.normalize_stroke("Relevo Combinado") == "medley_relay"
    assert parser.normalize_stroke("Breast 40 a 99 años") == "breaststroke"
    assert parser.normalize_stroke("Medley 120 a 159 años Relay") == "medley_relay"
    assert parser.normalize_stroke("Medley 280 y mas años Relay") == "medley_relay"


    assert parser.normalize_stroke("Estilo Libre Pre Master - Master") == "freestyle"
    assert parser.normalize_stroke("4x50 Mts Combinado") == "medley_relay"


def test_swim_time_normalization_and_milliseconds():
    assert parser.normalize_swim_time_text("35.40") == "35,40"
    assert parser.derive_result_time_ms("35.40") == 35400
    assert parser.normalize_swim_time_text("1:05.30") == "1:05,30"
    assert parser.derive_result_time_ms("1:05.30") == 65300
    assert parser.normalize_swim_time_text("1:02:03.45") == "62:03,45"
    assert parser.derive_result_time_ms("1:02:03.45") == 3723450
    assert parser.normalize_swim_time_text("DNS") == "DNS"
    assert parser.derive_result_time_ms("DNS") is None


def test_clean_athlete_name_removes_layout_artifacts_without_source_suffix():
    assert parser.clean_athlete_name("Fajardo |, Keytheen") == "Fajardo, Keytheen"
    assert parser.clean_athlete_name("Hermosilla1, Yasna") == "Hermosilla, Yasna"
    assert parser.clean_athlete_name("Rojas, 2") == "Rojas, 2"


def test_compute_file_sha256():
    expected = hashlib.sha256(FIXTURE_PATH.read_bytes()).hexdigest()

    assert parser.compute_file_sha256(FIXTURE_PATH) == expected


def test_result_status_from_text_statuses():
    assert parser.normalize_result_status(None, "DNS") == "dns"
    assert parser.normalize_result_status(None, "DNF") == "dnf"
    assert parser.normalize_result_status(None, "DQ") == "dsq"
    assert parser.normalize_result_status(None, "NT") == "unknown"
    assert parser.normalize_result_status(None, "1:05.30") == "valid"


def test_parse_event_header_in_english_and_spanish():
    english = parser.parse_event_header("Event 1 Women 35-39 100 LC Meter Free")
    spanish = parser.parse_event_header("Evento 2 Hombres 40-44 50 CP Metro Espalda")
    relay = parser.parse_event_header("Evento 3 Mixto 160-199 4x50 CP Metro Relevo Libre")
    age_suffix = parser.parse_event_header("Event 4 Women 40-44 100 SC Meter Breast 40 a 99 años")
    relay_age_suffix = parser.parse_event_header("Event 5 Mixed 120-159 200 LC Meter Medley 120 a 159 años Relay")

    sudamericano = parser.parse_event_header("Evento 1 Damas 18-24 400 SC Metros Comb. Ind.")
    compact = parser.parse_event_header("#1 Women 18-24 100 Meter IM")
    ocr_spaced = parser.parse_event_header("E vento 5 Mujeres 25-29 50 CC Metro Estilo Libre Pre Master - Master")

    assert english.event_name == "women 35-39 100 LC Meter freestyle"
    assert spanish.event_name == "men 40-44 50 SC Meter backstroke"
    assert relay.gender == "mixed"
    assert relay.distance_m == 200
    assert relay.stroke == "freestyle_relay"
    assert age_suffix.event_name == "women 40-44 100 SC Meter breaststroke"
    assert relay_age_suffix.distance_m == 200
    assert relay_age_suffix.stroke == "medley_relay"
    assert sudamericano.gender == "women"
    assert sudamericano.stroke == "individual_medley"
    assert compact.course_code == "SC"
    assert compact.stroke == "individual_medley"
    assert ocr_spaced.stroke == "freestyle"


def test_parse_combined_quadathlon_line_as_four_canon_events():
    ctx = parser.parse_combined_event_header("Mujeres 25-29 Quadathlon", event_number=9000)
    rows = parser.parse_combined_result_line(
        "1 Albornoz, Javiera 27 LOZAD 2:27,10 33,96 34,80 46,97 31,37",
        ctx,
        page_number=1,
        line_number=11,
        competition_year=2024,
    )

    assert [row.event_name for row in rows] == [
        "women 25-29 50 LC Meter butterfly",
        "women 25-29 50 LC Meter backstroke",
        "women 25-29 50 LC Meter breaststroke",
        "women 25-29 50 LC Meter freestyle",
    ]
    assert rows[0].athlete_name == "Albornoz, Javiera"
    assert rows[0].age_at_event == 27
    assert rows[0].birth_year_estimated == 1997
    assert rows[-1].result_time_text == "31,37"


def test_parse_brazil_event_header_and_age_group():
    individual = parser.parse_brazil_event_header("1ª PROVA - 400 METROS MEDLEY FEMININO (13/04/2026)")
    relay = parser.parse_brazil_event_header("35ª PROVA - REVEZAMENTO 4X50 METROS LIVRE MISTO (17/04/2026)")

    assert individual.event_number == 1
    assert individual.gender == "women"
    assert individual.distance_m == 400
    assert individual.stroke == "individual_medley"
    assert relay.gender == "mixed"
    assert relay.distance_label == "4x50"
    assert relay.distance_m == 200
    assert relay.stroke == "freestyle_relay"
    assert parser.parse_brazil_age_group("FAIXA: 25 + ----") == "25+"


def test_parse_brazil_result_row_from_columns():
    ctx = parser.with_event_age_group(
        parser.parse_brazil_event_header("1ª PROVA - 400 METROS MEDLEY FEMININO (13/04/2026)"),
        "30+",
    )
    words = [
        {"text": "2º", "x0": 63, "top": 10},
        {"text": "133322", "x0": 94, "top": 10},
        {"text": "ROSEMARY", "x0": 120, "top": 10},
        {"text": "BADUÑA", "x0": 165, "top": 10},
        {"text": "NÚÑEZ", "x0": 210, "top": 10},
        {"text": "MASTER", "x0": 316, "top": 10},
        {"text": "URUGUAY", "x0": 350, "top": 10},
        {"text": "5:56.03", "x0": 412, "top": 10},
        {"text": "0,00", "x0": 451, "top": 10},
    ]

    row = parser.parse_brazil_result_row(words, ctx, page_number=1, line_number=5)

    assert row.athlete_name == "ROSEMARY BADUÑA NÚÑEZ"
    assert row.club_name == "MASTER URUGUAY"
    assert row.result_time_text == "5:56,03"
    assert row.result_time_ms == "356030"
    assert row.points == "0,00"
    assert row.age_at_event is None


def test_parse_individual_result_line_with_seed_fixture():
    fixture = load_fixture("individual_with_seed")
    row = parser.parse_result_line(
        fixture["line"],
        individual_context(),
        page_number=1,
        line_number=10,
        competition_year=fixture["competition_year"],
    )

    assert row is not None
    assert row.athlete_name == "Juan Perez"
    assert row.club_name == "Club Deportivo"
    assert row.rank_position == "1"
    assert row.age_at_event == 35
    assert row.birth_year_estimated == 1991
    assert row.seed_time_text == "1:05,30"
    assert row.seed_time_ms == "65300"
    assert row.result_time_text == "1:03,21"
    assert row.result_time_ms == "63210"
    assert row.points == "9"
    assert row.status == "valid"


def test_parse_individual_result_line_without_seed_fixture():
    fixture = load_fixture("individual_without_seed")
    row = parser.parse_result_line(
        fixture["line"],
        individual_context(),
        page_number=1,
        line_number=11,
        competition_year=fixture["competition_year"],
    )

    assert row is not None
    assert row.athlete_name == "Maria Lopez"
    assert row.seed_time_text is None
    assert row.result_time_text == "35,40"
    assert row.result_time_ms == "35400"
    assert row.points is None


def test_parse_result_line_recovers_seed_time_before_status_result():
    row = parser.parse_result_line(
        "--- Cabrillana, Mariano 38 Club Sparta A C 49.33 DQ DQ",
        individual_context(),
        page_number=1,
        line_number=12,
        competition_year=2025,
    )

    assert row is not None
    assert row.club_name == "Club Sparta A C"
    assert row.seed_time_text == "49,33"
    assert row.seed_time_ms == "49330"
    assert row.result_time_text == "DQ"
    assert row.result_time_ms is None
    assert row.status == "dsq"


def test_parse_fragmented_result_line_from_hytek_multicolumn_ocr():
    row = parser.parse_result_line(
        "4 D e l g a d o , Ar t u r o 3 8 S T G O D 1 : 1 6 , 4 3",
        individual_context(),
        page_number=1,
        line_number=40,
        competition_year=2023,
    )

    assert row is not None
    assert row.athlete_name == "Delgado, Arturo"
    assert row.age_at_event == 38
    assert row.club_name == "STGOD"
    assert row.result_time_text == "1:16,43"


def test_parse_result_line_recovers_duplicated_age_digit_before_club():
    row = parser.parse_result_line(
        "--- Rojas, 2 20 Escuela de Suboficiales del Ej NT X1:30,58",
        individual_context(),
        page_number=25,
        line_number=29,
        competition_year=2024,
    )

    assert row is not None
    assert row.athlete_name == "Rojas,"
    assert row.age_at_event == 20
    assert row.birth_year_estimated == 2004
    assert row.club_name == "Escuela de Suboficiales del Ej"


def test_parse_result_line_keeps_single_digit_age_for_child_event():
    child_context = parser.EventContext(
        event_number=1,
        gender="men",
        age_group="9-10",
        distance_label="50",
        distance_m=50,
        course_code="LC",
        stroke="freestyle",
    )

    row = parser.parse_result_line(
        "1 Perez, Tomas 9 Escuela Infantil 45,12 44,90",
        child_context,
        page_number=1,
        line_number=3,
        competition_year=2026,
    )

    assert row is not None
    assert row.athlete_name == "Perez, Tomas"
    assert row.age_at_event == 9
    assert row.birth_year_estimated == 2017
    assert row.club_name == "Escuela Infantil"


def test_detects_hytek_two_column_layout():
    assert parser.looks_like_hytek_two_column(
        [
            (
                1,
                [
                    "Event 1 Women 18-24 200 LC Meter Butterfly Event 2 Women 45-49 50 LC Meter Breaststroke",
                ],
            )
        ]
    )


def test_parse_relay_team_line_fixture():
    fixture = load_fixture("relay_team")
    row = parser.parse_relay_team_line(
        fixture["line"],
        relay_context(),
        page_number=2,
        line_number=20,
    )

    assert row is not None
    assert row.relay_team_name == "Club Deportivo A"
    assert row.seed_time_text == "4:30,00"
    assert row.result_time_text == "4:22,50"
    assert row.result_time_ms == "262500"
    assert row.points == "18"


def test_parse_relay_swimmer_line_fixture():
    fixture = load_fixture("relay_swimmers")
    rows = parser.parse_relay_swimmer_line(
        fixture["line"],
        relay_context(),
        page_number=2,
        line_number=21,
        relay_team_name="Club Deportivo A",
        competition_year=fixture["competition_year"],
    )

    assert [row.leg_order for row in rows] == [1, 2, 3, 4]
    assert rows[0].swimmer_name == "Juan Perez"
    assert rows[0].gender == "male"
    assert rows[0].age_at_event == 35
    assert rows[0].birth_year_estimated == 1991


def test_parse_relay_swimmer_line_splits_embedded_next_marker_after_age():
    rows = parser.parse_relay_swimmer_line(
        "3) Chamorro M, Alejandra Leonor W340) Levrini, Aldo M42",
        relay_context(),
        page_number=4,
        line_number=10,
        relay_team_name="CDUC A",
        competition_year=2024,
    )

    assert [row.leg_order for row in rows] == [3, 4]
    assert rows[0].swimmer_name == "Chamorro M, Alejandra Leonor"
    assert rows[0].gender == "female"
    assert rows[0].age_at_event == 34
    assert rows[1].swimmer_name == "Levrini, Aldo"
    assert rows[1].gender == "male"
    assert rows[1].age_at_event == 42


def test_parse_relay_swimmer_line_splits_marker_deformed_with_extra_digit():
    rows = parser.parse_relay_swimmer_line(
        "1) Campos Carrasco, Alejandrina W26)0 Brain, Cynthia W60 3) Pasarin, Claudia W60 4) Valdivia, Adriana W61",
        relay_context(),
        page_number=5,
        line_number=38,
        relay_team_name="Peñalolen Master A",
        competition_year=2024,
    )

    assert [row.leg_order for row in rows] == [1, 2, 3, 4]
    assert rows[0].swimmer_name == "Campos Carrasco, Alejandrina"
    assert rows[0].age_at_event == 26
    assert rows[1].swimmer_name == "Brain, Cynthia"
    assert rows[1].age_at_event == 60


def test_build_output_frames_from_minimal_parsed_rows():
    individual_fixture = load_fixture("individual_with_seed")
    relay_team_fixture = load_fixture("relay_team")
    relay_swimmer_fixture = load_fixture("relay_swimmers")

    individual = parser.parse_result_line(
        individual_fixture["line"],
        individual_context(),
        page_number=1,
        line_number=10,
        competition_year=individual_fixture["competition_year"],
    )
    relay_team = parser.parse_relay_team_line(
        relay_team_fixture["line"],
        relay_context(),
        page_number=2,
        line_number=20,
    )
    relay_swimmers = parser.parse_relay_swimmer_line(
        relay_swimmer_fixture["line"],
        relay_context(),
        page_number=2,
        line_number=21,
        relay_team_name="Club Deportivo A",
        competition_year=relay_swimmer_fixture["competition_year"],
    )

    frames = parser.build_output_frames(
        parsed_rows=[individual],
        relay_team_rows=[relay_team],
        relay_swimmer_rows=relay_swimmers,
        competition_id=99,
        default_source_id=1,
        metadata={"competition_year": 2026},
    )

    assert set(frames) == {
        "club",
        "event",
        "athlete",
        "result",
        "raw_result",
        "relay_team",
        "relay_swimmer",
        "raw_relay_team",
        "raw_relay_swimmer",
    }
    assert frames["club"].to_dict("records") == [
        {"name": "Club Deportivo", "short_name": None, "city": None, "region": None, "source_id": "1"}
    ]
    assert frames["event"]["event_name"].tolist() == [
        "men 35-39 100 LC Meter freestyle",
        "men 160-199 4x50 SC Meter freestyle_relay",
    ]
    assert frames["result"].iloc[0].to_dict() == {
        "event_name": "men 35-39 100 LC Meter freestyle",
        "athlete_name": "Juan Perez",
        "club_name": "Club Deportivo",
        "rank_position": "1",
        "age_at_event": "35",
        "birth_year_estimated": "1991",
        "seed_time_text": "1:05,30",
        "seed_time_ms": "65300",
        "result_time_text": "1:03,21",
        "result_time_ms": "63210",
        "points": "9",
        "status": "valid",
        "source_id": "1",
    }
    assert frames["relay_team"].iloc[0]["relay_team_name"] == "Club Deportivo A"
    assert frames["relay_swimmer"]["leg_order"].tolist() == ["1", "2", "3", "4"]
