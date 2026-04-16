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


def test_swim_time_normalization_and_milliseconds():
    assert parser.normalize_swim_time_text("35.40") == "35,40"
    assert parser.derive_result_time_ms("35.40") == 35400
    assert parser.normalize_swim_time_text("1:05.30") == "1:05,30"
    assert parser.derive_result_time_ms("1:05.30") == 65300
    assert parser.normalize_swim_time_text("1:02:03.45") == "62:03,45"
    assert parser.derive_result_time_ms("1:02:03.45") == 3723450
    assert parser.normalize_swim_time_text("DNS") == "DNS"
    assert parser.derive_result_time_ms("DNS") is None


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

    assert english.event_name == "women 35-39 100 LC Meter freestyle"
    assert spanish.event_name == "men 40-44 50 SC Meter backstroke"
    assert relay.gender == "mixed"
    assert relay.distance_m == 200
    assert relay.stroke == "freestyle_relay"


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
