import sys
from pathlib import Path


BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from natacion_chile.domain.normalization import (
    derive_result_time_ms,
    normalize_athlete_gender,
    normalize_event_gender,
    normalize_result_status,
    normalize_stroke,
    normalize_swim_time_text,
    parse_hytek_event_identity,
)


def test_gender_normalization_uses_domain_canons():
    assert normalize_event_gender("Women") == "women"
    assert normalize_event_gender("Hombres") == "men"
    assert normalize_event_gender("Damas") == "women"
    assert normalize_event_gender("Varones") == "men"
    assert normalize_event_gender("Mixto") == "mixed"
    assert normalize_athlete_gender("W") == "female"
    assert normalize_athlete_gender("Mujer") == "female"
    assert normalize_athlete_gender("M") == "male"


def test_stroke_normalization_uses_domain_canons():
    assert normalize_stroke("Libre") == "freestyle"
    assert normalize_stroke("Espalda") == "backstroke"
    assert normalize_stroke("Pecho") == "breaststroke"
    assert normalize_stroke("Mariposa") == "butterfly"
    assert normalize_stroke("Combinado") == "individual_medley"
    assert normalize_stroke("Relevo Libre") == "freestyle_relay"
    assert normalize_stroke("Relevo Combinado") == "medley_relay"
    assert normalize_stroke("Estilo Libre Novicios") == "freestyle"
    assert normalize_stroke("4x50 Comb 200 a 239") == "medley_relay"
    assert normalize_stroke("4x50 Crol") == "freestyle_relay"
    assert normalize_stroke("4x100 mts Libres Relay") == "freestyle_relay"
    assert normalize_stroke("Medley Relay 280 y mas") == "medley_relay"
    assert normalize_stroke("Medley Relay Pre Master") == "medley_relay"


def test_hytek_event_identity_ignores_gender_age_and_course():
    assert parse_hytek_event_identity(
        "Women 25-29 50 SC Meter Freestyle"
    ) == (50, "freestyle")
    assert parse_hytek_event_identity(
        "Men 70-74 50 LC Meter Freestyle"
    ) == (50, "freestyle")


def test_hytek_event_identity_normalizes_relay_distance_and_stroke():
    assert parse_hytek_event_identity(
        "Mixed 120-159 4x50 SC Meter Medley Relay"
    ) == (200, "medley_relay")
    assert parse_hytek_event_identity(
        "Mixed 200 LC Meter Medley Relay"
    ) == (200, "medley_relay")
    assert parse_hytek_event_identity(
        "Mixed 200 LC Meter Medley Relay 160 a 199 años"
    ) == (200, "medley_relay")
    assert parse_hytek_event_identity(
        "Mixed 200 SC Meter Medley Relay 280 a�os y mas"
    ) == (200, "medley_relay")
    assert parse_hytek_event_identity(
        "Mixed 200 SC Meter Medley Relay PM 72 a 99 a�os"
    ) == (200, "medley_relay")
    assert parse_hytek_event_identity(
        "Mixed 200 SC Meter Medley Relay 120 a 159"
    ) == (200, "medley_relay")
    assert parse_hytek_event_identity(
        "Mixed 200 SC Meter Medley Relay 72-99"
    ) == (200, "medley_relay")
    assert parse_hytek_event_identity(
        "Mixed 200 SC Meter Medley Relay 280 y mas"
    ) == (200, "medley_relay")
    assert parse_hytek_event_identity(
        "Mixed 200 SC Meter Medley Relay Pre Master"
    ) == (200, "medley_relay")


def test_hytek_event_identity_fails_closed_for_unknown_names():
    assert parse_hytek_event_identity("Exhibition surprise event") is None
    assert parse_hytek_event_identity("Women 50 SC Meter Dog Paddle") is None
    assert parse_hytek_event_identity("Women 50 SC Meter Freestyle surprise") is None
    assert parse_hytek_event_identity(
        "Exhibition Women 50 SC Meter Freestyle"
    ) is None
    assert parse_hytek_event_identity(
        "Dog Paddle 50 SC Meter Freestyle"
    ) is None


def test_result_status_normalization_maps_explicit_statuses():
    assert normalize_result_status(None, "DNS") == "dns"
    assert normalize_result_status(None, "DNF") == "dnf"
    assert normalize_result_status(None, "DQ") == "dsq"
    assert normalize_result_status(None, "SCRATCH") == "scratch"
    assert normalize_result_status(None, "NT") == "unknown"
    assert normalize_result_status(None, "NS") == "dns"  # NS = No Show → dns
    assert normalize_result_status("valid", None) == "valid"
    assert normalize_result_status(None, "1:05.30") == "unknown"


def test_normalize_swim_time_text_and_milliseconds():
    assert normalize_swim_time_text("35.40") == "35,40"
    assert derive_result_time_ms("35.40") == 35400
    assert normalize_swim_time_text("1:05.30") == "1:05,30"
    assert derive_result_time_ms("1:05.30") == 65300
    assert normalize_swim_time_text("1:02:03.45") == "62:03,45"
    assert derive_result_time_ms("1:02:03.45") == 3723450
    assert normalize_swim_time_text("4:55.44S") == "4:55,44"
    assert normalize_swim_time_text("2'40'12") == "2:40,12"


def test_time_statuses_do_not_get_milliseconds():
    assert normalize_swim_time_text("DNS") == "DNS"
    assert derive_result_time_ms("DNS") is None
