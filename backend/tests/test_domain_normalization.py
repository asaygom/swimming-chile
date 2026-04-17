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
)


def test_gender_normalization_uses_domain_canons():
    assert normalize_event_gender("Women") == "women"
    assert normalize_event_gender("Hombres") == "men"
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


def test_result_status_normalization_maps_explicit_statuses():
    assert normalize_result_status(None, "DNS") == "dns"
    assert normalize_result_status(None, "DNF") == "dnf"
    assert normalize_result_status(None, "DQ") == "dsq"
    assert normalize_result_status(None, "SCRATCH") == "scratch"
    assert normalize_result_status(None, "NT") == "unknown"
    assert normalize_result_status("valid", None) == "valid"
    assert normalize_result_status(None, "1:05.30") == "unknown"


def test_normalize_swim_time_text_and_milliseconds():
    assert normalize_swim_time_text("35.40") == "35,40"
    assert derive_result_time_ms("35.40") == 35400
    assert normalize_swim_time_text("1:05.30") == "1:05,30"
    assert derive_result_time_ms("1:05.30") == 65300
    assert normalize_swim_time_text("1:02:03.45") == "62:03,45"
    assert derive_result_time_ms("1:02:03.45") == 3723450


def test_time_statuses_do_not_get_milliseconds():
    assert normalize_swim_time_text("DNS") == "DNS"
    assert derive_result_time_ms("DNS") is None
