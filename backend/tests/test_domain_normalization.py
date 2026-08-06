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


def test_hytek_event_identity_accepts_meet_manager_spanish_labels():
    """El export CSV de Meet Manager rotula en espanol; misma identidad canonica."""
    assert parse_hytek_event_identity(
        "Mixto 100 CL Metro Estilo Libre"
    ) == (100, "freestyle")
    assert parse_hytek_event_identity(
        "Mixto 50 CL Metro Estilo de Espalda"
    ) == (50, "backstroke")
    assert parse_hytek_event_identity(
        "Mixto 50 CC Metro Estilo de Pecho"
    ) == (50, "breaststroke")
    assert parse_hytek_event_identity(
        "Mixto 100 CL Metro Estilo de Mariposa"
    ) == (100, "butterfly")
    assert parse_hytek_event_identity("Mixto 200 CL Metro CI") == (
        200,
        "individual_medley",
    )
    assert parse_hytek_event_identity(
        "Mixto 400 CL Metro Combinado Relevo"
    ) == (400, "medley_relay")
    assert parse_hytek_event_identity(
        "Femenino 50 CL Metro Estilo Libre"
    ) == (50, "freestyle")


def test_hytek_event_identity_keeps_rejecting_non_event_text():
    """La apertura al espanol no debe volver permisivo al reconocedor."""
    assert parse_hytek_event_identity("Programa de Competencias - Copa 2026") is None
    assert parse_hytek_event_identity("Mixto 100 CL Metro") is None
    assert parse_hytek_event_identity("Mixto CL Metro Estilo Libre") is None
    assert parse_hytek_event_identity("100 CL Metro Estilo Libre") is None


def test_curated_names_cover_the_program_pdf_variants():
    """El acento agudo llega como glifo suelto; la correccion es por token."""
    from natacion_chile.domain.person_name import clean_athlete_name

    for artifact, expected in [
        ("Beltraán, Pedro", "Beltrán, Pedro"),
        ("Bastias, Bernabeá", "Bastias, Bernabé"),
        ("Maríán, Diego", "Marín, Diego"),
        ("Martorell, Reneá", "Martorell, René"),
        ("Pasaríán Pollanco, Claudia", "Pasarín Pollanco, Claudia"),
        ("Domíánguez, Jose", "Domínguez, José"),
        ("Cabello Tilleríá, Andreás", "Cabello Tillería, Andrés"),
    ]:
        assert clean_athlete_name(artifact) == expected


def test_curated_names_leave_correct_spanish_untouched():
    """La correccion es curada justamente porque no hay regla general:
    "Matías" y "Martínez" comparten patron con respuestas opuestas."""
    from natacion_chile.domain.person_name import clean_athlete_name

    for name in ["Sebastián Rojas", "Muñoz, Valeria", "Briceño, Carlos"]:
        assert clean_athlete_name(name) == name
    assert clean_athlete_name("Bascuñáán, Matíás") == "Bascuñán, Matías"
    assert clean_athlete_name("Martíánez, Anais") == "Martínez, Anais"


def test_curated_additions_do_not_capture_legitimate_neighbours():
    """El diccionario canonico se consulta de forma difusa.

    Apellidos cortos como "Marin" o "Rene" ahi capturan vecinos legitimos
    ("Mariño", "Reaño"), por eso las formas con artefacto van como token_fixes
    anclados y exactos.
    """
    from natacion_chile.domain.person_name import (
        CANONICAL_ATHLETE_NAME_TOKENS,
        clean_athlete_name,
    )

    for token in ("Marin", "Rene", "Beltran", "Bernabe", "Dominguez", "Pasarin"):
        assert token not in CANONICAL_ATHLETE_NAME_TOKENS

    assert clean_athlete_name("Mariño, Luis") == "Mariño, Luis"
    assert clean_athlete_name("Alva Reaño, Julio") == "Alva Reaño, Julio"
    assert clean_athlete_name("Marín, Diego") == "Marín, Diego"
