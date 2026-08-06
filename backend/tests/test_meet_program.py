import csv
import io
import json
from pathlib import Path
import sys

import pytest


SCRIPTS_DIR = Path(__file__).resolve().parents[1] / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

import run_meet_program as meet_program


VALID_HEADER_METADATA = {
    "source_competition_name": "X Torneo Master San Bernardo 2026",
    "source_competition_start_date": "2026-07-04",
    "source_competition_end_date": "2026-07-04",
}


@pytest.mark.parametrize(
    ("source_name", "database_name"),
    [
        ("X Copa Master San Bernardo", "X Copa Master San Bernardo"),
        ("X Torneo Master San Bernardo 2026", "X Copa Master San Bernardo"),
    ],
)
def test_competition_header_name_matching_accepts_safe_equivalents(
    source_name, database_name
):
    meet_program.validate_competition_identity(
        {**VALID_HEADER_METADATA, "source_competition_name": source_name},
        {
            "name": database_name,
            "start_date": "2026-07-04",
            "end_date": "2026-07-04",
        },
    )


@pytest.mark.parametrize(
    ("metadata", "competition", "message"),
    [
        (
            VALID_HEADER_METADATA,
            {
                "name": "Copa Master Valparaiso",
                "start_date": "2026-07-04",
                "end_date": "2026-07-04",
            },
            "name mismatch",
        ),
        (
            VALID_HEADER_METADATA,
            {
                "name": "X Copa Master San Bernardo",
                "start_date": "2026-07-05",
                "end_date": "2026-07-05",
            },
            "date mismatch",
        ),
        (
            {},
            {
                "name": "X Copa Master San Bernardo",
                "start_date": "2026-07-04",
                "end_date": "2026-07-04",
            },
            "header.*missing",
        ),
        (
            {
                "source_competition_name": "X Copa Master San Bernardo",
                "source_competition_start_date": None,
                "source_competition_end_date": None,
            },
            {
                "name": "X Copa Master San Bernardo",
                "start_date": "2026-07-04",
                "end_date": "2026-07-04",
            },
            "missing.*date",
        ),
        (
            VALID_HEADER_METADATA,
            {
                "name": "X Copa Master San Bernardo",
                "start_date": None,
                "end_date": None,
            },
            "missing.*date",
        ),
    ],
)
def test_competition_header_validation_fails_closed(metadata, competition, message):
    with pytest.raises(meet_program.MeetProgramError, match=message):
        meet_program.validate_competition_identity(metadata, competition)


def test_competition_header_name_matching_rejects_generic_type_substitution():
    assert not meet_program.competition_names_match("Torneo Chile", "Copa Chile")


def test_parse_source_header_metadata_from_fchmn_line():
    metadata = meet_program.extract_competition_header_metadata(
        [
            meet_program.SourceLine(
                1,
                1,
                1,
                "X Torneo Master San Bernardo 2026 - 04-07-2026",
            )
        ]
    )

    assert metadata == VALID_HEADER_METADATA


def test_conflicting_parseable_headers_require_review_but_identical_repeats_do_not():
    header = "X Torneo Master San Bernardo 2026 - 04-07-2026"
    repeated = [meet_program.SourceLine(page, 1, 1, header) for page in (1, 2)]
    assert meet_program.extract_competition_header_metadata(repeated) == VALID_HEADER_METADATA
    metadata = meet_program.extract_competition_header_metadata(
        repeated + [meet_program.SourceLine(3, 1, 1, "Copa Master Valparaiso - 04-07-2026")]
    )
    assert metadata["source_competition_header_conflict"] is True
    issue = meet_program._header_metadata_issues(metadata)[0]
    assert issue.issue_key == "conflicting_competition_headers"


def test_parse_fchmn_individual_lines_accepts_nt_and_comma_seed():
    lines = [
        meet_program.SourceLine(1, 1, 1, "#1 Women 200 SC Meter Freestyle"),
        meet_program.SourceLine(1, 1, 2, "Lane Name Age Team Seed Time"),
        meet_program.SourceLine(1, 1, 3, "Heat 1 of 10 Finals Starts at 08:30 AM"),
        meet_program.SourceLine(1, 1, 4, "1 Parada, Antonia 29 GOLDE NT"),
        meet_program.SourceLine(1, 1, 5, "3 Sayago, Alexis 33 NUMAS 2:51,37"),
    ]

    result = meet_program.parse_source_lines(lines)

    assert result.unparsed == []
    assert [entry.display_name for entry in result.entries] == [
        "Parada, Antonia",
        "Sayago, Alexis",
    ]
    assert result.entries[0].seed_time_text == "NT"
    assert result.entries[0].seed_time_ms is None
    assert {entry.estimated_start_time for entry in result.entries} == {"08:30"}
    assert result.entries[1].seed_time_text == "2:51,37"
    assert result.entries[1].seed_time_ms == 171370
    assert result.entries[1].team_name == "NUMAS"


def test_parse_fechida_event_layout_accepts_lane_zero():
    lines = [
        meet_program.SourceLine(1, 1, 1, "Meet Program - Quinta etapa piscina entrenamiento"),
        meet_program.SourceLine(1, 1, 2, "Event 25 Women 100 LC Meter Freestyle"),
        meet_program.SourceLine(1, 1, 3, "Lane Name Age Team Seed Time"),
        meet_program.SourceLine(1, 1, 4, "Heat 1 of 20 Finals Starts at 03:00 PM"),
        meet_program.SourceLine(1, 1, 5, "0 Lopez, Valentina 21 ITACP NT"),
    ]

    result = meet_program.parse_source_lines(lines)

    assert result.unparsed == []
    assert len(result.entries) == 1
    assert result.entries[0].lane == 0
    assert result.entries[0].display_name == "Lopez, Valentina"
    assert result.entries[0].estimated_start_time == "15:00"


def test_parse_fechida_repairs_name_age_and_team_column_overlap():
    lines = [
        meet_program.SourceLine(1, 1, 1, "#1 Women 800 LC Meter Freestyle"),
        meet_program.SourceLine(1, 1, 2, "Heat 1 of 1 Finals"),
        meet_program.SourceLine(1, 1, 3, "3 Sanchez, Tamara Co 3n0stanMzaMAG NT"),
        meet_program.SourceLine(1, 1, 4, "5 Fuentes, Melissa Ro 3c6io COQBO NT"),
        meet_program.SourceLine(1, 1, 5, "2 Pineda, Miguel Leo n3a0rdoOSW23 NT"),
        meet_program.SourceLine(1, 1, 6, "8 Jimenez, Guillermo 2A7ndreHs2O NT"),
        meet_program.SourceLine(1, 1, 7, "5 Fonseca, Daniela Eu 4g5enia100% 1:20,00"),
        meet_program.SourceLine(1, 1, 8, "5 Ragazzonestrelow, E6d8uarSdQoUAD 13:00,00"),
    ]

    result = meet_program.parse_source_lines(lines)

    assert result.unparsed == []
    assert [entry.display_name for entry in result.entries] == [
        "Sanchez, Tamara Constanza",
        "Fuentes, Melissa Rocio",
        "Pineda, Miguel Leonardo",
        "Jimenez, Guillermo Andres",
        "Fonseca, Daniela Eugenia",
        "Ragazzonestrelow, Eduardo",
    ]
    assert [(entry.age, entry.team_name) for entry in result.entries] == [
        (30, "MMAG"),
        (36, "COQBO"),
        (30, "OSW23"),
        (27, "H2O"),
        (45, "100%"),
        (68, "SQUAD"),
    ]


def test_infer_program_segment_defaults_and_fechida_pool_roles():
    assert meet_program.infer_program_segment("Jornada Unica") == (1, "main")
    assert meet_program.infer_program_segment(
        "Primera etapa piscina competencia"
    ) == (1, "competition")
    assert meet_program.infer_program_segment(
        "Sexta etapa piscina entrenamiento"
    ) == (6, "training")


def test_detect_page_column_count_from_heat_anchors():
    two_columns = [
        {"text": "Heat", "x0": 18.0},
        {"text": "Heat", "x0": 309.6},
        {"text": "Heat", "x0": 18.0},
    ]
    three_columns = [
        {"text": "Heat", "x0": 18.0},
        {"text": "Heat", "x0": 212.4},
        {"text": "Heat", "x0": 406.8},
    ]

    assert meet_program.detect_page_column_count(two_columns) == 2
    assert meet_program.detect_page_column_count(three_columns) == 3
    assert meet_program.detect_page_column_count(three_columns[:2]) == 3


def test_estimated_start_time_normalizes_noon_and_midnight():
    assert meet_program.normalize_estimated_start_time("12:05 AM") == "00:05"
    assert meet_program.normalize_estimated_start_time("12:05 PM") == "12:05"
    assert meet_program.normalize_estimated_start_time(None) is None


def test_parse_cleans_derived_names_without_mutating_source_or_debug_lines():
    source = meet_program.SourceLine(1, 1, 4, "1 Zunñiga, Ana 29 MVINÑA NT")
    lines = [
        meet_program.SourceLine(1, 1, 1, "#1 Women 50 SC Meter Freestyle"),
        meet_program.SourceLine(1, 1, 2, "Heat 1 of 1 Finals"),
        source,
        meet_program.SourceLine(1, 1, 5, "2 broken   lane"),
    ]

    result = meet_program.parse_source_lines(lines)

    assert source.text == "1 Zunñiga, Ana 29 MVINÑA NT"
    assert result.entries[0].display_name == "Zuñiga, Ana"
    assert result.entries[0].team_name == "MVIÑA"
    entry = result.entries[0]
    assert (entry.page_number, entry.column_number, entry.line_number) == (1, 1, 4)
    assert result.unparsed[0].raw_line == "2 broken   lane"
    debug = result.unparsed[0]
    assert (debug.page_number, debug.column_number, debug.line_number) == (1, 1, 5)


def test_parse_cleans_relay_display_team_members_and_session_name():
    lines = [
        meet_program.SourceLine(1, 1, 1, "Meet Program - Jornada MVINÑA"),
        meet_program.SourceLine(
            1, 1, 2, "#8 Mixed 200 SC Meter Medley Relay 200 a 239 anos"
        ),
        meet_program.SourceLine(1, 1, 3, "Heat 1 of 1 Finals"),
        meet_program.SourceLine(1, 1, 4, "2 MVINÑA A 3:02,00"),
        meet_program.SourceLine(
            1, 1, 5, "Zunñiga, Ana W55 Munñoz, Beto M64"
        ),
    ]

    entry = meet_program.parse_source_lines(lines).entries[0]

    assert entry.session_name == "Jornada MVIÑA"
    assert entry.display_name == "MVIÑA A"
    assert entry.team_name == "MVIÑA"
    assert entry.relay_members == ["Zuñiga, Ana", "Muñoz, Beto"]


def test_header_cleanup_repairs_only_derived_metadata():
    source = meet_program.SourceLine(
        1, 1, 1, "X Torneo Master MVINÑA 2026 - 04-07-2026"
    )

    metadata = meet_program.extract_competition_header_metadata([source])

    assert source.text == "X Torneo Master MVINÑA 2026 - 04-07-2026"
    assert metadata["source_competition_name"] == "X Torneo Master MVIÑA 2026"


def test_parse_fchmn_continuation_header_updates_event_context():
    lines = [
        meet_program.SourceLine(1, 1, 1, "#1 Women 200 SC Meter Freestyle"),
        meet_program.SourceLine(1, 1, 2, "Heat 10 of 10 Finals"),
        meet_program.SourceLine(1, 1, 3, "1 Uno, Ana 25 CLUB 3:00,00"),
        meet_program.SourceLine(
            2,
            1,
            1,
            "Heat 10 (#2 Men 200 SC Meter Freestyle)",
        ),
        meet_program.SourceLine(2, 1, 2, "2 Dos, Beto 30 TEAM 2:30,00"),
    ]

    result = meet_program.parse_source_lines(lines)

    assert [entry.event_number for entry in result.entries] == [1, 2]
    assert [entry.heat_number for entry in result.entries] == [10, 10]
    assert result.entries[1].event_name == "Men 200 SC Meter Freestyle"


def test_parse_uses_page_session_header_before_left_column_entries():
    lines = [
        meet_program.SourceLine(1, 1, 1, "#1 Women 50 SC Meter Freestyle"),
        meet_program.SourceLine(1, 1, 2, "Heat 1 of 1 Finals"),
        meet_program.SourceLine(1, 1, 3, "1 Uno, Ana 25 CLUB NT"),
        meet_program.SourceLine(1, 2, 1, "Meet Program - Jornada Tarde"),
    ]

    result = meet_program.parse_source_lines(lines)

    assert result.entries[0].session_name == "Jornada Tarde"
    assert result.entries[0].session_number == 1


def test_continuation_preserves_known_full_fchmn_event_name():
    full_name = "Mixed 200 LC Meter Medley Relay 160 a 199 anos"
    lines = [
        meet_program.SourceLine(1, 1, 1, f"#7 {full_name}"),
        meet_program.SourceLine(1, 1, 2, "Heat 1 of 2 Finals"),
        meet_program.SourceLine(1, 1, 3, "1 NUMAS X160 A 2:20,00"),
        meet_program.SourceLine(
            2, 1, 1, "Heat 2 (#7 Mixed 200 LC Meter Medley Relay 160 a 199"
        ),
        meet_program.SourceLine(2, 1, 2, "2 SDEPO X160 A 2:51,00"),
    ]

    result = meet_program.parse_source_lines(lines)

    assert {entry.event_name for entry in result.entries} == {full_name}


def test_parse_fchmn_repairs_name_overlapped_with_fixed_gender_age_column():
    lines = [
        meet_program.SourceLine(1, 1, 1, "#1 Mixed 200 LC Meter IM"),
        meet_program.SourceLine(1, 1, 2, "Heat 1 of 1 Finals"),
        meet_program.SourceLine(
            1, 1, 3, "9 Delgadillo, Maria ConstanzaW76 PEMAS 2:00,00"
        ),
        meet_program.SourceLine(
            1, 1, 4, "2 Schwarzemberg, Maria AngeWlic7a1 PEMAS 3:30,00"
        ),
    ]

    result = meet_program.parse_source_lines(lines)

    assert result.unparsed == []
    assert [(entry.display_name, entry.age) for entry in result.entries] == [
        ("Delgadillo, Maria Constanza", 76),
        ("Schwarzemberg, Maria Angelica", 71),
    ]


def test_parse_fchmn_relay_members_only_when_unambiguous():
    lines = [
        meet_program.SourceLine(
            1,
            1,
            1,
            "#8 Mixed 200 SC Meter Medley Relay 200 a 239 anos",
        ),
        meet_program.SourceLine(1, 1, 2, "Lane Team Relay Seed Time"),
        meet_program.SourceLine(1, 1, 3, "Heat 1 of 1 Finals"),
        meet_program.SourceLine(1, 1, 4, "2 SDEPO A 3:02,00"),
        meet_program.SourceLine(
            1,
            1,
            5,
            "Pino, Marcela W55 Von Marttens, Nelly W64",
        ),
        meet_program.SourceLine(
            1,
            1,
            6,
            "Troncoso, Rodolfo M49 Castro, Emerson M43",
        ),
        meet_program.SourceLine(1, 1, 7, "not an unambiguous relay member row"),
    ]

    result = meet_program.parse_source_lines(lines)

    assert len(result.entries) == 1
    assert result.entries[0].entry_type == "relay"
    assert result.entries[0].display_name == "SDEPO A"
    assert result.entries[0].team_name == "SDEPO"
    assert result.entries[0].relay_members == [
        "Pino, Marcela",
        "Von Marttens, Nelly",
        "Troncoso, Rodolfo",
        "Castro, Emerson",
    ]


def test_parse_fchmn_women_relay_members_without_gender_markers():
    assert meet_program.parse_relay_member_names(
        "Marin, Rosario 21 Hidalgo, Doris 48"
    ) == ["Marin, Rosario", "Hidalgo, Doris"]
    assert meet_program.parse_relay_member_names(
        "Van Der Schraft, Francisca 44Emmons, Rebecca 43"
    ) == ["Van Der Schraft, Francisca", "Emmons, Rebecca"]


def test_validate_entries_blocks_missing_invalid_and_duplicate_lane_identity():
    valid = meet_program.MeetProgramEntry(
        session_number=1,
        session_name="Jornada Unica",
        event_number=1,
        event_name="Women 50 SC Meter Freestyle",
        heat_number=1,
        heat_total=2,
        lane=1,
        display_name="Uno, Ana",
        age=25,
        team_name="CLUB",
        seed_time_text="NT",
        seed_time_ms=None,
        entry_type="individual",
        relay_members=[],
        page_number=1,
        column_number=1,
        line_number=4,
    )
    lane_zero = meet_program.MeetProgramEntry(**{**valid.__dict__, "lane": 0})
    invalid = meet_program.MeetProgramEntry(
        **{
            **valid.__dict__,
            "event_number": 0,
            "lane": -1,
            "display_name": "",
        }
    )

    issues = meet_program.validate_entries(
        [valid, valid, lane_zero, invalid], text_word_count=10
    )
    keys = {issue.issue_key for issue in issues}

    assert {
        "invalid_event_number",
        "invalid_lane",
        "missing_display_name",
        "duplicate_lane_assignment",
    } <= keys


def test_validate_entries_blocks_unparseable_event_identity():
    entry = valid_publication_entry()
    entry.event_name = "Surprise exhibition"

    issues = meet_program.validate_entries([entry], text_word_count=10)

    assert "unparseable_event_identity" in {issue.issue_key for issue in issues}


def test_validate_entries_blocks_image_only_and_zero_entry_inputs():
    issues = meet_program.validate_entries([], text_word_count=0)
    keys = {issue.issue_key for issue in issues}

    assert keys == {"image_only_or_no_text", "no_entries_found"}


def test_write_artifacts_and_validate_input_dir(tmp_path):
    parsed = meet_program.ParsedMeetProgram(
        entries=[
            meet_program.MeetProgramEntry(
                session_number=1,
                session_name="Jornada Unica",
                event_number=1,
                event_name="Women 50 SC Meter Freestyle",
                heat_number=1,
                heat_total=1,
                lane=4,
                display_name="Uno, Ana",
                age=25,
                team_name="CLUB",
                seed_time_text="NT",
                seed_time_ms=None,
                entry_type="individual",
                relay_members=[],
                page_number=1,
                column_number=1,
                line_number=4,
            )
        ],
        unparsed=[],
        metadata={
            "pdf_name": "program.pdf",
            "pdf_sha256": "a" * 64,
            "parser_version": meet_program.PARSER_VERSION,
            "text_word_count": 20,
            **VALID_HEADER_METADATA,
        },
    )

    summary = meet_program.write_artifacts(parsed, tmp_path)
    validated = meet_program.validate_input_dir(tmp_path)

    assert summary.state == "validated"
    assert validated.state == "validated"
    assert (tmp_path / "metadata.json").exists()
    assert (tmp_path / "meet_program_entries.csv").exists()
    assert (tmp_path / "debug_unparsed_lines.csv").exists()
    assert (tmp_path / "validation_summary.json").exists()
    assert json.loads((tmp_path / "metadata.json").read_text(encoding="utf-8"))[
        "pdf_sha256"
    ] == "a" * 64
    with (tmp_path / "meet_program_entries.csv").open(
        encoding="utf-8-sig", newline=""
    ) as stream:
        assert list(csv.DictReader(stream))[0]["display_name"] == "Uno, Ana"


def test_publish_requires_validated_artifacts_before_opening_database(
    tmp_path, monkeypatch
):
    (tmp_path / "validation_summary.json").write_text(
        json.dumps({"state": "requires_review"}),
        encoding="utf-8",
    )
    opened = False

    def forbidden_connect(_args):
        nonlocal opened
        opened = True

    monkeypatch.setattr(meet_program, "connect_database", forbidden_connect)

    with pytest.raises(meet_program.MeetProgramError, match="validated"):
        meet_program.publish_from_artifacts(
            tmp_path,
            competition_id=1,
            source_url=None,
            args=object(),
        )

    assert opened is False


def test_publish_requires_nonblank_artifact_parser_version_before_database(
    tmp_path, monkeypatch
):
    parsed = valid_parsed_program()
    parsed.metadata["parser_version"] = " "
    meet_program.write_artifacts(parsed, tmp_path)
    monkeypatch.setattr(
        meet_program,
        "connect_database",
        lambda _args: pytest.fail("database must remain untouched"),
    )

    with pytest.raises(meet_program.MeetProgramError, match="parser_version"):
        meet_program.publish_from_artifacts(
            tmp_path, competition_id=7, source_url=None, args=object()
        )


class FakeTransaction:
    def __init__(self, connection):
        self.connection = connection

    def __enter__(self):
        self.connection.transaction_started = True
        return self

    def __exit__(self, exc_type, _exc, _traceback):
        if exc_type is not None:
            self.connection.rolled_back = True
        else:
            self.connection.committed = True
        return False


class FakeCursor:
    def __init__(self, connection):
        self.connection = connection
        self.row = None

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, statement, params=None):
        normalized = " ".join(statement.lower().split())
        self.connection.statements.append((normalized, params))
        if "select id, source_id" in normalized and "from core.competition" in normalized:
            self.row = self.connection.competition
        elif "select id from core.meet_program_publication" in normalized:
            self.row = self.connection.existing_publication
        elif "insert into core.source_document" in normalized:
            self.row = (41,)
        elif "insert into core.meet_program_publication" in normalized:
            self.row = (51,)
        else:
            self.row = None

    def executemany(self, statement, rows):
        normalized = " ".join(statement.lower().split())
        materialized = list(rows)
        self.connection.statements.append((normalized, materialized))
        if self.connection.fail_entries:
            raise RuntimeError("entry insert failed")

    def fetchone(self):
        return self.row


class FakeConnection:
    def __init__(
        self,
        existing_publication=None,
        fail_entries=False,
        competition=(
            7,
            3,
            "X Copa Master San Bernardo",
            "2026-07-04",
            "2026-07-04",
        ),
    ):
        self.existing_publication = existing_publication
        self.fail_entries = fail_entries
        self.competition = competition
        self.statements = []
        self.transaction_started = False
        self.committed = False
        self.rolled_back = False

    def transaction(self):
        return FakeTransaction(self)

    def cursor(self):
        return FakeCursor(self)


def valid_publication_entry():
    return meet_program.MeetProgramEntry(
        session_number=1,
        session_name="Jornada Unica",
        event_number=1,
        event_name="Women 50 SC Meter Freestyle",
        heat_number=1,
        heat_total=1,
        lane=4,
        display_name="Uno, Ana",
        age=25,
        team_name="CLUB",
        seed_time_text="NT",
        seed_time_ms=None,
        entry_type="individual",
        relay_members=[],
        page_number=1,
        column_number=1,
        line_number=4,
    )


def valid_parsed_program(*, lane=4, checksum="a" * 64):
    entry = valid_publication_entry()
    entry.lane = lane
    return meet_program.ParsedMeetProgram(
        entries=[entry],
        unparsed=[],
        metadata={
            "pdf_name": "program.pdf",
            "pdf_sha256": checksum,
            "parser_version": meet_program.PARSER_VERSION,
            "text_word_count": 20,
            **VALID_HEADER_METADATA,
        },
    )


def test_missing_pdf_header_produces_review_artifacts_but_blocks_validation(tmp_path):
    parsed = valid_parsed_program()
    for key in VALID_HEADER_METADATA:
        parsed.metadata[key] = None

    summary = meet_program.write_artifacts(parsed, tmp_path)

    assert summary.state == "requires_review"
    assert {issue.issue_key for issue in summary.issues} == {
        "missing_competition_header"
    }
    assert (tmp_path / "meet_program_entries.csv").exists()
    assert (tmp_path / "validation_summary.json").exists()


def test_multiday_program_requires_segment_date_within_competition_range(tmp_path):
    parsed = valid_parsed_program()
    parsed.metadata.update(
        {
            "source_competition_start_date": "2026-07-23",
            "source_competition_end_date": "2026-07-26",
            "stage_number": 1,
            "pool_role": "competition",
            "scheduled_date": None,
        }
    )

    missing = meet_program.write_artifacts(parsed, tmp_path / "missing")
    assert "missing_segment_date" in {issue.issue_key for issue in missing.issues}

    parsed.metadata["scheduled_date"] = "2026-07-27"
    outside = meet_program.write_artifacts(parsed, tmp_path / "outside")
    assert "segment_date_outside_competition" in {
        issue.issue_key for issue in outside.issues
    }

    parsed.metadata["scheduled_date"] = "2026-07-23"
    valid = meet_program.write_artifacts(parsed, tmp_path / "valid")
    assert valid.state == "validated"


def test_artifact_binding_rejects_mixed_entry_snapshot(tmp_path):
    first, second = tmp_path / "first", tmp_path / "second"
    meet_program.write_artifacts(valid_parsed_program(lane=4), first)
    meet_program.write_artifacts(valid_parsed_program(lane=5, checksum="b" * 64), second)
    (first / "meet_program_entries.csv").write_bytes(
        (second / "meet_program_entries.csv").read_bytes()
    )

    with pytest.raises(meet_program.MeetProgramError, match="binding"):
        meet_program.validate_input_dir(first)


def test_publish_rejects_truncated_snapshot_before_database_mutation(tmp_path, monkeypatch):
    meet_program.write_artifacts(valid_parsed_program(), tmp_path)
    entries_path = tmp_path / "meet_program_entries.csv"
    entries_path.write_bytes(entries_path.read_bytes().splitlines(keepends=True)[0])
    monkeypatch.setattr(
        meet_program,
        "connect_database",
        lambda _args: pytest.fail("database must remain untouched"),
    )

    with pytest.raises(meet_program.MeetProgramError, match="binding"):
        meet_program.publish_from_artifacts(
            tmp_path, competition_id=7, source_url=None, args=object()
        )


def test_publish_rejects_tampered_header_metadata_before_database_mutation(
    tmp_path, monkeypatch
):
    meet_program.write_artifacts(valid_parsed_program(), tmp_path)
    metadata_path = tmp_path / "metadata.json"
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    metadata["source_competition_name"] = "Copa Master Valparaiso"
    metadata_path.write_text(json.dumps(metadata), encoding="utf-8")
    forbidden = lambda _args: pytest.fail("database must remain untouched")
    monkeypatch.setattr(meet_program, "connect_database", forbidden)
    with pytest.raises(meet_program.MeetProgramError, match="binding"):
        meet_program.publish_from_artifacts(
            tmp_path, competition_id=7, source_url=None, args=object()
        )


def test_validation_rejects_legacy_artifacts_without_identity_binding(tmp_path):
    meet_program.write_artifacts(valid_parsed_program(), tmp_path)
    metadata_path = tmp_path / "metadata.json"
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    del metadata["artifact_binding"]["artifact_identity"]
    metadata_path.write_text(json.dumps(metadata), encoding="utf-8")

    with pytest.raises(meet_program.MeetProgramError, match="identity binding"):
        meet_program.validate_input_dir(tmp_path)


def test_publish_rejects_tampered_parser_version_before_database_mutation(
    tmp_path, monkeypatch
):
    meet_program.write_artifacts(valid_parsed_program(), tmp_path)
    metadata_path = tmp_path / "metadata.json"
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    metadata["parser_version"] = "forged-999"
    metadata_path.write_text(json.dumps(metadata), encoding="utf-8")
    monkeypatch.setattr(
        meet_program,
        "connect_database",
        lambda _args: pytest.fail("database must remain untouched"),
    )

    with pytest.raises(meet_program.MeetProgramError, match="binding"):
        meet_program.publish_from_artifacts(
            tmp_path, competition_id=7, source_url=None, args=object()
        )


def test_publish_is_transactional_and_supersedes_only_after_entries_exist():
    connection = FakeConnection()

    publication_id, created = meet_program.publish_validated_program(
        connection,
        [valid_publication_entry()],
        {
            "pdf_name": "program.pdf",
            "pdf_sha256": "a" * 64,
            "parser_version": meet_program.PARSER_VERSION,
            **VALID_HEADER_METADATA,
        },
        competition_id=7,
        source_url="https://example.test/program.pdf",
        schema="core",
    )

    assert (publication_id, created) == (51, True)
    assert connection.transaction_started is True
    assert connection.committed is True
    statements = [statement for statement, _params in connection.statements]
    entry_insert_index = next(
        index
        for index, statement in enumerate(statements)
        if "insert into core.meet_program_entry" in statement
    )
    supersede_index = next(
        index
        for index, statement in enumerate(statements)
        if "update core.meet_program_publication" in statement
        and "set status = 'superseded'" in statement
    )
    assert entry_insert_index < supersede_index
    supersede_params = next(
        params
        for statement, params in connection.statements
        if "set status = 'superseded'" in statement
    )
    assert supersede_params == (7, 1, "main")


def test_publish_scopes_replacement_to_stage_and_pool_role():
    connection = FakeConnection()
    metadata = {
        "pdf_name": "program.pdf",
        "pdf_sha256": "a" * 64,
        "parser_version": meet_program.PARSER_VERSION,
        "stage_number": 5,
        "pool_role": "training",
        "scheduled_date": "2026-07-25",
        **VALID_HEADER_METADATA,
    }

    meet_program.publish_validated_program(
        connection,
        [valid_publication_entry()],
        metadata,
        competition_id=7,
        source_url=None,
        schema="core",
    )

    publication_insert = next(
        params
        for statement, params in connection.statements
        if "insert into core.meet_program_publication" in statement
    )
    assert publication_insert[1:4] == (5, "training", "2026-07-25")
    supersede_params = next(
        params
        for statement, params in connection.statements
        if "set status = 'superseded'" in statement
    )
    assert supersede_params == (7, 5, "training")


def test_publish_is_idempotent_for_same_competition_and_checksum():
    connection = FakeConnection(existing_publication=(88,))

    result = meet_program.publish_validated_program(
        connection,
        [valid_publication_entry()],
        {
            "pdf_name": "program.pdf",
            "pdf_sha256": "a" * 64,
            "parser_version": meet_program.PARSER_VERSION,
            **VALID_HEADER_METADATA,
        },
        competition_id=7,
        source_url=None,
        schema="core",
    )

    assert result == (88, False)
    assert not any(
        "insert into core.meet_program_publication" in statement
        for statement, _params in connection.statements
    )
    lookup = next(
        params
        for statement, params in connection.statements
        if "select id from core.meet_program_publication" in statement
    )
    assert lookup == (7, "a" * 64, meet_program.PARSER_VERSION)


def test_publish_same_checksum_with_new_parser_version_creates_revision():
    connection = FakeConnection(existing_publication=None)
    metadata = {
        "pdf_name": "program.pdf",
        "pdf_sha256": "a" * 64,
        "parser_version": "0.2.2",
        **VALID_HEADER_METADATA,
    }

    result = meet_program.publish_validated_program(
        connection,
        [valid_publication_entry()],
        metadata,
        competition_id=7,
        source_url=None,
        schema="core",
    )

    assert result == (51, True)
    assert any(
        "on conflict (checksum_sha256)" in statement
        for statement, _params in connection.statements
    )


def test_publish_failure_rolls_back_before_current_version_is_superseded():
    connection = FakeConnection(fail_entries=True)

    with pytest.raises(RuntimeError, match="entry insert failed"):
        meet_program.publish_validated_program(
            connection,
            [valid_publication_entry()],
            {
                "pdf_name": "program.pdf",
                "pdf_sha256": "a" * 64,
                "parser_version": meet_program.PARSER_VERSION,
                **VALID_HEADER_METADATA,
            },
            competition_id=7,
            source_url=None,
            schema="core",
        )

    assert connection.rolled_back is True
    assert not any(
        "set status = 'superseded'" in statement
        for statement, _params in connection.statements
    )


def test_publish_header_mismatch_occurs_before_any_database_write():
    connection = FakeConnection(
        competition=(
            7,
            3,
            "Copa Master Valparaiso",
            "2026-07-04",
            "2026-07-04",
        )
    )

    with pytest.raises(meet_program.MeetProgramError, match="name mismatch"):
        meet_program.publish_validated_program(
            connection,
            [valid_publication_entry()],
            {
                "pdf_name": "program.pdf",
                "pdf_sha256": "a" * 64,
                "parser_version": meet_program.PARSER_VERSION,
                **VALID_HEADER_METADATA,
            },
            competition_id=7,
            source_url=None,
            schema="core",
        )

    assert connection.rolled_back is True
    assert not any(
        statement.startswith(("insert ", "update "))
        for statement, _params in connection.statements
    )


def _meet_manager_row(
    *, event, heat_cell, lane, name, age, team, seed, pad=3, relay_members=None
):
    """Fila del export CSV de Meet Manager: encabezado repetido + una inscripcion.

    `pad` desplaza el bloque de datos para representar exports impresos a
    distinta cantidad de columnas.
    """
    is_relay = relay_members is not None
    row = [
        "Natatorio Chileno",
        "HY-TEK's MEET MANAGER 7.0 - 9:51 AM  29-07-2026  P\u00e1gina 1",
        "III COPA \u00d1U\u00d1OA MASTER 2026 - 08-08-2026",
        "",
        "",
        "Programa de Competencias - COPA NUNOA MASTER 2026",
        event,
    ]
    row += [""] * pad
    row += [
        "Carril",
        "Equipo" if is_relay else "Nombre",
        "Edad",
        "Relevo" if is_relay else "Equipo",
        "Tiempo para Sembrado",
    ]
    row += [heat_cell, str(lane), name, age, team, seed, ""]
    row += relay_members or []
    return row


def _write_meet_manager_csv(path, rows, encoding="cp1252"):
    buffer = io.StringIO()
    csv.writer(buffer, lineterminator="\n").writerows(rows)
    path.write_bytes(buffer.getvalue().encode(encoding))
    return path


def test_meet_manager_csv_anchors_on_lane_label_not_fixed_columns(tmp_path):
    """El ancho del export depende de a cuantas columnas se imprima el reporte."""
    rows = [
        _meet_manager_row(
            event="#1 Mixto 100 CL Metro Estilo Libre",
            heat_cell="Serie   1 of 13   Finales   Inicia a las  09:30 AM",
            lane=2, name="Zambrano, Juan", age="M24", team="NEURO", seed="NT",
            pad=3,
        ),
        _meet_manager_row(
            event="#1 Mixto 100 CL Metro Estilo Libre",
            heat_cell="Serie   1 of 13   Finales   Inicia a las  09:35 AM",
            lane=3, name="Lopez, Amparo", age="W64", team="MPROV", seed="3:45,00",
            pad=57,
        ),
    ]
    parsed = meet_program.parse_meet_manager_csv(
        _write_meet_manager_csv(tmp_path / "programa.csv", rows)
    )

    assert [entry.lane for entry in parsed.entries] == [2, 3]
    assert [entry.display_name for entry in parsed.entries] == [
        "Zambrano, Juan",
        "Lopez, Amparo",
    ]
    assert [entry.age for entry in parsed.entries] == [24, 64]
    assert [entry.team_name for entry in parsed.entries] == ["NEURO", "MPROV"]
    assert parsed.entries[0].seed_time_ms is None
    assert parsed.entries[1].seed_time_ms == 225000
    assert parsed.entries[0].heat_total == 13
    assert not parsed.unparsed


def test_meet_manager_csv_decodes_cp1252_and_reads_competition_header(tmp_path):
    rows = [
        _meet_manager_row(
            event="#1 Mixto 100 CL Metro Estilo Libre",
            heat_cell="Serie   1 of 2   Finales   Inicia a las  09:30 AM",
            lane=4, name="Mu\u00f1oz, Valeria", age="W43", team="LQBLO", seed="4:00,00",
        )
    ]
    parsed = meet_program.parse_meet_manager_csv(
        _write_meet_manager_csv(tmp_path / "programa.csv", rows)
    )

    assert parsed.entries[0].display_name == "Mu\u00f1oz, Valeria"
    assert parsed.metadata["source_kind"] == "meet_manager_csv"
    assert parsed.metadata["source_competition_name"] == "III COPA \u00d1U\u00d1OA MASTER 2026"
    assert parsed.metadata["scheduled_date"] == "2026-08-08"
    # El titulo del reporte tambien parsea como nombre; solo el que trae fecha cuenta.
    assert "source_competition_header_conflict" not in parsed.metadata


def test_meet_manager_csv_takes_start_time_from_first_row_of_each_heat(tmp_path):
    """El export corre la hora: la primera fila trae la suya, el resto la siguiente."""
    rows = [
        _meet_manager_row(
            event="#1 Mixto 100 CL Metro Estilo Libre",
            heat_cell="Serie   1 of 2   Finales   Inicia a las  09:30 AM",
            lane=2, name="Uno, Nadador", age="M24", team="NEURO", seed="NT",
        ),
        _meet_manager_row(
            event="#1 Mixto 100 CL Metro Estilo Libre",
            heat_cell="Serie   1 of 2   Finales   Inicia a las  09:35 AM",
            lane=3, name="Dos, Nadador", age="M25", team="NEURO", seed="NT",
        ),
        _meet_manager_row(
            event="#1 Mixto 100 CL Metro Estilo Libre",
            heat_cell="Serie   2 of 2   Finales   Inicia a las  09:35 AM",
            lane=2, name="Tres, Nadador", age="M26", team="NEURO", seed="NT",
        ),
        _meet_manager_row(
            event="#1 Mixto 100 CL Metro Estilo Libre",
            heat_cell="Serie   2  (#1 Mixto 100 CL Metro Estilo Libre)",
            lane=3, name="Cuatro, Nadador", age="M27", team="NEURO", seed="NT",
        ),
    ]
    parsed = meet_program.parse_meet_manager_csv(
        _write_meet_manager_csv(tmp_path / "programa.csv", rows)
    )

    assert [entry.estimated_start_time for entry in parsed.entries] == [
        "09:30", None, "09:35", None,
    ]
    # El encabezado de continuacion no trae total y no debe inventarlo.
    assert [entry.heat_number for entry in parsed.entries] == [1, 1, 2, 2]
    assert parsed.entries[3].heat_total is None


def test_meet_manager_csv_maps_relay_team_letter_and_members(tmp_path):
    rows = [
        _meet_manager_row(
            event="#7 Mixto 400 CL Metro Combinado Relevo",
            heat_cell="Serie   1 of 2   Finales   Inicia a las  11:22 AM",
            lane=2, name="SDEPO", age="X240", team="E", seed="NT",
            relay_members=[
                "Mora, Ivonne W53",
                "Von Marttens, Nelly W64",
                "Barraza, Mario M71",
                "Gallardo, Egmont M70",
            ],
        )
    ]
    entry = meet_program.parse_meet_manager_csv(
        _write_meet_manager_csv(tmp_path / "programa.csv", rows)
    ).entries[0]

    assert entry.entry_type == "relay"
    assert entry.display_name == "SDEPO E"
    assert entry.team_name == "SDEPO"
    # X240 es la edad sumada del equipo, no la de un nadador.
    assert entry.age is None
    assert entry.relay_members == [
        "Mora, Ivonne W53",
        "Von Marttens, Nelly W64",
        "Barraza, Mario M71",
        "Gallardo, Egmont M70",
    ]


def test_meet_manager_csv_sends_unusable_rows_to_debug_instead_of_dropping(tmp_path):
    rows = [
        ["Natatorio Chileno", "sin bloque de datos", "", ""],
        _meet_manager_row(
            event="#1 Mixto 100 CL Metro Estilo Libre",
            heat_cell="sin serie reconocible",
            lane=2, name="Uno, Nadador", age="M24", team="NEURO", seed="NT",
        ),
    ]
    parsed = meet_program.parse_meet_manager_csv(
        _write_meet_manager_csv(tmp_path / "programa.csv", rows)
    )

    assert not parsed.entries
    assert [line.reason for line in parsed.unparsed] == [
        "missing_lane_label",
        "incomplete_entry_row",
    ]


def _spanish_program_lines():
    """Programa HY-TEK rotulado en espanol, como los emite FCHMN."""
    texts = [
        "#1 Mixto 100 CL Metro Estilo Libre",
        "Carril Nombre EdadT Eieqmuippoo para Sembrado",
        "Serie 1 of 38 Finales Inicia a las 09:30 AM",
        "2 Zambrano, Juan M24 NEURO NT",
        "3 Miranda, Iris W43 LQBLO 4:00,00",
        "Serie 2 of 38 Finales Inicia a las 09:33 AM",
        "0 Bravo, Esteban M35 MUCH NT",
    ]
    return [meet_program.SourceLine(1, 1, index, text)
            for index, text in enumerate(texts, start=1)]


def test_parse_accepts_spanish_heat_labels_from_fchmn_programs():
    """Sin "Serie" no se establece contexto y toda inscripcion cae sin parsear."""
    parsed = meet_program.parse_source_lines(_spanish_program_lines())

    assert not parsed.unparsed
    assert [entry.lane for entry in parsed.entries] == [2, 3, 0]
    first = parsed.entries[0]
    assert first.event_number == 1
    assert first.event_name == "Mixto 100 CL Metro Estilo Libre"
    assert (first.heat_number, first.heat_total) == (1, 38)
    assert first.estimated_start_time == "09:30"
    assert (first.display_name, first.age, first.team_name) == ("Zambrano, Juan", 24, "NEURO")
    assert parsed.entries[1].seed_time_ms == 240000
    assert parsed.entries[2].heat_number == 2


def test_spanish_column_header_is_skipped_without_touching_surnames():
    """El encabezado llega con las celdas entrelazadas; "Carriles," no lo es."""
    lines = [
        meet_program.SourceLine(1, 1, 1, "#7 Mixto 400 CL Metro Combinado Relevo"),
        meet_program.SourceLine(1, 1, 2, "Carril Equipo TRieelmevpoo para Sembrado"),
        meet_program.SourceLine(1, 1, 3, "Serie 1 of 6 Finales Inicia a las 02:34 PM"),
        meet_program.SourceLine(1, 1, 4, "1 SDEPO X240 E NT"),
        meet_program.SourceLine(1, 1, 5, "Carriles, Matias M31 Munizaga, Javiera W30"),
    ]

    parsed = meet_program.parse_source_lines(lines)

    assert not parsed.unparsed
    entry = parsed.entries[0]
    assert entry.entry_type == "relay"
    assert entry.team_name == "SDEPO"
    # X240 es la edad sumada del equipo, no la de un nadador.
    assert entry.age is None
    assert entry.relay_members == ["Carriles, Matias", "Munizaga, Javiera"]


def test_spanish_continuation_header_keeps_event_context():
    lines = [
        meet_program.SourceLine(1, 1, 1, "#2 Mixto 50 CL Metro Estilo de Espalda"),
        meet_program.SourceLine(1, 1, 2, "Serie 3 (#2 Mixto 50 CL Metro Estilo de Espalda)"),
        meet_program.SourceLine(1, 1, 3, "4 Lopez, Amparo W64 MPROV 3:45,00"),
    ]

    parsed = meet_program.parse_source_lines(lines)

    assert not parsed.unparsed
    assert parsed.entries[0].event_number == 2
    assert parsed.entries[0].heat_number == 3


def test_column_count_and_body_split_anchor_on_both_languages():
    spanish_three = [
        {"text": "Serie", "x0": 18.0},
        {"text": "Serie", "x0": 212.4},
        {"text": "Serie", "x0": 406.8},
    ]
    spanish_two = [
        {"text": "Serie", "x0": 18.0},
        {"text": "Serie", "x0": 309.6},
    ]

    assert meet_program.detect_page_column_count(spanish_three) == 3
    assert meet_program.detect_page_column_count(spanish_two) == 2
    # El corte entre encabezado de pagina y cuerpo usa el mismo vocabulario. Sin
    # "serie", los encabezados que HY-TEK repite arriba de cada columna quedan en
    # la banda de ancho completo, se fusionan y las inscripciones de la segunda
    # columna heredan la serie de la primera.
    source = (SCRIPTS_DIR / "run_meet_program.py").read_text(encoding="utf-8")
    assert '{"heat", "event", "serie"}' in source


def test_relay_events_are_detected_by_spanish_label():
    assert meet_program._event_parts("Mixto 400 CL Metro Combinado Relevo")[1] == "relay"
    assert meet_program._event_parts("Mixed 200 LC Meter Medley Relay")[1] == "relay"
    assert meet_program._event_parts("Mixto 100 CL Metro Estilo Libre")[1] == "individual"
