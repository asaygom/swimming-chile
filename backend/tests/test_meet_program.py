import csv
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
    assert result.entries[1].seed_time_text == "2:51,37"
    assert result.entries[1].seed_time_ms == 171370
    assert result.entries[1].team_name == "NUMAS"


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
    invalid = meet_program.MeetProgramEntry(
        **{
            **valid.__dict__,
            "event_number": 0,
            "lane": 0,
            "display_name": "",
        }
    )

    issues = meet_program.validate_entries([valid, valid, invalid], text_word_count=10)
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
    with pytest.raises(meet_program.MeetProgramError, match="header binding"):
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
