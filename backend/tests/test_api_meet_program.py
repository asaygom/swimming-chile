import sys
from contextlib import contextmanager
from pathlib import Path

import pytest
from fastapi import HTTPException


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from api.routers import competitions


class FakeCursor:
    def __init__(self, one_rows, all_rows=None):
        self.one_rows = iter(one_rows)
        self.all_rows = all_rows or []
        self.executed = []

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, query, params):
        self.executed.append((" ".join(query.split()), params))

    def fetchone(self):
        return next(self.one_rows)

    def fetchall(self):
        return self.all_rows


class FakeConnection:
    def __init__(self, cursor):
        self.cursor_instance = cursor

    def cursor(self):
        return self.cursor_instance


def install_database(monkeypatch, one_rows, all_rows=None):
    cursor = FakeCursor(one_rows, all_rows)

    @contextmanager
    def fake_connection():
        yield FakeConnection(cursor)

    monkeypatch.setattr(competitions, "get_db_connection", fake_connection)
    return cursor


def test_meet_program_unknown_competition_returns_404(monkeypatch):
    install_database(monkeypatch, [None])

    with pytest.raises(HTTPException) as error:
        competitions.get_meet_program(404)

    assert error.value.status_code == 404


def test_meet_program_known_competition_without_publication_is_empty(monkeypatch):
    install_database(monkeypatch, [{"id": 7}, None])

    assert competitions.get_meet_program(7) == {
        "competition_id": 7,
        "publication": None,
        "events_count": 0,
        "sessions": [],
    }


def test_meet_program_exposes_only_active_publication_grouped_and_ordered(monkeypatch):
    rows = [
        {
            "session_number": 1,
            "session_name": "Jornada Unica",
            "event_number": 7,
            "event_name": "Mixed 200 LC Meter Medley Relay",
            "heat_number": 1,
            "heat_total": 2,
            "lane": 2,
            "entry_type": "relay",
            "display_name": "NUMAS X160 A",
            "club_name": "NUMAS",
            "seed_time_text": "2:20,00",
            "seed_time_ms": 140000,
            "relay_members": ["Uno, Ana", "Dos, Beto"],
        },
        {
            "session_number": 1,
            "session_name": "Jornada Unica",
            "event_number": 7,
            "event_name": "Mixed 200 LC Meter Medley Relay",
            "heat_number": 2,
            "heat_total": 2,
            "lane": 1,
            "entry_type": "relay",
            "display_name": "SDEPO X160 A",
            "club_name": "SDEPO",
            "seed_time_text": "NT",
            "seed_time_ms": None,
            "relay_members": [],
        },
    ]
    cursor = install_database(
        monkeypatch,
        [
            {"id": 7},
            {
                "id": 51,
                "published_at": "2026-07-30T10:00:00Z",
                "source_url": "https://example.test/program.pdf",
                "entry_count": 2,
            },
        ],
        rows,
    )

    payload = competitions.get_meet_program(7)

    assert payload["publication"] == {
        "published_at": "2026-07-30T10:00:00Z",
        "source_url": "https://example.test/program.pdf",
        "entry_count": 2,
    }
    assert payload["events_count"] == 1
    assert payload["sessions"][0]["events"][0]["distance_m"] == 200
    assert payload["sessions"][0]["events"][0]["stroke"] == "medley_relay"
    assert [heat["heat_number"] for heat in payload["sessions"][0]["events"][0]["heats"]] == [1, 2]
    assert payload["sessions"][0]["events"][0]["heats"][0]["entries"][0] == {
        "lane": 2,
        "entry_type": "relay",
        "display_name": "NUMAS X160 A",
        "club_name": "NUMAS",
        "seed_time_text": "2:20,00",
        "seed_time_ms": 140000,
        "relay_members": ["Uno, Ana", "Dos, Beto"],
    }
    assert "id" not in payload["publication"]
    assert "athlete_id" not in str(payload)
    assert "club_id" not in str(payload)
    assert any("status = 'published'" in query for query, _params in cursor.executed)
    assert any("ORDER BY" in query for query, _params in cursor.executed)


def test_meet_program_count_is_null_when_a_published_event_is_unparseable(monkeypatch):
    row = {
        "session_number": 1,
        "session_name": "Jornada Unica",
        "event_number": 1,
        "event_name": "Surprise exhibition",
        "heat_number": 1,
        "heat_total": 1,
        "lane": 1,
        "entry_type": "individual",
        "display_name": "Uno, Ana",
        "club_name": "CLUB",
        "seed_time_text": "NT",
        "seed_time_ms": None,
        "relay_members": [],
    }
    install_database(
        monkeypatch,
        [
            {"id": 7},
            {
                "id": 51,
                "published_at": "2026-07-30T10:00:00Z",
                "source_url": None,
                "entry_count": 1,
            },
        ],
        [row],
    )

    payload = competitions.get_meet_program(7)

    assert payload["events_count"] is None
    assert payload["sessions"][0]["events"][0]["distance_m"] is None
    assert payload["sessions"][0]["events"][0]["stroke"] is None


def test_transient_program_error_keeps_series_recovery_tab_visible():
    profile_page = (
        Path(__file__).resolve().parents[2]
        / "frontend/src/features/competitions/pages/CompetitionProfilePage.tsx"
    )
    source = " ".join(profile_page.read_text(encoding="utf-8").split())

    assert (
        "const showSeriesTab = hasPublishedProgram || meetProgramQuery.isError ||"
        in source
    )
    assert "requestedTab === null && hasPublishedProgram && resultsAreEmpty" in source
    assert "`${e.distance_m}-${e.stroke}`" in source
    assert "`${e.distance_m}-${e.stroke}-${e.gender}`" not in source
    assert "meetProgramQuery.data?.events_count" in source
    assert "totalProgramEvents ?? '—'" in source


def test_meet_program_frontend_has_accessible_independent_collapsible_sections():
    view = (
        Path(__file__).resolve().parents[2]
        / "frontend/src/features/competitions/components/MeetProgramView.tsx"
    )
    source = view.read_text(encoding="utf-8")

    assert "const [expandedEvents, setExpandedEvents]" in source
    assert "const [expandedHeats, setExpandedHeats]" in source
    assert "const eventIsExpanded = isFiltering || expandedEvents.has(eventKey)" in source
    assert "const heatIsExpanded = isFiltering || expandedHeats.has(heatKey)" in source
    assert "aria-expanded={eventIsExpanded}" in source
    assert "aria-controls={eventPanelId}" in source
    assert "aria-expanded={heatIsExpanded}" in source
    assert "aria-controls={heatPanelId}" in source
    assert "hidden={!eventIsExpanded}" in source
    assert "aria-hidden={!eventIsExpanded}" in source
    assert "hidden={!heatIsExpanded}" in source
    assert "aria-hidden={!heatIsExpanded}" in source
    assert "setExpandedEvents(new Set())" in source
    assert "setExpandedHeats(new Set())" in source
