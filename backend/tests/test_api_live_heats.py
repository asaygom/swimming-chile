import hashlib
import sys
from contextlib import contextmanager
from pathlib import Path

import pytest
from fastapi import HTTPException, Request, Response


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from api.routers import live_heats


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
        self.committed = False

    def cursor(self):
        return self.cursor_instance

    def commit(self):
        self.committed = True


def install_database(monkeypatch, one_rows, all_rows=None):
    cursor = FakeCursor(one_rows, all_rows)
    connection = FakeConnection(cursor)

    @contextmanager
    def fake_connection():
        yield connection

    monkeypatch.setattr(live_heats, "get_db_connection", fake_connection)
    return cursor, connection


def configure_auth(monkeypatch):
    monkeypatch.setenv("LIVE_HEAT_OPERATOR_COMPETITION_ID", "7")
    monkeypatch.setenv(
        "LIVE_HEAT_OPERATOR_CODE_SHA256",
        hashlib.sha256(b"pilot-code").hexdigest(),
    )
    monkeypatch.setenv("LIVE_HEAT_SESSION_SECRET", "test-secret-with-enough-entropy")
    monkeypatch.setenv("LIVE_HEAT_COOKIE_SECURE", "true")


def request_from(address="203.0.113.8", forwarded_for=None):
    headers = [] if forwarded_for is None else [(b"x-forwarded-for", forwarded_for.encode())]
    return Request({"type": "http", "client": (address, 1234), "headers": headers})


def test_operator_code_is_exchanged_for_scoped_http_only_cookie(monkeypatch):
    configure_auth(monkeypatch)
    response = Response()

    result = live_heats.create_operator_session(
        7, live_heats.OperatorCode(code="pilot-code"), response, request_from()
    )

    cookie = response.headers["set-cookie"]
    assert result == {"authenticated": True, "expires_in_seconds": 14400}
    assert live_heats.COOKIE_NAME in cookie
    assert "HttpOnly" in cookie
    assert "SameSite=lax" in cookie
    assert "Secure" in cookie
    assert "pilot-code" not in cookie


def test_operator_logout_clears_the_scoped_cookie_with_matching_security(monkeypatch):
    configure_auth(monkeypatch)
    response = Response()

    live_heats.delete_operator_session(7, response)

    cookie = response.headers["set-cookie"]
    assert cookie.startswith(f"{live_heats.COOKIE_NAME}=\"")
    assert "Max-Age=0" in cookie
    assert "HttpOnly" in cookie
    assert "SameSite=lax" in cookie
    assert "Secure" in cookie
    assert "Path=/api/competitions/7/live-heat" in cookie
    route = next(
        route for route in live_heats.router.routes
        if route.path == "/{competition_id}/live-heat/session/logout"
    )
    assert route.methods == {"POST"}
    assert route.status_code == 204


def test_operator_session_rejects_wrong_code(monkeypatch):
    configure_auth(monkeypatch)

    with pytest.raises(HTTPException) as error:
        live_heats.create_operator_session(
            7, live_heats.OperatorCode(code="wrong"), Response(), request_from()
        )

    assert error.value.status_code == 401


def test_public_snapshot_returns_current_heat_entries(monkeypatch):
    state = {
        "publication_id": 51,
        "stage_number": 1,
        "pool_role": "main",
        "session_number": 1,
        "event_number": 3,
        "event_name": "Women 100 LC Meter Freestyle",
        "heat_number": 2,
        "heat_total": 4,
        "status": "active",
        "revision": 5,
        "updated_at": "2026-07-31T12:00:00Z",
    }
    entries = [{"lane": 4, "display_name": "Uno, Ana", "club_name": "CLUB"}]
    install_database(monkeypatch, [{"id": 7}, state], entries)

    payload = live_heats.get_live_heat(7)

    assert payload == {"competition_id": 7, "state": state, "entries": entries}


def test_public_snapshot_ignores_state_from_superseded_publication(monkeypatch):
    cursor, _connection = install_database(monkeypatch, [{"id": 7}, None])

    assert live_heats.get_live_heat(7) == {
        "competition_id": 7, "state": None, "entries": []
    }
    state_query = cursor.executed[1][0]
    assert "JOIN core.meet_program_publication" in state_query
    assert "p.status = 'published'" in state_query
    assert "ORDER BY s.updated_at DESC" in state_query


def test_update_requires_matching_revision_and_published_heat(monkeypatch):
    configure_auth(monkeypatch)
    token, session_id = live_heats._create_session_token(7)
    updated = {
        "publication_id": 51,
        "stage_number": 1,
        "pool_role": "main",
        "session_number": 1,
        "event_number": 3,
        "heat_number": 2,
        "status": "active",
        "revision": 6,
        "updated_at": "2026-07-31T12:01:00Z",
    }
    cursor, connection = install_database(
        monkeypatch,
        [{"id": 51}, {"exists": 1}, {"revision": 5}, updated],
    )

    result = live_heats.update_live_heat(
        7,
        live_heats.LiveHeatUpdate(
            publication_id=51,
            stage_number=1,
            pool_role="main",
            session_number=1,
            event_number=3,
            heat_number=2,
            status="active",
            expected_revision=5,
        ),
        token,
    )

    assert result == {"competition_id": 7, "state": updated}
    assert connection.committed is True
    update_query, update_params = next(
        item for item in cursor.executed
        if item[0].startswith("UPDATE core.live_heat_state")
    )
    assert update_query.startswith("UPDATE core.live_heat_state")
    assert "revision = revision + 1" in update_query
    assert "revision = %(current_revision)s" in update_query
    assert update_params["updated_by_session"] == session_id
    history_query, history_params = cursor.executed[-1]
    assert history_query.startswith("INSERT INTO core.live_heat_movement")
    assert history_params["operator_session_fingerprint"] == hashlib.sha256(
        session_id.encode("utf-8")
    ).hexdigest()
    assert history_params["previous_revision"] == 5
    assert history_params["resulting_revision"] == 6


@pytest.mark.parametrize("existing", [{"revision": 3}, None])
def test_update_reports_conflict_for_stale_or_nonzero_initial_revision(
    monkeypatch, existing
):
    configure_auth(monkeypatch)
    token, _session_id = live_heats._create_session_token(7)
    cursor, connection = install_database(
        monkeypatch, [{"id": 51}, {"exists": 1}, existing]
    )

    with pytest.raises(HTTPException) as error:
        live_heats.update_live_heat(
            7,
            live_heats.LiveHeatUpdate(
                publication_id=51,
                session_number=1,
                event_number=3,
                heat_number=2,
                status="active",
                expected_revision=2,
            ),
            token,
        )

    assert error.value.status_code == 409
    assert connection.committed is False
    assert len(cursor.executed) == 3


def test_initial_update_uses_insert_only_for_revision_zero(monkeypatch):
    configure_auth(monkeypatch)
    token, _session_id = live_heats._create_session_token(7)
    created = {"publication_id": 51, "revision": 1}
    cursor, connection = install_database(
        monkeypatch, [{"id": 51}, {"exists": 1}, None, created]
    )

    result = live_heats.update_live_heat(
        7,
        live_heats.LiveHeatUpdate(
            publication_id=51, session_number=1, event_number=3,
            heat_number=2, status="active", expected_revision=0,
        ),
        token,
    )

    assert result["state"] == created
    state_insert = next(
        query for query, _params in cursor.executed
        if query.startswith("INSERT INTO core.live_heat_state")
    )
    assert "ON CONFLICT DO NOTHING" in state_insert
    history_query, history_params = cursor.executed[-1]
    assert history_query.startswith("INSERT INTO core.live_heat_movement")
    assert history_params["previous_publication_id"] is None
    assert history_params["previous_revision"] is None
    assert connection.committed is True


def test_revision_zero_adopts_replacement_after_publication_rollover(monkeypatch):
    configure_auth(monkeypatch)
    token, _session_id = live_heats._create_session_token(7)
    install_database(monkeypatch, [{"id": 7}, None])
    assert live_heats.get_live_heat(7)["state"] is None

    adopted = {"publication_id": 52, "revision": 10}
    cursor, connection = install_database(monkeypatch, [
        {"id": 52},
        {"exists": 1},
        {"publication_id": 51, "revision": 9, "publication_status": "superseded"},
        adopted,
    ])

    result = live_heats.update_live_heat(
        7,
        live_heats.LiveHeatUpdate(
            publication_id=52, session_number=1, event_number=1,
            heat_number=1, status="active", expected_revision=0,
        ),
        token,
    )

    assert result["state"] == adopted
    state_update = next(
        query for query, _params in cursor.executed
        if query.startswith("UPDATE core.live_heat_state")
    )
    assert "revision = revision + 1" in state_update
    assert cursor.executed[-1][1]["previous_publication_id"] == 51
    assert "FOR SHARE" in cursor.executed[0][0]
    assert connection.committed is True


def test_operator_code_throttle_locks_and_success_resets(monkeypatch):
    configure_auth(monkeypatch)
    live_heats._attempts.clear()
    request = request_from()
    for _attempt in range(live_heats.MAX_CODE_FAILURES):
        with pytest.raises(HTTPException) as error:
            live_heats.create_operator_session(
                7, live_heats.OperatorCode(code="wrong"), Response(), request
            )
        assert error.value.status_code == 401
    with pytest.raises(HTTPException) as error:
        live_heats.create_operator_session(
            7, live_heats.OperatorCode(code="pilot-code"), Response(), request
        )
    assert error.value.status_code == 429

    live_heats._attempts.clear()
    with pytest.raises(HTTPException):
        live_heats.create_operator_session(
            7, live_heats.OperatorCode(code="wrong"), Response(), request
        )
    assert live_heats._attempts
    live_heats.create_operator_session(
        7, live_heats.OperatorCode(code="pilot-code"), Response(), request
    )
    assert live_heats._attempts == {}


def test_forwarded_address_is_used_only_for_trusted_proxy(monkeypatch):
    monkeypatch.setenv("LIVE_HEAT_TRUSTED_PROXY_CIDRS", "10.0.0.0/8")
    forwarded = "198.51.100.4, 10.0.0.5"
    assert live_heats._client_address(request_from("10.0.0.9", forwarded)) == "198.51.100.4"
    assert live_heats._client_address(request_from("192.0.2.9", forwarded)) == "192.0.2.9"


def test_operator_history_is_scoped_bounded_newest_first_and_marks_self(monkeypatch):
    configure_auth(monkeypatch)
    token, session_id = live_heats._create_session_token(7)
    movement = {
        "id": 91, "previous_event_number": 3, "previous_heat_number": 1,
        "resulting_event_number": 3, "resulting_heat_number": 2,
        "resulting_status": "active", "resulting_revision": 6,
        "occurred_at": "2026-08-01T16:00:00Z", "is_current_session": True,
    }
    cursor, _connection = install_database(monkeypatch, [], [movement])

    result = live_heats.list_live_heat_history(7, 12, token)

    assert result == {"competition_id": 7, "movements": [movement]}
    query, params = cursor.executed[0]
    assert "FROM core.live_heat_movement" in query
    assert "competition_id = %s" in query
    assert "ORDER BY occurred_at DESC, id DESC" in query
    assert params == (
        hashlib.sha256(session_id.encode("utf-8")).hexdigest(), 7, 12
    )
