import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from api import database, main


class FakeCursor:
    def __init__(self):
        self.executed = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False

    def execute(self, query):
        self.executed.append(query)

    def fetchone(self):
        return (1,)


class FakeConnection:
    def __init__(self):
        self.cursor_instance = FakeCursor()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False

    def cursor(self):
        return self.cursor_instance


def test_database_readiness_uses_select_one_and_three_second_timeout(monkeypatch):
    connection = FakeConnection()
    connect_calls = []

    def fake_connect(connection_string, **kwargs):
        connect_calls.append((connection_string, kwargs))
        return connection

    monkeypatch.setattr(database, "get_connection_string", lambda: "test-dsn")
    monkeypatch.setattr(database.psycopg, "connect", fake_connect)

    assert database.is_database_ready() is True
    assert connect_calls == [("test-dsn", {"connect_timeout": 3})]
    assert connection.cursor_instance.executed == ["SELECT 1"]


def test_database_readiness_returns_false_without_exposing_connection_error(monkeypatch):
    def fail_to_connect(*args, **kwargs):
        raise RuntimeError("postgresql://admin:secret@private-host/swimming")

    monkeypatch.setattr(database.psycopg, "connect", fail_to_connect)

    assert database.is_database_ready() is False


def test_liveness_does_not_check_database(monkeypatch):
    def unexpected_database_check():
        raise AssertionError("liveness must not access PostgreSQL")

    monkeypatch.setattr(main, "is_database_ready", unexpected_database_check)

    assert main.health_check() == {"status": "ok", "message": "API running"}


def test_readiness_returns_exact_success_body(monkeypatch):
    monkeypatch.setattr(main, "is_database_ready", lambda: True)

    assert main.readiness_check() == {
        "status": "ready",
        "checks": {"database": "ok"},
    }


def test_readiness_returns_sanitized_service_unavailable_body(monkeypatch):
    monkeypatch.setattr(main, "is_database_ready", lambda: False)

    response = main.readiness_check()

    assert response.status_code == 503
    assert json.loads(response.body) == {
        "status": "not_ready",
        "checks": {"database": "unavailable"},
    }
    assert b"secret" not in response.body
    assert b"private-host" not in response.body


def test_readiness_route_is_registered():
    ready_routes = [
        route
        for route in main.app.routes
        if getattr(route, "path", None) == "/api/ready"
    ]

    assert len(ready_routes) == 1
    assert ready_routes[0].methods == {"GET"}
