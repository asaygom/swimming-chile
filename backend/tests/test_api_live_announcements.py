import sys
from contextlib import contextmanager
from pathlib import Path

import pytest
from fastapi import HTTPException
from pydantic import ValidationError


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from api import main
from api.routers import live_announcements


class FakeCursor:
    def __init__(self, rows=(), all_rows=()):
        self.rows = iter(rows)
        self.all_rows = list(all_rows)
        self.executed = []

    def __enter__(self): return self
    def __exit__(self, *_args): return False
    def execute(self, query, params): self.executed.append((" ".join(query.split()), params))
    def fetchone(self): return next(self.rows)
    def fetchall(self): return self.all_rows


class FakeConnection:
    def __init__(self, cursor):
        self.cursor_instance = cursor
        self.committed = False

    def cursor(self): return self.cursor_instance
    def commit(self): self.committed = True


def install_database(monkeypatch, rows=(), all_rows=()):
    connection = FakeConnection(FakeCursor(rows, all_rows))

    @contextmanager
    def fake_connection(): yield connection

    monkeypatch.setattr(live_announcements, "get_db_connection", fake_connection)
    return connection


ANNOUNCEMENT = {
    "id": 31, "message": "Warm-up pool closes at 10:00", "display_mode": "ticker",
    "is_active": True, "revision": 4, "created_at": "2026-08-01T10:00:00Z",
    "updated_at": "2026-08-01T10:05:00Z", "activated_at": "2026-08-01T10:05:00Z",
}


def test_public_read_returns_only_active_non_deleted_announcement(monkeypatch):
    connection = install_database(monkeypatch, [{"id": 7}, ANNOUNCEMENT])

    assert live_announcements.get_active_announcement(7) == {
        "competition_id": 7, "announcement": ANNOUNCEMENT
    }
    query = connection.cursor_instance.executed[1][0]
    assert "competition_id = %s" in query
    assert "is_active IS TRUE" in query and "deleted_at IS NULL" in query


def test_public_read_rejects_unknown_competition(monkeypatch):
    install_database(monkeypatch, [None])
    with pytest.raises(HTTPException) as error:
        live_announcements.get_active_announcement(404)
    assert error.value.status_code == 404


def test_admin_list_is_competition_scoped_and_excludes_deleted(monkeypatch):
    connection = install_database(monkeypatch, all_rows=[ANNOUNCEMENT])
    result = live_announcements.list_announcements(7, 19)
    assert result == {"competition_id": 7, "announcements": [ANNOUNCEMENT]}
    query, params = connection.cursor_instance.executed[0]
    assert "competition_id = %s" in query and "deleted_at IS NULL" in query
    assert params == (7,)


def test_create_validates_input_and_records_admin_actor(monkeypatch):
    for payload in [
        {"message": " ", "display_mode": "ticker"},
        {"message": "Valid", "display_mode": "banner"},
        {"message": "Valid", "display_mode": "ticker"},
    ]:
        with pytest.raises(ValidationError):
            live_announcements.AnnouncementCreate(**payload)
    with pytest.raises(ValidationError):
        live_announcements.AnnouncementCreate(
            message="x" * 241, display_mode="ticker", expected_revision=0
        )
    connection = install_database(monkeypatch, [ANNOUNCEMENT])
    body = live_announcements.AnnouncementCreate(
        message="Notice", display_mode="fullscreen", expected_revision=0
    )

    result = live_announcements.create_announcement(7, body, 19)

    assert result["announcement"] == ANNOUNCEMENT
    query, params = connection.cursor_instance.executed[0]
    assert query.startswith("INSERT INTO core.live_announcement")
    assert params == (7, "Notice", "fullscreen", 19, 19)
    assert connection.committed is True


def test_update_is_isolated_audited_and_revision_guarded(monkeypatch):
    connection = install_database(monkeypatch, [{"revision": 3}, ANNOUNCEMENT])
    body = live_announcements.AnnouncementUpdate(
        message="Updated", display_mode="ticker", expected_revision=3
    )
    result = live_announcements.update_announcement(7, 31, body, 19)
    assert result["announcement"] == ANNOUNCEMENT
    query, params = connection.cursor_instance.executed[-1]
    assert "revision = revision + 1" in query and "updated_by_user_id" in query
    assert "competition_id = %(competition_id)s" in query
    assert params["competition_id"] == 7 and params["user_id"] == 19

    install_database(monkeypatch, [{"revision": 4}])
    with pytest.raises(HTTPException) as error:
        live_announcements.update_announcement(7, 31, body, 19)
    assert error.value.status_code == 409


def test_activation_serializes_competition_and_deactivates_only_its_prior_row(monkeypatch):
    connection = install_database(monkeypatch, [{"id": 7}, {"revision": 2}, ANNOUNCEMENT])
    body = live_announcements.AnnouncementActivation(is_active=True, expected_revision=2)

    result = live_announcements.set_announcement_activation(7, 31, body, 19)

    assert result["announcement"] == ANNOUNCEMENT
    lock_query = connection.cursor_instance.executed[0][0]
    deactivate_query, deactivate_params = connection.cursor_instance.executed[2]
    activate_query, activate_params = connection.cursor_instance.executed[3]
    assert "core.competition" in lock_query and "FOR UPDATE" in lock_query
    assert "competition_id = %s" in deactivate_query and "id <> %s" in deactivate_query
    assert deactivate_params == (19, 7, 31)
    assert "activated_by_user_id" in activate_query
    assert activate_params["expected_revision"] == 2


def test_delete_is_soft_audited_and_rejects_stale_revision(monkeypatch):
    connection = install_database(monkeypatch, [{"revision": 4}, ANNOUNCEMENT])
    result = live_announcements.delete_announcement(7, 31, 4, 19)
    assert result["announcement"] == ANNOUNCEMENT
    query, params = connection.cursor_instance.executed[-1]
    assert "deleted_at = NOW()" in query and "deleted_by_user_id" in query
    assert "is_active = FALSE" in query and params["user_id"] == 19

    install_database(monkeypatch, [{"revision": 5}])
    with pytest.raises(HTTPException) as error:
        live_announcements.delete_announcement(7, 31, 4, 19)
    assert error.value.status_code == 409


def test_admin_routes_use_competition_admin_not_operator_auth():
    source = Path(live_announcements.__file__).read_text(encoding="utf-8")
    assert "Depends(require_competition_admin)" in source
    assert "swimstats_live_operator" not in source
    assert "LiveHeat" not in source
    assert "include_router(live_announcements.router" in Path(main.__file__).read_text(encoding="utf-8")
