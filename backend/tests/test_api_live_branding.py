import asyncio
import io
import sys
from contextlib import contextmanager
from pathlib import Path

import pytest
from fastapi import HTTPException, Request
from PIL import Image, PngImagePlugin


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from api import main
from api.routers import live_branding


class FakeCursor:
    def __init__(self, rows):
        self.rows = iter(rows)
        self.executed = []

    def __enter__(self): return self
    def __exit__(self, *_args): return False
    def execute(self, query, params): self.executed.append((" ".join(query.split()), params))
    def fetchone(self): return next(self.rows)


class FakeConnection:
    def __init__(self, rows):
        self.cursor_instance = FakeCursor(rows)
        self.committed = False

    def cursor(self): return self.cursor_instance
    def commit(self): self.committed = True


def install_database(monkeypatch, rows):
    connection = FakeConnection(rows)

    @contextmanager
    def fake_connection(): yield connection

    monkeypatch.setattr(live_branding, "get_db_connection", fake_connection)
    return connection


def png_bytes(size=(32, 16), metadata=True):
    output = io.BytesIO()
    info = PngImagePlugin.PngInfo()
    if metadata: info.add_text("unsafe-note", "remove me")
    Image.new("RGBA", size, (0, 150, 255, 255)).save(output, "PNG", pnginfo=info)
    return output.getvalue()


def raw_request(data, content_type="image/png"):
    delivered = False

    async def receive():
        nonlocal delivered
        if delivered: return {"type": "http.disconnect"}
        delivered = True
        return {"type": "http.request", "body": data, "more_body": False}

    return Request({"type": "http", "method": "PUT", "headers": [
        (b"content-type", content_type.encode()),
        (b"content-length", str(len(data)).encode()),
    ]}, receive)


BRANDING = {"has_logo": True, "revision": 1, "width": 32, "height": 16, "mime_type": "image/png"}


def test_logo_normalization_validates_format_dimensions_size_and_strips_metadata():
    normalized = live_branding._normalize_logo(png_bytes(), "image/png")
    assert normalized.width == 32 and normalized.height == 16
    assert normalized.mime_type == "image/png" and len(normalized.sha256) == 64
    assert "unsafe-note" not in Image.open(io.BytesIO(normalized.content)).info

    with pytest.raises(HTTPException) as error:
        live_branding._normalize_logo(b"x" * (2 * 1024 * 1024 + 1), "image/png")
    assert error.value.status_code == 413
    with pytest.raises(HTTPException) as error:
        live_branding._normalize_logo(png_bytes((4097, 1), False), "image/png")
    assert error.value.status_code == 422
    with pytest.raises(HTTPException) as error:
        live_branding._normalize_logo(png_bytes(), "image/svg+xml")
    assert error.value.status_code == 415


def test_admin_put_creates_normalized_competition_scoped_logo(monkeypatch):
    connection = install_database(monkeypatch, [{"id": 7}, None, BRANDING])
    result = asyncio.run(live_branding.put_live_branding(
        7, raw_request(png_bytes()), 0, 19,
    ))

    assert result == {"competition_id": 7, **BRANDING}
    insert_query, params = connection.cursor_instance.executed[-1]
    assert insert_query.startswith("INSERT INTO core.competition_live_branding")
    assert params["competition_id"] == 7 and params["updated_by_user_id"] == 19
    assert params["logo_bytes"] and b"remove me" not in params["logo_bytes"]
    assert connection.committed is True


def test_admin_put_rejects_stale_revision_without_mutation(monkeypatch):
    connection = install_database(monkeypatch, [{"id": 7}, {"revision": 3, "deleted_at": None}])
    with pytest.raises(HTTPException) as error:
        asyncio.run(live_branding.put_live_branding(7, raw_request(png_bytes()), 2, 19))
    assert error.value.status_code == 409
    assert len(connection.cursor_instance.executed) == 2
    assert connection.committed is False


def test_public_metadata_and_image_etag_are_scoped_and_cacheable(monkeypatch):
    install_database(monkeypatch, [{"id": 7}, {**BRANDING}])
    assert live_branding.get_live_branding(7) == {"competition_id": 7, **BRANDING}

    digest = "a" * 64
    install_database(monkeypatch, [{"logo_bytes": b"png", "mime_type": "image/png", "sha256": digest}])
    response = live_branding.get_live_branding_logo(7, None)
    assert response.body == b"png" and response.media_type == "image/png"
    assert response.headers["etag"] == f'"{digest}"'
    assert "must-revalidate" in response.headers["cache-control"]

    install_database(monkeypatch, [{"logo_bytes": b"png", "mime_type": "image/png", "sha256": digest}])
    assert live_branding.get_live_branding_logo(7, f'"{digest}"').status_code == 304


def test_admin_delete_preserves_audit_row_and_requires_revision(monkeypatch):
    deleted = {"has_logo": False, "revision": 5, "width": None, "height": None, "mime_type": None}
    connection = install_database(monkeypatch, [{"id": 7}, {"revision": 4, "deleted_at": None}, deleted])
    result = live_branding.delete_live_branding(7, 4, 19)
    assert result == {"competition_id": 7, **deleted}
    query, params = connection.cursor_instance.executed[-1]
    assert "logo_bytes = NULL" in query and "deleted_at = NOW()" in query
    assert params == {"competition_id": 7, "expected_revision": 4, "user_id": 19}


def test_branding_writes_use_competition_admin_only_and_router_is_registered():
    source = Path(live_branding.__file__).read_text(encoding="utf-8")
    assert source.count("Depends(require_competition_admin)") == 2
    assert "swimstats_live_operator" not in source
    assert "include_router(live_branding.router" in Path(main.__file__).read_text(encoding="utf-8")
    assert "Pillow" in (Path(__file__).parents[1] / "requirements.txt").read_text(encoding="utf-8")
