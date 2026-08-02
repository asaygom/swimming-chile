import asyncio
import csv
import io
import sys
from contextlib import contextmanager
from pathlib import Path

import pytest
from fastapi import HTTPException, Request


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from api import auth, main
from api.routers import meet_programs

SCRIPTS_DIR = Path(__file__).resolve().parents[1] / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))
import run_meet_program as meet_program


class FakeCursor:
    def __init__(self, rows):
        self.rows = iter(rows)
        self.executed = []

    def __enter__(self): return self
    def __exit__(self, *_args): return False
    def execute(self, query, params=None): self.executed.append((" ".join(query.split()), params))
    def fetchone(self): return next(self.rows)


class FakeConnection:
    def __init__(self, rows):
        self.cursor_instance = FakeCursor(rows)

    def cursor(self): return self.cursor_instance


def install_auth_database(monkeypatch, rows):
    connection = FakeConnection(rows)

    @contextmanager
    def fake_connection(): yield connection

    monkeypatch.setattr(auth, "get_db_connection", fake_connection)
    return connection


def raw_request(data, content_type="text/csv"):
    delivered = False

    async def receive():
        nonlocal delivered
        if delivered:
            return {"type": "http.disconnect"}
        delivered = True
        return {"type": "http.request", "body": data, "more_body": False}

    scope = {
        "type": "http",
        "method": "POST",
        "headers": [
            (b"content-type", content_type.encode()),
            (b"content-length", str(len(data)).encode()),
        ],
    }
    return Request(scope, receive)


def meet_manager_csv(rows=2, event="#1 Mixto 100 CL Metro Estilo Libre"):
    lines = []
    for lane in range(2, 2 + rows):
        lines.append([
            "Natatorio Chileno",
            "HY-TEK's MEET MANAGER 7.0",
            "III COPA ÑUÑOA MASTER 2026 - 08-08-2026",
            "", "",
            "Programa de Competencias",
            event,
            "", "", "",
            "Carril", "Nombre", "Edad", "Equipo", "Tiempo para Sembrado",
            "Serie   1 of 1   Finales   Inicia a las  09:30 AM",
            str(lane), f"Nadador{lane}, Prueba", f"M2{lane}", "NEURO", "NT", "",
        ])
    buffer = io.StringIO()
    csv.writer(buffer, lineterminator="\n").writerows(lines)
    return buffer.getvalue().encode("cp1252")


def test_upload_requires_platform_admin_not_competition_admin(monkeypatch):
    """El sembrado es global: un admin de competencia no debe poder publicarlo."""
    install_auth_database(monkeypatch, [{"user_id": 4}, None])

    with pytest.raises(HTTPException) as error:
        auth.require_platform_admin("session-token")

    assert error.value.status_code == 403
    for route in main.app.routes:
        if "meet-program/" in getattr(route, "path", ""):
            assert auth.require_platform_admin in [
                dependency.call for dependency in route.dependant.dependencies
            ]


def test_platform_admin_lookup_demands_global_scope(monkeypatch):
    connection = install_auth_database(monkeypatch, [{"user_id": 4}, {"allowed": 1}])

    assert auth.require_platform_admin("session-token") == 4
    role_query = connection.cursor_instance.executed[-1][0]
    assert "role = 'platform_admin'" in role_query
    assert "club_id IS NULL" in role_query


def test_missing_session_is_rejected_before_touching_roles(monkeypatch):
    connection = install_auth_database(monkeypatch, [])

    with pytest.raises(HTTPException) as error:
        auth.require_platform_admin(None)

    assert error.value.status_code == 401
    assert connection.cursor_instance.executed == []


def test_preview_reports_validation_without_writing_to_the_database(monkeypatch):
    def explode():
        raise AssertionError("preview must not open a database connection")

    monkeypatch.setattr(meet_programs, "get_db_connection", explode)

    payload = asyncio.run(
        meet_programs.preview_meet_program(
            9, raw_request(meet_manager_csv()), source_format="csv",
            source_name="sembrado triple.csv", source_url=None,
            stage_number=None, pool_role=None, scheduled_date=None,
            _admin_user_id=4,
        )
    )

    assert payload["state"] == "validated"
    assert payload["counts"]["entries"] == 2
    assert payload["issues"] == []
    assert payload["events"] == [(1, "Mixto 100 CL Metro Estilo Libre")]
    assert payload["source"]["source_kind"] == "meet_manager_csv"
    assert payload["source"]["scheduled_date"] == "2026-08-08"
    # El nombre real del archivo, no el del temporal, es lo que llega a
    # source_document.document_name al publicar.
    assert payload["source"]["pdf_name"] == "sembrado triple.csv"


def test_preview_accepts_both_formats_through_the_same_contract(monkeypatch):
    monkeypatch.setattr(
        meet_programs.meet_program, "parse_pdf",
        lambda path: meet_program.ParsedMeetProgram(entries=[], unparsed=[], metadata={}),
    )

    payload = asyncio.run(
        meet_programs.preview_meet_program(
            9, raw_request(b"%PDF-1.4 fake", content_type="application/pdf"),
            source_format="pdf", source_name="programa.pdf", source_url=None,
            stage_number=None, pool_role=None, scheduled_date=None,
            _admin_user_id=4,
        )
    )

    assert payload["counts"]["entries"] == 0
    assert payload["state"] == "requires_review"


def test_publish_refuses_artifacts_that_do_not_validate(monkeypatch):
    def explode():
        raise AssertionError("publishing must not start when validation fails")

    monkeypatch.setattr(meet_programs, "get_db_connection", explode)
    unusable = b"sin,bloque,de,datos\n"

    with pytest.raises(HTTPException) as error:
        asyncio.run(
            meet_programs.publish_meet_program(
                9, raw_request(unusable), source_format="csv",
                source_name="roto.csv", source_url=None, stage_number=None,
                pool_role=None, scheduled_date=None, _admin_user_id=4,
            )
        )

    assert error.value.status_code == 422
    assert error.value.detail["state"] == "requires_review"
    assert error.value.detail["issues"]


def test_publish_delegates_gates_to_the_shared_publisher(monkeypatch):
    """La app no debe reimplementar compuertas: publica con el mismo modulo."""
    captured = {}

    @contextmanager
    def fake_connection():
        yield "connection"

    def fake_publish(connection, entries, metadata, **kwargs):
        captured["connection"] = connection
        captured["entries"] = entries
        captured["metadata"] = metadata
        captured["kwargs"] = kwargs
        return 42, True

    monkeypatch.setattr(meet_programs, "get_db_connection", fake_connection)
    monkeypatch.setattr(meet_programs.meet_program, "publish_validated_program", fake_publish)

    payload = asyncio.run(
        meet_programs.publish_meet_program(
            9, raw_request(meet_manager_csv()), source_format="csv",
            source_name="sembrado triple.csv", source_url="https://origen/programa.csv",
            stage_number=None, pool_role=None, scheduled_date=None, _admin_user_id=4,
        )
    )

    assert payload["publication_id"] == 42
    assert payload["publication_created"] is True
    assert captured["kwargs"]["competition_id"] == 9
    assert captured["kwargs"]["schema"] == "core"
    assert captured["kwargs"]["source_url"] == "https://origen/programa.csv"
    assert len(captured["entries"]) == 2
    assert captured["metadata"]["pdf_name"] == "sembrado triple.csv"


def test_publish_surfaces_publisher_rejections_without_leaking_internals(monkeypatch):
    @contextmanager
    def fake_connection():
        yield "connection"

    def fake_publish(*_args, **_kwargs):
        raise meet_program.MeetProgramError("Competition name does not match the document.")

    monkeypatch.setattr(meet_programs, "get_db_connection", fake_connection)
    monkeypatch.setattr(meet_programs.meet_program, "publish_validated_program", fake_publish)

    with pytest.raises(HTTPException) as error:
        asyncio.run(
            meet_programs.publish_meet_program(
                9, raw_request(meet_manager_csv()), source_format="csv",
                source_name="sembrado.csv", source_url=None, stage_number=None,
                pool_role=None, scheduled_date=None, _admin_user_id=4,
            )
        )

    assert error.value.status_code == 422
    assert error.value.detail == "Competition name does not match the document."


def test_upload_is_bounded_and_rejects_empty_bodies():
    oversized = b"x" * (meet_programs.MAX_PROGRAM_BYTES + 1)

    with pytest.raises(HTTPException) as too_large:
        asyncio.run(meet_programs._read_program_body(raw_request(oversized)))
    assert too_large.value.status_code == 413

    with pytest.raises(HTTPException) as empty:
        asyncio.run(meet_programs._read_program_body(raw_request(b"")))
    assert empty.value.status_code == 422


def test_parsing_leaves_no_artifacts_behind(monkeypatch):
    """El filesystem del deploy es efimero: el resumen viaja en la respuesta."""
    created = []
    original = meet_programs.tempfile.TemporaryDirectory

    def tracking_tempdir(*args, **kwargs):
        handle = original(*args, **kwargs)
        created.append(Path(handle.name))
        return handle

    monkeypatch.setattr(meet_programs.tempfile, "TemporaryDirectory", tracking_tempdir)
    meet_programs._parsed_preview(meet_manager_csv(), "csv", {
        "source_name": "sembrado.csv", "source_url": None,
        "stage_number": None, "pool_role": None, "scheduled_date": None,
    })

    assert created and not any(path.exists() for path in created)


FRONTEND_DIR = Path(__file__).resolve().parents[2] / "frontend" / "src"
ADMIN_PAGE = FRONTEND_DIR / "features/competitions/pages/CompetitionMeetProgramAdminPage.tsx"
SERVICE = FRONTEND_DIR / "features/competitions/api/competitionService.ts"
ROUTER = FRONTEND_DIR / "app/router.tsx"


def test_meet_program_admin_route_is_standalone_like_the_other_live_pages():
    router = ROUTER.read_text(encoding="utf-8")
    route = "path: '/competitions/:id/live/program'"

    assert route in router
    assert router.index(route) < router.index("element: <MainLayout />")


def test_meet_program_upload_service_is_typed_scoped_and_sends_credentials():
    service = SERVICE.read_text(encoding="utf-8")
    upload = service.split("async uploadMeetProgram", 1)[1].split("async getLiveBranding", 1)[0]

    assert "meet-program/${action}" in upload
    assert "credentials: 'include'" in upload
    assert "source_format: sourceFormat" in upload
    # El nombre real del archivo viaja al backend: aterriza en source_document.
    assert "source_name: file.name" in upload
    assert "MeetProgramPreviewSchema.parse" in upload
    # Un 422 al publicar trae el resumen completo y debe conservarse.
    assert "response.status === 422" in upload
    assert "localStorage" not in service


def test_admin_page_requires_validation_before_enabling_publish():
    page = ADMIN_PAGE.read_text(encoding="utf-8")

    assert "'preview'" in page and "'publish'" in page
    # El boton de publicar depende del estado validado, no solo de haber subido.
    assert "disabled={busy || !file || !validated || published}" in page
    assert "const validated = preview?.state === 'validated'" in page
    assert "Validar sin publicar" in page
    assert "Publicar sembrado" in page


def test_admin_page_surfaces_the_evidence_the_operator_needs_to_decide():
    page = ADMIN_PAGE.read_text(encoding="utf-8")

    for fragment in [
        "preview.counts.entries",
        "preview.counts.debug_unparsed_lines",
        "preview.source.parser_version",
        "preview.source.source_competition_name",
        "preview.events.map",
        "preview.issues.map",
        "preview.unparsed_sample.map",
    ]:
        assert fragment in page
    assert 'aria-live="polite"' in page


def test_admin_page_validates_format_and_size_before_uploading():
    page = ADMIN_PAGE.read_text(encoding="utf-8")

    assert "accept=\".pdf,.csv\"" in page
    assert "MAX_PROGRAM_BYTES = 16 * 1024 * 1024" in page
    assert "endsWith('.pdf')" in page and "endsWith('.csv')" in page
