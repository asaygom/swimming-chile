import asyncio
import sys
from pathlib import Path

from starlette.requests import Request

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from api import main


SECRET = "secreto-de-borde-para-pruebas"
PASSED_THROUGH = object()


def request_for(path: str, headers: dict | None = None) -> Request:
    raw_headers = [
        (name.lower().encode(), value.encode())
        for name, value in (headers or {}).items()
    ]
    return Request(
        {
            "type": "http",
            "method": "GET",
            "path": path,
            "raw_path": path.encode(),
            "query_string": b"",
            "headers": raw_headers,
            "scheme": "https",
            "server": ("api.swimstats.cl", 443),
        }
    )


def call_middleware(path: str, headers: dict | None = None):
    async def call_next(_request):
        return PASSED_THROUGH

    return asyncio.run(main.require_edge_secret(request_for(path, headers), call_next))


def test_edge_filter_is_inert_without_configured_secret(monkeypatch):
    monkeypatch.delenv("EDGE_SHARED_SECRET", raising=False)

    assert call_middleware("/api/athletes/1") is PASSED_THROUGH


def test_api_request_without_edge_header_is_rejected(monkeypatch):
    monkeypatch.setenv("EDGE_SHARED_SECRET", SECRET)

    response = call_middleware("/api/athletes/1")

    assert response is not PASSED_THROUGH
    assert response.status_code == 403
    assert b"Direct origin access is not allowed" in response.body


def test_api_request_with_wrong_edge_header_is_rejected(monkeypatch):
    monkeypatch.setenv("EDGE_SHARED_SECRET", SECRET)

    response = call_middleware("/api/athletes/1", {"X-Edge-Auth": SECRET + "x"})

    assert response is not PASSED_THROUGH
    assert response.status_code == 403


def test_api_request_with_valid_edge_header_passes_through(monkeypatch):
    monkeypatch.setenv("EDGE_SHARED_SECRET", SECRET)

    assert call_middleware("/api/athletes/1", {"X-Edge-Auth": SECRET}) is PASSED_THROUGH


def test_edge_header_name_is_matched_case_insensitively(monkeypatch):
    monkeypatch.setenv("EDGE_SHARED_SECRET", SECRET)

    assert call_middleware("/api/clubs", {"x-edge-auth": SECRET}) is PASSED_THROUGH


def test_platform_probes_stay_reachable_without_the_edge_header(monkeypatch):
    monkeypatch.setenv("EDGE_SHARED_SECRET", SECRET)

    assert call_middleware("/api/health") is PASSED_THROUGH
    assert call_middleware("/api/ready") is PASSED_THROUGH


def test_non_api_routes_are_not_filtered(monkeypatch):
    monkeypatch.setenv("EDGE_SHARED_SECRET", SECRET)

    assert call_middleware("/") is PASSED_THROUGH


def test_live_operator_endpoints_are_covered_by_the_filter(monkeypatch):
    """El sembrado en vivo se opera durante la competencia: no puede quedar
    accesible saltandose el borde."""
    monkeypatch.setenv("EDGE_SHARED_SECRET", SECRET)

    response = call_middleware("/api/competitions/19/live-heat")

    assert response is not PASSED_THROUGH
    assert response.status_code == 403


def test_secret_is_read_per_request_and_trimmed(monkeypatch):
    monkeypatch.setenv("EDGE_SHARED_SECRET", "  " + SECRET + "  ")

    assert main.get_edge_shared_secret() == SECRET
    assert call_middleware("/api/clubs", {"X-Edge-Auth": SECRET}) is PASSED_THROUGH
