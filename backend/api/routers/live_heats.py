import hashlib
import hmac
import ipaddress
import os
import threading
import time
import uuid
from typing import Literal

from fastapi import APIRouter, Cookie, HTTPException, Request, Response
from pydantic import BaseModel, Field

from ..database import get_db_connection


router = APIRouter()
COOKIE_NAME = "swimstats_live_operator"
SESSION_TTL_SECONDS = 4 * 60 * 60
MAX_CODE_FAILURES = 5
CODE_FAILURE_WINDOW_SECONDS = 15 * 60
_MAX_ATTEMPT_KEYS = 10_000
_attempts: dict[tuple[int, str], list[float]] = {}
_attempts_lock = threading.Lock()


class OperatorCode(BaseModel):
    code: str = Field(min_length=1, max_length=256)


class LiveHeatUpdate(BaseModel):
    publication_id: int = Field(gt=0)
    stage_number: int = Field(default=1, gt=0)
    pool_role: Literal["main", "competition", "training"] = "main"
    session_number: int = Field(gt=0)
    event_number: int = Field(gt=0)
    heat_number: int = Field(gt=0)
    status: Literal["not_started", "active", "paused", "finished"]
    expected_revision: int = Field(ge=0)


def _auth_config(competition_id: int) -> tuple[str, bytes]:
    configured_competition = os.getenv("LIVE_HEAT_OPERATOR_COMPETITION_ID")
    code_hash = os.getenv("LIVE_HEAT_OPERATOR_CODE_SHA256")
    secret = os.getenv("LIVE_HEAT_SESSION_SECRET")
    if configured_competition != str(competition_id) or not code_hash or not secret:
        raise HTTPException(status_code=503, detail="Live heat control is not configured")
    if len(code_hash) != 64 or len(secret) < 16:
        raise HTTPException(status_code=503, detail="Live heat control is not configured")
    return code_hash.lower(), secret.encode("utf-8")


def _create_session_token(competition_id: int) -> tuple[str, str]:
    _code_hash, secret = _auth_config(competition_id)
    session_id = uuid.uuid4().hex
    payload = f"{competition_id}:{int(time.time()) + SESSION_TTL_SECONDS}:{session_id}"
    signature = hmac.new(secret, payload.encode(), hashlib.sha256).hexdigest()
    return f"{payload}:{signature}", session_id


def _require_session(competition_id: int, token: str | None) -> str:
    if not token:
        raise HTTPException(status_code=401, detail="Operator session required")
    _code_hash, secret = _auth_config(competition_id)
    try:
        raw_competition, raw_expiry, session_id, signature = token.split(":", 3)
        expiry = int(raw_expiry)
    except (TypeError, ValueError):
        raise HTTPException(status_code=401, detail="Invalid operator session") from None
    payload = f"{raw_competition}:{raw_expiry}:{session_id}"
    expected = hmac.new(secret, payload.encode(), hashlib.sha256).hexdigest()
    if (
        raw_competition != str(competition_id)
        or expiry < int(time.time())
        or not hmac.compare_digest(signature, expected)
    ):
        raise HTTPException(status_code=401, detail="Invalid operator session")
    return session_id


def _trusted_proxy_networks():
    networks = []
    for value in os.getenv("LIVE_HEAT_TRUSTED_PROXY_CIDRS", "").split(","):
        if value.strip():
            try:
                networks.append(ipaddress.ip_network(value.strip(), strict=False))
            except ValueError:
                continue
    return networks


def _client_address(request: Request) -> str:
    peer = request.client.host if request.client else "unknown"
    networks = _trusted_proxy_networks()
    try:
        peer_ip = ipaddress.ip_address(peer)
    except ValueError:
        return peer
    if not any(peer_ip in network for network in networks):
        return peer
    forwarded = request.headers.get("x-forwarded-for", "")
    try:
        addresses = [ipaddress.ip_address(value.strip()) for value in forwarded.split(",")]
    except ValueError:
        return peer
    for address in reversed(addresses):
        if not any(address in network for network in networks):
            return str(address)
    return str(addresses[0]) if addresses else peer


def _check_code_attempts(key: tuple[int, str], now: float) -> None:
    with _attempts_lock:
        recent = [stamp for stamp in _attempts.get(key, [])
                  if now - stamp < CODE_FAILURE_WINDOW_SECONDS]
        if recent:
            _attempts[key] = recent
        else:
            _attempts.pop(key, None)
        if len(recent) >= MAX_CODE_FAILURES:
            raise HTTPException(
                status_code=429,
                detail="Too many operator code attempts",
                headers={"Retry-After": str(CODE_FAILURE_WINDOW_SECONDS)},
            )


def _record_code_failure(key: tuple[int, str], now: float) -> None:
    with _attempts_lock:
        if len(_attempts) >= _MAX_ATTEMPT_KEYS and key not in _attempts:
            oldest = min(_attempts, key=lambda item: _attempts[item][-1])
            _attempts.pop(oldest, None)
        _attempts.setdefault(key, []).append(now)


def _reset_code_attempts(key: tuple[int, str]) -> None:
    with _attempts_lock:
        _attempts.pop(key, None)


@router.post("/{competition_id}/live-heat/session")
def create_operator_session(
    competition_id: int, credentials: OperatorCode, response: Response, request: Request
):
    expected_hash, _secret = _auth_config(competition_id)
    attempt_key = (competition_id, _client_address(request))
    now = time.monotonic()
    _check_code_attempts(attempt_key, now)
    supplied_hash = hashlib.sha256(credentials.code.encode("utf-8")).hexdigest()
    if not hmac.compare_digest(supplied_hash, expected_hash):
        _record_code_failure(attempt_key, now)
        raise HTTPException(status_code=401, detail="Invalid operator code")
    _reset_code_attempts(attempt_key)
    token, _session_id = _create_session_token(competition_id)
    response.set_cookie(
        COOKIE_NAME,
        token,
        max_age=SESSION_TTL_SECONDS,
        httponly=True,
        secure=os.getenv("LIVE_HEAT_COOKIE_SECURE", "true").lower() != "false",
        samesite="lax",
        path=f"/api/competitions/{competition_id}/live-heat",
    )
    return {"authenticated": True, "expires_in_seconds": SESSION_TTL_SECONDS}


@router.get("/{competition_id}/live-heat")
def get_live_heat(competition_id: int):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM core.competition WHERE id = %s", (competition_id,))
            if not cur.fetchone():
                raise HTTPException(status_code=404, detail="Competition not found")
            cur.execute("""
                SELECT s.publication_id, s.stage_number, s.pool_role,
                       s.session_number, s.event_number, e.event_name,
                       s.heat_number, e.heat_total, s.status, s.revision, s.updated_at
                FROM core.live_heat_state s
                JOIN core.meet_program_publication p
                  ON p.id = s.publication_id AND p.status = 'published'
                JOIN core.meet_program_entry e
                  ON e.publication_id = s.publication_id
                 AND e.session_number = s.session_number
                 AND e.event_number = s.event_number
                 AND e.heat_number = s.heat_number
                WHERE s.competition_id = %s
                ORDER BY s.updated_at DESC, s.stage_number DESC, s.id DESC
                LIMIT 1
            """, (competition_id,))
            state = cur.fetchone()
            if not state:
                return {"competition_id": competition_id, "state": None, "entries": []}
            cur.execute("""
                SELECT lane, entry_type, display_name, team_name AS club_name,
                       seed_time_text, relay_members
                FROM core.meet_program_entry
                WHERE publication_id = %s AND session_number = %s
                  AND event_number = %s AND heat_number = %s
                ORDER BY lane, id
            """, (
                state["publication_id"], state["session_number"],
                state["event_number"], state["heat_number"],
            ))
            entries = cur.fetchall()
    return {"competition_id": competition_id, "state": state, "entries": entries}


@router.put("/{competition_id}/live-heat")
def update_live_heat(
    competition_id: int,
    update: LiveHeatUpdate,
    operator_session: str | None = Cookie(default=None, alias=COOKIE_NAME),
):
    session_id = _require_session(competition_id, operator_session)
    values = {**update.model_dump(), "competition_id": competition_id,
              "updated_by_session": session_id}
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id FROM core.meet_program_publication
                WHERE id = %(publication_id)s AND competition_id = %(competition_id)s
                  AND stage_number = %(stage_number)s AND pool_role = %(pool_role)s
                  AND status = 'published'
                FOR SHARE
            """, values)
            if not cur.fetchone():
                raise HTTPException(status_code=422, detail="Published program segment not found")
            cur.execute("""
                SELECT 1 AS exists FROM core.meet_program_entry
                WHERE publication_id = %(publication_id)s
                  AND session_number = %(session_number)s
                  AND event_number = %(event_number)s AND heat_number = %(heat_number)s
                LIMIT 1
            """, values)
            if not cur.fetchone():
                raise HTTPException(status_code=422, detail="Heat not found in publication")
            cur.execute("""
                SELECT s.publication_id, s.revision, p.status AS publication_status
                FROM core.live_heat_state s
                JOIN core.meet_program_publication p ON p.id = s.publication_id
                WHERE s.competition_id = %(competition_id)s
                  AND s.stage_number = %(stage_number)s AND s.pool_role = %(pool_role)s
                FOR UPDATE OF s FOR SHARE OF p
            """, values)
            existing = cur.fetchone()
            adopts_replacement = bool(
                existing
                and update.expected_revision == 0
                and existing["publication_id"] != update.publication_id
                and existing["publication_status"] != "published"
            )
            if existing and not adopts_replacement and existing["revision"] != update.expected_revision:
                raise HTTPException(status_code=409, detail="Live heat revision conflict")
            if not existing and update.expected_revision != 0:
                raise HTTPException(status_code=409, detail="Live heat revision conflict")
            if existing:
                values["current_revision"] = existing["revision"]
                cur.execute("""
                    UPDATE core.live_heat_state SET
                        publication_id = %(publication_id)s,
                        session_number = %(session_number)s,
                        event_number = %(event_number)s,
                        heat_number = %(heat_number)s,
                        status = %(status)s,
                        revision = revision + 1,
                        updated_at = NOW(),
                        updated_by_session = %(updated_by_session)s
                    WHERE competition_id = %(competition_id)s
                      AND stage_number = %(stage_number)s AND pool_role = %(pool_role)s
                      AND revision = %(current_revision)s
                    RETURNING publication_id, stage_number, pool_role, session_number,
                              event_number, heat_number, status, revision, updated_at
                """, values)
            else:
                cur.execute("""
                    INSERT INTO core.live_heat_state (
                        competition_id, publication_id, stage_number, pool_role,
                        session_number, event_number, heat_number, status,
                        revision, updated_by_session
                    ) VALUES (
                        %(competition_id)s, %(publication_id)s, %(stage_number)s, %(pool_role)s,
                        %(session_number)s, %(event_number)s, %(heat_number)s, %(status)s,
                        1, %(updated_by_session)s
                    ) ON CONFLICT DO NOTHING
                    RETURNING publication_id, stage_number, pool_role, session_number,
                              event_number, heat_number, status, revision, updated_at
                """, values)
            state = cur.fetchone()
            if not state:
                raise HTTPException(status_code=409, detail="Live heat revision conflict")
            conn.commit()
    return {"competition_id": competition_id, "state": state}
