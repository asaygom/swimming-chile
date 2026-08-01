import hashlib
import sys
import time
from contextlib import contextmanager
from pathlib import Path

import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa
from fastapi import HTTPException, Response


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from api import auth, main
from api.routers import admin_auth


class FakeCursor:
    def __init__(self, rows):
        self.rows = iter(rows)
        self.executed = []

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, query, params):
        self.executed.append((" ".join(query.split()), params))

    def fetchone(self):
        return next(self.rows)


class FakeConnection:
    def __init__(self, rows):
        self.cursor_instance = FakeCursor(rows)
        self.committed = False

    def cursor(self):
        return self.cursor_instance

    def commit(self):
        self.committed = True


def install_database(monkeypatch, rows):
    connection = FakeConnection(rows)

    @contextmanager
    def fake_connection():
        yield connection

    monkeypatch.setattr(auth, "get_db_connection", fake_connection)
    return connection


def configure_oidc(monkeypatch):
    monkeypatch.setenv("ADMIN_OIDC_ISSUER", "https://project.supabase.co/auth/v1")
    monkeypatch.setenv("ADMIN_OIDC_AUDIENCE", "authenticated")
    monkeypatch.setenv("ADMIN_OIDC_JWKS_URL", "https://project.supabase.co/auth/v1/.well-known/jwks.json")
    monkeypatch.setenv("ADMIN_OIDC_PROVIDER", "supabase")


def signed_token(private_key, **overrides):
    claims = {
        "sub": "provider-user-7",
        "iss": "https://project.supabase.co/auth/v1",
        "aud": "authenticated",
        "exp": int(time.time()) + 300,
        **overrides,
    }
    return jwt.encode(claims, private_key, algorithm="RS256", headers={"kid": "test"})


def test_oidc_jwt_is_cryptographically_verified_with_issuer_audience_and_expiry(monkeypatch):
    configure_oidc(monkeypatch)
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    monkeypatch.setattr(auth, "_signing_key", lambda _token, _url: private_key.public_key())

    identity = auth.verify_oidc_token(signed_token(private_key))

    assert identity == auth.AdminIdentity(provider="supabase", subject="provider-user-7")
    for invalid in [
        signed_token(private_key, iss="https://attacker.invalid"),
        signed_token(private_key, aud="wrong"),
        signed_token(private_key, exp=int(time.time()) - 1),
        signed_token(rsa.generate_private_key(public_exponent=65537, key_size=2048)),
    ]:
        with pytest.raises(HTTPException) as error:
            auth.verify_oidc_token(invalid)
        assert error.value.status_code == 401


def test_exchange_maps_active_account_and_persists_only_session_hash(monkeypatch):
    monkeypatch.setattr(
        admin_auth,
        "verify_oidc_token",
        lambda _token: auth.AdminIdentity("supabase", "provider-user-7"),
    )
    connection = install_database(monkeypatch, [{"id": 19}])
    response = Response()

    result = admin_auth.create_admin_session(response, "Bearer verified-token")

    assert result["authenticated"] is True
    cookie = response.headers["set-cookie"]
    assert auth.ADMIN_COOKIE_NAME in cookie and "HttpOnly" in cookie and "Secure" in cookie
    insert_query, insert_params = connection.cursor_instance.executed[-1]
    assert insert_query.startswith("INSERT INTO auth.admin_session")
    assert "auth.user_competition_role" in connection.cursor_instance.executed[0][0]
    assert "role = 'competition_admin'" in connection.cursor_instance.executed[0][0]
    assert "ON CONFLICT (user_id) DO UPDATE" in insert_query
    assert "verified-token" not in repr(connection.cursor_instance.executed)
    assert len(insert_params[1]) == 64
    assert connection.committed is True


def test_exchange_rejects_unmapped_or_inactive_identity(monkeypatch):
    monkeypatch.setattr(
        admin_auth,
        "verify_oidc_token",
        lambda _token: auth.AdminIdentity("supabase", "missing"),
    )
    install_database(monkeypatch, [None])

    with pytest.raises(HTTPException) as error:
        admin_auth.create_admin_session(Response(), "Bearer verified-token")

    assert error.value.status_code == 403


def test_logout_revokes_hashed_session_and_clears_cookie(monkeypatch):
    connection = install_database(monkeypatch, [])
    response = Response()

    admin_auth.delete_admin_session(response, "opaque-session")

    query, params = connection.cursor_instance.executed[0]
    assert query.startswith("UPDATE auth.admin_session SET revoked_at")
    assert params == (hashlib.sha256(b"opaque-session").hexdigest(),)
    assert "opaque-session" not in repr(params)
    assert "Max-Age=0" in response.headers["set-cookie"]


def test_competition_admin_dependency_checks_session_user_role_and_scope(monkeypatch):
    allowed = install_database(monkeypatch, [{"user_id": 19}, {"allowed": 1}])

    assert auth.require_competition_admin(7, "opaque-session") == 19
    session_query, session_params = allowed.cursor_instance.executed[0]
    role_query, role_params = allowed.cursor_instance.executed[1]
    assert "s.revoked_at IS NULL" in session_query and "s.expires_at > NOW()" in session_query
    assert "u.status = 'active'" in session_query
    assert session_params[0] == hashlib.sha256(b"opaque-session").hexdigest()
    assert "role = 'competition_admin'" in role_query
    assert role_params == (19, 7)

    install_database(monkeypatch, [{"user_id": 19}, None])
    with pytest.raises(HTTPException) as error:
        auth.require_competition_admin(8, "opaque-session")
    assert error.value.status_code == 403

    install_database(monkeypatch, [None])
    with pytest.raises(HTTPException) as error:
        auth.require_competition_admin(8, "operator-token")
    assert error.value.status_code == 401
    assert "swimstats_live_operator" not in Path(auth.__file__).read_text(encoding="utf-8")


def test_admin_session_routes_are_registered():
    methods_by_path = {
        route.path: route.methods for route in admin_auth.router.routes
    }
    assert methods_by_path["/admin-session"] == {"POST"}
    assert methods_by_path["/admin-session/logout"] == {"POST"}
    assert "include_router(admin_auth.router, prefix=\"/api/auth\"" in Path(main.__file__).read_text(
        encoding="utf-8"
    )
