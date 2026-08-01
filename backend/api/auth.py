import hashlib
import os
import secrets
from dataclasses import dataclass
from functools import lru_cache

import jwt
from fastapi import Cookie, HTTPException, Response

from .database import get_db_connection


ADMIN_COOKIE_NAME = "swimstats_admin_session"
DEFAULT_SESSION_TTL_SECONDS = 4 * 60 * 60
ALLOWED_JWT_ALGORITHMS = ["RS256", "ES256"]


@dataclass(frozen=True)
class AdminIdentity:
    provider: str
    subject: str


def _required_config(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise HTTPException(status_code=503, detail="Administrative authentication is not configured")
    return value


@lru_cache(maxsize=4)
def _jwk_client(jwks_url: str) -> jwt.PyJWKClient:
    return jwt.PyJWKClient(jwks_url)


def _signing_key(token: str, jwks_url: str):
    return _jwk_client(jwks_url).get_signing_key_from_jwt(token).key


def verify_oidc_token(token: str) -> AdminIdentity:
    issuer = _required_config("ADMIN_OIDC_ISSUER")
    audience = _required_config("ADMIN_OIDC_AUDIENCE")
    jwks_url = _required_config("ADMIN_OIDC_JWKS_URL")
    provider = os.getenv("ADMIN_OIDC_PROVIDER", "supabase").strip() or "supabase"
    try:
        claims = jwt.decode(
            token,
            _signing_key(token, jwks_url),
            algorithms=ALLOWED_JWT_ALGORITHMS,
            audience=audience,
            issuer=issuer,
            options={"require": ["sub", "iss", "aud", "exp"]},
        )
    except jwt.PyJWTError:
        raise HTTPException(status_code=401, detail="Invalid administrative identity") from None
    subject = claims.get("sub")
    if not isinstance(subject, str) or not subject:
        raise HTTPException(status_code=401, detail="Invalid administrative identity")
    return AdminIdentity(provider=provider, subject=subject)


def create_admin_session_record(identity: AdminIdentity) -> tuple[str, int]:
    raw_token = secrets.token_urlsafe(32)
    token_hash = hashlib.sha256(raw_token.encode()).hexdigest()
    ttl = int(os.getenv("ADMIN_SESSION_TTL_SECONDS", DEFAULT_SESSION_TTL_SECONDS))
    if ttl <= 0:
        raise HTTPException(status_code=503, detail="Administrative authentication is not configured")
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT u.id FROM auth.user_account u
                WHERE u.external_provider = %s AND u.external_subject = %s
                  AND u.status = 'active' AND EXISTS (
                      SELECT 1 FROM auth.user_competition_role r
                      WHERE r.user_id = u.id AND r.role = 'competition_admin'
                  )
            """, (identity.provider, identity.subject))
            account = cur.fetchone()
            if not account:
                raise HTTPException(status_code=403, detail="Administrative access denied")
            cur.execute("""
                INSERT INTO auth.admin_session (user_id, token_hash, expires_at)
                VALUES (%s, %s, NOW() + (%s * INTERVAL '1 second'))
                ON CONFLICT (user_id) DO UPDATE SET
                    token_hash = EXCLUDED.token_hash, created_at = NOW(),
                    expires_at = EXCLUDED.expires_at, revoked_at = NULL
            """, (account["id"], token_hash, ttl))
            conn.commit()
    return raw_token, ttl


def set_admin_cookie(response: Response, raw_token: str, ttl: int) -> None:
    response.set_cookie(
        ADMIN_COOKIE_NAME,
        raw_token,
        max_age=ttl,
        httponly=True,
        secure=os.getenv("ADMIN_COOKIE_SECURE", "true").lower() != "false",
        samesite="lax",
        path="/api",
    )


def revoke_admin_session(raw_token: str | None) -> None:
    if not raw_token:
        return
    token_hash = hashlib.sha256(raw_token.encode()).hexdigest()
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE auth.admin_session SET revoked_at = COALESCE(revoked_at, NOW())
                WHERE token_hash = %s
            """, (token_hash,))
            conn.commit()


def require_competition_admin(
    competition_id: int,
    admin_session: str | None = Cookie(default=None, alias=ADMIN_COOKIE_NAME),
) -> int:
    if not admin_session:
        raise HTTPException(status_code=401, detail="Administrative session required")
    token_hash = hashlib.sha256(admin_session.encode()).hexdigest()
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT u.id AS user_id
                FROM auth.admin_session s
                JOIN auth.user_account u ON u.id = s.user_id
                WHERE s.token_hash = %s AND s.revoked_at IS NULL
                  AND s.expires_at > NOW() AND u.status = 'active'
            """, (token_hash,))
            account = cur.fetchone()
            if not account:
                raise HTTPException(status_code=401, detail="Invalid administrative session")
            cur.execute("""
                SELECT 1 AS allowed FROM auth.user_competition_role
                WHERE user_id = %s AND competition_id = %s
                  AND role = 'competition_admin'
            """, (account["user_id"], competition_id))
            if not cur.fetchone():
                raise HTTPException(status_code=403, detail="Competition administration denied")
    return account["user_id"]
