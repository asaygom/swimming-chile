from fastapi import APIRouter, Cookie, Header, HTTPException, Response, status

from ..auth import (
    ADMIN_COOKIE_NAME,
    create_admin_session_record,
    revoke_admin_session,
    set_admin_cookie,
    verify_oidc_token,
)


router = APIRouter()


def _bearer_token(authorization: str | None) -> str:
    scheme, _, token = (authorization or "").partition(" ")
    if scheme.lower() != "bearer" or not token:
        raise HTTPException(status_code=401, detail="Bearer identity required")
    return token


@router.post("/admin-session")
def create_admin_session(
    response: Response,
    authorization: str | None = Header(default=None),
):
    identity = verify_oidc_token(_bearer_token(authorization))
    raw_token, ttl = create_admin_session_record(identity)
    set_admin_cookie(response, raw_token, ttl)
    return {"authenticated": True, "expires_in_seconds": ttl}


@router.post("/admin-session/logout", status_code=status.HTTP_204_NO_CONTENT)
def delete_admin_session(
    response: Response,
    admin_session: str | None = Cookie(default=None, alias=ADMIN_COOKIE_NAME),
):
    revoke_admin_session(admin_session)
    response.delete_cookie(ADMIN_COOKIE_NAME, path="/api")
