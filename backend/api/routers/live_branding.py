import hashlib
import io
import warnings
from typing import NamedTuple

from fastapi import APIRouter, Depends, Header, HTTPException, Query, Request, Response
from PIL import Image, UnidentifiedImageError

from ..auth import require_competition_admin
from ..database import get_db_connection


router = APIRouter()
MAX_LOGO_BYTES = 2 * 1024 * 1024
MAX_LOGO_DIMENSION = 4096
MIME_FORMATS = {"image/png": "PNG", "image/jpeg": "JPEG", "image/webp": "WEBP"}
CACHE_CONTROL = "public, max-age=0, must-revalidate"
METADATA_FIELDS = """
    (logo_bytes IS NOT NULL AND deleted_at IS NULL) AS has_logo,
    revision, width, height, mime_type
"""


class NormalizedLogo(NamedTuple):
    content: bytes
    mime_type: str
    width: int
    height: int
    sha256: str


def _normalize_logo(content: bytes, content_type: str) -> NormalizedLogo:
    mime_type = content_type.split(";", 1)[0].strip().lower()
    expected_format = MIME_FORMATS.get(mime_type)
    if not expected_format:
        raise HTTPException(status_code=415, detail="PNG, JPEG, or WebP logo required")
    if not content or len(content) > MAX_LOGO_BYTES:
        raise HTTPException(status_code=413, detail="Logo exceeds 2 MiB limit")
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("error", Image.DecompressionBombWarning)
            with Image.open(io.BytesIO(content)) as probe:
                if probe.format != expected_format or getattr(probe, "is_animated", False):
                    raise HTTPException(status_code=422, detail="Invalid static logo format")
                width, height = probe.size
                if max(width, height) > MAX_LOGO_DIMENSION:
                    raise HTTPException(status_code=422, detail="Logo dimensions exceed 4096px")
                probe.verify()
            with Image.open(io.BytesIO(content)) as source:
                has_alpha = "A" in source.getbands() or "transparency" in source.info
                normalized = source.convert("RGBA" if has_alpha and expected_format != "JPEG" else "RGB")
                output = io.BytesIO()
                options = {"optimize": True} if expected_format == "PNG" else {"quality": 90}
                if expected_format == "WEBP": options["method"] = 6
                normalized.save(output, expected_format, **options)
    except HTTPException:
        raise
    except (Image.DecompressionBombError, Image.DecompressionBombWarning, UnidentifiedImageError, OSError, ValueError):
        raise HTTPException(status_code=422, detail="Invalid logo image") from None
    normalized_bytes = output.getvalue()
    if len(normalized_bytes) > MAX_LOGO_BYTES:
        raise HTTPException(status_code=413, detail="Normalized logo exceeds 2 MiB limit")
    return NormalizedLogo(
        normalized_bytes, mime_type, width, height,
        hashlib.sha256(normalized_bytes).hexdigest(),
    )


async def _read_logo_body(request: Request) -> bytes:
    raw_length = request.headers.get("content-length")
    if raw_length:
        try:
            if int(raw_length) > MAX_LOGO_BYTES:
                raise HTTPException(status_code=413, detail="Logo exceeds 2 MiB limit")
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid Content-Length") from None
    content = bytearray()
    async for chunk in request.stream():
        content.extend(chunk)
        if len(content) > MAX_LOGO_BYTES:
            raise HTTPException(status_code=413, detail="Logo exceeds 2 MiB limit")
    return bytes(content)


@router.get("/{competition_id}/live-branding")
def get_live_branding(competition_id: int):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM core.competition WHERE id = %s", (competition_id,))
            if not cur.fetchone():
                raise HTTPException(status_code=404, detail="Competition not found")
            cur.execute(f"SELECT {METADATA_FIELDS} FROM core.competition_live_branding WHERE competition_id = %s", (competition_id,))
            branding = cur.fetchone()
    return {"competition_id": competition_id, **(branding or {
        "has_logo": False, "revision": 0, "width": None, "height": None, "mime_type": None,
    })}


@router.get("/{competition_id}/live-branding/logo")
def get_live_branding_logo(
    competition_id: int,
    if_none_match: str | None = Header(default=None, alias="If-None-Match"),
):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT logo_bytes, mime_type, sha256
                FROM core.competition_live_branding
                WHERE competition_id = %s AND deleted_at IS NULL
            """, (competition_id,))
            logo = cur.fetchone()
    if not logo:
        raise HTTPException(status_code=404, detail="Competition logo not found")
    etag = f'"{logo["sha256"]}"'
    headers = {"ETag": etag, "Cache-Control": CACHE_CONTROL}
    if if_none_match == "*" or etag in [item.strip() for item in (if_none_match or "").split(",")]:
        return Response(status_code=304, headers=headers)
    return Response(content=logo["logo_bytes"], media_type=logo["mime_type"], headers=headers)


@router.put("/{competition_id}/live-branding")
async def put_live_branding(
    competition_id: int, request: Request,
    expected_revision: int = Query(ge=0),
    admin_user_id: int = Depends(require_competition_admin),
):
    normalized = _normalize_logo(await _read_logo_body(request), request.headers.get("content-type", ""))
    values = {
        "competition_id": competition_id, "expected_revision": expected_revision,
        "logo_bytes": normalized.content, "mime_type": normalized.mime_type,
        "width": normalized.width, "height": normalized.height, "sha256": normalized.sha256,
        "updated_by_user_id": admin_user_id,
    }
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM core.competition WHERE id = %s FOR UPDATE", (competition_id,))
            if not cur.fetchone():
                raise HTTPException(status_code=404, detail="Competition not found")
            cur.execute("SELECT revision, deleted_at FROM core.competition_live_branding WHERE competition_id = %s FOR UPDATE", (competition_id,))
            current = cur.fetchone()
            if (current and current["revision"] != expected_revision) or (not current and expected_revision != 0):
                raise HTTPException(status_code=409, detail="Live branding revision conflict")
            if current:
                cur.execute(f"""
                    UPDATE core.competition_live_branding SET
                        logo_bytes = %(logo_bytes)s, mime_type = %(mime_type)s,
                        width = %(width)s, height = %(height)s, sha256 = %(sha256)s,
                        revision = revision + 1, updated_at = NOW(),
                        updated_by_user_id = %(updated_by_user_id)s,
                        deleted_at = NULL, deleted_by_user_id = NULL
                    WHERE competition_id = %(competition_id)s AND revision = %(expected_revision)s
                    RETURNING {METADATA_FIELDS}
                """, values)
            else:
                cur.execute(f"""
                    INSERT INTO core.competition_live_branding (
                        competition_id, logo_bytes, mime_type, width, height, sha256,
                        updated_by_user_id
                    ) VALUES (
                        %(competition_id)s, %(logo_bytes)s, %(mime_type)s, %(width)s,
                        %(height)s, %(sha256)s, %(updated_by_user_id)s
                    ) RETURNING {METADATA_FIELDS}
                """, values)
            branding = cur.fetchone()
            if not branding:
                raise HTTPException(status_code=409, detail="Live branding revision conflict")
            conn.commit()
    return {"competition_id": competition_id, **branding}


@router.delete("/{competition_id}/live-branding")
def delete_live_branding(
    competition_id: int,
    expected_revision: int = Query(ge=1),
    admin_user_id: int = Depends(require_competition_admin),
):
    values = {"competition_id": competition_id, "expected_revision": expected_revision, "user_id": admin_user_id}
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM core.competition WHERE id = %s FOR UPDATE", (competition_id,))
            if not cur.fetchone():
                raise HTTPException(status_code=404, detail="Competition not found")
            cur.execute("SELECT revision, deleted_at FROM core.competition_live_branding WHERE competition_id = %s FOR UPDATE", (competition_id,))
            current = cur.fetchone()
            if not current or current["deleted_at"] is not None:
                raise HTTPException(status_code=404, detail="Competition logo not found")
            if current["revision"] != expected_revision:
                raise HTTPException(status_code=409, detail="Live branding revision conflict")
            cur.execute(f"""
                UPDATE core.competition_live_branding SET logo_bytes = NULL,
                    mime_type = NULL, width = NULL, height = NULL, sha256 = NULL,
                    revision = revision + 1, updated_at = NOW(), updated_by_user_id = %(user_id)s,
                    deleted_at = NOW(), deleted_by_user_id = %(user_id)s
                WHERE competition_id = %(competition_id)s AND revision = %(expected_revision)s
                RETURNING {METADATA_FIELDS}
            """, values)
            branding = cur.fetchone()
            if not branding:
                raise HTTPException(status_code=409, detail="Live branding revision conflict")
            conn.commit()
    return {"competition_id": competition_id, **branding}
