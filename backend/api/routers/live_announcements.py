from typing import Literal

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field, field_validator, model_validator

from ..auth import require_competition_admin
from ..database import get_db_connection


router = APIRouter()
RETURNING_FIELDS = """
    id, message, display_mode, is_active, revision,
    created_at, updated_at, activated_at
"""
EVENT_TYPES = Literal[
    "create", "update", "activate", "automatic_deactivate", "deactivate", "delete"
]


class AnnouncementContent(BaseModel):
    message: str = Field(min_length=1, max_length=1000)
    display_mode: Literal["fullscreen", "ticker"]

    @field_validator("message")
    @classmethod
    def message_not_blank(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("message must not be blank")
        return value

    @model_validator(mode="after")
    def ticker_message_length(self):
        if self.display_mode == "ticker" and len(self.message) > 240:
            raise ValueError("ticker message must not exceed 240 characters")
        return self


class AnnouncementCreate(AnnouncementContent):
    expected_revision: Literal[0]


class AnnouncementUpdate(AnnouncementContent):
    expected_revision: int = Field(ge=1)


class AnnouncementActivation(BaseModel):
    is_active: bool
    expected_revision: int = Field(ge=1)


def _locked_announcement(cur, competition_id: int, announcement_id: int):
    cur.execute("""
        SELECT revision FROM core.live_announcement
        WHERE id = %s AND competition_id = %s AND deleted_at IS NULL
        FOR UPDATE
    """, (announcement_id, competition_id))
    announcement = cur.fetchone()
    if not announcement:
        raise HTTPException(status_code=404, detail="Announcement not found")
    return announcement


def _require_revision(announcement, expected_revision: int) -> None:
    if announcement["revision"] != expected_revision:
        raise HTTPException(status_code=409, detail="Announcement revision conflict")


def _record_event(
    cur, competition_id: int, announcement, event_type: EVENT_TYPES,
    actor_user_id: int, *, is_deleted: bool = False,
) -> None:
    cur.execute("""
        INSERT INTO core.live_announcement_event (
            competition_id, announcement_id, event_type, revision, message,
            display_mode, is_active, is_deleted, actor_user_id
        ) VALUES (
            %(competition_id)s, %(announcement_id)s, %(event_type)s, %(revision)s,
            %(message)s, %(display_mode)s, %(is_active)s, %(is_deleted)s,
            %(actor_user_id)s
        )
    """, {
        "competition_id": competition_id, "announcement_id": announcement["id"],
        "event_type": event_type, "revision": announcement["revision"],
        "message": announcement["message"], "display_mode": announcement["display_mode"],
        "is_active": announcement["is_active"], "is_deleted": is_deleted,
        "actor_user_id": actor_user_id,
    })


@router.get("/{competition_id}/live-announcements/active")
def get_active_announcement(competition_id: int):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM core.competition WHERE id = %s", (competition_id,))
            if not cur.fetchone():
                raise HTTPException(status_code=404, detail="Competition not found")
            cur.execute(f"""
                SELECT {RETURNING_FIELDS} FROM core.live_announcement
                WHERE competition_id = %s AND is_active IS TRUE AND deleted_at IS NULL
                LIMIT 1
            """, (competition_id,))
            announcement = cur.fetchone()
    return {"competition_id": competition_id, "announcement": announcement}


@router.get("/{competition_id}/live-announcements")
def list_announcements(
    competition_id: int,
    _admin_user_id: int = Depends(require_competition_admin),
):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT {RETURNING_FIELDS} FROM core.live_announcement
                WHERE competition_id = %s AND deleted_at IS NULL
                ORDER BY updated_at DESC, id DESC
            """, (competition_id,))
            announcements = cur.fetchall()
    return {"competition_id": competition_id, "announcements": announcements}


@router.get("/{competition_id}/live-announcements/history")
def list_announcement_history(
    competition_id: int,
    limit: int = Query(default=25, ge=1, le=100),
    _admin_user_id: int = Depends(require_competition_admin),
):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, announcement_id, event_type, revision, message,
                       display_mode, is_active, is_deleted, actor_user_id, occurred_at
                FROM core.live_announcement_event
                WHERE competition_id = %s
                ORDER BY occurred_at DESC, id DESC
                LIMIT %s
            """, (competition_id, limit))
            events = cur.fetchall()
    return {"competition_id": competition_id, "events": events}


@router.post("/{competition_id}/live-announcements", status_code=201)
def create_announcement(
    competition_id: int,
    body: AnnouncementCreate,
    admin_user_id: int = Depends(require_competition_admin),
):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                INSERT INTO core.live_announcement (
                    competition_id, message, display_mode,
                    created_by_user_id, updated_by_user_id
                ) VALUES (%s, %s, %s, %s, %s)
                RETURNING {RETURNING_FIELDS}
            """, (competition_id, body.message, body.display_mode, admin_user_id, admin_user_id))
            announcement = cur.fetchone()
            _record_event(cur, competition_id, announcement, "create", admin_user_id)
            conn.commit()
    return {"competition_id": competition_id, "announcement": announcement}


@router.put("/{competition_id}/live-announcements/{announcement_id}")
def update_announcement(
    competition_id: int,
    announcement_id: int,
    body: AnnouncementUpdate,
    admin_user_id: int = Depends(require_competition_admin),
):
    values = {
        "competition_id": competition_id, "announcement_id": announcement_id,
        "message": body.message, "display_mode": body.display_mode,
        "expected_revision": body.expected_revision, "user_id": admin_user_id,
    }
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            current = _locked_announcement(cur, competition_id, announcement_id)
            _require_revision(current, body.expected_revision)
            cur.execute(f"""
                UPDATE core.live_announcement SET message = %(message)s,
                    display_mode = %(display_mode)s, revision = revision + 1,
                    updated_at = NOW(), updated_by_user_id = %(user_id)s
                WHERE id = %(announcement_id)s AND competition_id = %(competition_id)s
                  AND deleted_at IS NULL AND revision = %(expected_revision)s
                RETURNING {RETURNING_FIELDS}
            """, values)
            announcement = cur.fetchone()
            if not announcement:
                raise HTTPException(status_code=409, detail="Announcement revision conflict")
            _record_event(cur, competition_id, announcement, "update", admin_user_id)
            conn.commit()
    return {"competition_id": competition_id, "announcement": announcement}


@router.put("/{competition_id}/live-announcements/{announcement_id}/activation")
def set_announcement_activation(
    competition_id: int,
    announcement_id: int,
    body: AnnouncementActivation,
    admin_user_id: int = Depends(require_competition_admin),
):
    values = {
        "competition_id": competition_id, "announcement_id": announcement_id,
        "is_active": body.is_active, "expected_revision": body.expected_revision,
        "user_id": admin_user_id,
    }
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM core.competition WHERE id = %s FOR UPDATE", (competition_id,))
            if not cur.fetchone():
                raise HTTPException(status_code=404, detail="Competition not found")
            current = _locked_announcement(cur, competition_id, announcement_id)
            _require_revision(current, body.expected_revision)
            if body.is_active:
                cur.execute("""
                    UPDATE core.live_announcement SET is_active = FALSE,
                        revision = revision + 1, updated_at = NOW(), updated_by_user_id = %s
                    WHERE competition_id = %s AND id <> %s
                      AND is_active IS TRUE AND deleted_at IS NULL
                    RETURNING id, message, display_mode, is_active, revision,
                              created_at, updated_at, activated_at
                """, (admin_user_id, competition_id, announcement_id))
                for deactivated in cur.fetchall():
                    _record_event(
                        cur, competition_id, deactivated,
                        "automatic_deactivate", admin_user_id,
                    )
            cur.execute(f"""
                UPDATE core.live_announcement SET is_active = %(is_active)s,
                    revision = revision + 1, updated_at = NOW(),
                    updated_by_user_id = %(user_id)s,
                    activated_at = CASE WHEN %(is_active)s THEN NOW() ELSE activated_at END,
                    activated_by_user_id = CASE WHEN %(is_active)s THEN %(user_id)s ELSE activated_by_user_id END
                WHERE id = %(announcement_id)s AND competition_id = %(competition_id)s
                  AND deleted_at IS NULL AND revision = %(expected_revision)s
                RETURNING {RETURNING_FIELDS}
            """, values)
            announcement = cur.fetchone()
            if not announcement:
                raise HTTPException(status_code=409, detail="Announcement revision conflict")
            _record_event(
                cur, competition_id, announcement,
                "activate" if body.is_active else "deactivate", admin_user_id,
            )
            conn.commit()
    return {"competition_id": competition_id, "announcement": announcement}


@router.delete("/{competition_id}/live-announcements/{announcement_id}")
def delete_announcement(
    competition_id: int,
    announcement_id: int,
    expected_revision: int = Query(ge=1),
    admin_user_id: int = Depends(require_competition_admin),
):
    values = {
        "competition_id": competition_id, "announcement_id": announcement_id,
        "expected_revision": expected_revision, "user_id": admin_user_id,
    }
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            current = _locked_announcement(cur, competition_id, announcement_id)
            _require_revision(current, expected_revision)
            cur.execute(f"""
                UPDATE core.live_announcement SET is_active = FALSE,
                    deleted_at = NOW(), deleted_by_user_id = %(user_id)s,
                    updated_at = NOW(), updated_by_user_id = %(user_id)s,
                    revision = revision + 1
                WHERE id = %(announcement_id)s AND competition_id = %(competition_id)s
                  AND deleted_at IS NULL AND revision = %(expected_revision)s
                RETURNING {RETURNING_FIELDS}
            """, values)
            announcement = cur.fetchone()
            if not announcement:
                raise HTTPException(status_code=409, detail="Announcement revision conflict")
            _record_event(
                cur, competition_id, announcement, "delete", admin_user_id,
                is_deleted=True,
            )
            conn.commit()
    return {"competition_id": competition_id, "announcement": announcement}
