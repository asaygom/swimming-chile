"""Publicacion de sembrado desde la app, sin pasar por el terminal.

No reimplementa compuertas: parsea con el mismo modulo que la CLI y publica con
`publish_validated_program`, que ya exige identidad canonica de evento, checksum,
parser_version y coincidencia de nombre y fechas con la competencia.
"""

import sys
import tempfile
from dataclasses import asdict
from pathlib import Path
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from psycopg.rows import tuple_row

from ..auth import require_platform_admin
from ..database import get_db_connection

# El parser vive en scripts/ como CLI. Se importa en vez de duplicarlo para que
# la app y el terminal no puedan divergir en reglas de validacion.
SCRIPTS_DIR = Path(__file__).resolve().parents[2] / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

import run_meet_program as meet_program  # noqa: E402


router = APIRouter()
MAX_PROGRAM_BYTES = 16 * 1024 * 1024
SOURCE_SUFFIXES = {"pdf": ".pdf", "csv": ".csv"}
UNPARSED_SAMPLE = 20


async def _read_program_body(request: Request) -> bytes:
    raw_length = request.headers.get("content-length")
    if raw_length:
        try:
            if int(raw_length) > MAX_PROGRAM_BYTES:
                raise HTTPException(status_code=413, detail="Program exceeds 16 MiB limit")
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid Content-Length") from None
    content = bytearray()
    async for chunk in request.stream():
        content.extend(chunk)
        if len(content) > MAX_PROGRAM_BYTES:
            raise HTTPException(status_code=413, detail="Program exceeds 16 MiB limit")
    if not content:
        raise HTTPException(status_code=422, detail="Empty program upload")
    return bytes(content)


def _parse_upload(content: bytes, source_format: str, work_dir: Path):
    """Parsea a un directorio temporal: en Railway el filesystem es efimero y los
    artefactos no sobreviven, por eso el resumen se devuelve en la respuesta."""
    source_path = work_dir / f"program{SOURCE_SUFFIXES[source_format]}"
    source_path.write_bytes(content)
    try:
        parsed = (
            meet_program.parse_meet_manager_csv(source_path) if source_format == "csv"
            else meet_program.parse_pdf(source_path)
        )
    except meet_program.MeetProgramError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from None
    return parsed


def _apply_overrides(
    parsed, *, source_name, source_url, stage_number, pool_role, scheduled_date
) -> None:
    # `pdf_name` y `pdf_path` aterrizan en source_document.document_name y
    # storage_path. Sin esto quedaria el nombre del temporal y una ruta que no
    # existe en ninguna parte. Ninguno de los dos entra en el hash de identidad.
    parsed.metadata["pdf_name"] = source_name or parsed.metadata.get("pdf_name")
    parsed.metadata["pdf_path"] = f"upload://{source_name}" if source_name else None
    if source_url:
        parsed.metadata["source_url"] = source_url
    if stage_number is not None:
        parsed.metadata["stage_number"] = stage_number
    if pool_role:
        parsed.metadata["pool_role"] = pool_role
    if scheduled_date:
        parsed.metadata["scheduled_date"] = scheduled_date


def _preview_payload(parsed, summary) -> dict:
    return {
        "state": summary.state,
        "counts": summary.counts,
        "issues": [asdict(issue) for issue in summary.issues],
        "source": {
            key: parsed.metadata.get(key)
            for key in (
                "source_kind", "pdf_name", "pdf_sha256", "parser_version",
                "source_competition_name", "source_competition_start_date",
                "source_competition_end_date", "stage_number", "pool_role",
                "scheduled_date",
            )
        },
        "events": sorted(
            {(entry.event_number, entry.event_name) for entry in parsed.entries},
            key=lambda item: item[0],
        ),
        "unparsed_sample": [asdict(line) for line in parsed.unparsed[:UNPARSED_SAMPLE]],
    }


def _parsed_preview(content: bytes, source_format: str, overrides: dict):
    with tempfile.TemporaryDirectory(prefix="meet-program-") as work_dir:
        path = Path(work_dir)
        parsed = _parse_upload(content, source_format, path)
        _apply_overrides(parsed, **overrides)
        summary = meet_program.write_artifacts(parsed, path / "artifacts")
    return parsed, summary


@router.post("/{competition_id}/meet-program/preview")
async def preview_meet_program(
    competition_id: int,
    request: Request,
    source_format: Literal["pdf", "csv"] = Query(),
    source_name: str | None = Query(default=None, max_length=255),
    source_url: str | None = Query(default=None),
    stage_number: int | None = Query(default=None, ge=1),
    pool_role: Literal["main", "competition", "training"] | None = Query(default=None),
    scheduled_date: str | None = Query(default=None),
    _admin_user_id: int = Depends(require_platform_admin),
):
    content = await _read_program_body(request)
    parsed, summary = _parsed_preview(content, source_format, {
        "source_name": source_name, "source_url": source_url, "stage_number": stage_number,
        "pool_role": pool_role, "scheduled_date": scheduled_date,
    })
    return {"competition_id": competition_id, **_preview_payload(parsed, summary)}


@router.post("/{competition_id}/meet-program/publish")
async def publish_meet_program(
    competition_id: int,
    request: Request,
    source_format: Literal["pdf", "csv"] = Query(),
    source_name: str | None = Query(default=None, max_length=255),
    source_url: str | None = Query(default=None),
    stage_number: int | None = Query(default=None, ge=1),
    pool_role: Literal["main", "competition", "training"] | None = Query(default=None),
    scheduled_date: str | None = Query(default=None),
    _admin_user_id: int = Depends(require_platform_admin),
):
    content = await _read_program_body(request)
    parsed, summary = _parsed_preview(content, source_format, {
        "source_name": source_name, "source_url": source_url, "stage_number": stage_number,
        "pool_role": pool_role, "scheduled_date": scheduled_date,
    })
    payload = {"competition_id": competition_id, **_preview_payload(parsed, summary)}
    if summary.state != "validated":
        raise HTTPException(status_code=422, detail=payload)
    # publish_validated_program se comparte con la CLI, que conecta sin
    # row_factory y lee las filas por posicion. Con el dict_row por defecto de
    # los routers, `competition[1]` levanta KeyError y la publicacion cae en 500.
    with get_db_connection(row_factory=tuple_row) as conn:
        try:
            publication_id, created = meet_program.publish_validated_program(
                conn,
                parsed.entries,
                parsed.metadata,
                competition_id=competition_id,
                source_url=source_url,
                schema="core",
            )
        except meet_program.MeetProgramError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from None
    return {**payload, "publication_id": publication_id, "publication_created": created}
