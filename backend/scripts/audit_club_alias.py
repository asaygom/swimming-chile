#!/usr/bin/env python
"""Audita club_alias.csv contra core.club.

El caso "Koiko Pedro Aguirre Cerda" mostro que un alias puede revivir un club ya
fusionado: si el canonical_name de una fila no corresponde a un club vigente, la
proxima carga que vea ese alias lo vuelve a crear. Este script busca esa y otras
inconsistencias entre el archivo curado y el estado real de la base.

Es de solo lectura: no escribe en la base ni modifica club_alias.csv.

Hallazgos que reporta:

  canonical_ausente     canonical_name que no existe en core.club. Cargar ese
                        alias crea un club nuevo con ese nombre.
  club_es_alias         un club vigente de core.club que el archivo declara
                        alias de otro canonical. Es una fusion pendiente: los
                        datos nuevos van al canonical y los viejos se quedan en
                        el club antiguo.
  alias_duplicado       el mismo alias_name aparece dos veces con canonical
                        distinto; gana el ultimo y el otro se pierde en silencio.
  canonical_duplicado   dos clubes de core.club comparten nombre normalizado.
"""

from __future__ import annotations

import argparse
import csv
import sys
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from run_pipeline_results import (  # noqa: E402
    clean_extracted_text,
    load_club_aliases,
    normalize_match_text,
)

BACKEND_DIR = SCRIPT_DIR.parent
DEFAULT_ALIAS_CSV = BACKEND_DIR / "data" / "reference" / "club_alias.csv"


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--club-alias-csv", default=str(DEFAULT_ALIAS_CSV))
    parser.add_argument("--database-url", required=True)
    parser.add_argument("--out-csv", default=None, help="CSV de hallazgos, delimitador ';'.")
    return parser.parse_args(argv)


def read_raw_rows(path: Path) -> List[Tuple[int, str, str, str]]:
    """Filas crudas con su numero de linea, para poder senalar duplicados."""
    rows: List[Tuple[int, str, str, str]] = []
    with path.open(encoding="utf-8-sig", newline="") as handle:
        for line_no, row in enumerate(csv.DictReader(handle), start=2):
            alias_name = clean_extracted_text(row.get("alias_name"))
            canonical_name = clean_extracted_text(row.get("canonical_name"))
            if alias_name and canonical_name:
                rows.append((line_no, alias_name, canonical_name, row.get("notes") or ""))
    return rows


def fetch_clubs(database_url: str) -> List[dict]:
    import psycopg

    with psycopg.connect(database_url, connect_timeout=60) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT c.id, c.name, count(r.id)
            FROM core.club c
            LEFT JOIN core.result r ON r.club_id = c.id
            GROUP BY c.id, c.name
            ORDER BY c.id
            """
        )
        return [{"id": row[0], "name": row[1], "results": row[2]} for row in cur.fetchall()]


def audit(
    raw_rows: Sequence[Tuple[int, str, str, str]],
    aliases: Dict[str, str],
    clubs: Sequence[dict],
) -> List[dict]:
    by_key: Dict[str, List[dict]] = defaultdict(list)
    for club in clubs:
        key = normalize_match_text(club["name"])
        if key:
            by_key[key].append(club)

    findings: List[dict] = []

    # canonical_ausente: el destino final no es un club vigente. Se evalua el
    # canonical ya resuelto y no el de la fila, porque una cadena puede pasar
    # por nombres intermedios que no son clubes y aun asi terminar bien.
    seen_target: set = set()
    for line_no, alias_name, canonical_name, _ in raw_rows:
        alias_key = normalize_match_text(alias_name)
        if not alias_key:
            continue
        resolved = aliases.get(alias_key, canonical_name)
        target_key = normalize_match_text(resolved)
        if not target_key or target_key in seen_target or target_key in by_key:
            continue
        seen_target.add(target_key)
        findings.append(
            {
                "tipo": "canonical_ausente",
                "linea": line_no,
                "alias_name": alias_name,
                "canonical_name": resolved,
                "detalle": "no existe en core.club; cargarlo crearia un club nuevo",
            }
        )

    # club_es_alias: un club vigente que el archivo manda a otro canonical.
    for club in clubs:
        key = normalize_match_text(club["name"])
        if not key:
            continue
        target = aliases.get(key)
        if target and normalize_match_text(target) != key:
            findings.append(
                {
                    "tipo": "club_es_alias",
                    "linea": "",
                    "alias_name": f"{club['id']}:{club['name']}",
                    "canonical_name": target,
                    "detalle": (
                        f"club vigente con {club['results']} resultados declarado alias; "
                        "fusion pendiente"
                    ),
                }
            )

    # alias_duplicado: misma clave, canonical distinto.
    by_alias: Dict[str, List[Tuple[int, str]]] = defaultdict(list)
    for line_no, alias_name, canonical_name, _ in raw_rows:
        key = normalize_match_text(alias_name)
        if key:
            by_alias[key].append((line_no, canonical_name))
    for key, entries in by_alias.items():
        targets = {normalize_match_text(name) for _, name in entries}
        if len(targets) > 1:
            findings.append(
                {
                    "tipo": "alias_duplicado",
                    "linea": ",".join(str(line_no) for line_no, _ in entries),
                    "alias_name": key,
                    "canonical_name": " | ".join(name for _, name in entries),
                    "detalle": "mismo alias con canonical distinto; gana el ultimo",
                }
            )

    # canonical_duplicado: dos clubes con el mismo nombre normalizado.
    for key, group in by_key.items():
        if len(group) > 1:
            findings.append(
                {
                    "tipo": "canonical_duplicado",
                    "linea": "",
                    "alias_name": key,
                    "canonical_name": " | ".join(f"{c['id']}:{c['name']}" for c in group),
                    "detalle": "dos clubes de core.club comparten nombre normalizado",
                }
            )

    return findings


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    alias_path = Path(args.club_alias_csv)
    raw_rows = read_raw_rows(alias_path)
    aliases = load_club_aliases(str(alias_path))
    clubs = fetch_clubs(args.database_url)

    findings = audit(raw_rows, aliases, clubs)

    counts: Dict[str, int] = defaultdict(int)
    for finding in findings:
        counts[finding["tipo"]] += 1
    print(f"filas en {alias_path.name}: {len(raw_rows)} | clubes en core.club: {len(clubs)}")
    for tipo in sorted(counts):
        print(f"  {tipo}: {counts[tipo]}")
    if not findings:
        print("sin hallazgos")

    if args.out_csv:
        out_path = Path(args.out_csv)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with out_path.open("w", encoding="utf-8-sig", newline="") as handle:
            writer = csv.DictWriter(
                handle,
                fieldnames=["tipo", "linea", "alias_name", "canonical_name", "detalle"],
                delimiter=";",
            )
            writer.writeheader()
            writer.writerows(findings)
        print(f"escrito: {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
