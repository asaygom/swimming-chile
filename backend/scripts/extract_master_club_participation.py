#!/usr/bin/env python
"""Extrae short_name y region de los PDF "participacion-equipos-master-YYYY".

Los PDF de la FCHMN publican, por temporada, una tabla de clubes con su codigo
de cinco caracteres y su region en numeral romano. core.club tiene esas dos
columnas vacias para casi todos los clubes, asi que este script cruza ambas
fuentes y escribe artefactos de revision.

Es intencionalmente de solo lectura: no escribe en la base. Produce dos CSV con
delimitador ";".

  * <prefix>_rows.csv     una fila por linea de PDF, con el texto crudo, para
                          poder auditar de donde sale cada valor propuesto.
  * <prefix>_updates.csv  una fila por club consolidado, con el match contra
                          core.club y la accion propuesta.

Las columnas por torneo no se extraen: el layout colapsa las celdas vacias, asi
que los conteos no se pueden alinear con su torneo de forma confiable. Solo se
conserva el total de inscritos, que si es posicional y sirve de auditoria.

Dos inconsistencias vienen del PDF y no del parseo:

  * 2022 declara TOTALES 4.436, pero sus filas de club suman 4.399. Esa edicion
    no imprime la fila "Nadadores Extranjeros y/o sin equipo", asi que los 37
    restantes no son atribuibles a ningun club.
  * 2024 lista "Universidad Gabriela Mistral UGAMI RM 1 1 1": el total dice 1 y
    las columnas suman 2.

El resto de las 408 filas cuadra su total contra la suma de sus columnas.
"""

from __future__ import annotations

import argparse
import csv
import difflib
import re
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import pdfplumber

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from run_pipeline_results import (  # noqa: E402
    clean_extracted_text,
    load_club_aliases,
    normalize_match_text,
    resolve_club_alias,
)

BACKEND_DIR = SCRIPT_DIR.parent
DEFAULT_ALIAS_CSV = BACKEND_DIR / "data" / "reference" / "club_alias.csv"
DEFAULT_STAGING_DIR = BACKEND_DIR / "data" / "staging"

# Ortografia tal como ya esta almacenada en core.club, para no introducir una
# segunda variante de la misma region al poblar la columna.
REGION_BY_CODE: Dict[str, str] = {
    "XV": "Arica y Parinacota",
    "I": "Tarapacá",
    "II": "Antofagasta",
    "III": "Atacama",
    "IV": "Coquimbo",
    "V": "Valparaiso",
    "RM": "Metropolitana",
    "XIII": "Metropolitana",
    "VI": "OHiggins",
    "VII": "Maule",
    "XVI": "Ñuble",
    "VIII": "Biobío",
    "IX": "La Araucanía",
    "XIV": "Los Ríos",
    "X": "Los Lagos",
    "XI": "Aysen",
    "XII": "Magallanes",
}

# Alternancia ordenada de mayor a menor para que XVI gane sobre XV y VI sobre V.
_REGION_ALT = "|".join(
    sorted(REGION_BY_CODE, key=lambda code: (-len(code), code))
)
_SHORT = r"[A-Z0-9ÑÁÉÍÓÚÜ]{5}"
_COUNTS = r"[\d.]+(?:\s+[\d.]+)*"

ROW_WITH_REGION = re.compile(
    rf"^(?P<name>.+?)\s+(?:(?P<short>{_SHORT})\s+)?"
    rf"(?P<region>{_REGION_ALT})\s+(?P<counts>{_COUNTS})$"
)
ROW_WITHOUT_REGION = re.compile(
    rf"^(?P<name>.+?)\s+(?P<short>{_SHORT})\s+(?P<counts>{_COUNTS})$"
)

# Filas de encabezado/pie que nunca son un club.
SKIP_PREFIXES = (
    "PARTICIP",
    "FUENTE:",
    "TOTAL",
    "CLUBES",
    "TORNEOS",
    "INSCRITOS",
    "AÑO ",
    "DATOS AL",
    "ORGANIZA",
    "Nadadores Extranjeros",
)


@dataclass
class PdfRow:
    """Una linea de club tal como aparece en un PDF de temporada."""

    year: int
    line_no: int
    raw_line: str
    raw_name: str
    short_name: Optional[str]
    region_code: Optional[str]
    total_inscritos: Optional[int]


@dataclass
class ClubEvidence:
    """Evidencia acumulada de un club a lo largo de las temporadas."""

    match_key: str
    canonical_name: str
    names: Dict[str, List[int]] = field(default_factory=lambda: defaultdict(list))
    short_names: Dict[str, List[int]] = field(default_factory=lambda: defaultdict(list))
    region_codes: Dict[str, List[int]] = field(default_factory=lambda: defaultdict(list))
    totals: Dict[int, int] = field(default_factory=dict)


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--pdf",
        action="append",
        default=None,
        help="Ruta de un PDF de temporada. Repetible. Por defecto usa los 2022-2026 de data/staging.",
    )
    parser.add_argument("--club-alias-csv", default=str(DEFAULT_ALIAS_CSV))
    parser.add_argument(
        "--alias-review-resolved",
        default=None,
        help=(
            "CSV de revision con una columna 'id BD' resuelta a mano. Cada id se "
            "trata como match manual contra core.club."
        ),
    )
    parser.add_argument(
        "--out-prefix",
        required=True,
        help="Prefijo de salida; se generan <prefix>_rows.csv y <prefix>_updates.csv.",
    )
    parser.add_argument(
        "--database-url",
        default=None,
        help="Cadena de conexion para cruzar contra core.club. Sin ella el cruce queda vacio.",
    )
    return parser.parse_args(argv)


def default_pdfs() -> List[Path]:
    return sorted(DEFAULT_STAGING_DIR.glob("participacion-equipos-master-*.pdf"))


def year_from_path(path: Path) -> int:
    match = re.search(r"(\d{4})", path.stem)
    if not match:
        raise ValueError(f"No se pudo inferir el año desde {path.name}")
    return int(match.group(1))


def parse_total(counts: str) -> Optional[int]:
    first = counts.split()[0]
    try:
        return int(first.replace(".", ""))
    except ValueError:
        return None


def parse_pdf(path: Path) -> List[PdfRow]:
    year = year_from_path(path)
    rows: List[PdfRow] = []
    with pdfplumber.open(str(path)) as pdf:
        lines: List[str] = []
        for page in pdf.pages:
            lines.extend((page.extract_text() or "").splitlines())

    for line_no, raw_line in enumerate(lines, start=1):
        line = raw_line.strip()
        if not line or line.startswith(SKIP_PREFIXES):
            continue
        match = ROW_WITH_REGION.match(line) or ROW_WITHOUT_REGION.match(line)
        if not match:
            continue
        groups = match.groupdict()
        name = clean_extracted_text(groups["name"])
        if not name or not normalize_match_text(name):
            continue
        rows.append(
            PdfRow(
                year=year,
                line_no=line_no,
                raw_line=line,
                raw_name=name,
                short_name=groups.get("short"),
                region_code=groups.get("region"),
                total_inscritos=parse_total(groups["counts"]),
            )
        )
    return rows


def build_evidence(rows: Iterable[PdfRow], aliases: Dict[str, str]) -> Dict[str, ClubEvidence]:
    evidence: Dict[str, ClubEvidence] = {}
    for row in rows:
        canonical = resolve_club_alias(row.raw_name, aliases) or row.raw_name
        key = normalize_match_text(canonical)
        if not key:
            continue
        entry = evidence.get(key)
        if entry is None:
            entry = ClubEvidence(match_key=key, canonical_name=canonical)
            evidence[key] = entry
        entry.names[row.raw_name].append(row.year)
        if row.short_name:
            entry.short_names[row.short_name].append(row.year)
        if row.region_code:
            entry.region_codes[row.region_code].append(row.year)
        if row.total_inscritos is not None:
            entry.totals[row.year] = row.total_inscritos
    return evidence


def alias_canonical(value: Optional[str], aliases: Dict[str, str]) -> Optional[str]:
    """Nombre canonico solo si el valor esta declarado en club_alias.csv.

    resolve_club_alias devuelve el valor de entrada cuando no hay alias, lo que
    no permite distinguir "resuelto" de "desconocido". Aca esa diferencia
    importa, porque un codigo sin alias no es evidencia de nada.
    """
    key = normalize_match_text(clean_extracted_text(value))
    return aliases.get(key) if key else None


def latest_value(observations: Dict[str, List[int]]) -> Optional[str]:
    """Valor mas reciente; ante empate, el de mas temporadas."""
    if not observations:
        return None
    return max(
        observations.items(),
        key=lambda item: (max(item[1]), len(item[1])),
    )[0]


def fetch_db_clubs(database_url: str) -> List[dict]:
    import psycopg

    with psycopg.connect(database_url, connect_timeout=30) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT id, name, short_name, region, city FROM core.club ORDER BY id"
        )
        return [
            {"id": row[0], "name": row[1], "short_name": row[2], "region": row[3], "city": row[4]}
            for row in cur.fetchall()
        ]


def fetch_club_result_years(database_url: str) -> Dict[int, set]:
    """Temporadas en que cada club de core.club tiene resultados cargados.

    Es la evidencia mas fuerte para decidir un match dudoso: el parecido del
    nombre opina, pero haber nadado el mismo año no.
    """
    import psycopg

    with psycopg.connect(database_url, connect_timeout=60) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT r.club_id, cp.season_year
            FROM core.result r
            JOIN core.event e ON e.id = r.event_id
            JOIN core.competition cp ON cp.id = e.competition_id
            WHERE r.club_id IS NOT NULL AND cp.season_year IS NOT NULL
            GROUP BY 1, 2
            """
        )
        years: Dict[int, set] = defaultdict(set)
        for club_id, season_year in cur.fetchall():
            years[club_id].add(season_year)
        return years


def covered_years(pdf_years: set, result_years: set) -> List[int]:
    """Años del PDF respaldados por resultados, tolerando un año de desfase.

    El nacional de enero cierra la temporada del PDF pero cae en el season_year
    siguiente, asi que un desfase de uno no es una discrepancia.
    """
    return sorted(year for year in pdf_years if result_years & {year - 1, year, year + 1})


def index_db_clubs(
    db_clubs: Sequence[dict], aliases: Dict[str, str]
) -> Tuple[Dict[str, List[dict]], Dict[str, List[dict]]]:
    """Indexa clubes por nombre normalizado y por nombre resuelto vía alias."""
    by_name: Dict[str, List[dict]] = defaultdict(list)
    by_alias: Dict[str, List[dict]] = defaultdict(list)
    for club in db_clubs:
        key = normalize_match_text(club["name"])
        if key:
            by_name[key].append(club)
        canonical = resolve_club_alias(club["name"], aliases) or club["name"]
        alias_key = normalize_match_text(canonical)
        if alias_key:
            by_alias[alias_key].append(club)
    return by_name, by_alias


CANDIDATE_MIN_SCORE = 0.62
CANDIDATE_LIMIT = 3


def candidate_score(pdf_key: str, db_key: str) -> float:
    """Similitud entre dos nombres normalizados, 0.0 a 1.0.

    core.club guarda muchos clubes con el nombre abreviado ("Master Magallanes"
    frente a "Club Deportivo Master Magallanes"), asi que un subconjunto de
    tokens pesa tanto como el parecido literal.
    """
    if not pdf_key or not db_key:
        return 0.0
    if pdf_key == db_key:
        return 1.0
    ratio = difflib.SequenceMatcher(None, pdf_key, db_key).ratio()
    pdf_tokens = set(pdf_key.split())
    db_tokens = set(db_key.split())
    if not pdf_tokens or not db_tokens:
        return ratio
    shared = pdf_tokens & db_tokens
    containment = len(shared) / min(len(pdf_tokens), len(db_tokens))
    return max(ratio, containment * 0.9)


def find_candidates(
    pdf_key: str,
    db_clubs: Sequence[dict],
    pdf_years: set,
    result_years: Dict[int, set],
) -> List[Tuple[float, dict]]:
    """Candidatos ordenados por cobertura de años y luego por parecido.

    El nombre solo entra a la lista; lo que ordena es haber competido los mismos
    años, que es lo que de verdad separa un homonimo de un club renombrado.
    """
    scored = [
        (score, club)
        for club in db_clubs
        if (score := candidate_score(pdf_key, normalize_match_text(club["name"]) or ""))
        >= CANDIDATE_MIN_SCORE
    ]
    scored.sort(
        key=lambda item: (
            -len(covered_years(pdf_years, result_years.get(item[1]["id"], set()))),
            -item[0],
            item[1]["name"],
        )
    )
    return scored[:CANDIDATE_LIMIT]


def write_rows_csv(path: Path, rows: Sequence[PdfRow], aliases: Dict[str, str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.writer(handle, delimiter=";")
        writer.writerow(
            [
                "year",
                "pdf_line_no",
                "club_name_pdf",
                "club_name_canonical",
                "match_key",
                "short_name_pdf",
                "alias_canonical_from_short_name",
                "region_code_pdf",
                "region_name",
                "total_inscritos",
                "raw_line",
            ]
        )
        for row in rows:
            canonical = resolve_club_alias(row.raw_name, aliases) or row.raw_name
            writer.writerow(
                [
                    row.year,
                    row.line_no,
                    row.raw_name,
                    canonical,
                    normalize_match_text(canonical) or "",
                    row.short_name or "",
                    alias_canonical(row.short_name, aliases) or "",
                    row.region_code or "",
                    REGION_BY_CODE.get(row.region_code or "", ""),
                    "" if row.total_inscritos is None else row.total_inscritos,
                    row.raw_line,
                ]
            )


@dataclass
class Resolution:
    """Resultado de cruzar un club del PDF contra core.club y club_alias.csv."""

    entry: ClubEvidence
    short_name: Optional[str]
    region_code: Optional[str]
    region_name: str
    from_name: Optional[str]
    from_short: Optional[str]
    agreement: str
    match_type: str
    matches: List[dict]
    db_club: Optional[dict]
    shared_short_name: List[str]
    candidates: List[Tuple[float, dict]]
    action: str
    db_result_years: List[int] = field(default_factory=list)
    years_covered: List[int] = field(default_factory=list)


def resolve_entry(
    key: str,
    entry: ClubEvidence,
    by_name: Dict[str, List[dict]],
    by_alias: Dict[str, List[dict]],
    short_name_owners: Dict[str, List[str]],
    db_clubs: Sequence[dict],
    aliases: Dict[str, str],
    result_years: Dict[int, set],
) -> Resolution:
    short_name = latest_value(entry.short_names)
    region_code = latest_value(entry.region_codes)
    region_name = REGION_BY_CODE.get(region_code or "", "")

    # club_alias.csv declara tanto variantes de nombre como los codigos HY-TEK
    # de cinco letras, asi que el codigo del PDF es una segunda llave de cruce
    # independiente del nombre.
    from_name = alias_canonical(entry.canonical_name, aliases)
    from_short = alias_canonical(short_name, aliases)
    if from_name and from_short:
        agreement = (
            "agree"
            if normalize_match_text(from_name) == normalize_match_text(from_short)
            else "differ"
        )
    elif from_name:
        agreement = "only_name"
    elif from_short:
        agreement = "only_short_name"
    else:
        agreement = "none"

    matches = by_name.get(key) or []
    match_type = "name"
    if not matches:
        matches = by_alias.get(key) or []
        match_type = "alias_name" if matches else ""
    for source, canonical in (("alias_name", from_name), ("alias_short_name", from_short)):
        if matches:
            break
        canonical_key = normalize_match_text(canonical) if canonical else None
        if canonical_key:
            matches = by_name.get(canonical_key) or by_alias.get(canonical_key) or []
            match_type = source if matches else ""
    if not matches:
        match_type = "none"
    if len(matches) > 1:
        match_type += "_ambiguous"

    db_club = matches[0] if len(matches) == 1 else None
    shared = [other for other in short_name_owners.get(short_name or "", []) if other != key]
    pdf_years = entry_years(entry)
    candidates = (
        find_candidates(key, db_clubs, pdf_years, result_years) if db_club is None else []
    )

    if db_club is None:
        if matches:
            action = "review_ambiguous_db_match"
        elif candidates:
            action = "review_fuzzy_candidate"
        else:
            action = "review_no_db_match"
    elif agreement == "differ":
        action = "review_alias_disagreement"
    elif shared:
        action = "review_short_name_collision"
    elif len(entry.region_codes) > 1:
        action = "review_region_conflict"
    elif db_club["short_name"] and db_club["short_name"] != short_name:
        action = "review_short_name_differs"
    elif db_club["region"] and region_name and db_club["region"] != region_name:
        action = "review_region_differs"
    elif not short_name and not region_name:
        action = "no_data_in_pdf"
    else:
        action = "apply"

    return Resolution(
        entry=entry,
        short_name=short_name,
        region_code=region_code,
        region_name=region_name,
        from_name=from_name,
        from_short=from_short,
        agreement=agreement,
        match_type=match_type,
        matches=matches,
        db_club=db_club,
        shared_short_name=shared,
        candidates=candidates,
        action=action,
        db_result_years=sorted(result_years.get(db_club["id"], set())) if db_club else [],
        years_covered=(
            covered_years(pdf_years, result_years.get(db_club["id"], set())) if db_club else []
        ),
    )


def write_updates_csv(
    path: Path,
    resolutions: Dict[str, Resolution],
    result_years: Dict[int, set],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.writer(handle, delimiter=";")
        writer.writerow(
            [
                "match_key",
                "club_name_canonical",
                "club_names_pdf",
                "years",
                "totals_by_year",
                "short_name_proposed",
                "short_name_observations",
                "short_name_conflict",
                "short_name_shared_with",
                "region_code_proposed",
                "region_name_proposed",
                "region_observations",
                "region_conflict",
                "alias_canonical_from_name",
                "alias_canonical_from_short_name",
                "alias_agreement",
                "db_match_type",
                "db_club_id",
                "db_club_name",
                "db_short_name",
                "db_region",
                "db_region_matches",
                "db_result_years",
                "years_covered_by_results",
                "db_candidates",
                "proposed_action",
            ]
        )
        for key in sorted(
            resolutions, key=lambda k: resolutions[k].entry.canonical_name.lower()
        ):
            resolution = resolutions[key]
            entry = resolution.entry
            years = sorted({year for spans in entry.names.values() for year in spans})

            short_name = resolution.short_name
            region_code = resolution.region_code
            region_name = resolution.region_name
            from_name = resolution.from_name
            from_short = resolution.from_short
            agreement = resolution.agreement
            match_type = resolution.match_type
            matches = resolution.matches
            db_club = resolution.db_club
            shared = resolution.shared_short_name
            candidates = resolution.candidates
            action = resolution.action

            writer.writerow(
                [
                    key,
                    entry.canonical_name,
                    " | ".join(sorted(entry.names)),
                    ",".join(str(year) for year in years),
                    ",".join(f"{year}:{entry.totals[year]}" for year in sorted(entry.totals)),
                    short_name or "",
                    " | ".join(
                        f"{value}={','.join(str(y) for y in sorted(set(spans)))}"
                        for value, spans in sorted(entry.short_names.items())
                    ),
                    "yes" if len(entry.short_names) > 1 else "no",
                    " | ".join(sorted(shared)),
                    region_code or "",
                    region_name,
                    " | ".join(
                        f"{value}={','.join(str(y) for y in sorted(set(spans)))}"
                        for value, spans in sorted(entry.region_codes.items())
                    ),
                    "yes" if len(entry.region_codes) > 1 else "no",
                    from_name or "",
                    from_short or "",
                    agreement,
                    match_type,
                    db_club["id"] if db_club else "",
                    db_club["name"] if db_club else " | ".join(m["name"] for m in matches),
                    (db_club["short_name"] or "") if db_club else "",
                    (db_club["region"] or "") if db_club else "",
                    ""
                    if db_club is None or not db_club["region"] or not region_name
                    else ("yes" if db_club["region"] == region_name else "no"),
                    ",".join(str(year) for year in resolution.db_result_years),
                    ",".join(str(year) for year in resolution.years_covered),
                    " | ".join(
                        f"{club['id']}:{club['name']}={score:.2f}"
                        f"[{','.join(str(y) for y in sorted(result_years.get(club['id'], set())))}]"
                        for score, club in candidates
                    ),
                    action,
                ]
            )


def load_manual_links(path: Path) -> Dict[str, Tuple[str, int]]:
    """Lee 'id BD' resuelto a mano, indexado por nombre normalizado del PDF."""
    links: Dict[str, Tuple[str, int]] = {}
    with path.open(encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle, delimiter=";"):
            club_name = (row.get("club_name_pdf") or "").strip()
            raw_id = (row.get("id BD") or "").strip()
            key = normalize_match_text(club_name)
            if key and raw_id:
                links[key] = (club_name, int(raw_id))
    return links


def write_manual_links_csv(path: Path, links: Dict[str, Tuple[str, int]]) -> None:
    """Persiste los links acumulados en un archivo propio.

    El CSV de revision solo lista lo que sigue pendiente, asi que se encoge a
    medida que se resuelve y no sirve para conservar la curacion. Este archivo
    es la entrada durable del circuito.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.writer(handle, delimiter=";")
        writer.writerow(["club_name_pdf", "id BD"])
        for _, (club_name, club_id) in sorted(links.items(), key=lambda item: item[1][0].lower()):
            writer.writerow([club_name, club_id])


def apply_manual_links(
    resolutions: Dict[str, Resolution],
    links: Dict[str, Tuple[str, int]],
    db_clubs: Sequence[dict],
    result_years: Dict[int, set],
) -> List[str]:
    """Fuerza el match manual y devuelve los ids que no existen en core.club."""
    by_id = {club["id"]: club for club in db_clubs}
    missing: List[str] = []
    for key, (_, club_id) in links.items():
        resolution = resolutions.get(key)
        if resolution is None:
            continue
        club = by_id.get(club_id)
        if club is None:
            missing.append(f"{resolution.entry.canonical_name} -> id {club_id}")
            continue
        resolution.db_club = club
        resolution.matches = [club]
        resolution.match_type = "manual"
        resolution.candidates = []
        resolution.action = "apply"
        # La cobertura de años se calculo contra el club anterior, que aca era
        # ninguno; hay que rehacerla o el match manual queda sin respaldo.
        club_years = result_years.get(club["id"], set())
        resolution.db_result_years = sorted(club_years)
        resolution.years_covered = covered_years(entry_years(resolution.entry), club_years)
    return missing


# Fuerza de la evidencia con que se llego a un club de core.club, de mayor a
# menor. Un id puesto a mano resuelve una ambiguedad, pero no supera a un nombre
# que coincide exactamente.
MATCH_STRENGTH = {"name": 4, "alias_name": 3, "alias_short_name": 2, "manual": 1}


def entry_years(entry: ClubEvidence) -> set:
    return {year for spans in entry.names.values() for year in spans}


def flag_same_year_conflicts(resolutions: Dict[str, Resolution]) -> List[str]:
    """Marca dos clubes distintos del PDF apuntados al mismo club de core.club.

    Si ambos aparecen como filas separadas en la misma temporada no pueden ser
    la misma entidad, por mucho que el nombre o el codigo se parezcan: la tabla
    de la FCHMN los cuenta por separado ese año. Lo que falta entonces es un
    club en core.club, no un alias.
    """
    by_db_id: Dict[int, List[str]] = defaultdict(list)
    for key, resolution in resolutions.items():
        if resolution.db_club is not None:
            by_db_id[resolution.db_club["id"]].append(key)

    conflicts: List[str] = []
    for club_id, keys in by_db_id.items():
        if len(keys) < 2:
            continue
        for left in range(len(keys)):
            for right in range(left + 1, len(keys)):
                a, b = resolutions[keys[left]], resolutions[keys[right]]
                shared_years = entry_years(a.entry) & entry_years(b.entry)
                if not shared_years:
                    continue
                years_text = ",".join(str(year) for year in sorted(shared_years))
                # Uno de los dos sobra, no los dos. Se conserva el de evidencia
                # mas fuerte: el nombre exacto manda sobre un id puesto a mano.
                rank_a, rank_b = MATCH_STRENGTH.get(a.match_type, 0), MATCH_STRENGTH.get(
                    b.match_type, 0
                )
                weaker = [a] if rank_a < rank_b else [b] if rank_b < rank_a else [a, b]
                for resolution in weaker:
                    resolution.action = "review_same_year_conflict"
                kept = " (se conserva el otro)" if len(weaker) == 1 else ""
                conflicts.append(
                    f"id {club_id}: {a.entry.canonical_name} y {b.entry.canonical_name} "
                    f"coexisten en {years_text}{kept}"
                )
    return conflicts


def build_alias_additions(
    resolutions: Dict[str, Resolution],
    aliases: Dict[str, str],
    db_clubs: Sequence[dict],
) -> Tuple[List[Tuple[str, str, str]], List[Tuple[str, str, str, str]]]:
    """Propone filas nuevas para club_alias.csv y separa las que no son seguras.

    Solo se acepta una entrada cuando el club tiene un unico match en core.club
    y el alias todavia no existe. Los codigos que aparecen bajo dos nombres que
    resuelven a canonicos distintos quedan fuera: para decidirlos hay que saber
    si son el mismo club, y eso no lo dice el PDF.
    """
    additions: List[Tuple[str, str, str]] = []
    review: List[Tuple[str, str, str, str]] = []
    proposed: Dict[str, str] = {}

    # Nombres vivos de core.club. Un alias cuyo alias_name es el nombre de un
    # club existente convierte a ese club en alias sin que nadie haya decidido
    # fusionarlo, asi que se manda a revision en vez de emitirlo.
    live_club_names: Dict[str, dict] = {}
    for club in db_clubs:
        club_key = normalize_match_text(club["name"])
        if club_key:
            live_club_names.setdefault(club_key, club)

    for key in sorted(resolutions, key=lambda k: resolutions[k].entry.canonical_name.lower()):
        resolution = resolutions[key]
        entry = resolution.entry
        # Separados por espacio: una coma obligaria a citar el campo notes.
        years = " ".join(
            str(year) for year in sorted({y for spans in entry.names.values() for y in spans})
        )

        if resolution.db_club is None or resolution.action == "review_same_year_conflict":
            # Va a revision aunque no tenga codigo: los clubes de 2022 y 2023 no
            # lo traen y aun asi necesitan que alguien decida su club en core.
            review.append(
                (resolution.short_name or "", entry.canonical_name, resolution.action, years)
            )
            continue
        canonical_name = resolution.db_club["name"]

        # Un codigo compartido solo estorba si los dos lados terminan en clubes
        # distintos. Si ambos resuelven al mismo, el codigo es del mismo club
        # escrito de dos formas y la entrada es valida.
        divergent = sorted(
            other
            for other in resolution.shared_short_name
            if (resolutions[other].db_club or {}).get("id") != resolution.db_club["id"]
        )
        if divergent:
            review.append(
                (
                    resolution.short_name or "",
                    entry.canonical_name,
                    "codigo compartido con: " + ", ".join(divergent),
                    years,
                )
            )
            continue

        # El codigo del PDF y cada variante de nombre son candidatos a alias.
        candidates = [(resolution.short_name, "codigo de equipo")]
        candidates += [(name, "variante de nombre") for name in sorted(entry.names)]

        for value, kind in candidates:
            alias_key = normalize_match_text(value) if value else None
            if not alias_key or alias_key in aliases:
                continue
            if alias_key == normalize_match_text(canonical_name):
                continue
            collision = live_club_names.get(alias_key)
            if collision is not None and collision["id"] != resolution.db_club["id"]:
                review.append(
                    (
                        value or "",
                        entry.canonical_name,
                        f"alias_name es el nombre del club {collision['id']} en core.club; "
                        f"convertirlo en alias de {canonical_name} es una fusion sin decidir",
                        years,
                    )
                )
                continue
            if alias_key in proposed:
                if proposed[alias_key] != canonical_name:
                    review.append(
                        (value or "", entry.canonical_name, "alias ya propuesto para otro club", years)
                    )
                continue
            proposed[alias_key] = canonical_name
            additions.append(
                (
                    value,
                    canonical_name,
                    f"{kind} publicado en PDF de participacion master FCHMN {years}",
                )
            )

    return additions, review


def write_alias_additions_csv(path: Path, additions: Sequence[Tuple[str, str, str]]) -> None:
    """Escribe con el mismo esquema y delimitador que club_alias.csv."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["alias_name", "canonical_name", "notes"])
        writer.writerows(additions)


def write_alias_review_csv(
    path: Path,
    review: Sequence[Tuple[str, str, str, str]],
    known_ids: Dict[str, Tuple[str, int]],
    resolutions: Dict[str, Resolution],
    result_years: Dict[int, set],
) -> None:
    """Reescribe la revision conservando los 'id BD' resueltos a mano.

    Este archivo es a la vez salida y entrada del circuito, asi que regenerarlo
    no puede perder la curacion manual.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.writer(handle, delimiter=";")
        writer.writerow(
            [
                "short_name_pdf",
                "club_name_pdf",
                "reason",
                "years",
                "id BD",
                "candidatos_con_años_de_resultados",
            ]
        )
        for short_name, club_name, reason, years in review:
            key = normalize_match_text(club_name) or ""
            known = known_ids.get(key)
            resolution = resolutions.get(key)
            pdf_years = entry_years(resolution.entry) if resolution else set()
            candidates = " | ".join(
                "{}:{} años={} cubre={}".format(
                    club["id"],
                    club["name"],
                    ",".join(str(y) for y in sorted(result_years.get(club["id"], set()))) or "-",
                    ",".join(
                        str(y) for y in covered_years(pdf_years, result_years.get(club["id"], set()))
                    )
                    or "-",
                )
                for _, club in (resolution.candidates if resolution else [])
            )
            writer.writerow(
                [short_name, club_name, reason, years, known[1] if known else "", candidates]
            )


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    pdf_paths = [Path(p) for p in args.pdf] if args.pdf else default_pdfs()
    if not pdf_paths:
        print("No se encontraron PDF de participacion.", file=sys.stderr)
        return 1

    aliases = load_club_aliases(args.club_alias_csv)

    rows: List[PdfRow] = []
    for path in pdf_paths:
        parsed = parse_pdf(path)
        print(f"{path.name}: {len(parsed)} filas de club")
        rows.extend(parsed)

    evidence = build_evidence(rows, aliases)

    short_name_owners: Dict[str, List[str]] = defaultdict(list)
    for key, entry in evidence.items():
        for short_name in entry.short_names:
            short_name_owners[short_name].append(key)

    db_clubs = fetch_db_clubs(args.database_url) if args.database_url else []
    result_years = fetch_club_result_years(args.database_url) if args.database_url else {}
    by_name, by_alias = index_db_clubs(db_clubs, aliases)

    resolutions = {
        key: resolve_entry(
            key, entry, by_name, by_alias, short_name_owners, db_clubs, aliases, result_years
        )
        for key, entry in evidence.items()
    }
    prefix = Path(args.out_prefix)
    alias_review_path = prefix.with_name(prefix.name + "_alias_review.csv")
    manual_links_path = prefix.with_name(prefix.name + "_manual_links.csv")

    # Los links se acumulan: los ya persistidos mas los que traiga la revision
    # recien curada. Asi regenerar nunca pierde una decision manual.
    known_ids = load_manual_links(manual_links_path) if manual_links_path.exists() else {}
    if args.alias_review_resolved:
        known_ids.update(load_manual_links(Path(args.alias_review_resolved)))
    if known_ids:
        missing = apply_manual_links(resolutions, known_ids, db_clubs, result_years)
        print(f"matches manuales aplicados: {len(known_ids) - len(missing)}")
        for item in missing:
            print(f"  id inexistente en core.club: {item}", file=sys.stderr)

    for conflict in flag_same_year_conflicts(resolutions):
        print(f"  conflicto mismo año: {conflict}", file=sys.stderr)

    if result_years:
        unbacked = [
            r
            for r in resolutions.values()
            if r.db_club is not None and not r.years_covered
        ]
        print(f"matches sin respaldo en resultados: {len(unbacked)}")
        for r in unbacked:
            print(
                f"  {r.entry.canonical_name} -> id {r.db_club['id']} "
                f"({r.db_club['name']}): PDF={sorted(entry_years(r.entry))} "
                f"resultados={r.db_result_years}",
                file=sys.stderr,
            )

    additions, alias_review = build_alias_additions(resolutions, aliases, db_clubs)

    rows_path = prefix.with_name(prefix.name + "_rows.csv")
    updates_path = prefix.with_name(prefix.name + "_updates.csv")
    additions_path = prefix.with_name(prefix.name + "_alias_additions.csv")
    write_rows_csv(rows_path, rows, aliases)
    write_updates_csv(updates_path, resolutions, result_years)
    write_alias_additions_csv(additions_path, additions)
    write_alias_review_csv(
        alias_review_path, alias_review, known_ids, resolutions, result_years
    )
    write_manual_links_csv(manual_links_path, known_ids)

    print(f"clubes consolidados: {len(evidence)}")
    print(f"clubes en core.club: {len(db_clubs)}")
    print(f"entradas nuevas para club_alias.csv: {len(additions)}")
    print(f"alias descartados por revisar: {len(alias_review)}")
    for path in (rows_path, updates_path, additions_path, alias_review_path):
        print(f"escrito: {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
