"""Curaduria de nombres de personas extraidos de PDFs de natacion.

Vive en el dominio y no en un parser porque resultados y programas de
competencia comparten el mismo origen HY-TEK y los mismos artefactos: sin esto,
cada parser corregiria los nombres por su cuenta y divergirian.

La correccion es curada token por token, no una regla general. El acento agudo
llega como un glifo suelto y su ubicacion no es decidible: "Matias" y "Martinez"
comparten el patron con respuestas opuestas.
"""

from __future__ import annotations

from difflib import SequenceMatcher
import re
import unicodedata

from .extracted_text import clean_extracted_text


ACCENTED = "ÁÉÍÓÚáéíóúÑñÜü"


CANONICAL_ATHLETE_NAME_TOKENS = {
    "Abraham": "Abraham",
    "Alexandra": "Alexandra",
    "Anais": "Anaís",
    "Andrea": "Andrea",
    "Andres": "Andrés",
    "Angelica": "Angélica",
    "Ariadna": "Ariadna",
    "Bascunan": "Bascuñán",
    "Becerra": "Becerra",
    "Belen": "Belén",
    "Berroeta": "Berroeta",
    "Bocaz": "Bocaz",
    "Briceño": "Briceño",
    "Cabaret": "Cabaret",
    "Caceres": "Cáceres",
    "Cañete": "Cañete",
    "Cañas": "Cañas",
    "Cardenas": "Cárdenas",
    "Carolina": "Carolina",
    "Casassus": "Casassus",
    "Castro": "Castro",
    "Catalina": "Catalina",
    "Cerda": "Cerda",
    "Claudia": "Claudia",
    "Contreras": "Contreras",
    "Cordova": "Córdova",
    "Corvalan": "Corvalán",
    "Cortes": "Cortés",
    "Cristian": "Cristián",
    "Cristobal": "Cristóbal",
    "Daniel": "Daniel",
    "Diaz": "Díaz",
    "Droguett": "Droguett",
    "Echeverria": "Echeverría",
    "Eduardo": "Eduardo",
    "Elizabeth": "Elizabeth",
    "Erika": "Erika",
    "Espinoza": "Espinoza",
    "Fabricio": "Fabricio",
    "Felipe": "Felipe",
    "Fernanda": "Fernanda",
    "Fernandez": "Fernández",
    "Fuenzalida": "Fuenzalida",
    "Gabriela": "Gabriela",
    "Galvez": "Gálvez",
    "Garate": "Gárate",
    "Garcia": "García",
    "Gonzalez": "González",
    "Guzman": "Guzmán",
    "Gutierrez": "Gutiérrez",
    "Hardy": "Hardy",
    "Hector": "Héctor",
    "Henriquez": "Henríquez",
    "Hermosilla": "Hermosilla",
    "Hernan": "Hernán",
    "Jacqueline": "Jacqueline",
    "Jaime": "Jaime",
    "Jeldes": "Jeldes",
    "Jimenez": "Jiménez",
    "Jose": "José",
    "Job": "Job",
    "Karina": "Karina",
    "Labra": "Labra",
    "Lopez": "López",
    "Lourdes": "Lourdes",
    "Lukas": "Lukas",
    "Magaly": "Magaly",
    "Manuel": "Manuel",
    "Marcelo": "Marcelo",
    "Maria": "María",
    "Mario": "Mario",
    "Martin": "Martín",
    "Martinez": "Martínez",
    "Matias": "Matías",
    "Maurice": "Maurice",
    "Mauricio": "Mauricio",
    "Mendez": "Méndez",
    "Menadier": "Menadier",
    "Monica": "Mónica",
    "Montecinos": "Montecinos",
    "Montoya": "Montoya",
    "Mueller": "Müller",
    "Muller": "Müller",
    "Muñoz": "Muñoz",
    "Murua": "Murúa",
    "Navarro": "Navarro",
    "Nicolas": "Nicolás",
    "Nunez": "Núñez",
    "Olivares": "Olivares",
    "Ordenes": "Órdenes",
    "Orieta": "Orieta",
    "Pamela": "Pamela",
    "Panotto": "Panotto",
    "Paola": "Paola",
    "Patricio": "Patricio",
    "Paz": "Paz",
    "Perez": "Pérez",
    "Pia": "Pía",
    "Pilar": "Pilar",
    "Provoste": "Provoste",
    "Quilapan": "Quilapan",
    "Raul": "Raúl",
    "Ramirez": "Ramírez",
    "Rodigo": "Rodrigo",
    "Rodriguez": "Rodríguez",
    "Rondon": "Rondón",
    "Samuel": "Samuel",
    "Salfate": "Salfate",
    "Sanchez": "Sánchez",
    "Sanz": "Sanz",
    "Saez": "Sáez",
    "Schwarzemberg": "Schwarzemberg",
    "Sebastian": "Sebastián",
    "Sepulveda": "Sepúlveda",
    "Silvia": "Silvia",
    "Sofia": "Sofía",
    "Sonia": "Sonia",
    "Tania": "Tania",
    "Teran": "Terán",
    "Tomas": "Tomás",
    "Torrealba": "Torrealba",
    "Valdes": "Valdés",
    "Valentina": "Valentina",
    "Vasquez": "Vásquez",
    "Velasquez": "Velásquez",
    "Veronica": "Verónica",
    "Victor": "Víctor",
    "Vicente": "Vicente",
    "Vigouroux": "Vigouroux",
    "Villegas": "Villegas",
    "Yanez": "Yáñez",
}
CANONICAL_ATHLETE_NAME_TOKEN_KEYS = {
    re.sub(r"[^a-z]", "", unicodedata.normalize("NFD", key).encode("ascii", "ignore").decode("ascii").lower()): value
    for key, value in CANONICAL_ATHLETE_NAME_TOKENS.items()
}
NAME_CONNECTOR_TOKENS = {"da", "de", "del", "di", "do", "dos", "la", "las", "lo", "los", "van", "von", "y"}


def _strip_accents(text: str) -> str:
    normalized = unicodedata.normalize("NFD", text)
    return "".join(char for char in normalized if unicodedata.category(char) != "Mn")


def _name_token_key(text: str) -> str:
    return re.sub(r"[^a-z]", "", _strip_accents(text).lower())


def _name_token_consonant_skeleton(text: str) -> str:
    return re.sub(r"[aeiou]", "", _name_token_key(text))


def _generate_athlete_token_variants(token: str) -> list[str]:
    variants = {token}
    compact = token.replace(" ", "")
    variants.add(compact)

    vowel_chars = "AEIOUÁÉÍÓÚaeiouáéíóú"
    for current in list(variants):
        for idx in range(len(current) - 1):
            left = current[idx]
            right = current[idx + 1]
            if left in vowel_chars and right in vowel_chars and (_strip_accents(left).lower() != _strip_accents(right).lower() or left != right):
                variants.add(current[:idx] + current[idx + 1 :])
                variants.add(current[: idx + 1] + current[idx + 2 :])

    return [variant for variant in variants if variant]


def _collapse_fragmented_name_side(side: str) -> str:
    tokens = side.split()
    if len(tokens) <= 1:
        return side

    merged: list[str] = []
    idx = 0
    while idx < len(tokens):
        current = tokens[idx]
        while idx + 1 < len(tokens):
            nxt = tokens[idx + 1]
            nxt_key = _name_token_key(nxt)
            current_key = _name_token_key(current)
            if not nxt_key:
                break
            if nxt[:1].islower() and nxt_key not in NAME_CONNECTOR_TOKENS:
                current += nxt
                idx += 1
                continue
            if len(current_key) == 1:
                current += nxt
                idx += 1
                continue
            if (
                len(nxt_key) <= 2
                and nxt_key not in NAME_CONNECTOR_TOKENS
                and len(current_key) >= 4
                and nxt[:1].islower()
            ):
                current += nxt
                idx += 1
                continue
            break
        merged.append(current)
        idx += 1
    return " ".join(merged)


def _looks_suspicious_athlete_token(token: str) -> bool:
    return (
        bool(re.search(r"[ÁÉÍÓÚáéíóú].*[ÁÉÍÓÚáéíóú]", token))
        or bool(re.search(r"[aeiouáéíóú][ÁÉÍÓÚáéíóú]|[ÁÉÍÓÚáéíóú][aeiouáéíóú]", token))
        or "ñ" in token.lower()
        or "ññ" in token.lower()
        or "eñ" in token.lower()
        or len(token) != len(_strip_accents(token))
    )


def _preserve_token_case(original: str, canonical: str) -> str:
    if original.isupper():
        return canonical.upper()
    if original.islower():
        return canonical.lower()
    return canonical


def _repair_athlete_name_token(match: re.Match[str]) -> str:
    token = match.group(0)
    key = _name_token_key(token)
    if len(key) <= 2:
        return token

    suspicious = _looks_suspicious_athlete_token(token)
    canonical = CANONICAL_ATHLETE_NAME_TOKEN_KEYS.get(key)
    if canonical and suspicious:
        return _preserve_token_case(token, canonical)

    if not suspicious:
        return token

    best_ratio = 0.0
    best_canonical = None
    for variant in _generate_athlete_token_variants(token):
        variant_key = _name_token_key(variant)
        skeleton = _name_token_consonant_skeleton(variant)
        if not variant_key or not skeleton:
            continue
        exact = CANONICAL_ATHLETE_NAME_TOKEN_KEYS.get(variant_key)
        if exact:
            return _preserve_token_case(token, exact)

        for candidate_key, candidate in CANONICAL_ATHLETE_NAME_TOKEN_KEYS.items():
            if not candidate_key or candidate_key[:1] != variant_key[:1]:
                continue
            if _name_token_consonant_skeleton(candidate) != skeleton:
                continue
            if abs(len(candidate_key) - len(variant_key)) > 2:
                continue
            ratio = SequenceMatcher(None, variant_key, candidate_key).ratio()
            if ratio > best_ratio:
                best_ratio = ratio
                best_canonical = candidate

    if best_canonical and best_ratio >= 0.72:
        return _preserve_token_case(token, best_canonical)
    return token


def clean_athlete_name(value: str | None) -> str | None:
    value = clean_extracted_text(value)
    if value is None:
        return None
    # HY-TEK marks some exhibition/non-scoring athletes with a leading "*";
    # it is not part of the identity and must not reach athlete.csv.
    value = re.sub(r"^\*+\s*", "", value).strip()
    value = value.replace("Mª", "Maria")
    value = value.replace("M?", "Maria")
    value = value.replace("(cid:976)", "f")
    # OCR/layout artifacts observed inside names, not source-authored suffixes
    # like "Rojas, 2".
    value = re.sub(r"\s*\|\s*(?=,)", "", value)
    value = re.sub(r"(?<=[A-Za-zÁÉÍÓÚáéíóúÑñ])\d+(?=,\s*[A-Za-zÁÉÍÓÚáéíóúÑñ])", "", value)

    def collapse_prefixed_vowel_artifact(match: re.Match[str]) -> str:
        first = match.group(1)
        second = match.group(2)
        if _strip_accents(first).lower() == _strip_accents(second).lower():
            return second
        return match.group(0)

    # OCR can duplicate the opening vowel of a word, for example
    # "AÁlvarez". Keep the accented leading vowel.
    value = re.sub(
        r"\b([AEIOUaeiou])([ÁÉÍÓÚáéíóú])(?=[A-Za-zÁÉÍÓÚáéíóúÑñ])",
        collapse_prefixed_vowel_artifact,
        value,
    )

    token_fixes = [
        (r"\bJose(?:[áóú])?\b", "José"),
        (r"\bMaríóa\b", "María"),
        (r"\bGarcíóa\b", "García"),
        (r"\bMari(?:óa|ía)\b", "María"),
        (r"\bMaríáa\b", "Maríá"),
        (r"\bAndre(?:ás|és|ós)\b", "Andrés"),
        (r"\bCristia(?:án|én)\b", "Cristián"),
        (r"\bBele(?:án|én|ún)\b", "Belén"),
        (r"\bHe(?:á|ó)ctor\b", "Héctor"),
        (r"\bCristo(?:é|ó)bal\b", "Cristóbal"),
        (r"\bIvaén\b", "Iván"),
        (r"\bSa(?:á|í|ó)ez\b", "Sáez"),
        (r"\bAlarco(?:án|én)\b", "Alarcón"),
        (r"\bRamí(?:á|ó)rez\b", "Ramírez"),
        (r"\bVictor\b", "Víctor"),
        (r"\bTiller(?:ia|ía|íéa|íá)\b", "Tillería"),
        (r"\bCanto(?:á|é|í|ó)\b", "Canto"),
        (r"\bAÁlvarez\b", "Álvarez"),
        (r"\bA[ÁÓ]vila\b", "Ávila"),
        # Formas con el glifo de acento suelto observadas en programas HY-TEK.
        # Van ancladas y exactas, y no al diccionario canonico, porque ese se
        # consulta de forma difusa y estos apellidos tienen vecinos legitimos:
        # "Mariño" caeria en "Marín" y "Reaño" en "René".
        (r"\bBeltraán\b", "Beltrán"),
        (r"\bBernabeá\b", "Bernabé"),
        (r"\bDomíánguez\b", "Domínguez"),
        (r"\bMaríán\b", "Marín"),
        (r"\bPasaríán\b", "Pasarín"),
        (r"\bReneá\b", "René"),
    ]
    for pattern_text, replacement in token_fixes:
        value = re.sub(pattern_text, replacement, value, flags=re.IGNORECASE)

    value = re.sub(r"\s*ñ\s+ñ\s*", "ñ", value, flags=re.IGNORECASE)
    value = ", ".join(_collapse_fragmented_name_side(side.strip()) for side in value.split(","))
    value = re.sub(
        r"\b([AEIOUaeiou])([ÁÉÍÓÚáéíóú])(?=[A-Za-zÁÉÍÓÚáéíóúÑñ])",
        collapse_prefixed_vowel_artifact,
        value,
    )
    value = re.sub(r"[A-Za-zÁÉÍÓÚáéíóúÑñ]+", _repair_athlete_name_token, value)
    value = re.sub(r"\s+", " ", value).strip()
    return value if value else None
