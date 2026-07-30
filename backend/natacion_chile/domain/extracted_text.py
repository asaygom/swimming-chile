"""Conservative cleanup for text extracted from supported swimming PDFs."""

from __future__ import annotations

import re
import unicodedata


def normalize_extracted_text(value: object | None) -> str | None:
    """Apply source-agnostic Unicode and whitespace normalization."""
    if value is None:
        return None
    normalized = unicodedata.normalize("NFC", str(value))
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized or None


def clean_extracted_text(value: object | None) -> str | None:
    """Repair confirmed PDF extraction artifacts, then normalize whitespace."""
    normalized = normalize_extracted_text(value)
    if normalized is None:
        return None

    # These substitutions repair confirmed adjacent-enye/CID artifacts emitted
    # by supported PDF text layers; they are not athlete identity corrections.
    replacements = {
        "NÑ": "Ñ",
        "nñ": "ñ",
        "Penñ": "Peñ",
        "Munñ": "Muñ",
        "Espanñ": "Españ",
        "Canñ": "Cañ",
        "Vinñ": "Viñ",
        "Natacioán": "Natación",
        "Natacioón": "Natación",
        "N(cid:450) i": "Ñi",
        "N(cid:450) u": "Ñu",
        "n(cid:450) i": "ñi",
        "n(cid:450) u": "ñu",
        "(cid:976)": "f",
        "Ñ u": "Ñu",
        "ñ u": "ñu",
        "Ñ a": "Ña",
        "ñ a": "ña",
        "Ñ o": "Ño",
        "ñ o": "ño",
        "Ñ e": "Ñe",
        "ñ e": "ñe",
        "Ñ i": "Ñi",
        "ñ i": "ñi",
        "Joseí": "José",
    }
    for bad, good in replacements.items():
        normalized = normalized.replace(bad, good)

    consonants = r"(?=[bcdfghjklmnñpqrstvwxyzBCDFGHJKLMNÑPQRSTVWXYZ])"
    normalized = re.sub(rf"oí{consonants}", "ó", normalized)
    normalized = re.sub(rf"aí{consonants}", "á", normalized)
    normalized = re.sub(rf"eí{consonants}", "é", normalized)
    normalized = re.sub(rf"o\s+í{consonants}", "ó", normalized)
    normalized = re.sub(rf"a\s+í{consonants}", "á", normalized)
    normalized = re.sub(rf"e\s+í{consonants}", "é", normalized)

    for bad, good in (
        ("íi", "í"),
        ("íí", "í"),
        ("ÍI", "Í"),
        ("áa", "á"),
        ("ée", "é"),
        ("óo", "ó"),
        ("úu", "ú"),
        ("ÁA", "Á"),
        ("ÉE", "É"),
        ("ÓO", "Ó"),
        ("ÚU", "Ú"),
    ):
        normalized = normalized.replace(bad, good)

    normalized = re.sub(r"([A-Za-zÑñ])e\s+í\b", r"\1é", normalized)
    normalized = re.sub(r"([A-Za-zÑñ])o\s+í\b", r"\1ó", normalized)
    return normalize_extracted_text(normalized)
