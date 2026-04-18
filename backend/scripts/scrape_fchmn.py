#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import sys
import unicodedata
from dataclasses import asdict, dataclass
from html.parser import HTMLParser
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urljoin, urlparse
from urllib.request import urlopen


BACKEND_DIR = Path(__file__).resolve().parents[1]


@dataclass
class ManifestEntry:
    source_url: str
    pdf: str
    out_dir: str
    competition_id: int | None
    default_source_id: int


class PdfLinkParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.hrefs: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() != "a":
            return
        for name, value in attrs:
            if name.lower() == "href" and value:
                self.hrefs.append(value)


def read_html(args: argparse.Namespace) -> tuple[str, str]:
    if args.html_file:
        html_path = Path(args.html_file)
        if not html_path.exists() or not html_path.is_file():
            raise SystemExit(f"[ERROR] No existe el HTML: {html_path}")
        return html_path.read_text(encoding="utf-8"), args.base_url

    with urlopen(args.url, timeout=args.timeout_seconds) as response:
        encoding = response.headers.get_content_charset() or "utf-8"
        body = response.read().decode(encoding, errors="replace")
    return body, args.url


def discover_pdf_urls(html: str, base_url: str) -> list[str]:
    parser = PdfLinkParser()
    parser.feed(html)

    urls: list[str] = []
    seen: set[str] = set()
    for href in parser.hrefs:
        absolute_url = urljoin(base_url, href)
        path = urlparse(absolute_url).path.lower()
        if not path.endswith(".pdf") or absolute_url in seen:
            continue
        seen.add(absolute_url)
        urls.append(absolute_url)
    return urls


def slugify_pdf_url(url: str) -> str:
    parsed = urlparse(url)
    name = Path(unquote(parsed.path)).stem or "documento"
    normalized = unicodedata.normalize("NFKD", name)
    ascii_name = normalized.encode("ascii", "ignore").decode("ascii")
    slug = re.sub(r"[^a-zA-Z0-9]+", "-", ascii_name).strip("-").lower()
    return slug or "documento"


def build_manifest_entries(args: argparse.Namespace, urls: list[str]) -> list[ManifestEntry]:
    entries: list[ManifestEntry] = []
    slug_counts: dict[str, int] = {}
    pdf_dir = Path(args.pdf_dir)
    out_dir_root = Path(args.out_dir_root)

    for url in urls[: args.limit]:
        slug = slugify_pdf_url(url)
        slug_counts[slug] = slug_counts.get(slug, 0) + 1
        if slug_counts[slug] > 1:
            slug = f"{slug}-{slug_counts[slug]}"
        entries.append(
            ManifestEntry(
                source_url=url,
                pdf=str(pdf_dir / f"{slug}.pdf"),
                out_dir=str(out_dir_root / slug),
                competition_id=args.competition_id,
                default_source_id=args.default_source_id,
            )
        )
    return entries


def write_manifest(entries: list[ManifestEntry], manifest_path: Path) -> None:
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    lines = [json.dumps(asdict(entry), ensure_ascii=False) for entry in entries]
    manifest_path.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Descubre enlaces PDF de FCHMN y emite un manifest JSONL local sin descargar ni cargar a core."
    )
    source_group = parser.add_mutually_exclusive_group(required=True)
    source_group.add_argument("--html-file", help="HTML local ya descargado para descubrir enlaces PDF.")
    source_group.add_argument("--url", help="URL de una pagina FCHMN desde donde descubrir enlaces PDF.")
    parser.add_argument("--base-url", default="https://fchmn.cl/", help="Base URL para resolver enlaces relativos de --html-file.")
    parser.add_argument("--manifest", required=True, help="Ruta del manifest JSONL a escribir.")
    parser.add_argument("--pdf-dir", default=str(BACKEND_DIR / "data" / "raw" / "results_pdf" / "fchmn"))
    parser.add_argument("--out-dir-root", default=str(BACKEND_DIR / "data" / "raw" / "results_csv" / "fchmn"))
    parser.add_argument("--competition-id", type=int)
    parser.add_argument("--default-source-id", type=int, default=1)
    parser.add_argument("--limit", type=int, default=sys.maxsize, help="Maximo de PDFs a incluir.")
    parser.add_argument("--timeout-seconds", type=int, default=30, help="Timeout para --url.")
    parser.add_argument("--json", action="store_true", help="Imprime resumen como JSON.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    html, base_url = read_html(args)
    urls = discover_pdf_urls(html, base_url)
    entries = build_manifest_entries(args, urls)
    write_manifest(entries, Path(args.manifest))

    payload: dict[str, Any] = {
        "state": "discovered",
        "manifest_path": args.manifest,
        "documents": len(entries),
    }
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(f"Estado scraper: {payload['state']}")
        print(f"Manifest: {payload['manifest_path']}")
        print(f"Documentos: {payload['documents']}")


if __name__ == "__main__":
    main()
