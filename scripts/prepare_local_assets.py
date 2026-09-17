from __future__ import annotations

import argparse
import os
import shutil
import sys
import urllib.error
import urllib.request
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
ASSETS_DIR = REPO_ROOT / "assets"
ISO_TABLE_PATH = ASSETS_DIR / "iso-639-3.tab"
APP_ICON_PATH = ASSETS_DIR / "dani.png"

ISO_TABLE_URL = "https://iso639-3.sil.org/sites/iso639-3/files/downloads/iso-639-3.tab"
ISO_HEADER = "Id\tPart2b\tPart2t\tPart1\tScope\tLanguage_Type\tRef_Name\tComment"
PNG_SIGNATURE = b"\x89PNG\r\n\x1a\n"


def is_iso_table(path: Path) -> bool:
    try:
        with path.open("r", encoding="utf-8") as source:
            return source.readline().rstrip("\r\n") == ISO_HEADER
    except (OSError, UnicodeError):
        return False


def is_png(path: Path) -> bool:
    try:
        with path.open("rb") as source:
            return source.read(len(PNG_SIGNATURE)) == PNG_SIGNATURE
    except OSError:
        return False


def download(url: str, destination: Path, *, timeout: float) -> Path:
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(f".{destination.name}.download")
    temporary.unlink(missing_ok=True)
    request = urllib.request.Request(
        url,
        headers={"User-Agent": "media-catalog-books asset preparation"},
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            with temporary.open("wb") as target:
                shutil.copyfileobj(response, target)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise
    return temporary


def prepare_iso_table(*, force: bool, timeout: float) -> None:
    if not force and is_iso_table(ISO_TABLE_PATH):
        print(f"ISO 639-3: disponible en {ISO_TABLE_PATH}")
        return

    print(f"ISO 639-3: descargando desde {ISO_TABLE_URL}")
    downloaded = download(ISO_TABLE_URL, ISO_TABLE_PATH, timeout=timeout)
    try:
        if not is_iso_table(downloaded):
            raise RuntimeError("La descarga no contiene una tabla ISO 639-3 válida")
        downloaded.replace(ISO_TABLE_PATH)
    finally:
        downloaded.unlink(missing_ok=True)
    print(f"ISO 639-3: preparada en {ISO_TABLE_PATH}")


def prepare_app_icon(*, force: bool, timeout: float) -> None:
    icon_url = str(os.getenv("APP_ICON_URL", "") or "").strip()
    if not force and is_png(APP_ICON_PATH):
        print(f"Icono: disponible en {APP_ICON_PATH}")
        return
    if not icon_url:
        print(
            "Icono: opcional y no configurado; copia assets/dani.png o define "
            "APP_ICON_URL en .env"
        )
        return

    print("Icono: descargando desde APP_ICON_URL")
    downloaded = download(icon_url, APP_ICON_PATH, timeout=timeout)
    try:
        if not is_png(downloaded):
            raise RuntimeError("APP_ICON_URL no ha devuelto un PNG válido")
        downloaded.replace(APP_ICON_PATH)
    finally:
        downloaded.unlink(missing_ok=True)
    print(f"Icono: preparado en {APP_ICON_PATH}")


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Prepara los recursos locales no versionados de media-catalog-books"
        )
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Vuelve a descargar los recursos configurados",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=30.0,
        help="Timeout de descarga en segundos",
    )
    args = parser.parse_args()
    if args.timeout <= 0:
        parser.error("--timeout debe ser mayor que cero")

    try:
        prepare_iso_table(force=args.force, timeout=args.timeout)
        prepare_app_icon(force=args.force, timeout=args.timeout)
    except (OSError, RuntimeError, urllib.error.URLError) as exc:
        print(f"Error preparando recursos locales: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
