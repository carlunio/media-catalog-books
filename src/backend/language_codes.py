from __future__ import annotations

import re
from functools import lru_cache
from typing import Any


@lru_cache(maxsize=1)
def _load_language_libs() -> tuple[Any | None, Any | None]:
    try:
        import langcodes  # type: ignore
        import iso639  # type: ignore
    except Exception:
        return None, None
    return langcodes, iso639


def idioma_es_a_iso639_3(nombre: str | None) -> str | None:
    text = str(nombre or "").strip()
    if not text:
        return None

    # Multi-language values are encoded as "MUL" in the export view.
    if ";" in text:
        return "MUL"

    langcodes, iso639 = _load_language_libs()
    if langcodes is None or iso639 is None:
        return None

    try:
        language = langcodes.find(text)
    except Exception:
        return None

    base = str(language or "").strip().lower()
    if not base:
        return None
    base = re.split(r"[-_]", base, maxsplit=1)[0]
    if not base or base == "und":
        return None

    try:
        if len(base) == 2:
            return str(iso639.Language.from_part1(base).part3 or "").upper() or None
        if len(base) == 3:
            return str(iso639.Language.from_part3(base).part3 or "").upper() or None
    except Exception:
        return None
    return None
