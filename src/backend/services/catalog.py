import json
import re
import unicodedata
from typing import Any

from ..clients import ClientError, ollama_chat_text, openai_text_chat
from ..config import (
    CATALOG_ARBITER_ENABLED,
    CATALOG_ARBITER_MIN_CONFIDENCE,
    CATALOG_ARBITER_PROVIDER,
    CATALOG_PROVIDER,
    CATALOG_OLLAMA_MODEL,
    CATALOG_OPENAI_MODEL,
    OPENAI_API_KEY,
)
from ..normalizers import clean_isbn, is_valid_isbn
from . import books

SOURCE_ORDER = ("isbndb", "open_library", "google")

SOURCE_PRIORITY: dict[str, tuple[str, ...]] = {
    "title": ("isbndb", "open_library", "google"),
    "subtitle": ("isbndb", "google", "open_library"),
    "publisher": ("isbndb", "open_library", "google"),
    "year": ("isbndb", "open_library", "google"),
    "pages": ("isbndb", "google", "open_library"),
}

LANGUAGE_MAP = {
    "en": "ingles",
    "eng": "ingles",
    "es": "espanol",
    "spa": "espanol",
    "fr": "frances",
    "fre": "frances",
    "fra": "frances",
    "de": "aleman",
    "ger": "aleman",
    "deu": "aleman",
    "it": "italiano",
    "ita": "italiano",
    "pt": "portugues",
    "por": "portugues",
    "ca": "catalan",
    "cat": "catalan",
}

OPEN_LIBRARY_LANGUAGE_KEYS = {
    "eng": "ingles",
    "spa": "espanol",
    "fre": "frances",
    "fra": "frances",
    "ger": "aleman",
    "deu": "aleman",
    "ita": "italiano",
    "por": "portugues",
    "cat": "catalan",
}

LEGAL_SUFFIX_PATTERN = re.compile(
    r"(?:,?\s*(?:s\.?\s*a\.?\s*u?\.?|s\.?\s*l\.?\s*u?\.?|sociedad an[oó]nima|"
    r"limitada|limited|ltd\.?|inc\.?|corp\.?|corporation|llc|gmbh|co\.?))+$",
    re.IGNORECASE,
)


def _as_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _as_key(value: Any) -> str:
    text = str(value or "").strip()
    return _normalize_token_text(text)


def _strip_accents(text: str) -> str:
    normalized = unicodedata.normalize("NFKD", text)
    return "".join(char for char in normalized if not unicodedata.combining(char))


def _normalize_token_text(text: str) -> str:
    value = _strip_accents(str(text or "")).lower()
    value = re.sub(r"[^a-z0-9\s]", " ", value)
    value = re.sub(r"\s+", " ", value).strip()
    return value


def _source_rank(field: str, source: str) -> int:
    priority = SOURCE_PRIORITY.get(field, SOURCE_ORDER)
    try:
        return priority.index(source)
    except ValueError:
        return len(priority)


def _publisher_to_commercial(value: Any) -> str | None:
    text = _as_text(value)
    if not text:
        return None

    cleaned = re.sub(r"\s+", " ", text).strip().rstrip(".,;:- ")
    stripped = LEGAL_SUFFIX_PATTERN.sub("", cleaned).strip(" ,.;:-")
    return stripped or cleaned


def _canonical_scalar(value: Any, field: str) -> str:
    if field == "publisher":
        return _normalize_token_text(_publisher_to_commercial(value) or "")

    if isinstance(value, int):
        return str(value)

    return _normalize_token_text(str(value or ""))


def _dedupe_list(items: list[str]) -> list[str]:
    output: list[str] = []
    seen: set[str] = set()
    for item in items:
        value = str(item or "").strip()
        key = _as_key(value)
        if value and key and key not in seen:
            seen.add(key)
            output.append(value)
    return output


def _parse_year(value: Any) -> int | None:
    text = str(value or "")
    match = re.search(r"(1[5-9]\d{2}|20\d{2})", text)
    if not match:
        return None
    try:
        year = int(match.group(1))
    except ValueError:
        return None
    return year if 1500 <= year <= 2100 else None


def _parse_pages(value: Any) -> int | None:
    if isinstance(value, int):
        return value if value > 0 else None

    text = str(value or "").strip()
    if not text:
        return None

    match = re.search(r"(\d{1,5})", text)
    if not match:
        return None

    try:
        pages = int(match.group(1))
    except ValueError:
        return None

    return pages if 1 <= pages <= 20000 else None


def _language_to_es(value: Any) -> str | None:
    text = _as_text(value)
    if not text:
        return None

    key = _as_key(text)
    if key in LANGUAGE_MAP:
        return LANGUAGE_MAP[key]

    if len(key) in {2, 3}:
        mapped = LANGUAGE_MAP.get(key)
        if mapped:
            return mapped

    return text.lower()


def _extract_people(value: Any) -> list[str]:
    if value is None:
        return []

    if isinstance(value, list):
        people: list[str] = []
        for item in value:
            if isinstance(item, dict):
                name = _as_text(item.get("name") or item.get("author") or item.get("value"))
                if name:
                    people.append(name)
            else:
                name = _as_text(item)
                if name:
                    people.append(name)
        return _dedupe_list(people)

    if isinstance(value, dict):
        name = _as_text(value.get("name") or value.get("author") or value.get("value"))
        return [name] if name else []

    name = _as_text(value)
    return [name] if name else []


def _extract_open_library_languages(payload: dict[str, Any]) -> list[str]:
    result: list[str] = []

    raw_languages = payload.get("languages")
    if isinstance(raw_languages, list):
        for item in raw_languages:
            if isinstance(item, dict):
                key = _as_text(item.get("key"))
                if not key:
                    continue
                code = key.split("/")[-1].strip().lower()
                if code in OPEN_LIBRARY_LANGUAGE_KEYS:
                    result.append(OPEN_LIBRARY_LANGUAGE_KEYS[code])

    language = payload.get("language")
    if language is not None:
        if isinstance(language, list):
            for item in language:
                lang = _language_to_es(item)
                if lang:
                    result.append(lang)
        else:
            lang = _language_to_es(language)
            if lang:
                result.append(lang)

    return _dedupe_list(result)


def _extract_subjects(value: Any) -> list[str]:
    if value is None:
        return []

    if isinstance(value, list):
        output: list[str] = []
        for item in value:
            if isinstance(item, dict):
                name = _as_text(item.get("name") or item.get("subject"))
                if name:
                    output.append(name)
            else:
                name = _as_text(item)
                if name:
                    output.append(name)
        return _dedupe_list(output)

    name = _as_text(value)
    return [name] if name else []


def _normalize_google(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "title": _as_text(payload.get("title")),
        "subtitle": _as_text(payload.get("subtitle")),
        "authors": _extract_people(payload.get("authors")),
        "publisher": _as_text(payload.get("publisher")),
        "year": _parse_year(payload.get("publishedDate")),
        "languages": _dedupe_list([_language_to_es(payload.get("language"))] if payload.get("language") else []),
        "pages": _parse_pages(payload.get("pageCount")),
        "subjects": _extract_subjects(payload.get("categories")),
    }


def _normalize_open_library(payload: dict[str, Any]) -> dict[str, Any]:
    publishers = _extract_people(payload.get("publishers"))
    publisher = publishers[0] if publishers else None

    return {
        "title": _as_text(payload.get("title")),
        "subtitle": _as_text(payload.get("subtitle")),
        "authors": _extract_people(payload.get("authors")),
        "publisher": publisher,
        "year": _parse_year(payload.get("publish_date")),
        "languages": _extract_open_library_languages(payload),
        "pages": _parse_pages(payload.get("number_of_pages")),
        "subjects": _extract_subjects(payload.get("subjects")),
    }


def _normalize_isbndb(payload: dict[str, Any]) -> dict[str, Any]:
    book_payload = payload.get("book") if isinstance(payload.get("book"), dict) else {}

    return {
        "title": _as_text(book_payload.get("title")),
        "subtitle": _as_text(book_payload.get("subtitle")),
        "authors": _extract_people(book_payload.get("authors")),
        "publisher": _as_text(book_payload.get("publisher")),
        "year": _parse_year(book_payload.get("date_published")),
        "languages": _dedupe_list([_language_to_es(book_payload.get("language"))] if book_payload.get("language") else []),
        "pages": _parse_pages(book_payload.get("pages")),
        "subjects": _extract_subjects(book_payload.get("subjects")),
    }


def _normalize_sources(metadata: dict[str, Any]) -> dict[str, dict[str, Any]]:
    google = metadata.get("google") if isinstance(metadata.get("google"), dict) else {}
    open_library = metadata.get("open_library") if isinstance(metadata.get("open_library"), dict) else {}
    isbndb = metadata.get("isbndb") if isinstance(metadata.get("isbndb"), dict) else {}

    normalized = {
        "google": _normalize_google(google),
        "open_library": _normalize_open_library(open_library),
        "isbndb": _normalize_isbndb(isbndb),
    }

    return normalized


def _source_has_data(source_payload: dict[str, Any]) -> bool:
    for value in source_payload.values():
        if isinstance(value, list) and value:
            return True
        if value not in (None, ""):
            return True
    return False


def _collect_candidates(normalized: dict[str, dict[str, Any]], field: str) -> list[tuple[str, Any]]:
    candidates: list[tuple[str, Any]] = []
    for source in SOURCE_ORDER:
        source_payload = normalized.get(source) or {}
        value = source_payload.get(field)
        if isinstance(value, list):
            if value:
                candidates.append((source, value))
        elif value not in (None, ""):
            candidates.append((source, value))
    return candidates


def _resolve_scalar(normalized: dict[str, dict[str, Any]], field: str) -> tuple[Any, str | None, list[dict[str, Any]]]:
    candidates = _collect_candidates(normalized, field)
    if not candidates:
        return None, None, []

    grouped: dict[str, dict[str, Any]] = {}
    for source, raw_value in candidates:
        if field == "publisher":
            value = _publisher_to_commercial(raw_value)
        else:
            value = raw_value

        if value in (None, ""):
            continue

        key = _canonical_scalar(value, field)
        if not key:
            continue

        entry = grouped.setdefault(
            key,
            {
                "sources": set(),
                "items": [],
            },
        )
        entry["sources"].add(source)
        entry["items"].append((source, value))

    if not grouped:
        return None, None, []

    def group_sort_key(item: tuple[str, dict[str, Any]]) -> tuple[int, int]:
        _, payload = item
        support = len(payload["sources"])
        best_rank = min(_source_rank(field, source) for source in payload["sources"])
        return support, -best_rank

    chosen_key, chosen_group = max(grouped.items(), key=group_sort_key)
    chosen_items = list(chosen_group["items"])

    if field == "publisher":
        representative_source, representative_value = min(
            chosen_items,
            key=lambda item: (len(str(item[1])), _source_rank(field, item[0])),
        )
    elif isinstance(chosen_items[0][1], int):
        representative_source, representative_value = min(
            chosen_items,
            key=lambda item: (_source_rank(field, item[0]), item[1]),
        )
    else:
        representative_source, representative_value = max(
            chosen_items,
            key=lambda item: (len(str(item[1])), -_source_rank(field, item[0])),
        )

    alternatives: list[dict[str, Any]] = []
    for key, payload in grouped.items():
        if key == chosen_key:
            continue
        support = len(payload["sources"])
        sample_source, sample_value = sorted(
            payload["items"],
            key=lambda item: (_source_rank(field, item[0]), str(item[1])),
        )[0]
        alternatives.append(
            {
                "source": sample_source,
                "value": sample_value,
                "support_sources": support,
                "sources": sorted(payload["sources"]),
            }
        )

    return representative_value, representative_source, alternatives


def _resolve_authors(normalized: dict[str, dict[str, Any]]) -> tuple[list[str], str | None, list[dict[str, Any]]]:
    candidates = _collect_candidates(normalized, "authors")
    if not candidates:
        return [], None, []

    primary_source: str | None = None
    primary_authors: list[str] = []
    for source in SOURCE_ORDER:
        values = [value for candidate_source, value in candidates if candidate_source == source]
        if values:
            primary_source = source
            primary_authors = list(values[0])
            break

    support: dict[str, dict[str, Any]] = {}
    for source, authors in candidates:
        for author in authors:
            key = _canonical_scalar(author, "author")
            if not key:
                continue
            entry = support.setdefault(key, {"value": author, "sources": set()})
            entry["sources"].add(source)
            # Prefer longer human-readable representation
            if len(str(author)) > len(str(entry["value"])):
                entry["value"] = author

    chosen: list[str] = []
    chosen_keys: set[str] = set()

    for author in primary_authors:
        key = _canonical_scalar(author, "author")
        if key and key not in chosen_keys:
            chosen_keys.add(key)
            chosen.append(author)

    consensus_authors = sorted(
        support.items(),
        key=lambda item: (-len(item[1]["sources"]), item[1]["value"].lower()),
    )
    for key, payload in consensus_authors:
        if len(payload["sources"]) < 2:
            continue
        if key in chosen_keys:
            continue
        chosen_keys.add(key)
        chosen.append(str(payload["value"]))

    alternatives = [
        {"source": source, "value": value}
        for source, value in candidates
        if source != primary_source
    ]
    return _dedupe_list(chosen), primary_source, alternatives


def _resolve_languages(normalized: dict[str, dict[str, Any]]) -> tuple[list[str], str | None, list[dict[str, Any]]]:
    candidates = _collect_candidates(normalized, "languages")
    chosen: list[str] = []
    chosen_source: str | None = None

    for source in SOURCE_ORDER:
        source_values = [value for candidate_source, value in candidates if candidate_source == source]
        if source_values:
            chosen_source = source
            chosen = list(source_values[0])
            break

    for source, value in candidates:
        if source == chosen_source:
            continue
        chosen.extend(value)

    chosen = _dedupe_list(chosen)
    alternatives = [
        {"source": source, "value": value}
        for source, value in candidates
        if source != chosen_source
    ]
    return chosen, chosen_source, alternatives


def _merge_subjects(normalized: dict[str, dict[str, Any]]) -> list[str]:
    values: list[str] = []
    for source in SOURCE_ORDER:
        values.extend(normalized.get(source, {}).get("subjects") or [])
    return _dedupe_list(values)


def _provenance_entry(chosen_source: str | None, chosen_value: Any, alternatives: list[dict[str, Any]], strategy: str) -> dict[str, Any]:
    return {
        "source": chosen_source,
        "value": chosen_value,
        "alternatives": alternatives,
        "strategy": strategy,
    }


def _collect_conflicts(normalized: dict[str, dict[str, Any]]) -> dict[str, list[Any]]:
    conflicts: dict[str, list[Any]] = {}

    title_values = [
        str(value).strip()
        for _, value in _collect_candidates(normalized, "title")
        if str(value).strip()
    ]
    if len({_as_key(item) for item in title_values}) > 1:
        conflicts["titulo"] = _dedupe_list(title_values)

    year_values = [
        int(value)
        for _, value in _collect_candidates(normalized, "year")
        if isinstance(value, int)
    ]
    if len(set(year_values)) > 1:
        conflicts["anio"] = sorted(set(year_values))

    publisher_values = [
        _publisher_to_commercial(value)
        for _, value in _collect_candidates(normalized, "publisher")
        if _publisher_to_commercial(value)
    ]
    if len({_canonical_scalar(value, "publisher") for value in publisher_values}) > 1:
        conflicts["editorial"] = _dedupe_list([value for value in publisher_values if value])

    return conflicts


def _compute_quality(
    catalog: dict[str, Any],
    normalized: dict[str, dict[str, Any]],
    *,
    isbn_valid: bool,
    resolved_conflicts: set[str] | None = None,
) -> dict[str, Any]:
    missing_fields: list[str] = []
    if not catalog.get("titulo"):
        missing_fields.append("titulo")
    if not catalog.get("autor"):
        missing_fields.append("autor")
    if not catalog.get("editorial"):
        missing_fields.append("editorial")
    if not catalog.get("anio"):
        missing_fields.append("anio")
    if not catalog.get("idioma"):
        missing_fields.append("idioma")
    if not catalog.get("paginas"):
        missing_fields.append("paginas")

    conflicts = _collect_conflicts(normalized)
    if resolved_conflicts:
        conflicts = {
            field: values
            for field, values in conflicts.items()
            if field not in resolved_conflicts
        }

    weights = {
        "titulo": 0.28,
        "autor": 0.20,
        "editorial": 0.12,
        "anio": 0.12,
        "idioma": 0.10,
        "paginas": 0.08,
        "isbn": 0.10,
        "palabras_clave": 0.05,
    }

    score = 0.0
    if catalog.get("titulo"):
        score += weights["titulo"]
    if catalog.get("autor"):
        score += weights["autor"]
    if catalog.get("editorial"):
        score += weights["editorial"]
    if catalog.get("anio"):
        score += weights["anio"]
    if catalog.get("idioma"):
        score += weights["idioma"]
    if catalog.get("paginas"):
        score += weights["paginas"]
    if isbn_valid:
        score += weights["isbn"]
    if catalog.get("palabras_clave"):
        score += weights["palabras_clave"]

    sources_with_data = sum(1 for source in SOURCE_ORDER if _source_has_data(normalized.get(source, {})))
    source_bonus = min(0.10, 0.05 * max(0, sources_with_data - 1))
    score += source_bonus

    if "titulo" not in conflicts and catalog.get("titulo") and sources_with_data >= 2:
        score += 0.05

    conflict_penalty = 0.08 * len(conflicts)
    score -= conflict_penalty

    confidence = max(0.0, min(1.0, round(score, 3)))

    review_flags: list[str] = []
    if not isbn_valid:
        review_flags.append("isbn_missing_or_invalid")
    review_flags.extend([f"missing_{field}" for field in missing_fields])
    review_flags.extend([f"conflict_{field}" for field in conflicts])

    requires_manual_review = bool(
        ("titulo" in missing_fields)
        or confidence < 0.55
        or ("titulo" in conflicts and confidence < 0.75)
    )

    return {
        "confidence": confidence,
        "requires_manual_review": requires_manual_review,
        "missing_fields": missing_fields,
        "conflicts": conflicts,
        "review_flags": review_flags,
        "source_coverage": {
            "sources_with_data": sources_with_data,
            "sources_total": len(SOURCE_ORDER),
        },
    }


def _normalize_arbiter_provider(value: str | None) -> str:
    text = str(value or CATALOG_ARBITER_PROVIDER).strip().lower()
    if text in {"auto", "openai", "ollama", "none"}:
        return text
    return "auto"


def _arbiter_provider_plan(provider: str) -> list[str]:
    if provider == "none":
        return []
    if provider == "openai":
        return ["openai"]
    if provider == "ollama":
        return ["ollama"]

    plan: list[str] = []
    if OPENAI_API_KEY:
        plan.append("openai")
    plan.append("ollama")
    return plan


def _catalog_needs_arbiter(qa: dict[str, Any]) -> bool:
    confidence = float(qa.get("confidence") or 0.0)
    conflicts = qa.get("conflicts") if isinstance(qa.get("conflicts"), dict) else {}
    requires_review = bool(qa.get("requires_manual_review"))
    return requires_review or bool(conflicts) or (confidence < CATALOG_ARBITER_MIN_CONFIDENCE)


def _strip_code_fences(text: str) -> str:
    value = str(text or "").strip()
    if value.startswith("```"):
        value = re.sub(r"^```[a-zA-Z0-9_-]*\n?", "", value)
        value = re.sub(r"\n?```$", "", value).strip()
    return value


def _extract_json_object(text: str) -> dict[str, Any] | None:
    cleaned = _strip_code_fences(text)
    try:
        payload = json.loads(cleaned)
        return payload if isinstance(payload, dict) else None
    except Exception:
        pass

    start = cleaned.find("{")
    end = cleaned.rfind("}")
    if start < 0 or end <= start:
        return None

    snippet = cleaned[start : end + 1]
    try:
        payload = json.loads(snippet)
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def _build_arbiter_prompt(catalog: dict[str, Any], qa: dict[str, Any]) -> str:
    provenance = catalog.get("provenance") if isinstance(catalog.get("provenance"), dict) else {}

    candidate_payload = {
        "actual": {
            "titulo": catalog.get("titulo"),
            "subtitulo": catalog.get("subtitulo"),
            "autor": catalog.get("autor"),
            "editorial": catalog.get("editorial"),
            "anio": catalog.get("anio"),
            "idioma": catalog.get("idioma"),
            "paginas": catalog.get("paginas"),
        },
        "provenance": provenance,
        "qa": {
            "confidence": qa.get("confidence"),
            "missing_fields": qa.get("missing_fields"),
            "conflicts": qa.get("conflicts"),
            "review_flags": qa.get("review_flags"),
        },
    }

    return (
        "Actuas como arbitro de metadatos bibliograficos.\\n"
        "Debes proponer correcciones SOLO cuando haya conflicto evidente o baja confianza.\\n"
        "Reglas:\\n"
        "1) Prioriza consenso entre fuentes frente a una sola fuente aislada.\\n"
        "2) Para editorial, usa nombre comercial y evita forma fiscal (S.A., S.L., Ltd, Inc, etc.).\\n"
        "3) Si no hay evidencia clara, deja el campo sin cambios (null).\\n"
        "4) Devuelve JSON valido sin texto adicional.\\n\\n"
        "Formato de salida:\\n"
        "{\\n"
        '  \"titulo\": str|null,\\n'
        '  \"subtitulo\": str|null,\\n'
        '  \"autor\": [str]|null,\\n'
        '  \"editorial\": str|null,\\n'
        '  \"anio\": int|null,\\n'
        '  \"idioma\": [str]|null,\\n'
        '  \"paginas\": int|null,\\n'
        '  \"resolved_conflicts\": [str],\\n'
        '  \"rationale\": str\\n'
        "}\\n\\n"
        "Datos:\\n"
        f"{json.dumps(candidate_payload, ensure_ascii=False, indent=2)}"
    )


def _call_arbiter(provider: str, prompt: str) -> str:
    model = CATALOG_OPENAI_MODEL if provider == "openai" else CATALOG_OLLAMA_MODEL
    if provider == "openai":
        if not OPENAI_API_KEY:
            raise ClientError("OPENAI_API_KEY is not configured for catalog arbiter")
        return openai_text_chat(api_key=OPENAI_API_KEY, model=model, prompt=prompt)
    if provider == "ollama":
        return ollama_chat_text(model=model, prompt=prompt)
    raise ClientError(f"Unsupported catalog arbiter provider: {provider}")


def _validated_arbiter_values(payload: dict[str, Any]) -> dict[str, Any]:
    values: dict[str, Any] = {}

    title = _as_text(payload.get("titulo"))
    if title:
        values["titulo"] = title

    subtitle = _as_text(payload.get("subtitulo"))
    if subtitle:
        values["subtitulo"] = subtitle

    editorial = _publisher_to_commercial(payload.get("editorial"))
    if editorial:
        values["editorial"] = editorial

    year = _parse_year(payload.get("anio"))
    if year:
        values["anio"] = year

    pages = _parse_pages(payload.get("paginas"))
    if pages:
        values["paginas"] = pages

    authors = _extract_people(payload.get("autor"))
    if authors:
        values["autor"] = authors

    languages_raw = payload.get("idioma")
    if languages_raw is not None:
        if isinstance(languages_raw, list):
            languages = _dedupe_list([_language_to_es(item) or "" for item in languages_raw if _language_to_es(item)])
        else:
            single = _language_to_es(languages_raw)
            languages = [single] if single else []
        if languages:
            values["idioma"] = languages

    resolved_conflicts: set[str] = set()
    raw_conflicts = payload.get("resolved_conflicts")
    if isinstance(raw_conflicts, list):
        for item in raw_conflicts:
            key = _normalize_token_text(item)
            if key in {"titulo", "subtitulo", "editorial", "anio", "paginas", "autor", "idioma"}:
                resolved_conflicts.add(key)

    rationale = _as_text(payload.get("rationale"))
    return {
        "values": values,
        "resolved_conflicts": resolved_conflicts,
        "rationale": rationale,
    }


def _apply_arbiter_values(catalog: dict[str, Any], values: dict[str, Any], provider: str, rationale: str | None) -> None:
    if not values:
        return

    provenance = catalog.get("provenance")
    if not isinstance(provenance, dict):
        provenance = {}
        catalog["provenance"] = provenance

    for field, value in values.items():
        catalog[field] = value
        entry = provenance.get(field)
        if not isinstance(entry, dict):
            entry = {"source": None, "value": None, "alternatives": []}
            provenance[field] = entry
        entry["source"] = "llm_arbiter"
        entry["value"] = value
        entry["strategy"] = "llm_arbiter_override"
        entry["arbiter"] = {
            "provider": provider,
            "model": CATALOG_OPENAI_MODEL if provider == "openai" else CATALOG_OLLAMA_MODEL,
            "rationale": rationale,
        }

    title = _as_text(catalog.get("titulo"))
    subtitle = _as_text(catalog.get("subtitulo"))
    if title and subtitle:
        catalog["titulo_completo"] = f"{title}: {subtitle}"
    else:
        catalog["titulo_completo"] = title


CATALOG_SYSTEM_PROMPT = (
    "Eres un asistente experto en bibliografia. Tu tarea es consolidar informacion de libros "
    "a partir de varias fuentes y devolver JSON estricto.\n\n"
    "Criterios para conflictos:\n"
    "1) Prioridad de fuentes: pagina de creditos > isbndb > open library > google books.\n"
    "2) Si varias fuentes coinciden, usa la version mas completa y coherente.\n"
    "3) Para editorial, usa nombre comercial (sin S.A., S.L., Ltd, Inc, etc.).\n"
    "4) Para nombres de persona usa formato 'Apellido, Nombre'.\n"
    "5) Paises/categoria/genero en espanol.\n"
    "6) Si no esta claro, devuelve null.\n\n"
    "Devuelve SOLO JSON valido."
)

CATALOG_HUMAN_PROMPT_TEMPLATE = (
    "Aqui estan los datos extraidos de diversas fuentes sobre un libro:\n\n"
    "1) Texto de la pagina de creditos:\n{credits}\n\n"
    "2) Ficha Google Books:\n{google}\n\n"
    "3) Ficha Open Library:\n{open_library}\n\n"
    "4) Ficha ISBNdb:\n{isbndb}\n\n"
    "Respuesta JSON esperada:\n"
    "{{\n"
    '  "isbn": str|null,\n'
    '  "titulo": str|null,\n'
    '  "titulo_corto": str|null,\n'
    '  "subtitulo": str|null,\n'
    '  "titulo_completo": str|null,\n'
    '  "autor": [str]|null,\n'
    '  "pais_autor": [str]|null,\n'
    '  "editorial": str|null,\n'
    '  "pais_publicacion": str|null,\n'
    '  "anio": int|null,\n'
    '  "idioma": [str]|null,\n'
    '  "edicion": [str]|null,\n'
    '  "numero_impresion": str|null,\n'
    '  "coleccion": str|null,\n'
    '  "numero_coleccion": int|null,\n'
    '  "obra_completa": str|null,\n'
    '  "volumen": str|null,\n'
    '  "editor": [str]|null,\n'
    '  "traductor": [str]|null,\n'
    '  "ilustrador": [str]|null,\n'
    '  "introduccion_de": [str]|null,\n'
    '  "epilogo_de": [str]|null,\n'
    '  "fotografia_de": [str]|null,\n'
    '  "categoria": str|null,\n'
    '  "genero": str|null,\n'
    '  "ilustraciones": str|null,\n'
    '  "encuadernacion": str|null,\n'
    '  "paginas": int|null,\n'
    '  "palabras_clave": [str]|null\n'
    "}}"
)

GOOGLE_KEYS_TO_DROP = {
    "allowAnonLogging",
    "readingModes",
    "imageLinks",
    "previewLink",
    "infoLink",
    "canonicalVolumeLink",
}

OPEN_LIBRARY_KEYS_TO_DROP = {"url", "key"}
ISBNDB_PATHS_TO_DROP = (("book", "image"), ("book", "dimensions_structured"), ("book", "dimensions"), ("book", "msrp"))


def _delete_nested_key(payload: dict[str, Any], path: tuple[str, ...]) -> None:
    if not path:
        return
    if len(path) == 1:
        payload.pop(path[0], None)
        return
    head = payload.get(path[0])
    if not isinstance(head, dict):
        return
    _delete_nested_key(head, path[1:])


def _clean_sources_for_prompt(metadata: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    google = dict(metadata.get("google") or {}) if isinstance(metadata.get("google"), dict) else {}
    open_library = dict(metadata.get("open_library") or {}) if isinstance(metadata.get("open_library"), dict) else {}
    isbndb = json.loads(json.dumps(metadata.get("isbndb") or {})) if isinstance(metadata.get("isbndb"), dict) else {}

    for key in GOOGLE_KEYS_TO_DROP:
        google.pop(key, None)
    for key in OPEN_LIBRARY_KEYS_TO_DROP:
        open_library.pop(key, None)
    for path in ISBNDB_PATHS_TO_DROP:
        _delete_nested_key(isbndb, path)

    return google, open_library, isbndb


def _isbndb_dimensions_metric(metadata: dict[str, Any]) -> dict[str, Any]:
    isbndb = metadata.get("isbndb") if isinstance(metadata.get("isbndb"), dict) else {}
    book_payload = isbndb.get("book") if isinstance(isbndb.get("book"), dict) else {}
    dimensions = book_payload.get("dimensions_structured") if isinstance(book_payload.get("dimensions_structured"), dict) else {}
    if not dimensions:
        return {}

    inch_to_cm = 2.54
    pounds_to_grams = 453.592
    mapping = {"height": "alto", "length": "ancho", "width": "fondo", "weight": "peso"}
    output: dict[str, Any] = {}

    for key, meta in dimensions.items():
        if key not in mapping or not isinstance(meta, dict):
            continue
        raw_value = meta.get("value")
        unit = str(meta.get("unit") or "").strip().lower()
        try:
            number = float(raw_value)
        except Exception:
            continue

        if unit == "inches":
            converted = round(number * inch_to_cm, 2)
        elif unit == "pounds":
            converted = round(number * pounds_to_grams, 2)
        else:
            converted = round(number, 2)

        output[mapping[key]] = converted

    return output


def _normalize_catalog_provider(value: str | None) -> str:
    provider = str(value or CATALOG_PROVIDER).strip().lower()
    if provider not in {"auto", "openai", "ollama"}:
        provider = "auto"
    if provider == "auto":
        return "openai" if OPENAI_API_KEY else "ollama"
    return provider


def _catalog_model_for_provider(provider: str, model: str | None) -> str:
    explicit = str(model or "").strip()
    if explicit:
        return explicit
    if provider == "openai":
        return CATALOG_OPENAI_MODEL
    return CATALOG_OLLAMA_MODEL


def _call_catalog_llm(*, provider: str, model: str, prompt: str) -> str:
    if provider == "openai":
        if not OPENAI_API_KEY:
            raise ClientError("OPENAI_API_KEY is not configured for catalog provider openai")
        return openai_text_chat(api_key=OPENAI_API_KEY, model=model, prompt=prompt)
    if provider == "ollama":
        return ollama_chat_text(model=model, prompt=prompt)
    raise ClientError(f"Unsupported catalog provider: {provider}")


def _split_text_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        items = [str(item).strip() for item in value if str(item).strip()]
        return _dedupe_list(items)
    text = str(value).strip()
    if not text:
        return []
    parts = [chunk.strip() for chunk in re.split(r"[;\n]", text) if chunk.strip()]
    return _dedupe_list(parts)


def _parse_small_int(value: Any) -> int | None:
    if isinstance(value, int):
        return value if value >= 0 else None
    text = str(value or "").strip()
    if not text:
        return None
    match = re.search(r"\d{1,4}", text)
    if not match:
        return None
    try:
        return int(match.group(0))
    except Exception:
        return None


def _build_llm_catalog_quality(catalog: dict[str, Any]) -> dict[str, Any]:
    required_fields = ("titulo", "autor", "editorial", "anio", "idioma", "paginas")
    missing = [field for field in required_fields if not catalog.get(field)]
    isbn_ok = bool(catalog.get("isbn") and is_valid_isbn(catalog.get("isbn")))

    base = 1.0 - (len(missing) / len(required_fields))
    if not isbn_ok:
        base -= 0.15
    confidence = max(0.0, min(1.0, round(base, 3)))

    flags = [f"missing_{field}" for field in missing]
    if not isbn_ok:
        flags.append("isbn_missing_or_invalid")

    requires_review = bool("titulo" in missing or confidence < 0.55)
    return {
        "confidence": confidence,
        "requires_manual_review": requires_review,
        "missing_fields": missing,
        "conflicts": {},
        "review_flags": flags,
        "source_coverage": {"sources_with_data": 0, "sources_total": len(SOURCE_ORDER)},
    }


def build_catalog_payload(book: dict[str, Any], *, provider: str | None = None, model: str | None = None) -> dict[str, Any]:
    metadata = book.get("metadata") if isinstance(book.get("metadata"), dict) else {}
    credits_text = str(book.get("credits_text") or "").strip()
    normalized = _normalize_sources(metadata)

    google_clean, open_library_clean, isbndb_clean = _clean_sources_for_prompt(metadata)
    prompt = (
        f"SYSTEM:\n{CATALOG_SYSTEM_PROMPT}\n\n"
        "USER:\n"
        + CATALOG_HUMAN_PROMPT_TEMPLATE.format(
            credits=credits_text or "",
            google=json.dumps(google_clean, ensure_ascii=False, indent=2),
            open_library=json.dumps(open_library_clean, ensure_ascii=False, indent=2),
            isbndb=json.dumps(isbndb_clean, ensure_ascii=False, indent=2),
        )
    )

    chosen_provider = _normalize_catalog_provider(provider)
    chosen_model = _catalog_model_for_provider(chosen_provider, model)
    raw = _call_catalog_llm(provider=chosen_provider, model=chosen_model, prompt=prompt)

    parsed = _extract_json_object(raw)
    if not parsed:
        raise ClientError("Catalog LLM returned invalid JSON")

    isbn_candidate = clean_isbn(parsed.get("isbn") or book.get("isbn") or metadata.get("isbn") or book.get("isbn_raw"))
    isbn = isbn_candidate if isbn_candidate else None
    isbn_ok = bool(isbn and is_valid_isbn(isbn))

    titulo = _as_text(parsed.get("titulo"))
    subtitulo = _as_text(parsed.get("subtitulo"))
    titulo_corto = _as_text(parsed.get("titulo_corto"))
    titulo_completo = _as_text(parsed.get("titulo_completo"))
    if not titulo_completo:
        if titulo and subtitulo:
            titulo_completo = f"{titulo}: {subtitulo}"
        else:
            titulo_completo = titulo

    idioma_list = _dedupe_list([_language_to_es(item) or "" for item in _split_text_list(parsed.get("idioma")) if _language_to_es(item)])
    if not idioma_list:
        idioma_list = _resolve_languages(normalized)[0]

    keywords = _split_text_list(parsed.get("palabras_clave"))
    if not keywords:
        keywords = _merge_subjects(normalized)
    if not keywords and not isbn_ok:
        keywords = ["NOISBN"]

    payload = {
        "id": book.get("id"),
        "isbn": isbn,
        "titulo": titulo,
        "titulo_corto": titulo_corto or titulo,
        "subtitulo": subtitulo,
        "titulo_completo": titulo_completo,
        "autor": _extract_people(parsed.get("autor")),
        "pais_autor": _split_text_list(parsed.get("pais_autor")),
        "editorial": _publisher_to_commercial(parsed.get("editorial")),
        "pais_publicacion": _as_text(parsed.get("pais_publicacion")),
        "anio": _parse_year(parsed.get("anio")),
        "idioma": idioma_list,
        "edicion": _split_text_list(parsed.get("edicion")),
        "numero_impresion": _as_text(parsed.get("numero_impresion")),
        "coleccion": _as_text(parsed.get("coleccion")),
        "numero_coleccion": _parse_small_int(parsed.get("numero_coleccion")),
        "obra_completa": _as_text(parsed.get("obra_completa")),
        "volumen": _as_text(parsed.get("volumen")),
        "editor": _extract_people(parsed.get("editor")),
        "traductor": _extract_people(parsed.get("traductor")),
        "ilustrador": _extract_people(parsed.get("ilustrador")),
        "introduccion_de": _extract_people(parsed.get("introduccion_de")),
        "epilogo_de": _extract_people(parsed.get("epilogo_de")),
        "fotografia_de": _extract_people(parsed.get("fotografia_de")),
        "categoria": _as_text(parsed.get("categoria")),
        "genero": _as_text(parsed.get("genero")),
        "ilustraciones": _as_text(parsed.get("ilustraciones")),
        "encuadernacion": _as_text(parsed.get("encuadernacion")),
        "paginas": _parse_pages(parsed.get("paginas")),
        "palabras_clave": keywords,
        "creditos_texto": credits_text,
        "fuentes": [source for source in SOURCE_ORDER if _source_has_data(normalized.get(source, {}))],
        "catalog_provider": chosen_provider,
        "catalog_model": chosen_model,
    }

    for dim_key, dim_value in _isbndb_dimensions_metric(metadata).items():
        if dim_key not in payload or payload.get(dim_key) in (None, ""):
            payload[dim_key] = dim_value

    payload["qa"] = _build_llm_catalog_quality(payload)
    payload["raw_llm_output"] = raw
    return payload


def run_one(
    book_id: str,
    *,
    overwrite: bool = False,
    provider: str | None = None,
    model: str | None = None,
) -> dict[str, Any]:
    book = books.get_book(book_id)
    if book is None:
        return {"id": book_id, "status": "error", "error": "Book not found"}

    existing_status = str(book.get("catalog_status") or "").strip().lower()
    if existing_status in {"built", "manual"} and not overwrite:
        return {"id": book_id, "status": "skipped", "reason": "catalog already present"}

    metadata_status = str(book.get("metadata_status") or "").strip().lower()
    if metadata_status in {"", "error"} and not overwrite:
        books.update_catalog(
            book_id,
            catalog={},
            status="partial",
            error="Catalog built without metadata (metadata missing or failed)",
        )
        return {
            "id": book_id,
            "status": "partial",
            "reason": "metadata missing or failed",
        }

    try:
        payload = build_catalog_payload(book, provider=provider, model=model)
        title = str(payload.get("titulo") or "").strip()
        qa = payload.get("qa") if isinstance(payload.get("qa"), dict) else {}
        needs_review = bool(qa.get("requires_manual_review"))

        status = "built" if (title and not needs_review) else "partial"

        if status == "built":
            error = None
        else:
            flags = qa.get("review_flags") if isinstance(qa.get("review_flags"), list) else []
            error = "; ".join(str(flag) for flag in flags[:6]) if flags else "Catalog confidence below threshold"

        books.update_catalog(book_id, catalog=payload, status=status, error=error)
        return {
            "id": book_id,
            "status": status,
            "title": title,
            "catalog_provider": payload.get("catalog_provider"),
            "catalog_model": payload.get("catalog_model"),
            "confidence": qa.get("confidence"),
            "requires_manual_review": needs_review,
            "review_flags": qa.get("review_flags", []),
        }
    except Exception as exc:
        books.update_catalog(book_id, catalog={}, status="error", error=str(exc))
        return {"id": book_id, "status": "error", "error": str(exc)}
