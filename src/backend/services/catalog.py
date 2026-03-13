import json
import re
from typing import Any

from ..clients import ClientError, ollama_chat_text, openai_text_chat
from ..config import (
    CATALOG_PROVIDER,
    CATALOG_OLLAMA_MODEL,
    CATALOG_OPENAI_MODEL,
    OPENAI_API_KEY,
)
from . import books


CATALOG_SYSTEM_PROMPT = """
Eres un asistente experto en bibliografía. Tu tarea es extraer información precisa sobre libros a partir de diversas fuentes y consolidarla en un formato estructurado. 

⚖️ **Criterios para Resolver Conflictos entre Fuentes**:
1️⃣ **Prioridad de fuentes**:
  1° Página de créditos del libro  
  2° isbndb  
  3° Open Library  
  4° Google Books  
2️⃣ Si un dato aparece en varias fuentes, elige la versión más completa y detallada.  
3️⃣ Si hay variaciones menores en el título o autor, usa la versión más común o coherente. Opta por la ortografía más fiable.  
4️⃣ Para los campos con nombres de persona (autor, traductor, etc.) usa el formato "Apellido, Nombre", utilizando iniciales si es evidente. Partículas como "de", preferiblemente delante del apellido. Ejemplos: "Tolkien, J.R.R."; "Dickens, Charles"; "de Paul, Rodrigo".
5. Los campos de países y de entidades o nombres comunes, categoría, género... en español.
6. Devuelve nulo si no está claro, incluso en los campos con lista cerrada de opciones.

📝 **Campos a completar**:
- **ISBN** mejor el de 10 que el de 13. Solo caracteres alfanuméricos.
- **Título** título de la obra.
- **título corto**
- **Subtítulo**
- **Título completo**
- **Autor** 
- **País(es) de autor(es)**: en español, solo si es muy claro.
- **Editorial** (nombre comercial, sin "S.A." o similares).
- **País de publicación**
- **Año de publicación**
- **Idioma**: nombre en español. Pueden ser varios.
- **Edición** (elige como mucho dos de esta lista cerrada): 1ª edición, 2ª edición, 3ª edición, 4ª edición, 5.ª edición o posteriores, Edición especial, Edición ilustrada, Edición para el profesor, Edición del club del libro, Edición limitada, Edición internacional.
- **Número de impresión o tirada**: una opción de esta lista cerrada: 1ª impresión, 2ª impresión, 3ª impresión, 4ª impresión, 5.ª impresión o posteriores.
- **Colección**: indicar el nombre si pertenece a una colección dentro de la editorial.
- **Número en colección** el número dentro de la colección, si esta existe.
- **Obra completa**. El nombre de la obra completa, si se trata de un volumen de la misma.
- **Editor**: persona(s) a cargo de la edición.
- **Traductor(es)**
- **Ilustrador**: autor(es) de las ilustraciones, si las hay.
- **Introducción de** Autor(es) de la introducción o prólogo.
- **Epílogo de**: autor(es) del epílogo, 
- **Fotografía de** autor(es) de las fotografías que se incluyen, si las hay.
- **Categoría**: es español. Lista de sugerencias Ensayo,Novela,Ciencia,Historia,Poesía,Cuentos,Guía,Teatro,Clásicos,Clásicos griegos y latinos,Epistolar,Libro juego,Manuales,Memorias,Cómic,Aforismos,Sociología,Tauromaquia,Biografía,Fotografía
- **Género**: En español. Lista de sugerencias Ciencia ficción,Geología,Fantasía,Filosofía,Policíaco,Estudios literarios, etc. (cualquier campo temático)
- **Contiene ilustraciones**. Lista cerrada: "No", "Contiene ilustraciones", "Profusamente ilustrado", "Ilustraciones en blanco y negro".
- **Encuadernación**. Lista cerrada: "Tapa dura", "Tapa blanda", "Sin encuadernación".
- **Número de páginas**
- **Palabras clave** En español. Alguna etiqueta temática distinta de los valores de género y categoría. Solo si el libro no tiene ISBN, incluye "NOISBN" como palabra clave.

🔎 **Formato de Respuesta**  
Devuelve los datos en **JSON** con esta estructura:
```json
{{
  "isbn": str,
  "titulo": str,
  "titulo_corto": str,
  "subtitulo": str,
  "titulo_completo": str,
  "autor": [str],
  "pais_autor": [str],
  "editorial": str,
  "pais_publicacion": str,
  "anio": int,
  "idioma": [str],
  "edicion": [str],
  "numero_impresion": str,
  "coleccion": str,
  "numero_coleccion": int,
  "obra_completa": str,
  "editor": [str],
  "traductor": [str],
  "ilustrador": [str],
  "introduccion_de": [str],
  "epilogo_de": [str],
  "fotografia_de": [str],
  "categoria": str,
  "genero": str,
  "ilustraciones": str,
  "encuadernacion": str,
  "paginas": int,
  "palabras_clave": [str]
}}
```
""".strip()


CATALOG_HUMAN_PROMPT_TEMPLATE = """
Aquí están los datos extraídos de diversas fuentes sobre un libro:

📚 **Fuentes de Datos**:
1️⃣ **Texto extraído de la página de créditos del libro:**  
{credits}

2️⃣ **Ficha de la API de Google Books:**  
{google}

3️⃣ **Ficha de la API de Open Library:**  
{open_library}

4️⃣ **Ficha de la API de isbndb:**  
{isbndb}
""".strip()


GOOGLE_KEYS_TO_DROP = {
    "allowAnonLogging",
    "readingModes",
    "imageLinks",
    "previewLink",
    "infoLink",
    "canonicalVolumeLink",
}
OPEN_LIBRARY_KEYS_TO_DROP = {"url", "key"}
ISBNDB_PATHS_TO_DROP = (
    ("book", "image"),
    ("book", "dimensions_structured"),
    ("book", "dimensions"),
    ("book", "msrp"),
)


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


def build_catalog_payload(book: dict[str, Any], *, provider: str | None = None, model: str | None = None) -> dict[str, Any]:
    metadata = book.get("metadata") if isinstance(book.get("metadata"), dict) else {}
    credits_text = str(book.get("credits_text") or "").strip()
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

    payload = dict(parsed)
    payload.setdefault("id", book.get("id"))
    payload["catalog_provider"] = chosen_provider
    payload["catalog_model"] = chosen_model
    payload["raw_llm_output"] = raw

    dimensions = _isbndb_dimensions_metric(metadata)
    if dimensions:
        payload.update(dimensions)

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

    try:
        payload = build_catalog_payload(book, provider=provider, model=model)
        title = str(payload.get("titulo") or "").strip()
        status = "built" if payload else "partial"
        error = None if payload else "Catalog LLM returned empty payload"

        books.update_catalog(book_id, catalog=payload, status=status, error=error)
        return {
            "id": book_id,
            "status": status,
            "title": title,
            "catalog_provider": payload.get("catalog_provider"),
            "catalog_model": payload.get("catalog_model"),
            "confidence": None,
            "requires_manual_review": False,
            "review_flags": [],
        }
    except Exception as exc:
        books.update_catalog(book_id, catalog={}, status="error", error=str(exc))
        return {"id": book_id, "status": "error", "error": str(exc)}
