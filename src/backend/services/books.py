import json
import re
from decimal import Decimal
from pathlib import Path
from typing import Any

from ..config import DEFAULT_COVERS_DIR
from ..database import get_connection
from ..normalizers import extract_book_id_from_path, normalize_book_id, split_book_id

STAGES = ("ocr", "metadata", "catalog", "cover")
PAYLOAD_TYPES = {"catalog", "ocr_trace"}
VALID_BLOCKS = ("A", "B", "C")
MODULE_DIR_PATTERN = re.compile(r"^\d{2}$")
METADATA_PROVIDER_MAP = (
    ("google", "google"),
    ("isbndb", "isbndb"),
    ("open_library", "openlibrary"),
)
METADATA_PROVIDER_REVERSE_MAP = {provider: source for source, provider in METADATA_PROVIDER_MAP}
BOOK_ALLOWED_VALUES: list[tuple[str, str]] = [
    ("edicion", "1ª edición"),
    ("edicion", "2ª edición"),
    ("edicion", "3ª edición"),
    ("edicion", "4ª edición"),
    ("edicion", "5ª edición o posteriores"),
    ("edicion", "Edición especial"),
    ("edicion", "Edición limitada"),
    ("edicion", "Edición ilustrada"),
    ("edicion", "Edición internacional"),
    ("edicion", "Edición para el profesor"),
    ("numero_impresion", "1ª impresión"),
    ("numero_impresion", "2ª impresión"),
    ("numero_impresion", "3ª impresión"),
    ("numero_impresion", "4ª impresión"),
    ("numero_impresion", "5ª impresión o posteriores"),
    ("estado_stock", "En venta"),
    ("estado_stock", "Vendido"),
    ("estado_stock", "Extraviado"),
    ("estado_carga", "Subido"),
    ("estado_carga", "Para subir"),
    ("estado_carga", "Para actualizar"),
    ("estado_carga", "Más tarde"),
    ("tipo_articulo", "Libros"),
    ("tipo_articulo", "Mapas"),
    ("tipo_articulo", "Manuscritos y coleccionismo de papel"),
    ("tipo_articulo", "Comics"),
    ("tipo_articulo", "Revistas y publicaciones"),
    ("tipo_articulo", "Arte, grabados y pósters"),
    ("tipo_articulo", "Partituras"),
    ("tipo_articulo", "Fotografías"),
    ("estado_conservacion", "Nuevo"),
    ("estado_conservacion", "Como nuevo"),
    ("estado_conservacion", "Excelente"),
    ("estado_conservacion", "Muy bien"),
    ("estado_conservacion", "Bien"),
    ("estado_conservacion", "Aceptable"),
    ("estado_conservacion", "Regular"),
    ("estado_conservacion", "Pobre"),
    ("estado_cubierta", "Nuevo"),
    ("estado_cubierta", "Como nuevo"),
    ("estado_cubierta", "Excelente"),
    ("estado_cubierta", "Muy bien"),
    ("estado_cubierta", "Bien"),
    ("estado_cubierta", "Regular"),
    ("estado_cubierta", "Mal"),
    ("estado_cubierta", "Sin cubierta"),
    ("dedicatorias", "Firmado por el autor o artista"),
    ("dedicatorias", "Firmado por los autores o artistas"),
    ("dedicatorias", "Firmado e inscrito por el autor o artista"),
    ("dedicatorias", "Inscrito por el autor o artista"),
    ("dedicatorias", "Firmado por el ilustrador"),
    ("dedicatorias", "Inscrito por el ilustrador"),
    ("plantilla_envio", "A"),
    ("plantilla_envio", "B"),
    ("catalogo", "ejemplo 1"),
    ("catalogo", "ejemplo 2"),
    ("categoria", "Ensayo"),
    ("categoria", "Novela"),
    ("categoria", "Poesía"),
    ("categoria", "Cuentos"),
    ("genero", "Ciencia ficción"),
    ("genero", "Fantasía"),
    ("genero", "Filosofía"),
    ("genero", "Geología"),
    ("encuadernacion", "Tapa dura"),
    ("encuadernacion", "Tapa blanda"),
    ("encuadernacion", "Sin encuadernación"),
    ("ilustraciones", "Contiene ilustraciones"),
    ("ilustraciones", "Ilustraciones en blanco y negro"),
    ("ilustraciones", "Profusamente ilustrado"),
    ("ilustraciones", "Profusamente ilustrado, en blanco y negro"),
    ("estado_stock", "Descatalogado"),
]
CORE_BOOKS_COLUMNS: tuple[str, ...] = (
    "id",
    "estado_stock",
    "estado_carga",
    "titulo",
    "titulo_corto",
    "subtitulo",
    "titulo_completo",
    "autor",
    "pais_autor",
    "editorial",
    "pais_publicacion",
    "anio",
    "isbn",
    "idioma",
    "edicion",
    "numero_impresion",
    "coleccion",
    "numero_coleccion",
    "obra_completa",
    "volumen",
    "traductor",
    "ilustrador",
    "editor",
    "fotografia_de",
    "introduccion_de",
    "epilogo_de",
    "categoria",
    "genero",
    "tipo_articulo",
    "ilustraciones",
    "encuadernacion",
    "detalle_encuadernacion",
    "estado_conservacion",
    "estado_cubierta",
    "desperfectos",
    "dedicatorias",
    "dimensiones",
    "alto",
    "ancho",
    "fondo",
    "peso",
    "unidad_peso",
    "paginas",
    "plantilla_envio",
    "palabras_clave",
    "catalogo_1",
    "catalogo_2",
    "catalogo_3",
    "url_imagenes",
    "precio",
    "cantidad",
    "descripcion",
)
CORE_BOOKS_EDITABLE_COLUMNS: tuple[str, ...] = tuple(column for column in CORE_BOOKS_COLUMNS if column != "id")
CORE_BOOKS_INT_FIELDS = {"numero_coleccion", "alto", "ancho", "fondo", "peso", "paginas", "cantidad"}
CORE_BOOKS_DECIMAL_FIELDS = {"precio"}


def normalize_block(value: str | None) -> str | None:
    text = str(value or "").strip().upper()
    if not text:
        return None
    if text not in VALID_BLOCKS:
        raise ValueError(f"Invalid block: {value}. Expected one of {', '.join(VALID_BLOCKS)}")
    return text


def normalize_module(value: str | None) -> str | None:
    text = str(value or "").strip()
    if not text:
        return None
    if not text.isdigit():
        raise ValueError(f"Invalid module: {value}. Expected numeric value between 01 and 99")
    number = int(text)
    if number < 1 or number > 99:
        raise ValueError(f"Invalid module: {value}. Expected value between 01 and 99")
    return f"{number:02d}"


def resolve_scope(
    block: str | None,
    module: str | None,
    *,
    require: bool = False,
) -> tuple[str | None, str | None]:
    normalized_block = normalize_block(block)
    normalized_module = normalize_module(module)

    if bool(normalized_block) != bool(normalized_module):
        raise ValueError("block and module must be provided together")

    if require and (not normalized_block or not normalized_module):
        raise ValueError("block and module are required")

    return normalized_block, normalized_module


def _load_json(value: Any, default: Any) -> Any:
    if value in (None, ""):
        return default
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(value)
    except (TypeError, json.JSONDecodeError):
        return default


def _normalize_extensions(extensions: list[str] | None = None) -> set[str]:
    if not extensions:
        extensions = [".jpg", ".jpeg", ".png", ".webp", ".heic"]
    normalized: set[str] = set()
    for ext in extensions:
        text = str(ext).strip().lower()
        if not text:
            continue
        normalized.add(text if text.startswith(".") else f".{text}")
    return normalized


def _resolve_covers_dir(covers_dir: str | Path) -> Path:
    path = Path(covers_dir).expanduser().resolve()
    if not path.exists() or not path.is_dir():
        raise ValueError(f"Folder not found: {path}")
    return path


def _iter_modules_from_structure(base: Path) -> list[tuple[str, str, Path]]:
    missing_blocks: list[str] = []
    invalid_entries: list[str] = []
    modules: list[tuple[str, str, Path]] = []

    for block in VALID_BLOCKS:
        block_dir = base / block
        if not block_dir.exists() or not block_dir.is_dir():
            missing_blocks.append(str(block_dir))
            continue

        for child in sorted(block_dir.iterdir()):
            if child.name.startswith("."):
                continue

            rel = str(child.relative_to(base))
            if not child.is_dir():
                invalid_entries.append(rel)
                continue

            if not MODULE_DIR_PATTERN.fullmatch(child.name):
                invalid_entries.append(rel)
                continue

            modules.append((block, child.name, child))

    for child in sorted(base.iterdir()):
        if child.name.startswith("."):
            continue
        if child.is_dir() and child.name in VALID_BLOCKS:
            continue
        invalid_entries.append(str(child.relative_to(base)))

    if missing_blocks:
        joined = ", ".join(missing_blocks)
        raise ValueError(
            "Invalid input structure. Missing required block folders under "
            f"{base}: {joined}. Expected structure: data/input/A|B|C/<module 01..99>"
        )

    if invalid_entries:
        sample = ", ".join(invalid_entries[:10])
        raise ValueError(
            "Invalid input structure entries found: "
            f"{sample}. Expected structure: data/input/A|B|C/<module 01..99>"
        )

    return modules


def _create_books_core_schema(con: Any) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS books (
            id VARCHAR PRIMARY KEY,
            estado_stock VARCHAR,
            estado_carga VARCHAR,
            titulo VARCHAR,
            titulo_corto VARCHAR,
            subtitulo VARCHAR,
            titulo_completo VARCHAR,
            autor VARCHAR,
            pais_autor VARCHAR,
            editorial VARCHAR,
            pais_publicacion VARCHAR,
            anio VARCHAR,
            isbn VARCHAR,
            idioma VARCHAR,
            edicion VARCHAR,
            numero_impresion VARCHAR,
            coleccion VARCHAR,
            numero_coleccion INTEGER,
            obra_completa VARCHAR,
            volumen VARCHAR,
            traductor VARCHAR,
            ilustrador VARCHAR,
            editor VARCHAR,
            fotografia_de VARCHAR,
            introduccion_de VARCHAR,
            epilogo_de VARCHAR,
            categoria VARCHAR,
            genero VARCHAR,
            tipo_articulo VARCHAR,
            ilustraciones VARCHAR,
            encuadernacion VARCHAR,
            detalle_encuadernacion VARCHAR,
            estado_conservacion VARCHAR,
            estado_cubierta VARCHAR,
            desperfectos VARCHAR,
            dedicatorias VARCHAR,
            dimensiones VARCHAR,
            alto SMALLINT DEFAULT 0,
            ancho INTEGER DEFAULT 0,
            fondo INTEGER DEFAULT 0,
            peso INTEGER DEFAULT 0,
            unidad_peso VARCHAR DEFAULT 'GRAMS',
            paginas INTEGER DEFAULT 0,
            plantilla_envio VARCHAR,
            palabras_clave VARCHAR,
            catalogo_1 VARCHAR,
            catalogo_2 VARCHAR,
            catalogo_3 VARCHAR,
            url_imagenes VARCHAR,
            precio DECIMAL(18, 2) DEFAULT 1.00,
            cantidad INTEGER DEFAULT 1,
            descripcion VARCHAR
        )
        """
    )

    con.execute("CREATE INDEX IF NOT EXISTS idx_books_idioma ON books(idioma)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_books_palabras_clave ON books(palabras_clave)")

    con.execute("CREATE SCHEMA IF NOT EXISTS ref")
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS ref.iso_639_3 (
            id VARCHAR,
            part2b VARCHAR,
            part2t VARCHAR,
            part1 VARCHAR,
            scope VARCHAR,
            language_yype VARCHAR,
            ref_name VARCHAR,
            comment VARCHAR,
            spa_name VARCHAR
        )
        """
    )

    # One-time migration from legacy main.iso_639_3 -> ref.iso_639_3.
    main_iso_exists = bool(
        con.execute(
            """
            SELECT COUNT(*) > 0
            FROM information_schema.tables
            WHERE table_schema = 'main' AND table_name = 'iso_639_3'
            """
        ).fetchone()[0]
    )
    if main_iso_exists:
        legacy_columns = {
            str(row[0]).strip().lower()
            for row in con.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'main' AND table_name = 'iso_639_3'
                """
            ).fetchall()
        }
        spa_col = "spa_name" if "spa_name" in legacy_columns else ("nombre_spa" if "nombre_spa" in legacy_columns else "NULL")
        con.execute(
            f"""
            INSERT INTO ref.iso_639_3 (id, part2b, part2t, part1, scope, language_yype, ref_name, comment, spa_name)
            SELECT id, part2b, part2t, part1, scope, language_yype, ref_name, comment, {spa_col}
            FROM main.iso_639_3
            WHERE NOT EXISTS (SELECT 1 FROM ref.iso_639_3)
            """
        )
        con.execute("DROP TABLE IF EXISTS main.iso_639_3")

    con.execute(
        """
        CREATE TABLE IF NOT EXISTS book_field_allowed_values (
            table_name VARCHAR,
            field_name VARCHAR,
            field_value VARCHAR,
            sort_order INTEGER DEFAULT 0,
            PRIMARY KEY (table_name, field_name, field_value)
        )
        """
    )
    con.execute("ALTER TABLE book_field_allowed_values ADD COLUMN IF NOT EXISTS sort_order INTEGER DEFAULT 0")
    con.execute("DELETE FROM book_field_allowed_values WHERE table_name = 'books'")
    con.executemany(
        "INSERT INTO book_field_allowed_values (table_name, field_name, field_value, sort_order) VALUES (?, ?, ?, ?)",
        [("books", field_name, field_value, index) for index, (field_name, field_value) in enumerate(BOOK_ALLOWED_VALUES)],
    )

    con.execute(
        """
        CREATE OR REPLACE VIEW libros_carga_abebooks AS
        SELECT
            b.id AS listingid,
            b.titulo AS title,
            b.autor AS author,
            b.editorial AS publishername,
            b.isbn AS isbn,
            CASE
                WHEN strpos(COALESCE(b.idioma, ''), ';') > 0 THEN 'MUL'
                ELSE (
                    SELECT upper(i.id)
                    FROM ref.iso_639_3 i
                    WHERE i.spa_name = b.idioma
                    LIMIT 1
                )
            END AS language,
            b.tipo_articulo AS producttype,
            b.encuadernacion AS bindingtext,
            b.estado_conservacion AS bookcondition,
            b.palabras_clave AS keywords,
            b.url_imagenes AS imgurl,
            b.precio AS price,
            b.cantidad AS quantity,
            b.descripcion AS description
        FROM books b
        WHERE b.estado_carga IN ('Para subir', 'Para actualizar')
        """
    )


def init_table() -> None:
    with get_connection() as con:
        # Operational state lives in `book_items`; core output schema is created in `books`.
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS book_items (
                id VARCHAR PRIMARY KEY,
                block VARCHAR,
                module VARCHAR,
                seq VARCHAR,

                ocr_status VARCHAR,
                ocr_error VARCHAR,
                ocr_provider VARCHAR,
                ocr_model VARCHAR,
                ocr_trace_json VARCHAR,

                metadata_status VARCHAR,
                metadata_error VARCHAR,

                catalog_json VARCHAR,
                catalog_status VARCHAR,
                catalog_error VARCHAR,

                cover_path VARCHAR,
                cover_status VARCHAR,
                cover_error VARCHAR,

                workflow_status VARCHAR DEFAULT 'pending',
                workflow_current_node VARCHAR,
                workflow_attempt INTEGER DEFAULT 0,
                workflow_needs_review BOOLEAN DEFAULT FALSE,
                workflow_review_reason VARCHAR,

                pipeline_stage VARCHAR DEFAULT 'ocr',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        # Keep schema coherent if legacy columns exist in an already-created DB.
        for legacy_column in ("image_path", "image_count", "credits_text", "isbn_raw", "isbn", "metadata_json"):
            try:
                con.execute(f"ALTER TABLE book_items DROP COLUMN IF EXISTS {legacy_column}")
            except Exception:
                pass

        con.execute(
            """
            CREATE TABLE IF NOT EXISTS book_image_files (
                book_id VARCHAR,
                n_imagen INTEGER,
                filename VARCHAR,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY(book_id, n_imagen)
            )
            """
        )

        con.execute(
            """
            CREATE TABLE IF NOT EXISTS book_ocr_data (
                book_id VARCHAR PRIMARY KEY,
                extracted_text VARCHAR,
                isbn_raw VARCHAR,
                isbn VARCHAR,
                isbn_list VARCHAR,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        con.execute(
            """
            CREATE TABLE IF NOT EXISTS book_bibliographic_sources (
                book_id VARCHAR,
                provider VARCHAR,
                isbn VARCHAR,
                payload_json VARCHAR,
                provider_status VARCHAR,
                provider_error VARCHAR,
                fetched_at VARCHAR,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY(book_id, provider)
            )
            """
        )

        _create_books_core_schema(con)



def _payload_column(payload_type: str) -> str:
    if payload_type == "catalog":
        return "catalog_json"
    if payload_type == "ocr_trace":
        return "ocr_trace_json"
    raise ValueError(f"Invalid payload_type: {payload_type}")


def _payload_default(default: Any) -> Any:
    if isinstance(default, dict):
        return {}
    if isinstance(default, list):
        return []
    return default


def _image_filename(raw_path: str) -> str:
    return Path(str(raw_path or "").strip()).name


def _resolve_image_file_path(*, block: str | None, module: str | None, filename: str | None) -> str | None:
    block_value = str(block or "").strip().upper()
    try:
        module_value = normalize_module(module)
    except ValueError:
        return None
    filename_value = str(filename or "").strip()
    if not block_value or not module_value or not filename_value:
        return None
    return str((DEFAULT_COVERS_DIR / block_value / module_value / filename_value).resolve())


def _upsert_ocr_data(
    *,
    book_id: str,
    credits_text: str | None,
    isbn_raw: str | None,
    isbn: str | None,
    trace: dict[str, Any] | list[Any] | None,
) -> None:
    candidates: list[str] = []
    if isinstance(trace, dict):
        extraction = trace.get("isbn_extraction")
        if isinstance(extraction, dict):
            raw_candidates = extraction.get("candidates")
            if isinstance(raw_candidates, list):
                candidates = [str(item).strip() for item in raw_candidates if str(item).strip()]

    if not candidates:
        if isbn:
            candidates = [str(isbn)]
        elif isbn_raw:
            candidates = [str(isbn_raw)]

    isbn_list = ";".join(candidates) if candidates else None

    with get_connection() as con:
        con.execute("DELETE FROM book_ocr_data WHERE book_id = ?", [book_id])
        con.execute(
            """
            INSERT INTO book_ocr_data (
                book_id,
                extracted_text, isbn_raw, isbn, isbn_list,
                created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """,
            [
                book_id,
                credits_text,
                isbn_raw,
                isbn,
                isbn_list,
            ],
        )


def _upsert_bibliographic_sources(*, book_id: str, metadata: dict[str, Any]) -> None:
    isbn = str(metadata.get("isbn") or "").strip() or None
    fetched_at = str(metadata.get("fetched_at") or "").strip() or None
    errors = metadata.get("errors") if isinstance(metadata.get("errors"), dict) else {}

    with get_connection() as con:
        for source_key, provider in METADATA_PROVIDER_MAP:
            payload = metadata.get(source_key) if isinstance(metadata.get(source_key), dict) else {}
            provider_error = str(errors.get(source_key) or "").strip() if isinstance(errors, dict) else ""
            provider_status = "fetched" if payload else ("error" if provider_error else "empty")

            con.execute("DELETE FROM book_bibliographic_sources WHERE book_id = ? AND provider = ?", [book_id, provider])
            con.execute(
                """
                INSERT INTO book_bibliographic_sources (
                    book_id, provider,
                    isbn, payload_json, provider_status, provider_error, fetched_at,
                    created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                """,
                [
                    book_id,
                    provider,
                    isbn,
                    json.dumps(payload, ensure_ascii=False),
                    provider_status,
                    provider_error or None,
                    fetched_at,
                ],
            )


def _empty_metadata(book_id: str, isbn: str | None = None) -> dict[str, Any]:
    return {
        "id": book_id,
        "isbn": str(isbn or "").strip() or None,
        "google": {},
        "open_library": {},
        "isbndb": {},
        "errors": {},
    }


def _metadata_from_rows(book_id: str, rows: list[tuple[Any, ...]]) -> dict[str, Any]:
    metadata = _empty_metadata(book_id)
    fetched_values: list[str] = []

    for row in rows:
        provider = str(row[0] or "").strip()
        source_key = METADATA_PROVIDER_REVERSE_MAP.get(provider)
        if not source_key:
            continue

        payload = _load_json(row[1], {})
        metadata[source_key] = payload if isinstance(payload, dict) else {}

        isbn = str(row[2] or "").strip()
        if isbn and not metadata.get("isbn"):
            metadata["isbn"] = isbn

        provider_error = str(row[3] or "").strip()
        if provider_error:
            errors = metadata.get("errors")
            if isinstance(errors, dict):
                errors[source_key] = provider_error
            else:
                metadata["errors"] = {source_key: provider_error}

        fetched_at = str(row[4] or "").strip()
        if fetched_at:
            fetched_values.append(fetched_at)

    if fetched_values:
        metadata["fetched_at"] = max(fetched_values)

    return metadata


def _load_metadata_from_sources(book_id: str) -> dict[str, Any]:
    with get_connection() as con:
        rows = con.execute(
            """
            SELECT provider, payload_json, isbn, provider_error, fetched_at
            FROM book_bibliographic_sources
            WHERE book_id = ?
            ORDER BY provider
            """,
            [book_id],
        ).fetchall()

    if not rows:
        return _empty_metadata(book_id)

    return _metadata_from_rows(book_id, rows)


def _fetch_metadata_map(book_ids: list[str]) -> dict[str, dict[str, Any]]:
    if not book_ids:
        return {}

    placeholders = ", ".join(["?"] * len(book_ids))
    query = (
        "SELECT book_id, provider, payload_json, isbn, provider_error, fetched_at "
        "FROM book_bibliographic_sources "
        f"WHERE book_id IN ({placeholders}) "
        "ORDER BY book_id, provider"
    )

    grouped: dict[str, list[tuple[Any, ...]]] = {}
    with get_connection() as con:
        rows = con.execute(query, book_ids).fetchall()

    for row in rows:
        book_id = str(row[0] or "").strip()
        if not book_id:
            continue
        grouped.setdefault(book_id, []).append((row[1], row[2], row[3], row[4], row[5]))

    return {book_id: _metadata_from_rows(book_id, grouped_rows) for book_id, grouped_rows in grouped.items()}


def _clear_ocr_data(book_id: str) -> None:
    with get_connection() as con:
        con.execute("DELETE FROM book_ocr_data WHERE book_id = ?", [book_id])


def _load_ocr_data(book_id: str) -> dict[str, Any]:
    with get_connection() as con:
        row = con.execute(
            """
            SELECT extracted_text, isbn_raw, isbn, isbn_list
            FROM book_ocr_data
            WHERE book_id = ?
            """,
            [book_id],
        ).fetchone()

    if not row:
        return {"credits_text": None, "isbn_raw": None, "isbn": None, "isbn_list": None}

    return {
        "credits_text": str(row[0]).strip() if row[0] is not None else None,
        "isbn_raw": str(row[1]).strip() if row[1] is not None else None,
        "isbn": str(row[2]).strip() if row[2] is not None else None,
        "isbn_list": str(row[3]).strip() if row[3] is not None else None,
    }


def _fetch_ocr_map(book_ids: list[str]) -> dict[str, dict[str, Any]]:
    if not book_ids:
        return {}

    placeholders = ", ".join(["?"] * len(book_ids))
    query = (
        "SELECT book_id, extracted_text, isbn_raw, isbn, isbn_list "
        "FROM book_ocr_data "
        f"WHERE book_id IN ({placeholders})"
    )

    mapped: dict[str, dict[str, Any]] = {}
    with get_connection() as con:
        rows = con.execute(query, book_ids).fetchall()

    for row in rows:
        book_id = str(row[0] or "").strip()
        if not book_id:
            continue
        mapped[book_id] = {
            "credits_text": str(row[1]).strip() if row[1] is not None else None,
            "isbn_raw": str(row[2]).strip() if row[2] is not None else None,
            "isbn": str(row[3]).strip() if row[3] is not None else None,
            "isbn_list": str(row[4]).strip() if row[4] is not None else None,
        }

    return mapped


def _clear_bibliographic_sources(book_id: str) -> None:
    with get_connection() as con:
        con.execute("DELETE FROM book_bibliographic_sources WHERE book_id = ?", [book_id])


def _replace_book_images(book_id: str, image_paths: list[str]) -> None:
    rows = [_image_filename(str(path)) for path in image_paths if str(path).strip()]
    rows = [row for row in rows if row]

    with get_connection() as con:
        con.execute("DELETE FROM book_image_files WHERE book_id = ?", [book_id])
        for position, filename in enumerate(rows, start=1):
            con.execute(
                """
                INSERT INTO book_image_files (
                    book_id, n_imagen, filename,
                    created_at, updated_at
                )
                VALUES (?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                """,
                [book_id, int(position), filename],
            )


def _clear_payload(book_id: str, payload_type: str) -> None:
    if payload_type not in PAYLOAD_TYPES:
        raise ValueError(f"Invalid payload_type: {payload_type}")

    column = _payload_column(payload_type)
    with get_connection() as con:
        con.execute(
            f"UPDATE book_items SET {column} = NULL, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
            [book_id],
        )


def _replace_payload(book_id: str, payload_type: str, payload: Any) -> None:
    if payload_type not in PAYLOAD_TYPES:
        raise ValueError(f"Invalid payload_type: {payload_type}")

    column = _payload_column(payload_type)
    serialized = json.dumps(payload if payload is not None else {}, ensure_ascii=False)
    with get_connection() as con:
        con.execute(
            f"UPDATE book_items SET {column} = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
            [serialized, book_id],
        )


def _load_book_images(book_id: str, *, fallback: list[str] | None = None) -> list[str]:
    parts = split_book_id(book_id)
    module_value = parts[0] if parts else None
    block_value = parts[1] if parts else None

    with get_connection() as con:
        cur = con.execute(
            """
            SELECT filename
            FROM book_image_files
            WHERE book_id = ?
            ORDER BY n_imagen
            """,
            [book_id],
        )

        rows: list[str] = []
        for row in cur.fetchall():
            filename = str(row[0] or "").strip()
            resolved = _resolve_image_file_path(block=block_value, module=module_value, filename=filename)
            if resolved:
                rows.append(resolved)

    if rows:
        return rows

    if fallback is None:
        return []

    return [str(path).strip() for path in fallback if str(path).strip()]


def _load_payload(book_id: str, payload_type: str, *, default: Any) -> Any:
    if payload_type not in PAYLOAD_TYPES:
        raise ValueError(f"Invalid payload_type: {payload_type}")

    column = _payload_column(payload_type)
    with get_connection() as con:
        cur = con.execute(
            f"SELECT {column} FROM book_items WHERE id = ?",
            [book_id],
        )
        row = cur.fetchone()

    if not row:
        return _payload_default(default)
    return _load_json(row[0], _payload_default(default))


def _fetch_image_map(book_ids: list[str]) -> dict[str, list[str]]:
    if not book_ids:
        return {}

    placeholders = ", ".join(["?"] * len(book_ids))
    query = (
        "SELECT book_id, filename "
        "FROM book_image_files "
        f"WHERE book_id IN ({placeholders}) "
        "ORDER BY book_id, n_imagen"
    )

    grouped: dict[str, list[str]] = {}
    with get_connection() as con:
        rows = con.execute(query, book_ids).fetchall()

    for row in rows:
        book_id = str(row[0])
        parts = split_book_id(book_id)
        module_value = parts[0] if parts else None
        block_value = parts[1] if parts else None
        filename = str(row[1] or "").strip()
        image_path = _resolve_image_file_path(block=block_value, module=module_value, filename=filename)
        if not image_path:
            continue
        grouped.setdefault(book_id, []).append(image_path)

    return grouped


def _fetch_payload_map(book_ids: list[str], payload_type: str, *, default: Any) -> dict[str, Any]:
    if payload_type not in PAYLOAD_TYPES:
        raise ValueError(f"Invalid payload_type: {payload_type}")

    if not book_ids:
        return {}

    column = _payload_column(payload_type)
    placeholders = ", ".join(["?"] * len(book_ids))
    query = (
        f"SELECT id, {column} "
        "FROM book_items "
        f"WHERE id IN ({placeholders}) "
        "ORDER BY id"
    )
    params: list[Any] = [*book_ids]
    payload_map: dict[str, Any] = {}
    with get_connection() as con:
        rows = con.execute(query, params).fetchall()

    for row in rows:
        book_id = str(row[0] or "").strip()
        if not book_id:
            continue
        payload_map[book_id] = _load_json(row[1], _payload_default(default))

    return payload_map


def _row_to_dict(
    columns: list[str],
    row: tuple[Any, ...],
    *,
    image_map: dict[str, list[str]] | None = None,
    metadata_map: dict[str, Any] | None = None,
    ocr_map: dict[str, dict[str, Any]] | None = None,
    catalog_map: dict[str, Any] | None = None,
    ocr_trace_map: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload = dict(zip(columns, row))
    book_id = str(payload.get("id") or "").strip()

    if image_map is not None and book_id in image_map:
        image_paths = image_map.get(book_id, [])
    else:
        image_paths = _load_book_images(book_id, fallback=[]) if book_id else []

    payload["image_path"] = image_paths[0] if image_paths else None
    payload["image_paths"] = image_paths
    payload["image_count"] = len(image_paths)

    if metadata_map is not None:
        payload["metadata"] = metadata_map.get(book_id, {})
    else:
        payload["metadata"] = _load_metadata_from_sources(book_id) if book_id else {}

    if ocr_map is not None:
        ocr_data = ocr_map.get(book_id, {"credits_text": None, "isbn_raw": None, "isbn": None, "isbn_list": None})
    else:
        ocr_data = _load_ocr_data(book_id) if book_id else {"credits_text": None, "isbn_raw": None, "isbn": None, "isbn_list": None}
    payload["credits_text"] = ocr_data.get("credits_text")
    payload["isbn_raw"] = ocr_data.get("isbn_raw")
    payload["isbn"] = ocr_data.get("isbn")
    payload["isbn_list"] = ocr_data.get("isbn_list")

    if catalog_map is not None:
        payload["catalog"] = catalog_map.get(book_id, {})
    else:
        payload["catalog"] = _load_payload(book_id, "catalog", default={}) if book_id else {}

    if ocr_trace_map is not None:
        payload["ocr_trace"] = ocr_trace_map.get(book_id, {})
    else:
        payload["ocr_trace"] = _load_payload(book_id, "ocr_trace", default={}) if book_id else {}

    return payload


def get_book(book_id: str) -> dict[str, Any] | None:
    normalized = normalize_book_id(book_id)
    if not normalized:
        return None

    with get_connection() as con:
        cur = con.execute("SELECT * FROM book_items WHERE id = ?", [normalized])
        row = cur.fetchone()
        if not row:
            return None
        columns = [desc[0] for desc in cur.description]

    return _row_to_dict(columns, row)


def list_books(
    stage: str | None = None,
    limit: int = 500,
    *,
    block: str | None = None,
    module: str | None = None,
) -> list[dict[str, Any]]:
    scope_block, scope_module = resolve_scope(block, module, require=False)

    sql = "SELECT * FROM book_items"
    where: list[str] = []
    params: list[Any] = []

    normalized_stage = str(stage or "").strip().lower()
    if normalized_stage:
        if normalized_stage == "needs_workflow_review":
            where.append("workflow_needs_review = TRUE")
        else:
            where.append("pipeline_stage = ?")
            params.append(normalized_stage)

    if scope_block:
        where.append("block = ?")
        params.append(scope_block)
    if scope_module:
        where.append("module = ?")
        params.append(scope_module)

    if where:
        sql += " WHERE " + " AND ".join(where)

    sql += " ORDER BY id LIMIT ?"
    params.append(int(limit))

    with get_connection() as con:
        cur = con.execute(sql, params)
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]

    if not rows:
        return []

    id_index = columns.index("id")
    book_ids = [str(row[id_index]) for row in rows if str(row[id_index]).strip()]

    image_map = _fetch_image_map(book_ids)
    metadata_map = _fetch_metadata_map(book_ids)
    ocr_map = _fetch_ocr_map(book_ids)
    catalog_map = _fetch_payload_map(book_ids, "catalog", default={})
    ocr_trace_map = _fetch_payload_map(book_ids, "ocr_trace", default={})

    return [
        _row_to_dict(
            columns,
            row,
            image_map=image_map,
            metadata_map=metadata_map,
            ocr_map=ocr_map,
            catalog_map=catalog_map,
            ocr_trace_map=ocr_trace_map,
        )
        for row in rows
    ]


def _derive_pipeline_stage_from_dict(book: dict[str, Any]) -> str:
    if bool(book.get("workflow_needs_review")):
        return "review"

    workflow_status = str(book.get("workflow_status") or "").strip().lower()
    current_node = str(book.get("workflow_current_node") or "").strip()
    if workflow_status == "running":
        return f"running:{current_node}" if current_node else "running"

    cover_status = str(book.get("cover_status") or "").strip().lower()
    if cover_status in {"downloaded", "missing", "skipped"}:
        return "done"

    catalog_status = str(book.get("catalog_status") or "").strip().lower()
    if catalog_status in {"built", "partial", "manual"}:
        return "cover"

    metadata_status = str(book.get("metadata_status") or "").strip().lower()
    if metadata_status in {"fetched", "partial", "skipped", "manual"}:
        return "catalog"

    ocr_status = str(book.get("ocr_status") or "").strip().lower()
    if ocr_status in {"processed", "skipped", "manual"}:
        return "metadata"

    return "ocr"


def refresh_pipeline_stage(book_id: str) -> None:
    book = get_book(book_id)
    if book is None:
        return

    stage = _derive_pipeline_stage_from_dict(book)
    with get_connection() as con:
        con.execute(
            "UPDATE book_items SET pipeline_stage = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
            [stage, book_id],
        )


def _update_book(book_id: str, fields: dict[str, Any]) -> None:
    if not fields:
        return

    assignments = []
    params: list[Any] = []
    for key, value in fields.items():
        assignments.append(f"{key} = ?")
        params.append(value)

    assignments.append("updated_at = CURRENT_TIMESTAMP")
    params.append(book_id)

    with get_connection() as con:
        con.execute(
            f"UPDATE book_items SET {', '.join(assignments)} WHERE id = ?",
            params,
        )


def set_workflow_running(book_id: str, *, node: str, action: str | None = None) -> None:
    _update_book(
        book_id,
        {
            "workflow_status": "running",
            "workflow_current_node": node,
            "workflow_needs_review": False,
            "workflow_review_reason": None,
            "pipeline_stage": f"running:{node}" if node else "running",
        },
    )


def set_workflow_pending(book_id: str, *, node: str, reason: str | None = None) -> None:
    _update_book(
        book_id,
        {
            "workflow_status": "pending",
            "workflow_current_node": node,
            "workflow_needs_review": False,
            "workflow_review_reason": reason,
        },
    )
    refresh_pipeline_stage(book_id)


def set_workflow_error(book_id: str, *, node: str, error: str) -> None:
    _update_book(
        book_id,
        {
            "workflow_status": "error",
            "workflow_current_node": node,
            "workflow_review_reason": error,
        },
    )


def set_workflow_review(
    book_id: str,
    *,
    node: str,
    reason: str | None = None,
    error: str | None = None,
) -> None:
    text = (reason or error or "Manual review requested").strip()
    _update_book(
        book_id,
        {
            "workflow_status": "review",
            "workflow_current_node": node,
            "workflow_needs_review": True,
            "workflow_review_reason": text,
            "pipeline_stage": "review",
        },
    )


def clear_workflow_review(book_id: str) -> None:
    _update_book(
        book_id,
        {
            "workflow_status": "pending",
            "workflow_needs_review": False,
            "workflow_review_reason": None,
        },
    )
    refresh_pipeline_stage(book_id)


def set_workflow_done(book_id: str, *, node: str) -> None:
    _update_book(
        book_id,
        {
            "workflow_status": "done",
            "workflow_current_node": node,
            "workflow_needs_review": False,
            "workflow_review_reason": None,
            "pipeline_stage": "done",
        },
    )


def increment_workflow_attempt(book_id: str) -> int:
    with get_connection() as con:
        cur = con.execute(
            """
            UPDATE book_items
            SET workflow_attempt = COALESCE(workflow_attempt, 0) + 1,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            RETURNING workflow_attempt
            """,
            [book_id],
        )
        row = cur.fetchone()
    return int(row[0]) if row else 0


def reset_workflow_attempt(book_id: str) -> None:
    _update_book(book_id, {"workflow_attempt": 0})


def reset_from_stage(book_id: str, stage: str) -> None:
    normalized_stage = str(stage or "").strip().lower()
    if normalized_stage not in STAGES:
        raise ValueError(f"Invalid stage: {stage}")

    fields: dict[str, Any] = {
        "workflow_status": "pending",
        "workflow_current_node": None,
        "workflow_needs_review": False,
        "workflow_review_reason": None,
    }

    if normalized_stage == "ocr":
        fields.update(
            {
                "ocr_status": None,
                "ocr_error": None,
                "ocr_provider": None,
                "ocr_model": None,
                "metadata_status": None,
                "metadata_error": None,
                "catalog_status": None,
                "catalog_error": None,
                "cover_path": None,
                "cover_status": None,
                "cover_error": None,
            }
        )
        _clear_payload(book_id, "ocr_trace")
        _clear_payload(book_id, "catalog")
        _clear_bibliographic_sources(book_id)
        _clear_ocr_data(book_id)
    elif normalized_stage == "metadata":
        fields.update(
            {
                "metadata_status": None,
                "metadata_error": None,
                "catalog_status": None,
                "catalog_error": None,
                "cover_path": None,
                "cover_status": None,
                "cover_error": None,
            }
        )
        _clear_payload(book_id, "catalog")
        _clear_bibliographic_sources(book_id)
    elif normalized_stage == "catalog":
        fields.update(
            {
                "catalog_status": None,
                "catalog_error": None,
                "cover_path": None,
                "cover_status": None,
                "cover_error": None,
            }
        )
        _clear_payload(book_id, "catalog")
    elif normalized_stage == "cover":
        fields.update(
            {
                "cover_path": None,
                "cover_status": None,
                "cover_error": None,
            }
        )

    _update_book(book_id, fields)
    refresh_pipeline_stage(book_id)


def recover_stale_running_workflows(*, reason: str = "Recovered after backend restart") -> int:
    with get_connection() as con:
        cur = con.execute(
            """
            UPDATE book_items
            SET workflow_status = 'pending',
                workflow_current_node = 'recovered',
                workflow_review_reason = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE workflow_status = 'running'
            RETURNING id
            """,
            [reason],
        )
        rows = cur.fetchall()

    for row in rows:
        refresh_pipeline_stage(str(row[0]))

    return len(rows)


def _image_files(folder: Path, recursive: bool, extensions: set[str]) -> list[Path]:
    if recursive:
        candidates = folder.rglob("*")
    else:
        candidates = folder.glob("*")

    files = [
        path.resolve()
        for path in candidates
        if path.is_file() and path.suffix.lower() in extensions
    ]
    return sorted(files)


def ingest_covers(
    folder: str | Path,
    *,
    recursive: bool = True,
    extensions: list[str] | None = None,
    overwrite_existing_paths: bool = False,
    block: str | None = None,
    module: str | None = None,
) -> dict[str, Any]:
    base = _resolve_covers_dir(folder)
    scope_block, scope_module = resolve_scope(block, module, require=False)
    valid_ext = _normalize_extensions(extensions)

    modules = _iter_modules_from_structure(base)
    if scope_block and scope_module:
        modules = [item for item in modules if item[0] == scope_block and item[1] == scope_module]
        if not modules:
            raise ValueError(f"Module not found in folder structure: {scope_block}/{scope_module}")

    grouped: dict[str, list[str]] = {}
    skipped_invalid = 0
    skipped_invalid_examples: list[str] = []
    skipped_scope_mismatch = 0
    skipped_scope_mismatch_examples: list[str] = []
    files_found = 0

    for module_block, module_name, module_path in modules:
        files = _image_files(module_path, recursive=recursive, extensions=valid_ext)
        files_found += len(files)

        for file_path in files:
            book_id = extract_book_id_from_path(file_path)
            if not book_id:
                skipped_invalid += 1
                if len(skipped_invalid_examples) < 30:
                    skipped_invalid_examples.append(str(file_path.relative_to(base)))
                continue

            parts = split_book_id(book_id)
            if parts is None:
                skipped_invalid += 1
                if len(skipped_invalid_examples) < 30:
                    skipped_invalid_examples.append(str(file_path.relative_to(base)))
                continue

            id_module, id_block, _ = parts
            if id_block != module_block or id_module != module_name:
                skipped_scope_mismatch += 1
                if len(skipped_scope_mismatch_examples) < 30:
                    skipped_scope_mismatch_examples.append(str(file_path.relative_to(base)))
                continue

            grouped.setdefault(book_id, []).append(str(file_path))

    inserted = 0
    updated = 0

    for book_id, image_paths in grouped.items():
        image_paths = sorted(set(image_paths))
        parts = split_book_id(book_id)
        block_value = parts[1] if parts else None
        module_value = parts[0] if parts else None
        seq = parts[2] if parts else None

        current = get_book(book_id)
        if current is None:
            with get_connection() as con:
                con.execute(
                    """
                    INSERT INTO book_items (
                        id, block, module, seq,
                        pipeline_stage, workflow_status, workflow_attempt,
                        created_at, updated_at
                    )
                    VALUES (?, ?, ?, ?, 'ocr', 'pending', 0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                    """,
                    [
                        book_id,
                        block_value,
                        module_value,
                        seq,
                    ],
                )

            _replace_book_images(book_id, image_paths)
            inserted += 1
            continue

        previous_paths = [str(path) for path in current.get("image_paths", []) if str(path).strip()]
        if overwrite_existing_paths:
            merged_paths = image_paths
        else:
            merged_paths = sorted(set(previous_paths) | set(image_paths))

        changed = merged_paths != previous_paths
        fields = {
            "block": block_value,
            "module": module_value,
            "seq": seq,
        }

        _update_book(book_id, fields)
        _replace_book_images(book_id, merged_paths)

        if changed:
            reset_from_stage(book_id, "ocr")
            updated += 1

    return {
        "folder": str(base),
        "scope_block": scope_block,
        "scope_module": scope_module,
        "modules_scanned": [f"{item[0]}/{item[1]}" for item in modules],
        "files_found": files_found,
        "books_detected": len(grouped),
        "inserted": inserted,
        "updated": updated,
        "skipped_invalid": skipped_invalid,
        "skipped_invalid_examples": skipped_invalid_examples,
        "skipped_scope_mismatch": skipped_scope_mismatch,
        "skipped_scope_mismatch_examples": skipped_scope_mismatch_examples,
    }


def ensure_local_image_path(book_id: str) -> str | None:
    book = get_book(book_id)
    if not book:
        return None

    paths: list[str] = []
    for value in book.get("image_paths", []):
        text = str(value).strip()
        if text and text not in paths:
            paths.append(text)

    for raw in paths:
        if not raw:
            continue
        path = Path(raw)
        if path.exists() and path.is_file():
            return str(path.resolve())

    return None


def update_ocr(
    book_id: str,
    *,
    credits_text: str | None,
    isbn_raw: str | None,
    isbn: str | None,
    status: str,
    provider: str | None = None,
    model: str | None = None,
    trace: dict[str, Any] | list[Any] | None = None,
    error: str | None = None,
) -> None:
    _update_book(
        book_id,
        {
            "ocr_status": status,
            "ocr_error": error,
            "ocr_provider": provider,
            "ocr_model": model,
        },
    )
    _replace_payload(book_id, "ocr_trace", trace if trace is not None else {})
    _upsert_ocr_data(
        book_id=book_id,
        credits_text=credits_text,
        isbn_raw=isbn_raw,
        isbn=isbn,
        trace=trace,
    )
    refresh_pipeline_stage(book_id)


def update_metadata(book_id: str, *, metadata: dict[str, Any], status: str, error: str | None = None) -> None:
    _update_book(
        book_id,
        {
            "metadata_status": status,
            "metadata_error": error,
        },
    )
    _upsert_bibliographic_sources(book_id=book_id, metadata=metadata)
    refresh_pipeline_stage(book_id)


def update_catalog(book_id: str, *, catalog: dict[str, Any], status: str, error: str | None = None) -> None:
    _update_book(
        book_id,
        {
            "catalog_status": status,
            "catalog_error": error,
        },
    )
    _replace_payload(book_id, "catalog", catalog)
    if status in {"built", "partial", "manual"} and isinstance(catalog, dict):
        sync_core_book_from_catalog(book_id)
    refresh_pipeline_stage(book_id)


def update_cover(book_id: str, *, cover_path: str | None, status: str, error: str | None = None) -> None:
    _update_book(
        book_id,
        {
            "cover_path": cover_path,
            "cover_status": status,
            "cover_error": error,
        },
    )
    refresh_pipeline_stage(book_id)


def books_for_stage(
    limit: int,
    *,
    stage: str,
    overwrite: bool,
    block: str | None = None,
    module: str | None = None,
) -> list[dict[str, Any]]:
    normalized_stage = str(stage or "").strip().lower()
    if normalized_stage not in STAGES:
        raise ValueError(f"Invalid stage: {stage}")

    scope_block, scope_module = resolve_scope(block, module, require=False)

    where = ["workflow_needs_review = FALSE"]
    params: list[Any] = []

    if scope_block:
        where.append("block = ?")
        params.append(scope_block)
    if scope_module:
        where.append("module = ?")
        params.append(scope_module)

    if not overwrite:
        if normalized_stage == "ocr":
            where.append("COALESCE(ocr_status, '') NOT IN ('processed', 'manual', 'skipped')")
        elif normalized_stage == "metadata":
            where.append("COALESCE(metadata_status, '') NOT IN ('fetched', 'partial', 'manual', 'skipped')")
        elif normalized_stage == "catalog":
            where.append("COALESCE(catalog_status, '') NOT IN ('built', 'partial', 'manual')")
        elif normalized_stage == "cover":
            where.append("COALESCE(cover_status, '') NOT IN ('downloaded', 'missing', 'skipped')")

    sql = "SELECT id FROM book_items"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY id LIMIT ?"
    params.append(int(limit))

    with get_connection() as con:
        cur = con.execute(sql, params)
        rows = [str(row[0]) for row in cur.fetchall()]

    return [{"id": book_id} for book_id in rows]


def book_ids_for_workflow(
    *,
    limit: int,
    start_stage: str,
    overwrite: bool,
    block: str | None = None,
    module: str | None = None,
) -> list[str]:
    rows = books_for_stage(
        limit,
        stage=start_stage,
        overwrite=overwrite,
        block=block,
        module=module,
    )
    return [str(row.get("id")) for row in rows if str(row.get("id") or "").strip()]


def _json_safe_db_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    return value


def _as_clean_text(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _list_to_text(value: Any, *, separator: str = "; ") -> str | None:
    if isinstance(value, list):
        items = [str(item).strip() for item in value if str(item).strip()]
        return separator.join(items) if items else None
    text = str(value or "").strip()
    return text or None


def _first_image_filename(book_id: str) -> str | None:
    with get_connection() as con:
        row = con.execute(
            """
            SELECT filename
            FROM book_image_files
            WHERE book_id = ?
            ORDER BY n_imagen
            LIMIT 1
            """,
            [book_id],
        ).fetchone()
    if not row:
        return None
    filename = str(row[0] or "").strip()
    return filename or None


def get_books_allowed_values() -> dict[str, list[str]]:
    with get_connection() as con:
        rows = con.execute(
            """
            SELECT field_name, field_value
            FROM book_field_allowed_values
            WHERE table_name = 'books'
            ORDER BY field_name, sort_order, field_value
            """
        ).fetchall()

    grouped: dict[str, list[str]] = {}
    for field_name, field_value in rows:
        name = str(field_name or "").strip()
        value = str(field_value or "").strip()
        if not name or not value:
            continue
        grouped.setdefault(name, [])
        if value not in grouped[name]:
            grouped[name].append(value)
    return grouped


def _normalize_core_input_value(field: str, value: Any) -> Any:
    if field not in CORE_BOOKS_EDITABLE_COLUMNS:
        raise ValueError(f"Field is not editable: {field}")

    if value is None:
        return None

    if field in CORE_BOOKS_INT_FIELDS:
        text = str(value).strip()
        if text == "":
            return None
        try:
            return int(float(text))
        except (TypeError, ValueError):
            raise ValueError(f"Invalid integer value for {field}: {value}") from None

    if field in CORE_BOOKS_DECIMAL_FIELDS:
        text = str(value).strip().replace(",", ".")
        text = text.replace("€", "").strip()
        if text == "":
            return None
        try:
            return round(float(text), 2)
        except (TypeError, ValueError):
            raise ValueError(f"Invalid decimal value for {field}: {value}") from None

    text = str(value).strip()
    return text or None


def _split_unique_values(value: Any) -> list[str]:
    text = str(value or "").strip()
    if not text:
        return []
    chunks = re.split(r"[;\n]+", text)
    output: list[str] = []
    seen: set[str] = set()
    for chunk in chunks:
        item = str(chunk).strip()
        key = item.lower()
        if not item or key in seen:
            continue
        seen.add(key)
        output.append(item)
    return output


def _format_names(value: Any) -> str:
    names: list[str] = []
    seen: set[str] = set()
    for raw in _split_unique_values(value):
        if "," in raw:
            left, right = raw.split(",", maxsplit=1)
            normalized = f"{right.strip()} {left.strip()}".strip()
        else:
            normalized = raw

        key = normalized.lower()
        if not normalized or key in seen:
            continue
        seen.add(key)
        names.append(normalized)

    if not names:
        return ""
    if len(names) == 1:
        return names[0]
    if len(names) == 2:
        return f"{names[0]} y {names[1]}"
    return f"{', '.join(names[:-1])} y {names[-1]}"


def _format_volume(value: Any, *, with_collection_title: bool) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    lowered = text.lower()
    if "vol" in lowered or "tomo" in lowered:
        return text
    if with_collection_title:
        return f"vol. {text}"
    return f"Volumen {text}"


def build_core_description(record: dict[str, Any]) -> str:
    author_country_items = _split_unique_values(record.get("pais_autor"))
    author_countries = ", ".join(author_country_items)
    author_country_keys = {item.lower() for item in author_country_items}
    publication_country = _as_clean_text(record.get("pais_publicacion"))

    parts: list[str] = []

    categoria = _as_clean_text(record.get("categoria"))
    if categoria:
        parts.append(f"{categoria}.")

    idioma = _as_clean_text(record.get("idioma"))
    if idioma and idioma.lower() != "español":
        if ";" in idioma:
            parts.append(f"Idiomas: {idioma.replace('; ', ', ')}.")
        else:
            parts.append(f"Idioma: {idioma}.")

    genero = _as_clean_text(record.get("genero"))
    if genero:
        parts.append(f"{genero}.")

    if author_countries:
        parts.append(f"{author_countries}.")

    if publication_country and publication_country.lower() not in author_country_keys:
        parts.append(f"{publication_country}.")

    editor = _format_names(record.get("editor"))
    if editor:
        parts.append(f"Edición a cargo de {editor}.")

    coleccion = _as_clean_text(record.get("coleccion"))
    numero_coleccion = _as_clean_text(record.get("numero_coleccion"))
    if coleccion:
        if numero_coleccion:
            parts.append(f"{coleccion}, nº {numero_coleccion}.")
        else:
            parts.append(f"{coleccion}.")

    editorial = _as_clean_text(record.get("editorial"))
    if editorial:
        parts.append(f"{editorial}.")

    anio = _as_clean_text(record.get("anio"))
    if anio:
        parts.append(f"{anio}.")

    edicion = _as_clean_text(record.get("edicion"))
    if edicion:
        parts.append(f"{edicion.replace('; ', ', ')}.")

    numero_impresion = _as_clean_text(record.get("numero_impresion"))
    if numero_impresion:
        parts.append(f"{numero_impresion}.")

    obra_completa = _as_clean_text(record.get("obra_completa"))
    volumen = _as_clean_text(record.get("volumen"))
    titulo = _as_clean_text(record.get("titulo"))
    if obra_completa and obra_completa != titulo:
        volume_text = _format_volume(volumen, with_collection_title=True) if volumen else ""
        if volume_text:
            parts.append(f"{obra_completa}, {volume_text}.")
        else:
            parts.append(f"{obra_completa}.")
    elif volumen:
        parts.append(f"{_format_volume(volumen, with_collection_title=False)}.")

    paginas = _as_clean_text(record.get("paginas"))
    if paginas:
        parts.append(f"{paginas} págs.")

    dimensiones = _as_clean_text(record.get("dimensiones"))
    if dimensiones:
        parts.append(f"{dimensiones}.")

    peso = _as_clean_text(record.get("peso"))
    if peso:
        parts.append(f"{peso} g.")

    detalle_encuadernacion = _as_clean_text(record.get("detalle_encuadernacion"))
    if detalle_encuadernacion:
        parts.append(f"{detalle_encuadernacion}.")

    desperfectos = _as_clean_text(record.get("desperfectos"))
    if desperfectos:
        parts.append(f"{desperfectos}.")

    introduccion = _format_names(record.get("introduccion_de"))
    if introduccion:
        parts.append(f"Introducción de {introduccion}.")

    epilogo = _format_names(record.get("epilogo_de"))
    if epilogo:
        parts.append(f"Epílogo de {epilogo}.")

    traductor = _format_names(record.get("traductor"))
    if traductor:
        parts.append(f"Traducción de {traductor}.")

    ilustraciones = _as_clean_text(record.get("ilustraciones"))
    if ilustraciones:
        parts.append(f"{ilustraciones}.")

    fotografia = _format_names(record.get("fotografia_de"))
    if fotografia:
        parts.append(f"Fotografía de {fotografia}.")

    ilustrador = _format_names(record.get("ilustrador"))
    if ilustrador:
        parts.append(f"Ilustraciones de {ilustrador}.")

    description = " ".join(parts).strip()
    book_id = _as_clean_text(record.get("id"))
    if book_id:
        suffix = f"Nº de ref. del artículo: {book_id}"
        if description:
            description = f"{description}\n\n{suffix}"
        else:
            description = suffix

    return description


def _core_autofill_fields_from_catalog(book_id: str, book: dict[str, Any]) -> dict[str, Any]:
    catalog = book.get("catalog") if isinstance(book.get("catalog"), dict) else {}
    metadata = book.get("metadata") if isinstance(book.get("metadata"), dict) else {}

    titulo = _as_clean_text(catalog.get("titulo"))
    subtitulo = _as_clean_text(catalog.get("subtitulo"))
    titulo_completo = _as_clean_text(catalog.get("titulo_completo"))
    if not titulo_completo and titulo and subtitulo:
        titulo_completo = f"{titulo}: {subtitulo}"
    elif not titulo_completo:
        titulo_completo = titulo

    autor = _list_to_text(catalog.get("autor"), separator="; ")
    idioma = _list_to_text(catalog.get("idioma"), separator="; ")
    palabras = _list_to_text(catalog.get("palabras_clave"), separator=", ")

    isbn = _as_clean_text(catalog.get("isbn")) or _as_clean_text(book.get("isbn")) or _as_clean_text(metadata.get("isbn"))
    image_filename = _first_image_filename(book_id)

    pages = catalog.get("paginas")
    pages_int: int | None = None
    if isinstance(pages, int):
        pages_int = pages
    else:
        pages_text = _as_clean_text(pages)
        if pages_text:
            try:
                pages_int = int(float(pages_text))
            except (TypeError, ValueError):
                pages_int = None

    def _as_int_value(value: Any) -> int | None:
        if isinstance(value, int):
            return value
        text = _as_clean_text(value)
        if not text:
            return None
        try:
            return int(float(text))
        except (TypeError, ValueError):
            return None

    return {
        "id": book_id,
        "titulo": titulo,
        "titulo_corto": titulo,
        "subtitulo": subtitulo,
        "titulo_completo": titulo_completo,
        "autor": autor,
        "pais_autor": _list_to_text(catalog.get("pais_autor"), separator="; "),
        "editorial": _as_clean_text(catalog.get("editorial")),
        "pais_publicacion": _as_clean_text(catalog.get("pais_publicacion")),
        "anio": _as_clean_text(catalog.get("anio")),
        "isbn": isbn,
        "idioma": idioma,
        "edicion": _list_to_text(catalog.get("edicion"), separator="; "),
        "numero_impresion": _as_clean_text(catalog.get("numero_impresion")),
        "coleccion": _as_clean_text(catalog.get("coleccion")),
        "numero_coleccion": _as_int_value(catalog.get("numero_coleccion")),
        "obra_completa": _as_clean_text(catalog.get("obra_completa")),
        "volumen": _as_clean_text(catalog.get("volumen")),
        "traductor": _list_to_text(catalog.get("traductor"), separator="; "),
        "ilustrador": _list_to_text(catalog.get("ilustrador"), separator="; "),
        "editor": _list_to_text(catalog.get("editor"), separator="; "),
        "fotografia_de": _list_to_text(catalog.get("fotografia_de"), separator="; "),
        "introduccion_de": _list_to_text(catalog.get("introduccion_de"), separator="; "),
        "epilogo_de": _list_to_text(catalog.get("epilogo_de"), separator="; "),
        "categoria": _as_clean_text(catalog.get("categoria")),
        "genero": _as_clean_text(catalog.get("genero")),
        "ilustraciones": _as_clean_text(catalog.get("ilustraciones")),
        "encuadernacion": _as_clean_text(catalog.get("encuadernacion")),
        "paginas": pages_int,
        "palabras_clave": palabras,
        "alto": _as_int_value(catalog.get("alto")),
        "ancho": _as_int_value(catalog.get("ancho")),
        "fondo": _as_int_value(catalog.get("fondo")),
        "peso": _as_int_value(catalog.get("peso")),
        "url_imagenes": image_filename,
        "tipo_articulo": "Libros",
        "estado_stock": "En venta",
        "estado_carga": "Para subir",
        "cantidad": 1,
        "precio": 1.00,
        "unidad_peso": "GRAMS",
    }


def sync_core_book_from_catalog(book_id: str) -> dict[str, Any] | None:
    normalized_id = normalize_book_id(book_id)
    if not normalized_id:
        return None

    book = get_book(normalized_id)
    if not book:
        return None

    values = _core_autofill_fields_from_catalog(normalized_id, book)

    with get_connection() as con:
        existing_cur = con.execute(
            f"SELECT {', '.join(CORE_BOOKS_COLUMNS)} FROM books WHERE id = ?",
            [normalized_id],
        )
        existing_row = existing_cur.fetchone()
        if existing_row is None:
            insert_columns = [column for column in CORE_BOOKS_COLUMNS if values.get(column) not in (None, "")]
            if "id" not in insert_columns:
                insert_columns = ["id", *insert_columns]
            insert_params = [values.get(column) for column in insert_columns]
            placeholders = ", ".join(["?"] * len(insert_columns))
            con.execute(
                f"INSERT INTO books ({', '.join(insert_columns)}) VALUES ({placeholders})",
                insert_params,
            )
        else:
            existing = {column: existing_row[index] for index, column in enumerate(CORE_BOOKS_COLUMNS)}
            updates: dict[str, Any] = {}
            for column, value in values.items():
                if column == "id":
                    continue
                if value in (None, ""):
                    continue
                current = existing.get(column)
                if current in (None, ""):
                    updates[column] = value

            if updates:
                assignments = ", ".join(f"{column} = ?" for column in updates)
                params = [updates[column] for column in updates]
                params.append(normalized_id)
                con.execute(f"UPDATE books SET {assignments} WHERE id = ?", params)

        cur = con.execute(
            f"SELECT {', '.join(CORE_BOOKS_COLUMNS)} FROM books WHERE id = ?",
            [normalized_id],
        )
        row = cur.fetchone()

    if not row:
        return None
    return {column: _json_safe_db_value(row[index]) for index, column in enumerate(CORE_BOOKS_COLUMNS)}


def bootstrap_core_books(
    *,
    block: str | None,
    module: str | None,
    limit: int = 2000,
) -> dict[str, Any]:
    scope_block, scope_module = resolve_scope(block, module, require=False)

    sql = (
        "SELECT id FROM book_items "
        "WHERE COALESCE(catalog_status, '') IN ('built', 'partial', 'manual')"
    )
    params: list[Any] = []
    if scope_block:
        sql += " AND block = ?"
        params.append(scope_block)
    if scope_module:
        sql += " AND module = ?"
        params.append(scope_module)
    sql += " ORDER BY id LIMIT ?"
    params.append(int(limit))

    with get_connection() as con:
        ids = [str(row[0]) for row in con.execute(sql, params).fetchall() if str(row[0]).strip()]

    inserted_or_updated = 0
    for book_id in ids:
        result = sync_core_book_from_catalog(book_id)
        if result is not None:
            inserted_or_updated += 1

    return {
        "scope_block": scope_block,
        "scope_module": scope_module,
        "requested": len(ids),
        "upserted": inserted_or_updated,
    }


def list_core_books(
    *,
    limit: int = 500,
    block: str | None = None,
    module: str | None = None,
) -> list[dict[str, Any]]:
    scope_block, scope_module = resolve_scope(block, module, require=False)

    sql = (
        "SELECT b.id, b.titulo, b.autor, b.editorial, b.estado_stock, b.estado_carga, b.precio, "
        "bi.block, bi.module "
        "FROM books b "
        "LEFT JOIN book_items bi ON bi.id = b.id "
    )
    where: list[str] = []
    params: list[Any] = []
    if scope_block:
        where.append("bi.block = ?")
        params.append(scope_block)
    if scope_module:
        where.append("bi.module = ?")
        params.append(scope_module)
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY b.id LIMIT ?"
    params.append(int(limit))

    with get_connection() as con:
        rows = con.execute(sql, params).fetchall()

    output: list[dict[str, Any]] = []
    for row in rows:
        output.append(
            {
                "id": str(row[0] or "").strip(),
                "titulo": _as_clean_text(row[1]),
                "autor": _as_clean_text(row[2]),
                "editorial": _as_clean_text(row[3]),
                "estado_stock": _as_clean_text(row[4]),
                "estado_carga": _as_clean_text(row[5]),
                "precio": _json_safe_db_value(row[6]),
                "block": _as_clean_text(row[7]),
                "module": _as_clean_text(row[8]),
            }
        )
    return output


def get_core_book(book_id: str, *, bootstrap: bool = True) -> dict[str, Any] | None:
    normalized_id = normalize_book_id(book_id)
    if not normalized_id:
        return None

    if bootstrap:
        sync_core_book_from_catalog(normalized_id)

    with get_connection() as con:
        cur = con.execute(
            f"SELECT {', '.join(CORE_BOOKS_COLUMNS)} FROM books WHERE id = ?",
            [normalized_id],
        )
        row = cur.fetchone()
    if not row:
        return None
    return {column: _json_safe_db_value(row[index]) for index, column in enumerate(CORE_BOOKS_COLUMNS)}


def update_core_book(
    book_id: str,
    *,
    fields: dict[str, Any],
    recompute_description: bool = False,
) -> dict[str, Any]:
    normalized_id = normalize_book_id(book_id)
    if not normalized_id:
        raise ValueError(f"Invalid book id: {book_id}")

    current = get_core_book(normalized_id, bootstrap=True)
    if current is None:
        raise ValueError(f"Book not found in core table: {normalized_id}")

    updates: dict[str, Any] = {}
    for raw_key, raw_value in fields.items():
        key = str(raw_key or "").strip()
        if not key:
            continue
        if key not in CORE_BOOKS_EDITABLE_COLUMNS:
            continue
        updates[key] = _normalize_core_input_value(key, raw_value)

    merged = {**current, **updates}
    if recompute_description:
        updates["descripcion"] = build_core_description(merged)

    if not updates:
        return current

    assignments = ", ".join(f"{field} = ?" for field in updates)
    params = [updates[field] for field in updates]
    params.append(normalized_id)
    with get_connection() as con:
        con.execute(f"UPDATE books SET {assignments} WHERE id = ?", params)

    refreshed = get_core_book(normalized_id, bootstrap=False)
    if refreshed is None:
        raise ValueError(f"Book not found after update: {normalized_id}")
    return refreshed


def _append_scope_where(sql: str, scope_where: list[str]) -> str:
    if not scope_where:
        return sql
    connector = " AND " if " WHERE " in sql.upper() else " WHERE "
    return sql + connector + " AND ".join(scope_where)


def get_stats(*, block: str | None = None, module: str | None = None) -> dict[str, int]:
    scope_block, scope_module = resolve_scope(block, module, require=False)

    scope_where: list[str] = []
    scope_params: list[Any] = []
    if scope_block:
        scope_where.append("block = ?")
        scope_params.append(scope_block)
    if scope_module:
        scope_where.append("module = ?")
        scope_params.append(scope_module)

    with get_connection() as con:
        total = int(con.execute(_append_scope_where("SELECT COUNT(*) FROM book_items", scope_where), scope_params).fetchone()[0])

        needs_ocr = int(
            con.execute(
                _append_scope_where(
                    "SELECT COUNT(*) FROM book_items WHERE COALESCE(ocr_status, '') NOT IN ('processed', 'manual', 'skipped')",
                    scope_where,
                ),
                scope_params,
            ).fetchone()[0]
        )

        needs_metadata = int(
            con.execute(
                _append_scope_where(
                    "SELECT COUNT(*) FROM book_items WHERE COALESCE(metadata_status, '') NOT IN ('fetched', 'partial', 'manual', 'skipped')",
                    scope_where,
                ),
                scope_params,
            ).fetchone()[0]
        )

        needs_catalog = int(
            con.execute(
                _append_scope_where(
                    "SELECT COUNT(*) FROM book_items WHERE COALESCE(catalog_status, '') NOT IN ('built', 'partial', 'manual')",
                    scope_where,
                ),
                scope_params,
            ).fetchone()[0]
        )

        needs_cover = int(
            con.execute(
                _append_scope_where(
                    "SELECT COUNT(*) FROM book_items WHERE COALESCE(cover_status, '') NOT IN ('downloaded', 'missing', 'skipped')",
                    scope_where,
                ),
                scope_params,
            ).fetchone()[0]
        )

        needs_review = int(
            con.execute(
                _append_scope_where("SELECT COUNT(*) FROM book_items WHERE workflow_needs_review = TRUE", scope_where),
                scope_params,
            ).fetchone()[0]
        )

    return {
        "total": total,
        "needs_ocr": needs_ocr,
        "needs_metadata": needs_metadata,
        "needs_catalog": needs_catalog,
        "needs_cover": needs_cover,
        "needs_workflow_review": needs_review,
    }
