import json
import re
from pathlib import Path
from typing import Any

from ..config import DEFAULT_COVERS_DIR
from ..database import get_connection
from ..normalizers import extract_book_id_from_path, normalize_book_id, split_book_id

STAGES = ("ocr", "metadata", "catalog", "cover")
PAYLOAD_TYPES = {"metadata", "catalog", "ocr_trace"}
VALID_BLOCKS = ("A", "B", "C")
MODULE_DIR_PATTERN = re.compile(r"^\d{2}$")
METADATA_PROVIDER_MAP = (
    ("google", "google"),
    ("isbndb", "isbndb"),
    ("open_library", "openlibrary"),
)
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

    con.execute(
        """
        CREATE TABLE IF NOT EXISTS iso_639_3 (
            id VARCHAR,
            part2b VARCHAR,
            part2t VARCHAR,
            part1 VARCHAR,
            scope VARCHAR,
            language_yype VARCHAR,
            ref_name VARCHAR,
            comment VARCHAR,
            nombre_spa VARCHAR
        )
        """
    )

    con.execute(
        """
        CREATE TABLE IF NOT EXISTS book_field_allowed_values (
            table_name VARCHAR,
            field_name VARCHAR,
            field_value VARCHAR,
            PRIMARY KEY (table_name, field_name, field_value)
        )
        """
    )
    con.execute("DELETE FROM book_field_allowed_values WHERE table_name = 'books'")
    con.executemany(
        "INSERT INTO book_field_allowed_values (table_name, field_name, field_value) VALUES (?, ?, ?)",
        [("books", field_name, field_value) for field_name, field_value in BOOK_ALLOWED_VALUES],
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
                    FROM iso_639_3 i
                    WHERE i.nombre_spa = b.idioma
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

                image_path VARCHAR,
                image_count INTEGER DEFAULT 0,

                credits_text VARCHAR,
                isbn_raw VARCHAR,
                isbn VARCHAR,
                ocr_status VARCHAR,
                ocr_error VARCHAR,
                ocr_provider VARCHAR,
                ocr_model VARCHAR,
                ocr_trace_json VARCHAR,

                metadata_json VARCHAR,
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
    if payload_type == "metadata":
        return "metadata_json"
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
    catalog_map: dict[str, Any] | None = None,
    ocr_trace_map: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload = dict(zip(columns, row))
    book_id = str(payload.get("id") or "").strip()

    if image_map is not None and book_id in image_map:
        image_paths = image_map.get(book_id, [])
    else:
        image_paths = _load_book_images(book_id, fallback=[]) if book_id else []

    payload["image_paths"] = image_paths
    if image_paths:
        payload["image_count"] = len(image_paths)
        payload["image_path"] = image_paths[0]

    if metadata_map is not None:
        payload["metadata"] = metadata_map.get(book_id, {})
    else:
        payload["metadata"] = _load_payload(book_id, "metadata", default={}) if book_id else {}

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
    metadata_map = _fetch_payload_map(book_ids, "metadata", default={})
    catalog_map = _fetch_payload_map(book_ids, "catalog", default={})
    ocr_trace_map = _fetch_payload_map(book_ids, "ocr_trace", default={})

    return [
        _row_to_dict(
            columns,
            row,
            image_map=image_map,
            metadata_map=metadata_map,
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
                "credits_text": None,
                "isbn_raw": None,
                "isbn": None,
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
        _clear_payload(book_id, "metadata")
        _clear_payload(book_id, "catalog")
        _clear_bibliographic_sources(book_id)
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
        _clear_payload(book_id, "metadata")
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
    if normalized_stage == "ocr":
        _upsert_ocr_data(
            book_id=book_id,
            credits_text=None,
            isbn_raw=None,
            isbn=None,
            trace=None,
        )
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
        primary_image = _image_filename(image_paths[0]) if image_paths else None
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
                        image_path, image_count,
                        pipeline_stage, workflow_status, workflow_attempt,
                        created_at, updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, 'ocr', 'pending', 0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                    """,
                    [
                        book_id,
                        block_value,
                        module_value,
                        seq,
                        primary_image,
                        len(image_paths),
                    ],
                )

            _replace_book_images(book_id, image_paths)
            _upsert_ocr_data(
                book_id=book_id,
                credits_text=None,
                isbn_raw=None,
                isbn=None,
                trace=None,
            )
            inserted += 1
            continue

        previous_paths = [str(path) for path in current.get("image_paths", []) if str(path).strip()]
        if overwrite_existing_paths:
            merged_paths = image_paths
        else:
            merged_paths = sorted(set(previous_paths) | set(image_paths))

        changed = merged_paths != previous_paths
        fields = {
            "image_count": len(merged_paths),
            "block": block_value,
            "module": module_value,
            "seq": seq,
        }
        if merged_paths:
            fields["image_path"] = _image_filename(merged_paths[0])

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

    paths = [str(book.get("image_path") or "").strip()]
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
            "credits_text": credits_text,
            "isbn_raw": isbn_raw,
            "isbn": isbn,
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
    _replace_payload(book_id, "metadata", metadata)
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
