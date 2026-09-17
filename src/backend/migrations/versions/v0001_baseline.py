from __future__ import annotations

from functools import lru_cache
from typing import Any

from ...config import PROJECT_ROOT

ISO_639_3_TAB_PATH = PROJECT_ROOT / "assets" / "iso-639-3.tab"

BOOK_ALLOWED_VALUES: tuple[tuple[str, str], ...] = (
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
)


@lru_cache(maxsize=1)
def _load_langcodes_module() -> Any | None:
    try:
        import langcodes  # type: ignore
    except Exception:
        return None
    return langcodes


def _iso639_3_to_spanish_name(code: str | None) -> str | None:
    text = str(code or "").strip().lower()
    if len(text) != 3:
        return None
    if text == "mul":
        return "múltiples idiomas"

    langcodes = _load_langcodes_module()
    if langcodes is None:
        return None

    try:
        name = str(langcodes.Language.get(text).display_name("es") or "").strip()
    except Exception:
        return None

    if not name or name.lower() in {"unknown language", "idioma desconocido"}:
        return None
    return name.lower()


def _ensure_iso_639_3_table(con: Any) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS ref")
    con.execute("""
        CREATE TABLE IF NOT EXISTS ref.iso_639_3 (
            id VARCHAR PRIMARY KEY,
            part2b VARCHAR,
            part2t VARCHAR,
            part1 VARCHAR,
            scope VARCHAR,
            language_type VARCHAR,
            ref_name VARCHAR,
            comment VARCHAR,
            spa_name VARCHAR
        )
        """)
    con.execute("ALTER TABLE ref.iso_639_3 ADD COLUMN IF NOT EXISTS spa_name VARCHAR")
    con.execute(
        "ALTER TABLE ref.iso_639_3 " "ADD COLUMN IF NOT EXISTS language_type VARCHAR"
    )

    legacy_nombre_spa_exists = bool(con.execute("""
            SELECT COUNT(*) > 0
            FROM information_schema.columns
            WHERE table_schema = 'ref'
              AND table_name = 'iso_639_3'
              AND lower(column_name) = 'nombre_spa'
            """).fetchone()[0])
    if legacy_nombre_spa_exists:
        con.execute("""
            UPDATE ref.iso_639_3
            SET spa_name = nombre_spa
            WHERE (spa_name IS NULL OR trim(spa_name) = '')
              AND nombre_spa IS NOT NULL
              AND trim(nombre_spa) <> ''
            """)

    if ISO_639_3_TAB_PATH.exists():
        con.execute("""
            CREATE OR REPLACE TEMP TABLE _iso_prev AS
            SELECT id, spa_name
            FROM ref.iso_639_3
            """)
        con.execute("DELETE FROM ref.iso_639_3")
        con.execute(
            """
            INSERT INTO ref.iso_639_3 (
                id, part2b, part2t, part1, scope, language_type,
                ref_name, comment, spa_name
            )
            SELECT
                src.id,
                src.part2b,
                src.part2t,
                src.part1,
                src.scope,
                src.language_type,
                src.ref_name,
                src.comment,
                prev.spa_name
            FROM (
                SELECT
                    Id AS id,
                    Part2b AS part2b,
                    Part2t AS part2t,
                    Part1 AS part1,
                    Scope AS scope,
                    Language_Type AS language_type,
                    Ref_Name AS ref_name,
                    Comment AS comment
                FROM read_csv_auto(?, delim='\t', header=true, all_varchar=true)
            ) AS src
            LEFT JOIN _iso_prev AS prev ON prev.id = src.id
            """,
            [str(ISO_639_3_TAB_PATH)],
        )

    rows = con.execute("SELECT id, spa_name FROM ref.iso_639_3").fetchall()
    updates: list[tuple[str, str]] = []
    for row in rows:
        iso_id = str(row[0] or "").strip().lower()
        spa_name = str(row[1] or "").strip()
        if not iso_id or spa_name:
            continue
        inferred = _iso639_3_to_spanish_name(iso_id)
        if inferred:
            updates.append((inferred, iso_id))

    if updates:
        con.executemany(
            "UPDATE ref.iso_639_3 SET spa_name = ? WHERE id = ?",
            updates,
        )

    con.execute(
        "CREATE INDEX IF NOT EXISTS idx_ref_iso_639_3_spa_name "
        "ON ref.iso_639_3(spa_name)"
    )


def _create_books_core_schema(con: Any) -> None:
    con.execute("""
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
        """)

    con.execute("CREATE INDEX IF NOT EXISTS idx_books_idioma ON books(idioma)")
    con.execute(
        "CREATE INDEX IF NOT EXISTS idx_books_palabras_clave ON books(palabras_clave)"
    )

    try:
        con.execute("ALTER TABLE books DROP COLUMN IF EXISTS dimensiones")
    except Exception:
        pass

    _ensure_iso_639_3_table(con)

    con.execute("""
        CREATE TABLE IF NOT EXISTS book_field_allowed_values (
            table_name VARCHAR,
            field_name VARCHAR,
            field_value VARCHAR,
            sort_order INTEGER DEFAULT 0,
            PRIMARY KEY (table_name, field_name, field_value)
        )
        """)
    con.execute(
        "ALTER TABLE book_field_allowed_values "
        "ADD COLUMN IF NOT EXISTS sort_order INTEGER DEFAULT 0"
    )
    target_rows = [
        ("books", field_name, field_value, index)
        for index, (field_name, field_value) in enumerate(BOOK_ALLOWED_VALUES)
    ]
    target_keys = {(row[1], row[2]) for row in target_rows}

    current_rows = con.execute("""
        SELECT field_name, field_value, sort_order
        FROM book_field_allowed_values
        WHERE table_name = 'books'
        """).fetchall()
    current_map = {
        (str(row[0] or "").strip(), str(row[1] or "").strip()): int(row[2] or 0)
        for row in current_rows
        if str(row[0] or "").strip() and str(row[1] or "").strip()
    }

    insert_rows = [row for row in target_rows if (row[1], row[2]) not in current_map]
    if insert_rows:
        con.executemany(
            """
            INSERT INTO book_field_allowed_values
                (table_name, field_name, field_value, sort_order)
            VALUES (?, ?, ?, ?)
            """,
            insert_rows,
        )

    update_rows = [
        (row[3], row[0], row[1], row[2])
        for row in target_rows
        if current_map.get((row[1], row[2])) is not None
        and current_map.get((row[1], row[2])) != row[3]
    ]
    if update_rows:
        con.executemany(
            """
            UPDATE book_field_allowed_values
            SET sort_order = ?
            WHERE table_name = ? AND field_name = ? AND field_value = ?
            """,
            update_rows,
        )

    delete_rows = [
        ("books", field_name, field_value)
        for field_name, field_value in current_map
        if (field_name, field_value) not in target_keys
    ]
    if delete_rows:
        con.executemany(
            """
            DELETE FROM book_field_allowed_values
            WHERE table_name = ? AND field_name = ? AND field_value = ?
            """,
            delete_rows,
        )

    con.execute("""
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
                    FROM ref.iso_639_3 AS i
                    WHERE lower(trim(i.spa_name)) = lower(trim(b.idioma))
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
        """)


def apply(con: Any) -> None:
    con.execute("""
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
        """)

    for legacy_column in (
        "image_path",
        "image_count",
        "credits_text",
        "isbn_raw",
        "isbn",
        "metadata_json",
    ):
        try:
            con.execute(f"ALTER TABLE book_items DROP COLUMN IF EXISTS {legacy_column}")
        except Exception:
            pass

    con.execute("""
        CREATE TABLE IF NOT EXISTS book_image_files (
            book_id VARCHAR,
            n_imagen INTEGER,
            filename VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY(book_id, n_imagen)
        )
        """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS book_ocr_data (
            book_id VARCHAR PRIMARY KEY,
            extracted_text VARCHAR,
            isbn_raw VARCHAR,
            isbn VARCHAR,
            isbn_list VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)

    con.execute("""
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
        """)

    _create_books_core_schema(con)
