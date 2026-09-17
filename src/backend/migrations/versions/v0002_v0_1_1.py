from __future__ import annotations

from typing import Any


def apply(con: Any) -> None:
    con.execute(
        "ALTER TABLE book_items ADD COLUMN IF NOT EXISTS workflow_action VARCHAR"
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
            CASE
                WHEN b.precio IS NULL THEN NULL
                ELSE CAST(CAST(b.precio AS DECIMAL(18, 2)) AS VARCHAR) || ' €'
            END AS price,
            b.cantidad AS quantity,
            b.descripcion AS description
        FROM books b
        WHERE b.estado_carga IN ('Para subir', 'Para actualizar')
        """)
