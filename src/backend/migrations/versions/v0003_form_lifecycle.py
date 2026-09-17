from __future__ import annotations

from typing import Any


def apply(con: Any) -> None:
    con.execute(
        "ALTER TABLE book_items "
        "ADD COLUMN IF NOT EXISTS form_status VARCHAR DEFAULT 'not_started'"
    )
    con.execute(
        "ALTER TABLE book_items "
        "ADD COLUMN IF NOT EXISTS form_consolidated_at TIMESTAMP"
    )
    con.execute(
        "ALTER TABLE book_items "
        "ADD COLUMN IF NOT EXISTS isbn_missing_accepted_at TIMESTAMP"
    )
    con.execute("""
        UPDATE book_items AS bi
        SET form_status = CASE
            WHEN EXISTS (SELECT 1 FROM books AS b WHERE b.id = bi.id) THEN 'draft'
            ELSE 'not_started'
        END
        WHERE form_status IS NULL
           OR trim(form_status) = ''
           OR form_status NOT IN ('not_started', 'draft', 'consolidated')
           OR (
               form_status = 'not_started'
               AND EXISTS (SELECT 1 FROM books AS b WHERE b.id = bi.id)
           )
        """)
