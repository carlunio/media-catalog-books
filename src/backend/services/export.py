import csv
import re
import unicodedata
from pathlib import Path
from typing import Any

from ..database import get_connection

EXPORT_VIEW_NAME = "libros_carga_abebooks"
EXPORT_ENCODINGS = {"windows-1252", "utf-8"}
BLOCK_OPTIONS = {"A", "B", "C"}
MODULE_PATTERN = re.compile(r"^\d{2}$")
PREFIX_PATTERN = re.compile(r"^\d{2}[ABC]$")
MOJIBAKE_PATTERNS = (
    "Ã¡",
    "Ã©",
    "Ã­",
    "Ã³",
    "Ãº",
    "Ã±",
    "Ãœ",
    "Ã¼",
    "Âº",
    "Âª",
    "Â¿",
    "Â¡",
)


def _normalize_encoding(encoding: str | None) -> str:
    text = str(encoding or "windows-1252").strip().lower()
    aliases = {
        "cp1252": "windows-1252",
        "windows1252": "windows-1252",
        "win1252": "windows-1252",
        "utf8": "utf-8",
    }
    text = aliases.get(text, text)
    if text not in EXPORT_ENCODINGS:
        allowed = ", ".join(sorted(EXPORT_ENCODINGS))
        raise ValueError(f"Invalid encoding: {encoding}. Expected one of: {allowed}")
    return text


def _python_encoding(encoding: str) -> str:
    return "cp1252" if encoding == "windows-1252" else "utf-8"


def _normalize_block(block: str | None) -> str | None:
    text = str(block or "").strip().upper()
    if not text:
        return None
    if text not in BLOCK_OPTIONS:
        allowed = ", ".join(sorted(BLOCK_OPTIONS))
        raise ValueError(f"Invalid block: {block}. Expected one of: {allowed}")
    return text


def _split_tokens(value: str | list[str] | tuple[str, ...] | None) -> list[str]:
    if value is None:
        return []
    if isinstance(value, (list, tuple)):
        tokens = [str(item).strip() for item in value]
    else:
        text = str(value).strip()
        if not text:
            return []
        tokens = [
            chunk.strip()
            for chunk in re.split(r"[,\s;]+", text)
            if chunk.strip()
        ]
    return [token for token in tokens if token]


def _normalize_ids(ids: list[str] | tuple[str, ...] | None) -> list[str]:
    normalized_ids: list[str] = []
    for raw_id in ids or []:
        item_id = str(raw_id or "").strip()
        if item_id and item_id not in normalized_ids:
            normalized_ids.append(item_id)
    return normalized_ids


def _resolve_prefixes(
    *,
    block: str | None,
    modules: str | list[str] | tuple[str, ...] | None,
) -> list[str]:
    normalized_block = _normalize_block(block)
    tokens = _split_tokens(modules)
    prefixes: list[str] = []

    for token in tokens:
        upper = token.upper()
        if PREFIX_PATTERN.fullmatch(upper):
            prefix = upper
        else:
            module = upper.zfill(2)
            if not MODULE_PATTERN.fullmatch(module):
                raise ValueError(
                    f"Invalid module token: {token}. Use 01..99 or explicit prefix 01A."
                )
            if normalized_block is None:
                raise ValueError("block is required when modules are provided as 01..99")
            prefix = f"{module}{normalized_block}"

        if prefix not in prefixes:
            prefixes.append(prefix)

    return prefixes


def _contains_mojibake(text: str) -> bool:
    return any(pattern in text for pattern in MOJIBAKE_PATTERNS)


def _repair_mojibake(text: str) -> str:
    if not _contains_mojibake(text):
        return text
    try:
        repaired = text.encode("windows-1252").decode("utf-8")
    except UnicodeError:
        return text

    before = sum(pattern in text for pattern in MOJIBAKE_PATTERNS)
    after = sum(pattern in repaired for pattern in MOJIBAKE_PATTERNS)
    return repaired if after < before else text


def _normalize_for_windows_1252(text: str) -> str:
    text = _repair_mojibake(text)
    result: list[str] = []
    for ch in text:
        try:
            ch.encode("windows-1252")
            result.append(ch)
            continue
        except UnicodeEncodeError:
            pass

        decomposed = unicodedata.normalize("NFKD", ch)
        base = "".join(c for c in decomposed if not unicodedata.combining(c))
        try:
            base.encode("windows-1252")
            result.append(base)
        except UnicodeEncodeError:
            result.append("?")
    return "".join(result)


def _serialize_value(value: Any, *, encoding: str) -> str:
    if value is None:
        return ""
    text = str(value)
    if encoding == "windows-1252":
        return _normalize_for_windows_1252(text)
    return text


def query_export_rows(
    *,
    block: str | None = None,
    modules: str | list[str] | tuple[str, ...] | None = None,
    ids: list[str] | tuple[str, ...] | None = None,
    limit: int | None = None,
) -> tuple[list[str], list[dict[str, Any]], str | None, list[str]]:
    normalized_block = _normalize_block(block)
    prefixes = _resolve_prefixes(block=normalized_block, modules=modules)
    normalized_ids = _normalize_ids(ids)

    sql = f"SELECT * FROM {EXPORT_VIEW_NAME}"
    where: list[str] = []
    params: list[Any] = []

    if ids is not None:
        if not normalized_ids:
            where.append("FALSE")
        else:
            placeholders = ", ".join(["?"] * len(normalized_ids))
            where.append(f"listingid IN ({placeholders})")
            params.extend(normalized_ids)

    if prefixes:
        placeholders = ", ".join(["?"] * len(prefixes))
        where.append(f"substr(listingid, 1, 3) IN ({placeholders})")
        params.extend(prefixes)
    elif normalized_block:
        where.append("substr(listingid, 3, 1) = ?")
        params.append(normalized_block)

    if where:
        sql = f"{sql} WHERE {' AND '.join(where)}"

    sql = f"{sql} ORDER BY listingid"

    if limit is not None:
        if limit < 1:
            raise ValueError("limit must be >= 1")
        sql = f"{sql} LIMIT ?"
        params.append(int(limit))

    with get_connection() as con:
        cur = con.execute(sql, params)
        columns = [desc[0] for desc in cur.description]
        tuples = cur.fetchall()

    rows = [dict(zip(columns, row)) for row in tuples]
    return columns, rows, normalized_block, prefixes


def _is_blank(value: Any) -> bool:
    return not str(value or "").strip()


def _validation_row(row: dict[str, Any]) -> dict[str, Any]:
    errors: list[str] = []
    listingid = str(row.get("listingid") or "").strip()

    if _is_blank(row.get("listingid")):
        errors.append("Falta listingid.")
    if _is_blank(row.get("title")):
        errors.append("Falta title.")
    if _is_blank(row.get("price")):
        errors.append("Falta price.")
    if _is_blank(row.get("quantity")):
        errors.append("Falta quantity.")

    return {
        "id": listingid,
        "listingid": listingid,
        "title": row.get("title"),
        "price": row.get("price"),
        "quantity": row.get("quantity"),
        "is_valid": not errors,
        "errors": errors,
    }


def validate_export_rows(
    *,
    block: str | None = None,
    modules: str | list[str] | tuple[str, ...] | None = None,
    ids: list[str] | tuple[str, ...] | None = None,
) -> dict[str, Any]:
    _, rows, normalized_block, prefixes = query_export_rows(
        block=block,
        modules=modules,
        ids=ids,
        limit=None,
    )
    validation_rows = [_validation_row(row) for row in rows]

    if ids is not None:
        found_ids = {str(row.get("id") or "").strip() for row in validation_rows}
        for item_id in _normalize_ids(ids):
            if item_id not in found_ids:
                validation_rows.append(
                    {
                        "id": item_id,
                        "listingid": item_id,
                        "title": None,
                        "price": None,
                        "quantity": None,
                        "is_valid": False,
                        "errors": [
                            "La ficha no existe o no esta en estado exportable."
                        ],
                    }
                )

    valid_ids = [str(row["id"]) for row in validation_rows if row.get("is_valid")]
    invalid_rows = [row for row in validation_rows if not row.get("is_valid")]
    return {
        "ok": True,
        "rows": validation_rows,
        "rows_count": len(validation_rows),
        "valid_count": len(valid_ids),
        "invalid_count": len(invalid_rows),
        "valid_ids": valid_ids,
        "invalid_ids": [str(row["id"]) for row in invalid_rows],
        "ids": [str(row["id"]) for row in validation_rows],
        "block": normalized_block,
        "prefixes": prefixes,
    }


def export_books_tsv(
    output_path: Path,
    *,
    block: str | None = None,
    modules: str | list[str] | tuple[str, ...] | None = None,
    ids: list[str] | tuple[str, ...] | None = None,
    encoding: str = "windows-1252",
) -> dict[str, Any]:
    target_encoding = _normalize_encoding(encoding)
    py_encoding = _python_encoding(target_encoding)

    columns, rows, normalized_block, prefixes = query_export_rows(
        block=block,
        modules=modules,
        ids=ids,
        limit=None,
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding=py_encoding, newline="") as file:
        writer = csv.DictWriter(
            file,
            fieldnames=columns,
            delimiter="\t",
            quoting=csv.QUOTE_MINIMAL,
            extrasaction="ignore",
        )
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    key: _serialize_value(row.get(key), encoding=target_encoding)
                    for key in columns
                }
            )

    return {
        "path": output_path,
        "rows": len(rows),
        "columns": columns,
        "encoding": target_encoding,
        "block": normalized_block,
        "prefixes": prefixes,
        "ids": [
            str(row.get("listingid") or "").strip()
            for row in rows
            if str(row.get("listingid") or "").strip()
        ],
    }


def mark_exported_books_uploaded(ids: list[str] | tuple[str, ...]) -> dict[str, Any]:
    normalized_ids = _normalize_ids(ids)
    if not normalized_ids:
        return {"updated": 0, "ids": []}

    placeholders = ", ".join(["?"] * len(normalized_ids))
    params: list[Any] = list(normalized_ids)

    with get_connection() as con:
        rows = con.execute(
            f"""
            SELECT id
            FROM books
            WHERE id IN ({placeholders})
              AND estado_carga IN ('Para subir', 'Para actualizar')
            """,
            params,
        ).fetchall()
        matched_ids = [
            str(row[0] or "").strip()
            for row in rows
            if str(row[0] or "").strip()
        ]
        if matched_ids:
            matched_placeholders = ", ".join(["?"] * len(matched_ids))
            con.execute(
                f"""
                UPDATE books
                SET estado_carga = 'Subido'
                WHERE id IN ({matched_placeholders})
                """,
                matched_ids,
            )

    return {"updated": len(matched_ids), "ids": matched_ids}
