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
        tokens = [chunk.strip() for chunk in re.split(r"[,\s;]+", text) if chunk.strip()]
    return [token for token in tokens if token]


def _resolve_prefixes(*, block: str | None, modules: str | list[str] | tuple[str, ...] | None) -> list[str]:
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
    limit: int | None = None,
) -> tuple[list[str], list[dict[str, Any]], str | None, list[str]]:
    normalized_block = _normalize_block(block)
    prefixes = _resolve_prefixes(block=normalized_block, modules=modules)

    sql = f"SELECT * FROM {EXPORT_VIEW_NAME}"
    where: list[str] = []
    params: list[Any] = []

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


def export_books_tsv(
    output_path: Path,
    *,
    block: str | None = None,
    modules: str | list[str] | tuple[str, ...] | None = None,
    encoding: str = "windows-1252",
) -> dict[str, Any]:
    target_encoding = _normalize_encoding(encoding)
    py_encoding = _python_encoding(target_encoding)

    columns, rows, normalized_block, prefixes = query_export_rows(
        block=block,
        modules=modules,
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
            writer.writerow({key: _serialize_value(row.get(key), encoding=target_encoding) for key in columns})

    return {
        "path": output_path,
        "rows": len(rows),
        "columns": columns,
        "encoding": target_encoding,
        "block": normalized_block,
        "prefixes": prefixes,
    }
