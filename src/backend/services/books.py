import json
import re
from pathlib import Path
from typing import Any

from ..database import get_connection
from ..normalizers import extract_book_id_from_path, normalize_book_id, split_book_id

STAGES = ("ocr", "metadata", "catalog", "cover")
PAYLOAD_TYPES = {"metadata", "catalog", "ocr_trace"}
VALID_BLOCKS = ("A", "B", "C")
MODULE_DIR_PATTERN = re.compile(r"^\d{2}$")


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


def init_table() -> None:
    with get_connection() as con:
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS books (
                id VARCHAR PRIMARY KEY,
                block VARCHAR,
                module VARCHAR,
                seq VARCHAR,

                image_path VARCHAR,
                image_paths_json VARCHAR DEFAULT '[]',
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

        con.execute("ALTER TABLE books ADD COLUMN IF NOT EXISTS ocr_provider VARCHAR")
        con.execute("ALTER TABLE books ADD COLUMN IF NOT EXISTS ocr_model VARCHAR")
        con.execute("ALTER TABLE books ADD COLUMN IF NOT EXISTS ocr_trace_json VARCHAR")

        con.execute(
            """
            CREATE TABLE IF NOT EXISTS book_images (
                book_id VARCHAR,
                position INTEGER,
                image_path VARCHAR,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY(book_id, position)
            )
            """
        )

        con.execute(
            """
            CREATE TABLE IF NOT EXISTS book_payload_fields (
                book_id VARCHAR,
                payload_type VARCHAR,
                path VARCHAR,
                value_type VARCHAR,
                value_text VARCHAR,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY(book_id, payload_type, path)
            )
            """
        )


def _leaf_to_storage(value: Any) -> tuple[str, str | None]:
    if value is None:
        return "null", None
    if isinstance(value, bool):
        return "bool", "1" if value else "0"
    if isinstance(value, int):
        return "int", str(value)
    if isinstance(value, float):
        return "float", repr(value)
    return "str", str(value)


def _leaf_from_storage(value_type: str, value_text: str | None) -> Any:
    if value_type == "null":
        return None
    if value_type == "bool":
        return str(value_text or "").strip().lower() in {"1", "true", "t", "yes", "y"}
    if value_type == "int":
        try:
            return int(str(value_text or "0"))
        except ValueError:
            return None
    if value_type == "float":
        try:
            return float(str(value_text or "0"))
        except ValueError:
            return None
    return str(value_text or "")


def _flatten_payload(value: Any, *, prefix: str = "") -> list[tuple[str, str, str | None]]:
    rows: list[tuple[str, str, str | None]] = []

    if isinstance(value, dict):
        for key, item in value.items():
            text_key = str(key)
            path = f"{prefix}/{text_key}" if prefix else text_key
            rows.extend(_flatten_payload(item, prefix=path))
        return rows

    if isinstance(value, list):
        for index, item in enumerate(value):
            path = f"{prefix}/{index}" if prefix else str(index)
            rows.extend(_flatten_payload(item, prefix=path))
        return rows

    path = prefix or "$"
    value_type, value_text = _leaf_to_storage(value)
    rows.append((path, value_type, value_text))
    return rows


def _path_segments(path: str) -> list[str]:
    text = str(path or "").strip()
    if not text or text == "$":
        return []
    return [segment for segment in text.split("/") if segment]


def _assign_payload_value(root: dict[str, Any] | list[Any], segments: list[str], value: Any) -> None:
    cursor: dict[str, Any] | list[Any] = root

    for index, segment in enumerate(segments):
        last = index == len(segments) - 1
        is_list_index = segment.isdigit()
        next_is_list_index = (index + 1) < len(segments) and segments[index + 1].isdigit()

        if is_list_index:
            if not isinstance(cursor, list):
                return

            position = int(segment)
            while len(cursor) <= position:
                cursor.append(None)

            if last:
                cursor[position] = value
                return

            current = cursor[position]
            expected_type = list if next_is_list_index else dict
            if not isinstance(current, expected_type):
                current = [] if next_is_list_index else {}
                cursor[position] = current
            cursor = current
            continue

        if not isinstance(cursor, dict):
            return

        if last:
            cursor[segment] = value
            return

        current = cursor.get(segment)
        expected_type = list if next_is_list_index else dict
        if not isinstance(current, expected_type):
            current = [] if next_is_list_index else {}
            cursor[segment] = current
        cursor = current


def _rebuild_payload(rows: list[tuple[str, str, str | None]], default: Any) -> Any:
    if not rows:
        if isinstance(default, dict):
            return {}
        if isinstance(default, list):
            return []
        return default

    first_segments = _path_segments(rows[0][0])
    if not first_segments:
        root: Any = _leaf_from_storage(rows[0][1], rows[0][2])
    else:
        root = [] if first_segments[0].isdigit() else {}

    for path, value_type, value_text in rows:
        segments = _path_segments(path)
        value = _leaf_from_storage(value_type, value_text)

        if not segments:
            root = value
            continue

        if isinstance(root, (dict, list)):
            _assign_payload_value(root, segments, value)

    if isinstance(default, dict) and isinstance(root, dict):
        return root
    if isinstance(default, list) and isinstance(root, list):
        return root
    return root


def _payload_default(default: Any) -> Any:
    if isinstance(default, dict):
        return {}
    if isinstance(default, list):
        return []
    return default


def _replace_book_images(book_id: str, image_paths: list[str]) -> None:
    rows = [str(path).strip() for path in image_paths if str(path).strip()]

    with get_connection() as con:
        con.execute("DELETE FROM book_images WHERE book_id = ?", [book_id])
        for position, image_path in enumerate(rows):
            con.execute(
                """
                INSERT INTO book_images (book_id, position, image_path, created_at, updated_at)
                VALUES (?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                """,
                [book_id, int(position), image_path],
            )


def _clear_payload(book_id: str, payload_type: str) -> None:
    if payload_type not in PAYLOAD_TYPES:
        raise ValueError(f"Invalid payload_type: {payload_type}")

    with get_connection() as con:
        con.execute(
            "DELETE FROM book_payload_fields WHERE book_id = ? AND payload_type = ?",
            [book_id, payload_type],
        )


def _replace_payload(book_id: str, payload_type: str, payload: Any) -> None:
    if payload_type not in PAYLOAD_TYPES:
        raise ValueError(f"Invalid payload_type: {payload_type}")

    rows = _flatten_payload(payload if payload is not None else {})

    with get_connection() as con:
        con.execute(
            "DELETE FROM book_payload_fields WHERE book_id = ? AND payload_type = ?",
            [book_id, payload_type],
        )

        for path, value_type, value_text in rows:
            con.execute(
                """
                INSERT INTO book_payload_fields (
                    book_id, payload_type, path, value_type, value_text,
                    created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                """,
                [book_id, payload_type, path, value_type, value_text],
            )


def _load_book_images(book_id: str, *, fallback: list[str] | None = None) -> list[str]:
    with get_connection() as con:
        cur = con.execute(
            """
            SELECT image_path
            FROM book_images
            WHERE book_id = ?
            ORDER BY position
            """,
            [book_id],
        )
        rows = [str(row[0]) for row in cur.fetchall() if str(row[0]).strip()]

    if rows:
        return rows

    if fallback is None:
        return []

    return [str(path).strip() for path in fallback if str(path).strip()]


def _load_payload(book_id: str, payload_type: str, *, default: Any) -> Any:
    if payload_type not in PAYLOAD_TYPES:
        raise ValueError(f"Invalid payload_type: {payload_type}")

    with get_connection() as con:
        cur = con.execute(
            """
            SELECT path, value_type, value_text
            FROM book_payload_fields
            WHERE book_id = ? AND payload_type = ?
            ORDER BY path
            """,
            [book_id, payload_type],
        )
        rows = [(str(row[0]), str(row[1]), row[2]) for row in cur.fetchall()]

    if not rows:
        return _payload_default(default)

    return _rebuild_payload(rows, default)


def _fetch_image_map(book_ids: list[str]) -> dict[str, list[str]]:
    if not book_ids:
        return {}

    placeholders = ", ".join(["?"] * len(book_ids))
    query = (
        "SELECT book_id, image_path "
        "FROM book_images "
        f"WHERE book_id IN ({placeholders}) "
        "ORDER BY book_id, position"
    )

    grouped: dict[str, list[str]] = {}
    with get_connection() as con:
        rows = con.execute(query, book_ids).fetchall()

    for row in rows:
        book_id = str(row[0])
        image_path = str(row[1]).strip()
        if not image_path:
            continue
        grouped.setdefault(book_id, []).append(image_path)

    return grouped


def _fetch_payload_map(book_ids: list[str], payload_type: str, *, default: Any) -> dict[str, Any]:
    if payload_type not in PAYLOAD_TYPES:
        raise ValueError(f"Invalid payload_type: {payload_type}")

    if not book_ids:
        return {}

    placeholders = ", ".join(["?"] * len(book_ids))
    query = (
        "SELECT book_id, path, value_type, value_text "
        "FROM book_payload_fields "
        f"WHERE payload_type = ? AND book_id IN ({placeholders}) "
        "ORDER BY book_id, path"
    )
    params: list[Any] = [payload_type, *book_ids]

    grouped_rows: dict[str, list[tuple[str, str, str | None]]] = {}
    with get_connection() as con:
        rows = con.execute(query, params).fetchall()

    for row in rows:
        book_id = str(row[0])
        grouped_rows.setdefault(book_id, []).append((str(row[1]), str(row[2]), row[3]))

    payload_map: dict[str, Any] = {}
    for book_id, payload_rows in grouped_rows.items():
        payload_map[book_id] = _rebuild_payload(payload_rows, default)

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

    legacy_images = _load_json(payload.get("image_paths_json"), [])

    if image_map is not None and book_id in image_map:
        image_paths = image_map.get(book_id, [])
    else:
        image_paths = _load_book_images(book_id, fallback=legacy_images) if book_id else legacy_images

    payload["image_paths"] = image_paths
    if image_paths:
        payload["image_count"] = len(image_paths)
        if not str(payload.get("image_path") or "").strip():
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
        cur = con.execute("SELECT * FROM books WHERE id = ?", [normalized])
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

    sql = "SELECT * FROM books"
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
            "UPDATE books SET pipeline_stage = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
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
            f"UPDATE books SET {', '.join(assignments)} WHERE id = ?",
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
            UPDATE books
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
            UPDATE books
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
        primary_image = image_paths[0] if image_paths else None
        parts = split_book_id(book_id)
        block_value = parts[1] if parts else None
        module_value = parts[0] if parts else None
        seq = parts[2] if parts else None

        current = get_book(book_id)
        if current is None:
            with get_connection() as con:
                con.execute(
                    """
                    INSERT INTO books (
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
            fields["image_path"] = merged_paths[0]

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

    sql = "SELECT id FROM books"
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
        total = int(con.execute(_append_scope_where("SELECT COUNT(*) FROM books", scope_where), scope_params).fetchone()[0])

        needs_ocr = int(
            con.execute(
                _append_scope_where(
                    "SELECT COUNT(*) FROM books WHERE COALESCE(ocr_status, '') NOT IN ('processed', 'manual', 'skipped')",
                    scope_where,
                ),
                scope_params,
            ).fetchone()[0]
        )

        needs_metadata = int(
            con.execute(
                _append_scope_where(
                    "SELECT COUNT(*) FROM books WHERE COALESCE(metadata_status, '') NOT IN ('fetched', 'partial', 'manual', 'skipped')",
                    scope_where,
                ),
                scope_params,
            ).fetchone()[0]
        )

        needs_catalog = int(
            con.execute(
                _append_scope_where(
                    "SELECT COUNT(*) FROM books WHERE COALESCE(catalog_status, '') NOT IN ('built', 'partial', 'manual')",
                    scope_where,
                ),
                scope_params,
            ).fetchone()[0]
        )

        needs_cover = int(
            con.execute(
                _append_scope_where(
                    "SELECT COUNT(*) FROM books WHERE COALESCE(cover_status, '') NOT IN ('downloaded', 'missing', 'skipped')",
                    scope_where,
                ),
                scope_params,
            ).fetchone()[0]
        )

        needs_review = int(
            con.execute(
                _append_scope_where("SELECT COUNT(*) FROM books WHERE workflow_needs_review = TRUE", scope_where),
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
