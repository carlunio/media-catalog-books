from datetime import datetime, timezone
from typing import Any

import requests

from ..config import ISBNDB_API_KEY, REQUEST_TIMEOUT_SECONDS
from ..normalizers import clean_isbn, is_valid_isbn
from . import books


def _safe_get(url: str, *, headers: dict[str, str] | None = None, timeout: float = REQUEST_TIMEOUT_SECONDS) -> dict[str, Any]:
    response = requests.get(url, headers=headers, timeout=timeout)
    response.raise_for_status()
    payload = response.json()
    return payload if isinstance(payload, dict) else {}


def _google_books(isbn: str, *, timeout: float) -> dict[str, Any]:
    data = _safe_get(f"https://www.googleapis.com/books/v1/volumes?q=isbn:{isbn}", timeout=timeout)
    items = data.get("items") if isinstance(data, dict) else None
    if isinstance(items, list) and items:
        first = items[0]
        if isinstance(first, dict):
            volume = first.get("volumeInfo")
            return volume if isinstance(volume, dict) else {}
    return {}


def _open_library(isbn: str, *, timeout: float) -> dict[str, Any]:
    data = _safe_get(
        f"https://openlibrary.org/api/books?bibkeys=ISBN:{isbn}&format=json&jscmd=data",
        timeout=timeout,
    )
    value = data.get(f"ISBN:{isbn}")
    return value if isinstance(value, dict) else {}


def _isbndb(isbn: str, *, timeout: float) -> dict[str, Any]:
    if not ISBNDB_API_KEY:
        return {}

    data = _safe_get(
        f"https://api2.isbndb.com/book/{isbn}",
        headers={"Authorization": ISBNDB_API_KEY},
        timeout=timeout,
    )
    return data if isinstance(data, dict) else {}


def run_one(book_id: str, *, overwrite: bool = False, timeout: float = REQUEST_TIMEOUT_SECONDS) -> dict[str, Any]:
    book = books.get_book(book_id)
    if book is None:
        return {"id": book_id, "status": "error", "error": "Book not found"}

    existing_status = str(book.get("metadata_status") or "").strip().lower()
    if existing_status in {"fetched", "manual"} and not overwrite:
        return {"id": book_id, "status": "skipped", "reason": "metadata already present"}

    isbn = clean_isbn(book.get("isbn") or book.get("isbn_raw"))
    if not is_valid_isbn(isbn):
        books.update_metadata(
            book_id,
            metadata={"isbn": isbn, "google": {}, "open_library": {}, "isbndb": {}},
            status="skipped",
            error="Invalid or missing ISBN",
        )
        return {"id": book_id, "status": "skipped", "reason": "Invalid or missing ISBN", "isbn": isbn}

    sources: dict[str, dict[str, Any]] = {
        "google": {},
        "open_library": {},
        "isbndb": {},
    }
    errors: dict[str, str] = {}

    for source_name, fetcher in (
        ("google", _google_books),
        ("open_library", _open_library),
        ("isbndb", _isbndb),
    ):
        try:
            sources[source_name] = fetcher(isbn, timeout=timeout)
        except Exception as exc:
            errors[source_name] = str(exc)

    non_empty_sources = sum(1 for item in sources.values() if item)

    payload = {
        "isbn": isbn,
        "google": sources["google"],
        "open_library": sources["open_library"],
        "isbndb": sources["isbndb"],
        "errors": errors,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
    }

    if non_empty_sources > 0:
        status = "fetched"
        error = None if not errors else f"Partial provider errors: {', '.join(sorted(errors.keys()))}"
    else:
        status = "partial"
        error = "No data from providers"
        if errors:
            error = f"No provider data. Errors: {errors}"

    books.update_metadata(book_id, metadata=payload, status=status, error=error)
    return {
        "id": book_id,
        "status": status,
        "isbn": isbn,
        "sources_with_data": non_empty_sources,
        "errors": errors,
    }
