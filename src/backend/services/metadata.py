from datetime import datetime, timezone
import time
from threading import Lock
from typing import Any

import requests

from ..config import (
    GOOGLE_BOOKS_MIN_INTERVAL_SECONDS,
    ISBNDB_API_KEY,
    OPENLIBRARY_MIN_INTERVAL_SECONDS,
    REQUEST_TIMEOUT_SECONDS,
)
from ..normalizers import clean_isbn, is_valid_isbn
from . import books

_RATE_LOCK = Lock()
_last_call_monotonic_by_provider: dict[str, float] = {
    "google": 0.0,
    "open_library": 0.0,
}


def _safe_get(url: str, *, headers: dict[str, str] | None = None, timeout: float = REQUEST_TIMEOUT_SECONDS) -> dict[str, Any]:
    response = requests.get(url, headers=headers, timeout=timeout)
    response.raise_for_status()
    payload = response.json()
    return payload if isinstance(payload, dict) else {}


def _wait_for_provider_slot(provider: str, *, min_interval_seconds: float) -> None:
    interval = float(min_interval_seconds or 0.0)
    if interval <= 0:
        return

    provider_key = str(provider or "").strip().lower() or "unknown"

    while True:
        with _RATE_LOCK:
            now = time.monotonic()
            last_call = float(_last_call_monotonic_by_provider.get(provider_key, 0.0) or 0.0)
            wait_seconds = interval - (now - last_call)
            if wait_seconds <= 0:
                _last_call_monotonic_by_provider[provider_key] = now
                return

        time.sleep(min(wait_seconds, 1.0))


def _google_books(isbn: str, *, timeout: float) -> dict[str, Any]:
    _wait_for_provider_slot("google", min_interval_seconds=GOOGLE_BOOKS_MIN_INTERVAL_SECONDS)
    data = _safe_get(f"https://www.googleapis.com/books/v1/volumes?q=isbn:{isbn}", timeout=timeout)
    items = data.get("items") if isinstance(data, dict) else None
    if isinstance(items, list) and items:
        first = items[0]
        if isinstance(first, dict):
            volume = first.get("volumeInfo")
            return volume if isinstance(volume, dict) else {}
    return {}


def _open_library(isbn: str, *, timeout: float) -> dict[str, Any]:
    _wait_for_provider_slot("open_library", min_interval_seconds=OPENLIBRARY_MIN_INTERVAL_SECONDS)
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
