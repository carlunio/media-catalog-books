from datetime import datetime, timezone
import time
from threading import Lock
from typing import Any

import requests

from ..config import (
    GOOGLE_BOOKS_API_KEY,
    GOOGLE_BOOKS_MIN_INTERVAL_SECONDS,
    ISBNDB_API_KEY,
    OPENLIBRARY_CONTACT,
    OPENLIBRARY_MIN_INTERVAL_SECONDS,
    REQUEST_TIMEOUT_SECONDS,
)
from ..normalizers import clean_isbn, is_valid_isbn
from . import books

_RATE_LOCK = Lock()
_GOOGLE_HEALTH_LOCK = Lock()
_last_call_monotonic_by_provider: dict[str, float] = {
    "google": 0.0,
    "open_library": 0.0,
}

_GOOGLE_RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}
_GOOGLE_MAX_ATTEMPTS = 3
_GOOGLE_HEALTH_TTL_SECONDS = 600.0
_google_field_search_health: tuple[float, bool] | None = None


class GoogleFieldSearchUnavailable(RuntimeError):
    """Raised when Google Books field operators return false empty results."""


def _http_error_detail(response: requests.Response) -> str:
    try:
        payload = response.json()
    except ValueError:
        payload = {}

    if isinstance(payload, dict):
        error = payload.get("error")
        if isinstance(error, dict):
            message = str(error.get("message") or "").strip()
            if message:
                return message
        message = str(payload.get("message") or "").strip()
        if message:
            return message

    return str(response.reason or "request failed").strip()


def _safe_get(
    url: str,
    *,
    headers: dict[str, str] | None = None,
    params: dict[str, Any] | None = None,
    timeout: float = REQUEST_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    response = requests.get(url, headers=headers, params=params, timeout=timeout)
    try:
        response.raise_for_status()
    except requests.HTTPError:
        detail = _http_error_detail(response)
        raise requests.HTTPError(
            f"HTTP {response.status_code}: {detail}",
            response=response,
        ) from None

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
            last_call = float(
                _last_call_monotonic_by_provider.get(provider_key, 0.0) or 0.0
            )
            wait_seconds = interval - (now - last_call)
            if wait_seconds <= 0:
                _last_call_monotonic_by_provider[provider_key] = now
                return

        time.sleep(min(wait_seconds, 1.0))


def _google_get(params: dict[str, Any], *, timeout: float) -> dict[str, Any]:
    data: dict[str, Any] = {}
    for attempt in range(_GOOGLE_MAX_ATTEMPTS):
        _wait_for_provider_slot(
            "google", min_interval_seconds=GOOGLE_BOOKS_MIN_INTERVAL_SECONDS
        )
        try:
            data = _safe_get(
                "https://www.googleapis.com/books/v1/volumes",
                headers={"X-Goog-Api-Key": GOOGLE_BOOKS_API_KEY},
                params=params,
                timeout=timeout,
            )
            break
        except requests.HTTPError as exc:
            status_code = int(getattr(exc.response, "status_code", 0) or 0)
            is_last_attempt = attempt + 1 >= _GOOGLE_MAX_ATTEMPTS
            if status_code not in _GOOGLE_RETRYABLE_STATUS_CODES or is_last_attempt:
                raise
            time.sleep(2**attempt)
    return data


def _isbn_equivalents(value: str) -> set[str]:
    isbn = clean_isbn(value)
    values = {isbn} if isbn else set()
    if len(isbn) == 10:
        base = f"978{isbn[:9]}"
        total = sum(
            int(character) * (1 if index % 2 == 0 else 3)
            for index, character in enumerate(base)
        )
        values.add(f"{base}{(10 - total % 10) % 10}")
    elif len(isbn) == 13 and isbn.startswith("978"):
        base = isbn[3:12]
        total = sum(
            (10 - index) * int(character) for index, character in enumerate(base)
        )
        check = (11 - total % 11) % 11
        values.add(f"{base}{'X' if check == 10 else check}")
    return values


def _google_exact_volume(data: dict[str, Any], isbn: str) -> dict[str, Any]:
    items = data.get("items") if isinstance(data, dict) else None
    if not isinstance(items, list):
        return {}

    requested = _isbn_equivalents(isbn)
    for item in items:
        if not isinstance(item, dict):
            continue
        volume = item.get("volumeInfo")
        if not isinstance(volume, dict):
            continue
        identifiers = volume.get("industryIdentifiers")
        if not isinstance(identifiers, list):
            continue
        returned = {
            clean_isbn(identifier.get("identifier"))
            for identifier in identifiers
            if isinstance(identifier, dict)
        }
        if requested.intersection(returned):
            payload = dict(volume)
            volume_id = str(item.get("id") or "").strip()
            if volume_id:
                payload["google_volume_id"] = volume_id
            return payload
    return {}


def _google_field_search_available(*, timeout: float) -> bool:
    global _google_field_search_health

    now = time.monotonic()
    with _GOOGLE_HEALTH_LOCK:
        cached = _google_field_search_health
        if cached is not None and now - cached[0] < _GOOGLE_HEALTH_TTL_SECONDS:
            return cached[1]

    unrestricted = _google_get({"q": "flowers", "maxResults": 1}, timeout=timeout)
    unrestricted_items = unrestricted.get("items")
    if not isinstance(unrestricted_items, list) or not unrestricted_items:
        raise GoogleFieldSearchUnavailable(
            "Google Books search health check was inconclusive"
        )

    restricted = _google_get({"q": "intitle:flowers", "maxResults": 1}, timeout=timeout)
    restricted_items = restricted.get("items")
    available = isinstance(restricted_items, list) and bool(restricted_items)
    with _GOOGLE_HEALTH_LOCK:
        _google_field_search_health = (time.monotonic(), available)
    return available


def _google_books(
    isbn: str,
    *,
    timeout: float,
    title_candidates: list[str] | None = None,
) -> dict[str, Any]:
    if not GOOGLE_BOOKS_API_KEY:
        raise RuntimeError(
            "GOOGLE_BOOKS_API_KEY is not configured; Google Books requires "
            "an API key for public-data requests"
        )

    exact_data = _google_get(
        {"q": f"isbn:{isbn}", "maxResults": 1},
        timeout=timeout,
    )
    items = exact_data.get("items") if isinstance(exact_data, dict) else None
    if isinstance(items, list) and items:
        first = items[0]
        if isinstance(first, dict):
            volume = first.get("volumeInfo")
            if isinstance(volume, dict):
                payload = dict(volume)
                payload["lookup_method"] = "isbn"
                return payload

    unique_titles: list[str] = []
    seen_titles: set[str] = set()
    for title in title_candidates or []:
        candidate = str(title or "").strip()
        marker = candidate.casefold()
        if candidate and marker not in seen_titles:
            seen_titles.add(marker)
            unique_titles.append(candidate)

    for title in unique_titles[:2]:
        fallback_data = _google_get(
            {"q": title, "maxResults": 40, "printType": "books"},
            timeout=timeout,
        )
        exact_volume = _google_exact_volume(fallback_data, isbn)
        if exact_volume:
            exact_volume["lookup_method"] = "title_validated_by_isbn"
            return exact_volume

    if not _google_field_search_available(timeout=timeout):
        raise GoogleFieldSearchUnavailable(
            "Google Books field search is unavailable: plain queries return "
            "results but isbn:/intitle: return false empty responses"
        )
    return {}


OPENLIBRARY_WORK_FIELDS = ",".join(
    (
        "key",
        "title",
        "subtitle",
        "author_name",
        "author_key",
        "first_publish_year",
        "subject",
        "description",
        "first_sentence",
    )
)


def _open_library_headers() -> dict[str, str]:
    user_agent = "media-catalog-books/1.0"
    if OPENLIBRARY_CONTACT:
        user_agent = f"{user_agent} ({OPENLIBRARY_CONTACT})"
    return {"User-Agent": user_agent}


def _open_library_work_result(document: dict[str, Any]) -> dict[str, Any]:
    payload = dict(document)

    author_names = document.get("author_name")
    author_keys = document.get("author_key")
    names = author_names if isinstance(author_names, list) else []
    keys = author_keys if isinstance(author_keys, list) else []
    authors = []
    for index, name in enumerate(names):
        author_name = str(name or "").strip()
        if not author_name:
            continue
        author = {"name": author_name}
        if index < len(keys) and str(keys[index] or "").strip():
            author["url"] = (
                f"https://openlibrary.org/authors/{str(keys[index]).strip()}"
            )
        authors.append(author)
    if authors:
        payload["authors"] = authors

    work_key = str(document.get("key") or "").strip()
    if work_key:
        payload["url"] = f"https://openlibrary.org{work_key}"

    return payload


def _open_library_edition_result(
    document: dict[str, Any],
    *,
    requested_isbn: str,
) -> dict[str, Any]:
    payload = dict(document)
    isbn_10 = document.get("isbn_10")
    isbn_13 = document.get("isbn_13")
    identifiers = (
        dict(document.get("identifiers"))
        if isinstance(document.get("identifiers"), dict)
        else {}
    )
    identifiers["isbn_10"] = isbn_10 if isinstance(isbn_10, list) else []
    identifiers["isbn_13"] = isbn_13 if isinstance(isbn_13, list) else []
    payload["identifiers"] = identifiers
    payload["requested_isbn"] = requested_isbn

    cover_ids = document.get("covers")
    if isinstance(cover_ids, list):
        normalized_cover_id = next(
            (
                int(value)
                for value in cover_ids
                if str(value).lstrip("-").isdigit() and int(value) > 0
            ),
            0,
        )
        if normalized_cover_id:
            base = f"https://covers.openlibrary.org/b/id/{normalized_cover_id}"
            payload["cover"] = {
                "small": f"{base}-S.jpg",
                "medium": f"{base}-M.jpg",
                "large": f"{base}-L.jpg",
            }

    edition_key = str(document.get("key") or "").strip()
    if edition_key:
        payload["url"] = f"https://openlibrary.org{edition_key}"
    return payload


def _http_status(exc: requests.HTTPError) -> int:
    return int(getattr(exc.response, "status_code", 0) or 0)


def _open_library(isbn: str, *, timeout: float) -> dict[str, Any]:
    edition: dict[str, Any] = {}
    _wait_for_provider_slot(
        "open_library",
        min_interval_seconds=OPENLIBRARY_MIN_INTERVAL_SECONDS,
    )
    try:
        edition_data = _safe_get(
            f"https://openlibrary.org/isbn/{isbn}.json",
            headers=_open_library_headers(),
            timeout=timeout,
        )
        if edition_data:
            edition = _open_library_edition_result(
                edition_data,
                requested_isbn=isbn,
            )
    except requests.HTTPError as exc:
        if _http_status(exc) != 404:
            raise

    _wait_for_provider_slot(
        "open_library",
        min_interval_seconds=OPENLIBRARY_MIN_INTERVAL_SECONDS,
    )
    params: dict[str, Any] = {
        "isbn": isbn,
        "limit": 1,
        "fields": OPENLIBRARY_WORK_FIELDS,
    }
    if OPENLIBRARY_CONTACT:
        params["email"] = OPENLIBRARY_CONTACT

    data = _safe_get(
        "https://openlibrary.org/search.json",
        headers=_open_library_headers(),
        params=params,
        timeout=timeout,
    )
    documents = data.get("docs")
    work: dict[str, Any] = {}
    if isinstance(documents, list) and documents:
        first = documents[0]
        if isinstance(first, dict):
            work = _open_library_work_result(first)

    payload: dict[str, Any] = {"requested_isbn": isbn}
    if edition:
        payload["edition"] = edition
    if work:
        payload["work"] = work
    return payload if edition or work else {}


def _isbndb(isbn: str, *, timeout: float) -> dict[str, Any]:
    if not ISBNDB_API_KEY:
        return {}

    data = _safe_get(
        f"https://api2.isbndb.com/book/{isbn}",
        headers={"Authorization": ISBNDB_API_KEY},
        timeout=timeout,
    )
    return data if isinstance(data, dict) else {}


def _google_title_candidates(
    sources: dict[str, dict[str, Any]],
) -> list[str]:
    candidates: list[str] = []
    open_library = sources.get("open_library") or {}
    if isinstance(open_library, dict):
        edition = open_library.get("edition")
        work = open_library.get("work")
        for payload in (edition, work, open_library):
            if not isinstance(payload, dict):
                continue
            for key in ("title", "subtitle"):
                value = str(payload.get(key) or "").strip()
                if value:
                    candidates.append(value)

    isbndb = sources.get("isbndb") or {}
    book = isbndb.get("book") if isinstance(isbndb, dict) else {}
    if isinstance(book, dict):
        for key in ("title_long", "title"):
            value = str(book.get(key) or "").strip()
            if value:
                candidates.append(value)

    unique: list[str] = []
    seen: set[str] = set()
    for candidate in candidates:
        marker = candidate.casefold()
        if marker not in seen:
            seen.add(marker)
            unique.append(candidate)
    return unique


def run_one(
    book_id: str,
    *,
    overwrite: bool = False,
    timeout: float = REQUEST_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    book = books.get_book(book_id)
    if book is None:
        return {"id": book_id, "status": "error", "error": "Book not found"}

    existing_status = str(book.get("metadata_status") or "").strip().lower()
    if existing_status in {"fetched", "manual"} and not overwrite:
        return {
            "id": book_id,
            "status": "skipped",
            "reason": "metadata already present",
        }

    isbn = clean_isbn(book.get("isbn") or book.get("isbn_raw"))
    if not is_valid_isbn(isbn):
        books.update_metadata(
            book_id,
            metadata={
                "isbn": isbn,
                "google": {},
                "open_library": {},
                "isbndb": {},
            },
            status="skipped",
            error="Invalid or missing ISBN",
        )
        return {
            "id": book_id,
            "status": "skipped",
            "reason": "Invalid or missing ISBN",
            "isbn": isbn,
        }

    sources: dict[str, dict[str, Any]] = {
        "google": {},
        "open_library": {},
        "isbndb": {},
    }
    errors: dict[str, str] = {}

    for source_name, fetcher in (
        ("open_library", _open_library),
        ("isbndb", _isbndb),
    ):
        try:
            sources[source_name] = fetcher(isbn, timeout=timeout)
        except Exception as exc:
            errors[source_name] = str(exc)

    try:
        sources["google"] = _google_books(
            isbn,
            timeout=timeout,
            title_candidates=_google_title_candidates(sources),
        )
    except Exception as exc:
        errors["google"] = str(exc)

    provider_statuses = {
        source_name: (
            "fetched"
            if source_payload
            else ("error" if source_name in errors else "empty")
        )
        for source_name, source_payload in sources.items()
    }
    non_empty_sources = sum(
        1
        for provider_status in provider_statuses.values()
        if provider_status == "fetched"
    )

    payload = {
        "isbn": isbn,
        "google": sources["google"],
        "open_library": sources["open_library"],
        "isbndb": sources["isbndb"],
        "provider_statuses": provider_statuses,
        "errors": errors,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
    }

    if non_empty_sources > 0:
        status = "fetched"
        error = (
            None
            if not errors
            else f"Partial provider errors: {', '.join(sorted(errors.keys()))}"
        )
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
        "provider_statuses": provider_statuses,
        "google_status": provider_statuses["google"],
        "openlibrary_status": provider_statuses["open_library"],
        "isbndb_status": provider_statuses["isbndb"],
        "errors": errors,
    }
