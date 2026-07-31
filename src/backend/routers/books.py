import time

from fastapi import APIRouter, HTTPException

from ..normalizers import clean_isbn, is_valid_isbn
from ..schemas.review import UpdateCatalogRequest, UpdateMetadataRequest, UpdateOcrRequest
from ..services import books, ocr

router = APIRouter()

_TRANSIENT_DB_ERROR_TOKENS = (
    "conflicting lock",
    "write-write conflict",
    "database is locked",
    "transaction conflict",
)


def _is_transient_db_error(exc: Exception) -> bool:
    message = str(exc or "").strip().lower()
    if not message:
        return False
    return any(token in message for token in _TRANSIENT_DB_ERROR_TOKENS)


def _update_ocr_with_retry(
    book_id: str,
    *,
    credits_text: str | None,
    isbn_raw: str | None,
    isbn: str | None,
    trace: dict,
) -> None:
    attempts = 3
    for attempt in range(attempts):
        try:
            books.update_ocr(
                book_id,
                credits_text=credits_text,
                isbn_raw=isbn_raw,
                isbn=isbn,
                status="manual",
                provider="manual",
                model=None,
                trace=trace,
                error=None,
            )
            return
        except Exception as exc:
            if attempt == attempts - 1 or not _is_transient_db_error(exc):
                raise
            time.sleep(0.2 * (attempt + 1))


@router.get("/books")
def list_books(stage: str | None = None, limit: int = 500, block: str | None = None, module: str | None = None):
    try:
        return books.list_books(stage=stage, limit=limit, block=block, module=module)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/books/{book_id}")
def get_book(book_id: str):
    item = books.get_book(book_id)
    if item is None:
        raise HTTPException(status_code=404, detail="Book not found")
    return item


@router.put("/books/{book_id}/ocr")
def update_book_ocr(book_id: str, payload: UpdateOcrRequest):
    if books.get_book(book_id) is None:
        raise HTTPException(status_code=404, detail="Book not found")

    credits_text = str(payload.credits_text or "").strip() or None
    manual_isbn_raw = str(payload.isbn_raw or "").strip() or None
    manual_isbn = str(payload.isbn or "").strip() or None

    derived = ocr.derive_isbn_from_text(credits_text)

    isbn_raw_value = manual_isbn_raw or manual_isbn or derived.get("isbn_raw")
    final_isbn: str | None = None
    isbn_source = "derived_from_text"

    if manual_isbn:
        normalized = clean_isbn(manual_isbn)
        if is_valid_isbn(normalized):
            final_isbn = normalized
            isbn_source = "manual_isbn"
        else:
            isbn_source = "manual_isbn_invalid"
    elif manual_isbn_raw:
        normalized_raw = clean_isbn(manual_isbn_raw)
        if is_valid_isbn(normalized_raw):
            final_isbn = normalized_raw
            isbn_source = "manual_isbn_raw_valid"
        elif derived.get("isbn"):
            final_isbn = str(derived.get("isbn"))
            isbn_source = "manual_isbn_raw_invalid_fallback"
    elif derived.get("isbn"):
        final_isbn = str(derived.get("isbn"))
        isbn_source = str(derived.get("source") or "derived_from_text")

    derived_candidates = derived.get("raw_candidates") if isinstance(derived.get("raw_candidates"), list) else []
    compact_candidates = [str(item) for item in derived_candidates[:5] if str(item).strip()]

    trace = {
        "source": "manual_update",
        "isbn_extraction": {
            "provider": "manual",
            "source": isbn_source,
            "isbn_raw": isbn_raw_value,
            "isbn": final_isbn,
            "is_valid": bool(final_isbn),
            "candidates": compact_candidates,
            "candidates_count": len(derived_candidates),
            "manual_input": {
                "isbn_raw": manual_isbn_raw,
                "isbn": manual_isbn,
            },
        },
    }

    try:
        _update_ocr_with_retry(
            book_id,
            credits_text=credits_text,
            isbn_raw=isbn_raw_value,
            isbn=final_isbn,
            trace=trace,
        )
    except Exception as exc:
        if _is_transient_db_error(exc):
            raise HTTPException(
                status_code=503,
                detail=(
                    "DuckDB busy or write conflict while saving OCR/ISBN. "
                    "Try again in a few seconds."
                ),
            ) from exc
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    return {
        "ok": True,
        "isbn": final_isbn,
        "isbn_raw": isbn_raw_value,
        "isbn_valid": bool(final_isbn),
        "isbn_source": isbn_source,
        "validation": {
            "manual_input": {
                "isbn_raw": manual_isbn_raw,
                "isbn": manual_isbn,
            },
            "derived_candidates": compact_candidates,
            "derived_candidates_count": len(derived_candidates),
            "final": {
                "isbn_raw": isbn_raw_value,
                "isbn": final_isbn,
                "is_valid": bool(final_isbn),
                "source": isbn_source,
            },
        },
    }


@router.put("/books/{book_id}/metadata")
def update_book_metadata(book_id: str, payload: UpdateMetadataRequest):
    if books.get_book(book_id) is None:
        raise HTTPException(status_code=404, detail="Book not found")

    books.update_metadata(book_id, metadata=payload.metadata, status="manual", error=None)
    return {"ok": True}


@router.put("/books/{book_id}/catalog")
def update_book_catalog(book_id: str, payload: UpdateCatalogRequest):
    if books.get_book(book_id) is None:
        raise HTTPException(status_code=404, detail="Book not found")

    books.update_catalog(book_id, catalog=payload.catalog, status="manual", error=None)
    return {"ok": True}
