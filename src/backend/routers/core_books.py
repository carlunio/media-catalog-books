from fastapi import APIRouter, HTTPException

from ..schemas.core_books import UpdateCoreBookRequest
from ..services import books

router = APIRouter()


@router.post("/core-books/bootstrap")
def bootstrap_core_books(block: str | None = None, module: str | None = None, limit: int = 2000):
    if limit < 1 or limit > 50000:
        raise HTTPException(status_code=400, detail="limit must be between 1 and 50000")
    try:
        return books.bootstrap_core_books(block=block, module=module, limit=limit)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/core-books/{book_id}/sync")
def sync_core_book(book_id: str, force_overwrite: bool = True):
    item = books.sync_core_book_from_catalog(book_id, force_overwrite=bool(force_overwrite))
    if item is None:
        raise HTTPException(status_code=404, detail="Core book not found")
    return {"ok": True, "book": item, "force_overwrite": bool(force_overwrite)}


@router.get("/core-books/options")
def core_books_options():
    return {"allowed_values": books.get_books_allowed_values()}


@router.get("/core-books")
def list_core_books(limit: int = 500, block: str | None = None, module: str | None = None):
    if limit < 1 or limit > 50000:
        raise HTTPException(status_code=400, detail="limit must be between 1 and 50000")
    try:
        return books.list_core_books(limit=limit, block=block, module=module)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/core-books/{book_id}")
def get_core_book(book_id: str, bootstrap: bool = True):
    item = books.get_core_book(book_id, bootstrap=bootstrap)
    if item is None:
        raise HTTPException(status_code=404, detail="Core book not found")
    return item


@router.put("/core-books/{book_id}")
def update_core_book(book_id: str, payload: UpdateCoreBookRequest):
    try:
        item = books.update_core_book(
            book_id,
            fields=payload.fields,
            recompute_description=bool(payload.recompute_description),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"ok": True, "book": item}
