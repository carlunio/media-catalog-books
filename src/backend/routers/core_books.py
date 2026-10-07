from fastapi import APIRouter, HTTPException
from fastapi.responses import Response

from ..schemas.core_books import ReviewWorkbookRequest, UpdateCoreBookRequest
from ..services import books, review_workbooks

router = APIRouter()

XLSX_MEDIA_TYPE = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"


@router.post("/core-books/bootstrap")
def bootstrap_core_books(
    block: str | None = None, module: str | None = None, limit: int = 2000
):
    if limit < 1 or limit > 50000:
        raise HTTPException(status_code=400, detail="limit must be between 1 and 50000")
    try:
        return books.bootstrap_core_books(block=block, module=module, limit=limit)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/core-books/review-workbook")
def export_review_workbook(
    block: str,
    module: str,
    form_status: str = "draft",
):
    try:
        result = review_workbooks.create_review_workbook(
            block=block,
            module=module,
            form_status=form_status,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return Response(
        content=result["content"],
        media_type=XLSX_MEDIA_TYPE,
        headers={
            "Content-Disposition": (f'attachment; filename="{result["filename"]}"'),
            "X-Workbook-Rows": str(result["rows"]),
        },
    )


@router.post("/core-books/review-workbook/preview")
def preview_review_workbook(payload: ReviewWorkbookRequest):
    try:
        content = review_workbooks.decode_workbook_base64(payload.content_base64)
        return review_workbooks.preview_review_workbook(content)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/core-books/review-workbook/apply")
def apply_review_workbook(payload: ReviewWorkbookRequest):
    if not payload.confirm:
        raise HTTPException(
            status_code=400,
            detail="La importación requiere confirmación.",
        )
    if not payload.workbook_sha256:
        raise HTTPException(
            status_code=400,
            detail="Analiza el Excel antes de aplicar la importación.",
        )
    try:
        content = review_workbooks.decode_workbook_base64(payload.content_base64)
        return review_workbooks.apply_review_workbook(
            content,
            expected_sha256=payload.workbook_sha256,
            consolidate_after_import=payload.consolidate_after_import,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/core-books/{book_id}/sync")
def sync_core_book(book_id: str, force_overwrite: bool = True):
    try:
        item = books.sync_core_book_from_catalog(
            book_id, force_overwrite=bool(force_overwrite)
        )
    except books.CoreBookLockedError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    if item is None:
        raise HTTPException(status_code=404, detail="Core book not found")
    return {"ok": True, "book": item, "force_overwrite": bool(force_overwrite)}


@router.post("/core-books/{book_id}/create")
def create_core_book(book_id: str):
    item = books.create_core_book_draft(book_id)
    if item is None:
        raise HTTPException(status_code=404, detail="Book not found")
    return {"ok": True, "book": item}


@router.post("/core-books/{book_id}/consolidate")
def consolidate_core_book(book_id: str):
    try:
        item = books.consolidate_core_book(book_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"ok": True, "book": item}


@router.post("/core-books/{book_id}/reopen")
def reopen_core_book(book_id: str):
    try:
        item = books.reopen_core_book(book_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"ok": True, "book": item}


@router.get("/core-books/options")
def core_books_options():
    return {"allowed_values": books.get_books_allowed_values()}


@router.get("/core-books")
def list_core_books(
    limit: int = 500, block: str | None = None, module: str | None = None
):
    if limit < 1 or limit > 50000:
        raise HTTPException(status_code=400, detail="limit must be between 1 and 50000")
    try:
        return books.list_core_books(limit=limit, block=block, module=module)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/core-books/{book_id}")
def get_core_book(book_id: str, bootstrap: bool = False):
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
    except books.CoreBookLockedError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"ok": True, "book": item}
