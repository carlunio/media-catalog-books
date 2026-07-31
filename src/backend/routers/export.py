from datetime import datetime
from pathlib import Path

from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse

from ..config import EXPORTS_DIR
from ..schemas.export import ExportBooksRequest
from ..services import export

router = APIRouter()


def _export_txt_response(result: dict) -> dict:
    path = Path(str(result["path"]))
    return {
        "ok": True,
        "path": str(path),
        "filename": path.name,
        "rows": int(result["rows"]),
        "encoding": str(result["encoding"]),
        "block": result.get("block"),
        "prefixes": result.get("prefixes", []),
        "ids": result.get("ids", []),
    }


@router.get("/export/books/txt")
@router.get("/export/books/tsv")
def export_txt(
    block: str | None = None,
    modules: str | None = None,
    encoding: str = "windows-1252",
):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output = EXPORTS_DIR / f"books_{timestamp}.txt"
    try:
        result = export.export_books_tsv(
            output,
            block=block,
            modules=modules,
            encoding=encoding,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return _export_txt_response(result)


@router.post("/export/books/txt")
@router.post("/export/books/tsv")
def export_txt_selected(payload: ExportBooksRequest, encoding: str = "windows-1252"):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output = EXPORTS_DIR / f"books_{timestamp}.txt"
    try:
        result = export.export_books_tsv(
            output,
            ids=payload.ids,
            encoding=encoding,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return _export_txt_response(result)


@router.get("/export/books/file")
def export_file(filename: str):
    name = str(filename or "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="filename is required")
    if "/" in name or "\\" in name:
        raise HTTPException(status_code=400, detail="invalid filename")
    if not name.lower().endswith(".txt"):
        raise HTTPException(status_code=400, detail="only .txt exports are allowed")

    path = EXPORTS_DIR / name
    if not path.exists() or not path.is_file():
        raise HTTPException(status_code=404, detail="export file not found")

    return FileResponse(
        path=str(path),
        media_type="text/plain",
        filename=name,
    )


@router.get("/export/books/preview")
def export_preview(
    limit: int = 300,
    block: str | None = None,
    modules: str | None = None,
):
    if limit < 1 or limit > 50000:
        raise HTTPException(status_code=400, detail="limit must be between 1 and 50000")

    try:
        columns, rows, normalized_block, prefixes = export.query_export_rows(
            block=block,
            modules=modules,
            limit=limit,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    ids = [str(row.get("listingid") or "").strip() for row in rows if str(row.get("listingid") or "").strip()]
    return {
        "ok": True,
        "columns": columns,
        "rows": rows,
        "count": len(rows),
        "block": normalized_block,
        "prefixes": prefixes,
        "ids": ids,
        "validation": export.validate_export_rows(ids=ids),
    }


@router.get("/export/books/validate")
def export_validate_all(
    block: str | None = None,
    modules: str | None = None,
):
    return export.validate_export_rows(block=block, modules=modules)


@router.post("/export/books/validate")
def export_validate_selected(payload: ExportBooksRequest):
    return export.validate_export_rows(ids=payload.ids)


@router.post("/export/books/mark-uploaded")
@router.post("/export/books/clear-operation")
def export_mark_uploaded(payload: ExportBooksRequest):
    return {"ok": True, **export.mark_exported_books_uploaded(payload.ids)}
