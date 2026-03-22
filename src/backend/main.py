from datetime import datetime
from pathlib import Path
import time

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse

from .clients import ClientError, list_ollama_models
from .config import (
    CATALOG_PROVIDER,
    OCR_PROVIDER,
    OCR_RESIZE_TO_1800_DEFAULT,
    WORKFLOW_MAX_ATTEMPTS,
)
from .normalizers import clean_isbn, is_valid_isbn
from .schemas.ingest import (
    IngestRequest,
    RunCatalogRequest,
    RunCoverRequest,
    RunMetadataRequest,
    RunOcrRequest,
)
from .schemas.core_books import UpdateCoreBookRequest
from .schemas.review import UpdateCatalogRequest, UpdateMetadataRequest, UpdateOcrRequest
from .schemas.workflow import WorkflowMarkReviewRequest, WorkflowReviewRequest, WorkflowRunRequest
from .services import books, export, ocr, workflow

app = FastAPI(title="Media Catalog Books API", version="0.1.0")

books.init_table()
_recovered_stale_runs = books.recover_stale_running_workflows()
if _recovered_stale_runs:
    print(f"[startup] recovered {_recovered_stale_runs} stale workflow runs")


def _resolve_max_attempts(value: int | None) -> int:
    return WORKFLOW_MAX_ATTEMPTS if value is None else int(value)


def _resolve_ocr_resize_to_1800(value: bool | None) -> bool:
    return OCR_RESIZE_TO_1800_DEFAULT if value is None else bool(value)


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


def _update_ocr_with_retry(book_id: str, *, credits_text: str | None, isbn_raw: str | None, isbn: str | None, trace: dict) -> None:
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


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/stats")
def stats(block: str | None = None, module: str | None = None) -> dict[str, int]:
    try:
        return books.get_stats(block=block, module=module)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/models/ollama")
def ollama_models():
    try:
        return {"models": list_ollama_models()}
    except ClientError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.post("/covers/ingest")
def ingest_covers(payload: IngestRequest):
    try:
        return books.ingest_covers(
            payload.folder,
            block=payload.block,
            module=payload.module,
            recursive=payload.recursive,
            extensions=payload.extensions,
            overwrite_existing_paths=payload.overwrite_existing_paths,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/workflow/run")
def workflow_run(payload: WorkflowRunRequest):
    try:
        return workflow.run_batch(
            book_id=payload.book_id,
            block=payload.block,
            module=payload.module,
            limit=payload.limit,
            start_stage=payload.start_stage,
            stop_after=payload.stop_after,
            action=payload.action,
            overwrite=payload.overwrite,
            max_attempts=_resolve_max_attempts(payload.max_attempts),
            ocr_provider=payload.ocr_provider or OCR_PROVIDER,
            ocr_model=payload.ocr_model,
            ocr_resize_to_1800=_resolve_ocr_resize_to_1800(payload.ocr_resize_to_1800),
            catalog_provider=payload.catalog_provider or CATALOG_PROVIDER,
            catalog_model=payload.catalog_model,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/workflow/graph")
def workflow_graph():
    return workflow.graph_definition()


@app.get("/workflow/snapshot")
def workflow_snapshot(
    limit: int = 5000,
    review_limit: int = 200,
    block: str | None = None,
    module: str | None = None,
):
    if limit < 1 or limit > 50000:
        raise HTTPException(status_code=400, detail="limit must be between 1 and 50000")
    if review_limit < 1 or review_limit > 5000:
        raise HTTPException(status_code=400, detail="review_limit must be between 1 and 5000")
    try:
        return workflow.snapshot(limit=limit, review_limit=review_limit, block=block, module=module)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/workflow/eligible")
def workflow_eligible(
    start_stage: str = "ocr",
    overwrite: bool = False,
    block: str | None = None,
    module: str | None = None,
):
    try:
        return workflow.eligible_count(
            start_stage=start_stage,
            overwrite=overwrite,
            block=block,
            module=module,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/workflow/review/{book_id}")
def workflow_review_action(book_id: str, payload: WorkflowReviewRequest):
    if books.get_book(book_id) is None:
        raise HTTPException(status_code=404, detail="Book not found")

    try:
        result = workflow.review_action(
            book_id,
            action=payload.action,
            max_attempts=_resolve_max_attempts(payload.max_attempts),
            ocr_provider=payload.ocr_provider or OCR_PROVIDER,
            ocr_model=payload.ocr_model,
            ocr_resize_to_1800=_resolve_ocr_resize_to_1800(payload.ocr_resize_to_1800),
            catalog_provider=payload.catalog_provider or CATALOG_PROVIDER,
            catalog_model=payload.catalog_model,
        )
        return {"ok": True, "result": result}
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.post("/workflow/review/{book_id}/mark")
def workflow_mark_review(book_id: str, payload: WorkflowMarkReviewRequest):
    if books.get_book(book_id) is None:
        raise HTTPException(status_code=404, detail="Book not found")

    try:
        result = workflow.mark_review(book_id, reason=payload.reason, node=payload.node)
        return {"ok": True, "result": result}
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# Legacy-compatible stage endpoints
@app.post("/ocr/run")
def run_ocr(payload: RunOcrRequest):
    try:
        return workflow.run_batch(
            book_id=payload.book_id,
            block=payload.block,
            module=payload.module,
            limit=payload.limit,
            start_stage="ocr",
            stop_after="ocr",
            overwrite=payload.overwrite,
            max_attempts=WORKFLOW_MAX_ATTEMPTS,
            ocr_provider=payload.ocr_provider or OCR_PROVIDER,
            ocr_model=payload.ocr_model,
            ocr_resize_to_1800=_resolve_ocr_resize_to_1800(payload.ocr_resize_to_1800),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.post("/metadata/fetch")
def run_metadata(payload: RunMetadataRequest):
    try:
        return workflow.run_batch(
            book_id=payload.book_id,
            block=payload.block,
            module=payload.module,
            limit=payload.limit,
            start_stage="metadata",
            stop_after="metadata",
            overwrite=payload.overwrite,
            max_attempts=WORKFLOW_MAX_ATTEMPTS,
            catalog_provider=payload.catalog_provider or CATALOG_PROVIDER,
            catalog_model=payload.catalog_model,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.post("/catalog/build")
def run_catalog(payload: RunCatalogRequest):
    try:
        return workflow.run_batch(
            book_id=payload.book_id,
            block=payload.block,
            module=payload.module,
            limit=payload.limit,
            start_stage="catalog",
            stop_after="catalog",
            overwrite=payload.overwrite,
            max_attempts=WORKFLOW_MAX_ATTEMPTS,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.post("/cover/download")
def run_cover(payload: RunCoverRequest):
    try:
        return workflow.run_batch(
            book_id=payload.book_id,
            block=payload.block,
            module=payload.module,
            limit=payload.limit,
            start_stage="cover",
            stop_after="cover",
            overwrite=payload.overwrite,
            max_attempts=WORKFLOW_MAX_ATTEMPTS,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/books")
def list_books(stage: str | None = None, limit: int = 500, block: str | None = None, module: str | None = None):
    try:
        return books.list_books(stage=stage, limit=limit, block=block, module=module)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/books/{book_id}")
def get_book(book_id: str):
    item = books.get_book(book_id)
    if item is None:
        raise HTTPException(status_code=404, detail="Book not found")
    return item


@app.put("/books/{book_id}/ocr")
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


@app.put("/books/{book_id}/metadata")
def update_book_metadata(book_id: str, payload: UpdateMetadataRequest):
    if books.get_book(book_id) is None:
        raise HTTPException(status_code=404, detail="Book not found")

    books.update_metadata(book_id, metadata=payload.metadata, status="manual", error=None)
    return {"ok": True}


@app.put("/books/{book_id}/catalog")
def update_book_catalog(book_id: str, payload: UpdateCatalogRequest):
    if books.get_book(book_id) is None:
        raise HTTPException(status_code=404, detail="Book not found")

    books.update_catalog(book_id, catalog=payload.catalog, status="manual", error=None)
    return {"ok": True}


@app.post("/core-books/bootstrap")
def bootstrap_core_books(block: str | None = None, module: str | None = None, limit: int = 2000):
    if limit < 1 or limit > 50000:
        raise HTTPException(status_code=400, detail="limit must be between 1 and 50000")
    try:
        return books.bootstrap_core_books(block=block, module=module, limit=limit)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/core-books/{book_id}/sync")
def sync_core_book(book_id: str, force_overwrite: bool = True):
    item = books.sync_core_book_from_catalog(book_id, force_overwrite=bool(force_overwrite))
    if item is None:
        raise HTTPException(status_code=404, detail="Core book not found")
    return {"ok": True, "book": item, "force_overwrite": bool(force_overwrite)}


@app.get("/core-books/options")
def core_books_options():
    return {"allowed_values": books.get_books_allowed_values()}


@app.get("/core-books")
def list_core_books(limit: int = 500, block: str | None = None, module: str | None = None):
    if limit < 1 or limit > 50000:
        raise HTTPException(status_code=400, detail="limit must be between 1 and 50000")
    try:
        return books.list_core_books(limit=limit, block=block, module=module)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/core-books/{book_id}")
def get_core_book(book_id: str, bootstrap: bool = True):
    item = books.get_core_book(book_id, bootstrap=bootstrap)
    if item is None:
        raise HTTPException(status_code=404, detail="Core book not found")
    return item


@app.put("/core-books/{book_id}")
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


@app.get("/export/books/txt")
@app.get("/export/books/tsv")
def export_txt(
    block: str | None = None,
    modules: str | None = None,
    encoding: str = "windows-1252",
):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = Path("data/output/exports")
    output = output_dir / f"books_{timestamp}.txt"
    try:
        result = export.export_books_tsv(
            output,
            block=block,
            modules=modules,
            encoding=encoding,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return {
        "ok": True,
        "path": str(result["path"]),
        "filename": Path(str(result["path"])).name,
        "rows": int(result["rows"]),
        "encoding": str(result["encoding"]),
        "block": result.get("block"),
        "prefixes": result.get("prefixes", []),
    }


@app.get("/export/books/file")
def export_file(filename: str):
    name = str(filename or "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="filename is required")
    if "/" in name or "\\" in name:
        raise HTTPException(status_code=400, detail="invalid filename")
    if not name.lower().endswith(".txt"):
        raise HTTPException(status_code=400, detail="only .txt exports are allowed")

    path = Path("data/output/exports") / name
    if not path.exists() or not path.is_file():
        raise HTTPException(status_code=404, detail="export file not found")

    return FileResponse(
        path=str(path),
        media_type="text/plain",
        filename=name,
    )


@app.get("/export/books/preview")
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

    return {
        "ok": True,
        "columns": columns,
        "rows": rows,
        "count": len(rows),
        "block": normalized_block,
        "prefixes": prefixes,
    }
