from fastapi import APIRouter, HTTPException

from ..config import (
    CATALOG_PROVIDER,
    OCR_PROVIDER,
    OCR_RESIZE_TO_1800_DEFAULT,
    WORKFLOW_MAX_ATTEMPTS,
)
from ..schemas.ingest import (
    RunCatalogRequest,
    RunCoverRequest,
    RunMetadataRequest,
    RunOcrRequest,
)
from ..schemas.workflow import (
    WorkflowMarkReviewRequest,
    WorkflowReviewRequest,
    WorkflowRunRequest,
)
from ..services import books, workflow

router = APIRouter()


def _resolve_max_attempts(value: int | None) -> int:
    return WORKFLOW_MAX_ATTEMPTS if value is None else int(value)


def _resolve_ocr_resize_to_1800(value: bool | None) -> bool:
    return OCR_RESIZE_TO_1800_DEFAULT if value is None else bool(value)


@router.post("/workflow/run")
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


@router.get("/workflow/graph")
def workflow_graph():
    return workflow.graph_definition()


@router.get("/workflow/snapshot")
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


@router.get("/workflow/eligible")
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


@router.post("/workflow/review/{book_id}")
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


@router.post("/workflow/review/{book_id}/mark")
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


@router.post("/ocr/run")
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


@router.post("/metadata/fetch")
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


@router.post("/catalog/build")
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


@router.post("/cover/download")
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
