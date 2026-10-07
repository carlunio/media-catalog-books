from fastapi import APIRouter, HTTPException

from ..schemas.ingest import IngestRequest
from ..services import books

router = APIRouter()


@router.post("/covers/ingest/plan")
def plan_ingest_covers(payload: IngestRequest):
    try:
        return books.plan_ingest_covers(
            payload.folder,
            block=payload.block,
            module=payload.module,
            recursive=payload.recursive,
            extensions=payload.extensions,
            normalize_image_names=payload.normalize_image_names,
            convert_heic=payload.convert_heic,
            delete_original_heic=payload.delete_original_heic,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/covers/ingest")
def ingest_covers(payload: IngestRequest):
    try:
        return books.ingest_covers(
            payload.folder,
            block=payload.block,
            module=payload.module,
            recursive=payload.recursive,
            extensions=payload.extensions,
            overwrite_existing_paths=payload.overwrite_existing_paths,
            normalize_image_names=payload.normalize_image_names,
            convert_heic=payload.convert_heic,
            delete_original_heic=payload.delete_original_heic,
            preparation_fingerprint=payload.preparation_fingerprint,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
