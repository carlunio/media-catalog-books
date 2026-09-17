from fastapi import APIRouter, HTTPException

from ..schemas.ingest import IngestRequest
from ..services import books

router = APIRouter()


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
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
