from fastapi import APIRouter, HTTPException

from ..clients import ClientError, list_ollama_models
from ..services import books

router = APIRouter()


@router.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@router.get("/stats")
def stats(block: str | None = None, module: str | None = None) -> dict[str, int]:
    try:
        return books.get_stats(block=block, module=module)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/models/ollama")
def ollama_models():
    try:
        return {"models": list_ollama_models()}
    except ClientError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
