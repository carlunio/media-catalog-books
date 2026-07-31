from fastapi import APIRouter, HTTPException

from ..schemas.snapshots import SnapshotImportRequest, SnapshotPublishRequest
from ..services import snapshots
from ..services.snapshots import SnapshotError

router = APIRouter()


def _raise_snapshot_error(exc: Exception) -> None:
    if isinstance(exc, SnapshotError):
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/snapshots/status")
def snapshots_status():
    return snapshots.get_status()


@router.get("/snapshots")
def snapshots_list():
    return {"ok": True, "snapshots": snapshots.list_snapshots()}


@router.post("/snapshots/publish")
def snapshots_publish(payload: SnapshotPublishRequest | None = None):
    request = payload or SnapshotPublishRequest()
    try:
        return snapshots.publish_snapshot(notes=request.notes, cleanup=request.cleanup)
    except Exception as exc:
        _raise_snapshot_error(exc)


@router.post("/snapshots/import")
def snapshots_import(payload: SnapshotImportRequest):
    try:
        return snapshots.import_snapshot(
            snapshot_id=payload.snapshot_id, confirm=payload.confirm
        )
    except Exception as exc:
        _raise_snapshot_error(exc)


@router.post("/snapshots/cleanup")
def snapshots_cleanup():
    try:
        return {"ok": True, **snapshots.cleanup_snapshots()}
    except Exception as exc:
        _raise_snapshot_error(exc)
