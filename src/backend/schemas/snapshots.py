from pydantic import BaseModel


class SnapshotPublishRequest(BaseModel):
    notes: str | None = None
    cleanup: bool = True


class SnapshotImportRequest(BaseModel):
    snapshot_id: str
    confirm: bool = False
