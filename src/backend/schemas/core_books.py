from typing import Any

from pydantic import BaseModel, Field


class UpdateCoreBookRequest(BaseModel):
    fields: dict[str, Any] = Field(default_factory=dict)
    recompute_description: bool = False


class ReviewWorkbookRequest(BaseModel):
    content_base64: str
    workbook_sha256: str | None = None
    confirm: bool = False
    consolidate_after_import: bool = False
