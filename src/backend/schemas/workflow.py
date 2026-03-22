from typing import Literal

from pydantic import BaseModel, Field

WorkflowStage = Literal["ocr", "metadata", "catalog", "cover"]
OcrProvider = Literal["auto", "openai", "ollama", "none"]
CatalogProvider = Literal["auto", "openai", "ollama", "none"]
WorkflowReviewAction = Literal[
    "approve",
    "retry_from_ocr",
    "retry_from_metadata",
    "retry_from_catalog",
    "retry_from_cover",
]


class WorkflowRunRequest(BaseModel):
    book_id: str | None = None
    block: str | None = None
    module: str | None = None
    limit: int = Field(default=20, ge=1, le=5000)
    start_stage: WorkflowStage = "ocr"
    stop_after: WorkflowStage | None = None
    action: str | None = None
    overwrite: bool = False
    max_attempts: int | None = Field(default=None, ge=0, le=20)
    ocr_provider: OcrProvider | None = None
    ocr_model: str | None = None
    ocr_resize_to_1800: bool | None = None
    catalog_provider: CatalogProvider | None = None
    catalog_model: str | None = None


class WorkflowReviewRequest(BaseModel):
    action: WorkflowReviewAction
    max_attempts: int | None = Field(default=None, ge=0, le=20)
    ocr_provider: OcrProvider | None = None
    ocr_model: str | None = None
    ocr_resize_to_1800: bool | None = None
    catalog_provider: CatalogProvider | None = None
    catalog_model: str | None = None


class WorkflowMarkReviewRequest(BaseModel):
    reason: str | None = None
    node: str = "manual"
