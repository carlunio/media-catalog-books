from typing import Any

from pydantic import BaseModel, Field


class UpdateOcrRequest(BaseModel):
    credits_text: str | None = None
    isbn_raw: str | None = None
    isbn: str | None = None


class UpdateMetadataRequest(BaseModel):
    metadata: dict[str, Any] = Field(default_factory=dict)


class UpdateCatalogRequest(BaseModel):
    catalog: dict[str, Any] = Field(default_factory=dict)
