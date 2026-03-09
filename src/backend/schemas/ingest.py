from typing import Literal

from pydantic import BaseModel, Field


OcrProvider = Literal["auto", "openai", "ollama", "none"]
CatalogProvider = Literal["auto", "openai", "ollama", "none"]


class IngestRequest(BaseModel):
    folder: str
    block: str | None = None
    module: str | None = None
    recursive: bool = True
    extensions: list[str] | None = None
    overwrite_existing_paths: bool = False


class RunOcrRequest(BaseModel):
    book_id: str | None = None
    block: str | None = None
    module: str | None = None
    limit: int = Field(default=20, ge=1, le=5000)
    overwrite: bool = False
    ocr_provider: OcrProvider | None = None
    ocr_model: str | None = None


class RunMetadataRequest(BaseModel):
    book_id: str | None = None
    block: str | None = None
    module: str | None = None
    limit: int = Field(default=20, ge=1, le=5000)
    overwrite: bool = False


class RunCatalogRequest(BaseModel):
    book_id: str | None = None
    block: str | None = None
    module: str | None = None
    limit: int = Field(default=20, ge=1, le=5000)
    overwrite: bool = False
    catalog_provider: CatalogProvider | None = None
    catalog_model: str | None = None


class RunCoverRequest(BaseModel):
    book_id: str | None = None
    block: str | None = None
    module: str | None = None
    limit: int = Field(default=20, ge=1, le=5000)
    overwrite: bool = False
