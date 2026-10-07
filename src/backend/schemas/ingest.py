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
    normalize_image_names: bool = True
    convert_heic: bool = True
    delete_original_heic: bool = False
    preparation_fingerprint: str | None = None


class RunOcrRequest(BaseModel):
    book_id: str | None = None
    block: str | None = None
    module: str | None = None
    limit: int = Field(default=20, ge=1, le=5000)
    overwrite: bool = False
    ocr_provider: OcrProvider | None = None
    ocr_model: str | None = None
    ocr_resize_to_1800: bool | None = None


class RunMetadataRequest(BaseModel):
    book_id: str | None = None
    block: str | None = None
    module: str | None = None
    limit: int = Field(default=20, ge=1, le=5000)
    overwrite: bool = False
    download_cover_after_metadata: bool = True


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
