from pydantic import BaseModel, Field


class ExportBooksRequest(BaseModel):
    ids: list[str] = Field(default_factory=list)
