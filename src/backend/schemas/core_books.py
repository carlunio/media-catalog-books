from typing import Any

from pydantic import BaseModel, Field


class UpdateCoreBookRequest(BaseModel):
    fields: dict[str, Any] = Field(default_factory=dict)
    recompute_description: bool = False
