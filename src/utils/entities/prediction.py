from typing import Optional

from .content import ContentBase
from pydantic import BaseModel


class Prediction(BaseModel):
    setId: str
    content: ContentBase
    meta: Optional[dict] = None

    class Config:
        validate_assignment = True
