from typing import Optional

from .content import ContentBase
from pydantic import BaseModel


class AnnotationVersion(BaseModel):
    id: Optional[str] = None
    content: Optional[ContentBase] = None
    meta: Optional[dict] = None

    class Config:
        validate_assignment = True
