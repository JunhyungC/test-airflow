from typing import Optional, List

from .annotation_version import AnnotationVersion
from pydantic import BaseModel


class Annotation(BaseModel):
    versions: Optional[List[AnnotationVersion]] = None
    meta: Optional[dict] = None

    class Config:
        validate_assignment = True
