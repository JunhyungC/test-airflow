from datetime import datetime
from typing import Optional

from .content_base import ContentBase


class Content(ContentBase):
    key: Optional[str] = None
    location: Optional[dict] = None
    createdAt: Optional[datetime] = None
    createdBy: Optional[str] = None

    class Config:
        validate_assignment = True
