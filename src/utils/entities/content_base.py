from typing import Optional

from pydantic import BaseModel


class ContentBase(BaseModel):
    id: Optional[str] = None

    class Config:
        validate_assignment = True
