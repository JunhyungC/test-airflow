from typing import Any

from pydantic import BaseModel


class DataMeta(BaseModel):
    key: str
    type: str
    value: Any

    class Config:
        use_enum_values = True
        validate_assignment = True
