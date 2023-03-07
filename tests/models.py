from enum import Enum
from typing import Optional

from pydantic import BaseModel


class ExtraModel(BaseModel):
    class Config:
        extra = "allow"


class FieldModel(BaseModel):
    test_field: str
    failures: Optional[int]


class ComposedFieldModel(BaseModel):
    composed: FieldModel
    test_field: str
    failures: Optional[int]


class TestEnum(Enum):
    One = "one"
    Two = "two"
    Three = "three"
