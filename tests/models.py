from datetime import date, time, datetime
from enum import Enum
from typing import Optional, Dict, Any, List, Set

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


class CountEnum(Enum):
    One = "one"
    Two = "two"
    Three = "three"


class Example(BaseModel):
    dict_field: Dict[str, Any]
    model_field: FieldModel
    list_field: List[Any]
    set_field: Set[Any]
    date_field: date
    time_field: time
    datetime_field: datetime
    enum_field: CountEnum
    int_field: int
    optional_field: Optional[int]
