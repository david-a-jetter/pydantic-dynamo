from abc import ABC, abstractmethod
from datetime import datetime
from typing import (
    Generic,
    TypeVar,
    List,
    Optional,
    Dict,
    Any,
    Union,
    Set,
    Sequence,
    Tuple,
    Iterator,
    Iterable,
)

from pydantic import BaseModel
from pydantic.generics import GenericModel

ObjT = TypeVar("ObjT", bound=BaseModel)


class PartitionedContent(GenericModel, Generic[ObjT]):
    partition_ids: List[str]
    content_ids: List[str]
    item: ObjT
    current_version: int = 1
    expiry: Optional[datetime]


class UpdateCommand(BaseModel):
    current_version: Optional[int]
    set_commands: Dict[str, Any] = {}
    increment_attrs: Dict[str, int] = {}
    append_attrs: Dict[str, Optional[List[Union[str, Dict]]]] = {}
    expiry: Optional[datetime]


class FilterCommand(BaseModel):
    not_exists: Set[str] = set()
    equals: Dict[str, Any] = {}
    not_equals: Dict[str, Any] = {}


class ReadOnlyAbstractRepository(ABC, Generic[ObjT]):
    @abstractmethod
    def get(
        self, partition_id: Optional[Sequence[str]], content_id: Optional[Sequence[str]]
    ) -> Optional[ObjT]:
        pass

    @abstractmethod
    def get_batch(
        self,
        request_ids: Sequence[Tuple[Optional[Sequence[str]], Optional[Sequence[str]]]],
    ) -> List[ObjT]:
        pass

    @abstractmethod
    def list(
        self,
        partition_id: Optional[Sequence[str]],
        content_prefix: Optional[Sequence[str]],
        sort_ascending: bool = True,
        limit: Optional[int] = None,
        filters: Optional[FilterCommand] = None,
    ) -> Iterator[ObjT]:
        pass

    @abstractmethod
    def list_between(
        self,
        partition_id: Optional[Sequence[str]],
        content_start: Optional[Sequence[str]],
        content_end: Optional[Sequence[str]],
        sort_ascending: bool = True,
        limit: Optional[int] = None,
        filters: Optional[FilterCommand] = None,
    ) -> Iterator[ObjT]:
        pass


class AbstractRepository(ReadOnlyAbstractRepository[ObjT], ABC):
    @abstractmethod
    def put(self, content: PartitionedContent[ObjT]) -> None:
        pass

    @abstractmethod
    def put_batch(self, content: Iterable[PartitionedContent[ObjT]]) -> None:
        pass

    @abstractmethod
    def update(
        self,
        partition_id: Optional[Sequence[str]],
        content_id: Optional[Sequence[str]],
        command: UpdateCommand,
        require_exists: bool,
    ) -> None:
        pass

    @abstractmethod
    def delete(
        self,
        partition_id: Optional[Sequence[str]],
        content_prefix: Optional[Sequence[str]],
    ) -> None:
        pass
