from abc import abstractmethod, ABC
from typing import Generic, Optional, List, Iterable, Sequence, Tuple

from pydantic.generics import GenericModel

from pydantic_dynamo.models import ObjT, PartitionedContent, FilterCommand, UpdateCommand


class GetResponse(GenericModel, Generic[ObjT]):
    item: Optional[PartitionedContent[ObjT]]


class BatchResponse(GenericModel, Generic[ObjT]):
    items: List[PartitionedContent[ObjT]]


class QueryResponse(GenericModel, Generic[ObjT]):
    items: Iterable[PartitionedContent[ObjT]]


class ReadOnlyAbstractRepository(ABC, Generic[ObjT]):
    @abstractmethod
    def get(
        self, partition_id: Optional[Sequence[str]], content_id: Optional[Sequence[str]]
    ) -> GetResponse:
        pass

    @abstractmethod
    def get_batch(
        self,
        request_ids: Sequence[Tuple[Optional[Sequence[str]], Optional[Sequence[str]]]],
    ) -> BatchResponse:
        pass

    @abstractmethod
    def list(
        self,
        partition_id: Optional[Sequence[str]],
        content_prefix: Optional[Sequence[str]],
        sort_ascending: bool = True,
        limit: Optional[int] = None,
        filters: Optional[FilterCommand] = None,
    ) -> QueryResponse:
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
    ) -> QueryResponse:
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
    ) -> None:
        pass

    @abstractmethod
    def delete(
        self,
        partition_id: Optional[Sequence[str]],
        content_prefix: Optional[Sequence[str]],
    ) -> None:
        pass
