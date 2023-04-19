from abc import abstractmethod, ABC
from typing import Generic, Optional, Iterable, Sequence, Tuple, AsyncIterable

from pydantic.generics import GenericModel

from pydantic_dynamo.models import ObjT, PartitionedContent, FilterCommand, UpdateCommand


class GetResponse(GenericModel, Generic[ObjT]):
    content: Optional[PartitionedContent[ObjT]]


class BatchResponse(GenericModel, Generic[ObjT]):
    contents: Iterable[PartitionedContent[ObjT]]


class ReadOnlyAbstractRepository(ABC, Generic[ObjT]):
    @abstractmethod
    async def get(
        self, partition_id: Optional[Sequence[str]], content_id: Optional[Sequence[str]]
    ) -> GetResponse:
        pass

    @abstractmethod
    def get_batch(
        self,
        request_ids: Sequence[Tuple[Optional[Sequence[str]], Optional[Sequence[str]]]],
    ) -> AsyncIterable[BatchResponse]:
        pass

    @abstractmethod
    def list(
        self,
        partition_id: Optional[Sequence[str]],
        content_prefix: Optional[Sequence[str]],
        sort_ascending: bool = True,
        limit: Optional[int] = None,
        filters: Optional[FilterCommand] = None,
    ) -> AsyncIterable[BatchResponse]:
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
    ) -> AsyncIterable[BatchResponse]:
        pass


class AbstractRepository(ReadOnlyAbstractRepository[ObjT], ABC):
    @abstractmethod
    async def put(self, content: PartitionedContent[ObjT]) -> None:
        pass

    @abstractmethod
    async def put_batch(self, content: Iterable[PartitionedContent[ObjT]]) -> None:
        pass

    @abstractmethod
    async def update(
        self,
        partition_id: Optional[Sequence[str]],
        content_id: Optional[Sequence[str]],
        command: UpdateCommand,
        require_exists: bool = True,
    ) -> None:
        pass

    @abstractmethod
    async def delete(
        self,
        partition_id: Optional[Sequence[str]],
        content_prefix: Optional[Sequence[str]],
    ) -> None:
        pass


class SyncReadOnlyAbstractRepository(ABC, Generic[ObjT]):
    @abstractmethod
    def get(
        self, partition_id: Optional[Sequence[str]], content_id: Optional[Sequence[str]]
    ) -> GetResponse:
        pass

    @abstractmethod
    def get_batch(
        self,
        request_ids: Sequence[Tuple[Optional[Sequence[str]], Optional[Sequence[str]]]],
    ) -> Iterable[BatchResponse]:
        pass

    @abstractmethod
    def list(
        self,
        partition_id: Optional[Sequence[str]],
        content_prefix: Optional[Sequence[str]],
        sort_ascending: bool = True,
        limit: Optional[int] = None,
        filters: Optional[FilterCommand] = None,
    ) -> Iterable[BatchResponse]:
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
    ) -> Iterable[BatchResponse]:
        pass


class SyncAbstractRepository(SyncReadOnlyAbstractRepository[ObjT], ABC):
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
        require_exists: bool = True,
    ) -> None:
        pass

    @abstractmethod
    def delete(
        self,
        partition_id: Optional[Sequence[str]],
        content_prefix: Optional[Sequence[str]],
    ) -> None:
        pass
