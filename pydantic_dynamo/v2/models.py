from abc import abstractmethod
from contextlib import AbstractContextManager, AbstractAsyncContextManager
from typing import (
    Generic,
    Optional,
    Iterable,
    Sequence,
    Tuple,
    AsyncIterator,
    Iterator,
)

from pydantic.generics import GenericModel

from pydantic_dynamo.models import ObjT, PartitionedContent, FilterCommand, UpdateCommand


class GetResponse(GenericModel, Generic[ObjT]):
    content: Optional[PartitionedContent[ObjT]]


class BatchResponse(GenericModel, Generic[ObjT]):
    contents: Iterable[PartitionedContent[ObjT]]


class ReadOnlyAbstractRepository(AbstractAsyncContextManager, Generic[ObjT]):
    @abstractmethod
    async def get(
        self, partition_id: Optional[Sequence[str]], content_id: Optional[Sequence[str]]
    ) -> GetResponse:
        pass

    @abstractmethod
    def get_batch(
        self,
        request_ids: Sequence[Tuple[Optional[Sequence[str]], Optional[Sequence[str]]]],
    ) -> AsyncIterator[BatchResponse]:
        pass

    @abstractmethod
    def list(
        self,
        partition_id: Optional[Sequence[str]],
        content_prefix: Optional[Sequence[str]],
        sort_ascending: bool = True,
        limit: Optional[int] = None,
        filters: Optional[FilterCommand] = None,
    ) -> AsyncIterator[BatchResponse]:
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
    ) -> AsyncIterator[BatchResponse]:
        pass


class AbstractRepository(ReadOnlyAbstractRepository[ObjT]):
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


class SyncReadOnlyAbstractRepository(AbstractContextManager, Generic[ObjT]):
    @abstractmethod
    def get(
        self, partition_id: Optional[Sequence[str]], content_id: Optional[Sequence[str]]
    ) -> GetResponse:
        pass

    @abstractmethod
    def get_batch(
        self,
        request_ids: Sequence[Tuple[Optional[Sequence[str]], Optional[Sequence[str]]]],
    ) -> Iterator[BatchResponse]:
        pass

    @abstractmethod
    def list(
        self,
        partition_id: Optional[Sequence[str]],
        content_prefix: Optional[Sequence[str]],
        sort_ascending: bool = True,
        limit: Optional[int] = None,
        filters: Optional[FilterCommand] = None,
    ) -> Iterator[BatchResponse]:
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
    ) -> Iterator[BatchResponse]:
        pass


class SyncAbstractRepository(SyncReadOnlyAbstractRepository[ObjT]):
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
