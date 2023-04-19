import asyncio
from typing import Optional, Sequence, Iterable, Tuple, AsyncIterable, TypeVar

from pydantic_dynamo.models import ObjT, FilterCommand, UpdateCommand, PartitionedContent
from pydantic_dynamo.v2.models import (
    SyncAbstractRepository,
    BatchResponse,
    GetResponse,
    AbstractRepository,
)


Output = TypeVar("Output")


class SyncDynamoRepository(SyncAbstractRepository[ObjT]):
    def __init__(self, async_repo: AbstractRepository[ObjT]):
        self._async_repo = async_repo

    def put(self, content: PartitionedContent[ObjT]) -> None:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self._async_repo.put(content))

    def put_batch(self, content: Iterable[PartitionedContent[ObjT]]) -> None:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self._async_repo.put_batch(content))

    def update(
        self,
        partition_id: Optional[Sequence[str]],
        content_id: Optional[Sequence[str]],
        command: UpdateCommand,
        require_exists: bool = True,
    ) -> None:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(
            self._async_repo.update(partition_id, content_id, command, require_exists)
        )

    def delete(
        self, partition_id: Optional[Sequence[str]], content_prefix: Optional[Sequence[str]]
    ) -> None:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self._async_repo.delete(partition_id, content_prefix))

    def get(
        self, partition_id: Optional[Sequence[str]], content_id: Optional[Sequence[str]]
    ) -> GetResponse:
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(self._async_repo.get(partition_id, content_id))

    def get_batch(
        self, request_ids: Sequence[Tuple[Optional[Sequence[str]], Optional[Sequence[str]]]]
    ) -> Iterable[BatchResponse]:
        loop = asyncio.get_event_loop()
        return iter_over_async(self._async_repo.get_batch(request_ids), loop)

    def list(
        self,
        partition_id: Optional[Sequence[str]],
        content_prefix: Optional[Sequence[str]],
        sort_ascending: bool = True,
        limit: Optional[int] = None,
        filters: Optional[FilterCommand] = None,
    ) -> Iterable[BatchResponse]:
        loop = asyncio.get_event_loop()
        return iter_over_async(
            self._async_repo.list(partition_id, content_prefix, sort_ascending, limit, filters),
            loop,
        )

    def list_between(
        self,
        partition_id: Optional[Sequence[str]],
        content_start: Optional[Sequence[str]],
        content_end: Optional[Sequence[str]],
        sort_ascending: bool = True,
        limit: Optional[int] = None,
        filters: Optional[FilterCommand] = None,
    ) -> Iterable[BatchResponse]:
        loop = asyncio.get_event_loop()
        return iter_over_async(
            self._async_repo.list_between(
                partition_id, content_start, content_end, sort_ascending, limit, filters
            ),
            loop,
        )


def iter_over_async(async_iter: AsyncIterable[Output], loop) -> Iterable[Output]:
    ait = async_iter.__aiter__()

    async def get_next():
        try:
            obj = await ait.__anext__()
            return False, obj
        except StopAsyncIteration:
            return True, None

    while True:
        done, obj = loop.run_until_complete(get_next())
        if done:
            break
        yield obj
