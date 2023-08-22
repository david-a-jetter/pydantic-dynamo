from typing import List, AsyncIterator, TypeVar, Generic

from pydantic_dynamo.models import ObjT, PartitionedContent
from pydantic_dynamo.v2.models import BatchResponse

T = TypeVar("T")


class AIter(AsyncIterator, Generic[T]):
    """
    Convenience class helpful for testing to generate async iterables from lists
    """

    def __init__(self, data: List[T]):
        self._data = data
        self._i = -1

    def __aiter__(self):
        self._i = -1
        return self

    async def __anext__(self) -> T:
        self._i = self._i + 1
        if self._i >= len(self._data):
            raise StopAsyncIteration()
        return self._data[self._i]


async def list_contents_from_batches(
    batches: AsyncIterator[BatchResponse[ObjT]],
) -> List[PartitionedContent[ObjT]]:
    """
    Convenience function to minimize async for loop boilerplate
    :param batches: Response from repository list methods
    :return: Python List of the PartitionedContent objects from the repository
    """
    return [c async for response in batches for c in response.contents]


async def list_items_from_batches(batches: AsyncIterator[BatchResponse[ObjT]]) -> List[ObjT]:
    """
    Convenience function to minimize async for loop boilerplate
    :param batches: Response from repository list methods
    :return: Python List of your specified Pydantic model for this repository
    """
    return [c.item for c in await list_contents_from_batches(batches)]
