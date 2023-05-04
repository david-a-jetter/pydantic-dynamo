import logging
from collections import defaultdict
from typing import Generic, Iterable, List, Dict, Sequence, AsyncIterable, Union

from pydantic_dynamo.models import ObjT, PartitionedContent
from pydantic_dynamo.v2.models import AbstractRepository

logger = logging.getLogger(__name__)


class WriteOnceRepository(Generic[ObjT]):
    def __init__(self, async_repo: AbstractRepository[ObjT]):
        self._async_repo = async_repo

    async def write(
        self,
        input_contents: Union[
            Iterable[PartitionedContent[ObjT]], AsyncIterable[PartitionedContent[ObjT]]
        ],
    ) -> List[PartitionedContent[ObjT]]:
        """
        :param input_contents: contents to write if, they do not exist
            or are not identical to existing key's content
        :return: list of contents actually written after checking existing data
        """
        partitioned_lists: Dict[Sequence[str], List[PartitionedContent[ObjT]]] = defaultdict(list)
        if isinstance(input_contents, AsyncIterable):
            async for input_content in input_contents:
                partitioned_lists[tuple(input_content.partition_ids)].append(input_content)
        else:
            for input_content in input_contents:
                partitioned_lists[tuple(input_content.partition_ids)].append(input_content)

        if len(partitioned_lists) == 0:
            logger.info("Empty input content to save")
            return []

        new_contents: List[PartitionedContent[ObjT]] = []
        for partition_key, contents in partitioned_lists.items():
            contents.sort()
            content_start = contents[0]
            content_end = contents[-1]
            existing_map = {
                tuple(existing.content_ids): existing
                async for response in self._async_repo.list_between(
                    list(partition_key), content_start.content_ids, content_end.content_ids
                )
                for existing in response.contents
            }
            for input_content in contents:
                existing_item = existing_map.get(tuple(input_content.content_ids))
                if existing_item is None or existing_item != input_content:
                    new_contents.append(input_content)
        if new_contents:
            logger.info(
                "New contents found to save",
                extra={
                    "new_count": len(new_contents),
                },
            )
            await self._async_repo.put_batch(new_contents)
        else:
            logger.info("No new input content found to save")
        return new_contents
