from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Type, Dict, List, Any, Iterable, Union, Optional, Sequence, Tuple, AsyncIterable

from boto3.dynamodb.conditions import Key

from pydantic_dynamo.constants import (
    EMPTY_LIST,
    INTERNAL_OBJECT_VERSION,
    INTERNAL_TTL,
    FILTER_EXPRESSION,
    LAST_EVALUATED_KEY,
)
from pydantic_dynamo.models import ObjT, PartitionedContent, UpdateCommand, FilterCommand
from pydantic_dynamo.utils import (
    clean_dict,
    internal_timestamp,
    validate_command_for_schema,
    chunks,
    validate_filters_for_schema,
    build_filter_expression,
    build_update_args_for_command,
    execute_update_item,
)
from pydantic_dynamo.v2.models import AbstractRepository, GetResponse, BatchResponse

logger = logging.getLogger(__name__)


class DynamoRepository(AbstractRepository[ObjT]):
    def __init__(
        self,
        *,
        item_class: Type[ObjT],
        partition_prefix: str,
        partition_name: str,
        content_type: str,
        table_name: str,
        partition_key: str,
        sort_key: str,
        table,
        resource,
    ):
        self._item_class = item_class
        self._item_schema = self._item_class.schema()
        self._partition_prefix = partition_prefix
        self._partition_name = partition_name
        self._content_type = content_type
        self._table_name = table_name
        self._partition_key = partition_key
        self._sort_key = sort_key
        self._table = table
        self._resource = resource

    @property
    def context(self) -> Dict[str, str]:
        return {
            "item_class": self._item_class.__name__,
            "partition_prefix": self._partition_id(EMPTY_LIST),
            "content_prefix": self._content_id(EMPTY_LIST),
        }

    async def put(self, content: PartitionedContent[ObjT]) -> None:
        log_context: Dict[str, Any] = {
            "partition_id": content.partition_ids,
            "content_id": content.content_ids,
            **self.context,
        }
        logger.info("Putting single content", extra=log_context)
        await self._put_content(self._table, content)
        logger.info("Put single content", extra=log_context)

    async def put_batch(self, batch: Iterable[PartitionedContent[ObjT]]) -> None:
        logger.info("Putting batch content", extra=self.context)
        count = 0
        async with self._table.batch_writer() as writer:
            for content in batch:
                await self._put_content(writer, content)
                count += 1
        logger.info("Finished putting batch content", extra={"count": count, **self.context})

    async def _put_content(self, table, content: PartitionedContent[ObjT]) -> None:
        item_dict = clean_dict(content.item.dict())
        item_dict[INTERNAL_OBJECT_VERSION] = content.current_version
        item_dict.update(**internal_timestamp())
        put_item: Dict[str, Union[str, int]] = {
            self._partition_key: self._partition_id(content.partition_ids),
            self._sort_key: self._content_id(content.content_ids),
            **item_dict,
        }
        if expiry := content.expiry:
            put_item[INTERNAL_TTL] = int(expiry.timestamp())
        await table.put_item(Item=put_item)

    async def update(
        self,
        partition_id: Optional[Sequence[str]],
        content_id: Optional[Sequence[str]],
        command: UpdateCommand,
        require_exists: bool = True,
    ) -> None:
        validate_command_for_schema(self._item_schema, command)
        key = {
            self._partition_key: self._partition_id(partition_id),
            self._sort_key: self._content_id(content_id),
        }
        build_kwargs: Dict[str, Any] = {
            "command": command,
        }
        if require_exists:
            build_kwargs["key"] = key
        args = build_update_args_for_command(**build_kwargs)  # type: ignore
        await execute_update_item(self._table, key, args)

    async def get(
        self, partition_id: Optional[Sequence[str]], content_id: Optional[Sequence[str]]
    ) -> GetResponse:
        if partition_id is None:
            partition_id = EMPTY_LIST
        if content_id is None:
            content_id = EMPTY_LIST
        log_context = {
            "partition_id": partition_id,
            "content_id": content_id,
            **self.context,
        }
        logger.info("Getting item from table by key", extra=log_context)
        response = await self._table.get_item(
            Key={
                self._partition_key: self._partition_id(partition_id),
                self._sort_key: self._content_id(content_id),
            }
        )
        db_item = response.get("Item")
        if db_item:
            logger.info("Found item from table by key", extra=log_context)
            content = self._db_item_to_object(db_item)
        else:
            logger.info("No item found in table by key", extra=log_context)
            content = None
        return GetResponse(content=content)

    async def get_batch(
        self,
        request_ids: Sequence[Tuple[Optional[Sequence[str]], Optional[Sequence[str]]]],
    ) -> AsyncIterable[BatchResponse]:
        batch_number = 0
        for request_id_batch in chunks(request_ids, size=100):
            batch_number += 1
            logger.info(
                "Starting batch get items request",
                extra={
                    "batch_number": batch_number,
                    "batch_size": len(request_id_batch),
                },
            )
            batch_keys = [
                {
                    self._partition_key: self._partition_id(partition_id),
                    self._sort_key: self._content_id(content_id),
                }
                for partition_id, content_id in request_id_batch
            ]
            batch_response = await self._resource.batch_get_item(
                RequestItems={self._table_name: {"Keys": batch_keys}}
            )
            yield BatchResponse(
                contents=(
                    self._db_item_to_object(db_item)
                    for db_item in batch_response["Responses"].get(self._table_name, [])
                )
            )
            while unprocessed_keys := batch_response.get("UnprocessedKeys"):
                logger.info(
                    "Getting unprocessed keys",
                    extra={
                        "batch_number": batch_number,
                        "batch_size": len(unprocessed_keys),
                    },
                )
                batch_response = await self._resource.batch_get_item(
                    RequestItems={self._table_name: {"Keys": unprocessed_keys}}
                )
                yield BatchResponse(
                    contents=(
                        self._db_item_to_object(db_item)
                        for db_item in batch_response["Responses"].get(self._table_name, [])
                    )
                )

    async def list(
        self,
        partition_id: Optional[Sequence[str]],
        content_prefix: Optional[Sequence[str]],
        sort_ascending: bool = True,
        limit: Optional[int] = None,
        filters: Optional[FilterCommand] = None,
    ) -> AsyncIterable[BatchResponse]:
        if partition_id is None:
            partition_id = EMPTY_LIST
        if content_prefix is None:
            content_prefix = EMPTY_LIST

        condition = Key(self._partition_key).eq(self._partition_id(partition_id)) & Key(
            self._sort_key
        ).begins_with(self._content_id(content_prefix))
        logger.info("Starting query for content with prefix query")
        contents = [
            self._db_item_to_object(db_item)
            async for items in self._query_all_data(condition, sort_ascending, limit, filters)
            for db_item in items
        ]
        yield BatchResponse(contents=contents)

    async def list_between(
        self,
        partition_id: Optional[Sequence[str]],
        content_start: Optional[Sequence[str]],
        content_end: Optional[Sequence[str]],
        sort_ascending: bool = True,
        limit: Optional[int] = None,
        filters: Optional[FilterCommand] = None,
    ) -> AsyncIterable[BatchResponse]:
        log_context = {
            "partition_id": partition_id,
            "content_start": content_start,
            "content_end": content_end,
            **self.context,
        }
        if partition_id is None:
            partition_id = EMPTY_LIST
        if content_start is None:
            content_start = EMPTY_LIST
        if content_end is None:
            content_end = EMPTY_LIST

        if content_start == content_end:
            logger.info(
                "Content start and end filters are equal. Deferring to list",
                extra=log_context,
            )
            async for response in self.list(partition_id, content_start):
                yield response
        else:
            partition_id = self._partition_id(partition_id)
            sort_start = self._content_id(content_start)
            sort_end = self._content_id(content_end)
            condition = Key(self._partition_key).eq(partition_id) & Key(self._sort_key).between(
                low_value=sort_start, high_value=sort_end
            )
            logger.info(
                "Starting query for content in range query",
                extra={
                    "partition_id": partition_id,
                    "low_value": sort_start,
                    "high_value": sort_end,
                    **log_context,
                },
            )
            contents = [
                self._db_item_to_object(db_item)
                async for items in self._query_all_data(condition, sort_ascending, limit, filters)
                for db_item in items
            ]
            yield BatchResponse(contents=contents)

    def _db_item_to_object(self, db_item: Dict[str, Any]) -> PartitionedContent[ObjT]:
        expiry: Optional[datetime] = None
        if db_expiry := db_item.pop(INTERNAL_TTL, None):
            expiry = datetime.fromtimestamp(db_expiry, tz=timezone.utc)
        return PartitionedContent[self._item_class](  # type: ignore[name-defined]
            partition_ids=db_item.pop(self._partition_key)
            .replace(self._partition_id(EMPTY_LIST), "", 1)
            .split("#"),
            content_ids=db_item.pop(self._sort_key)
            .replace(self._content_id(EMPTY_LIST), "", 1)
            .split("#"),
            item=self._item_class(**db_item),
            current_version=db_item.pop(INTERNAL_OBJECT_VERSION),
            expiry=expiry,
        )

    async def delete(
        self,
        partition_id: Optional[Sequence[str]],
        content_prefix: Optional[Sequence[str]],
    ) -> None:
        if partition_id is None:
            partition_id = EMPTY_LIST
        if content_prefix is None:
            content_prefix = EMPTY_LIST

        log_context = {
            "partition_id": partition_id,
            "content_prefix": content_prefix,
            **self.context,
        }
        condition = Key(self._partition_key).eq(self._partition_id(partition_id)) & Key(
            self._sort_key
        ).begins_with(self._content_id(content_prefix))
        logger.info("Starting query for content to delete with prefix query", extra=log_context)
        responses = self._query_all_data(
            condition, select_fields=(self._partition_key, self._sort_key)
        )
        async with self._table.batch_writer() as writer:
            async for items in responses:
                for item in items:
                    await writer.delete_item(
                        Key={
                            self._partition_key: item[self._partition_key],
                            self._sort_key: item[self._sort_key],
                        }
                    )
        logger.info("Finished deleting content from prefix query", extra=log_context)

    def _partition_id(self, partition_ids: Optional[Union[str, Sequence[str]]]) -> str:
        if partition_ids is None:
            partition_ids = EMPTY_LIST
        if isinstance(partition_ids, str):
            return f"{self._partition_prefix}#{self._partition_name}#{partition_ids}"
        else:
            return f"{self._partition_prefix}#{self._partition_name}#{'#'.join(partition_ids)}"

    def _content_id(self, content_ids: Optional[Union[str, Sequence[str]]]) -> str:
        if content_ids is None:
            content_ids = EMPTY_LIST
        if isinstance(content_ids, str):
            return f"{self._content_type}#{content_ids}"
        else:
            return f"{self._content_type}#{'#'.join(content_ids)}"

    async def _query_all_data(
        self,
        key_condition_expression: Key,
        sort_ascending: bool = True,
        limit: Optional[int] = None,
        filters: Optional[FilterCommand] = None,
        select_fields: Optional[Sequence[str]] = None,
    ) -> AsyncIterable[List[Dict]]:
        query_kwargs = {
            "KeyConditionExpression": key_condition_expression,
            "ScanIndexForward": sort_ascending,
        }
        if filters:
            validate_filters_for_schema(self._item_schema, filters)
            if filter_expression := build_filter_expression(filters):
                query_kwargs[FILTER_EXPRESSION] = filter_expression
        if limit and FILTER_EXPRESSION not in query_kwargs:
            query_kwargs["Limit"] = limit
        if select_fields:
            expression_attribute_names = {
                f"#att{i}": field for i, field in enumerate(select_fields)
            }
            query_kwargs.update(
                **{
                    "Select": "SPECIFIC_ATTRIBUTES",
                    "ProjectionExpression": ", ".join(expression_attribute_names.keys()),
                    "ExpressionAttributeNames": expression_attribute_names,
                }
            )

        response = await self._table.query(**query_kwargs)
        total_count = response["Count"]
        items = response.get("Items", [])

        yield items

        while last_evaluated_key := response.get(LAST_EVALUATED_KEY):
            # TODO: Add tests for limit
            if limit and total_count >= limit:
                return
            logger.info(
                "Getting next batch of items from content table",
                extra={
                    "last_evaluated_key": last_evaluated_key,
                },
            )
            query_kwargs["ExclusiveStartKey"] = last_evaluated_key
            response = await self._table.query(**query_kwargs)
            total_count += response["Count"]
            items = response.get("Items", [])
            yield items
