from __future__ import annotations

import logging

from boto3 import Session
from boto3.dynamodb.conditions import Key
from typing import (
    Optional,
    Dict,
    Iterable,
    Type,
    List,
    Any,
    Union,
    Iterator,
    Tuple,
    Sequence,
)

from pydantic_dynamo.constants import (
    EMPTY_LIST,
    INTERNAL_OBJECT_VERSION,
    INTERNAL_TTL,
    FILTER_EXPRESSION,
    LAST_EVALUATED_KEY,
)
from pydantic_dynamo.exceptions import RequestObjectStateError
from pydantic_dynamo.utils import (
    chunks,
    get_error_code,
    internal_timestamp,
    validate_filters_for_schema,
    build_filter_expression,
    validate_command_for_schema,
    clean_dict,
    build_update_args_for_command,
)
from pydantic_dynamo.models import (
    UpdateCommand,
    FilterCommand,
    ObjT,
    PartitionedContent,
    AbstractRepository,
)

logger = logging.getLogger(__name__)


class DynamoRepository(AbstractRepository[ObjT]):
    @classmethod
    def build(
        cls,
        table_name: str,
        item_class: Type[ObjT],
        partition_prefix: str,
        partition_name: str,
        content_type: str,
    ) -> DynamoRepository[ObjT]:
        resource = Session().resource("dynamodb")
        table = resource.Table(table_name)
        return cls(
            item_class=item_class,
            partition_prefix=partition_prefix,
            partition_name=partition_name,
            content_type=content_type,
            table_name=table_name,
            partition_key="_table_item_id",
            sort_key="_table_content_id",
            table=table,
            resource=resource,
        )

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

    def put(self, content: PartitionedContent[ObjT]) -> None:
        log_context: Dict[str, Any] = {
            "partition_id": content.partition_ids,
            "content_id": content.content_ids,
            **self.context,
        }
        logger.info("Putting single content", extra=log_context)
        self._put_content(self._table, content)
        logger.info("Put single content", extra=log_context)

    def put_batch(self, batch: Iterable[PartitionedContent[ObjT]]) -> None:
        logger.info("Putting batch content", extra=self.context)
        count = 0
        with self._table.batch_writer() as writer:
            for content in batch:
                self._put_content(writer, content)
                count += 1
        logger.info("Finished putting batch content", extra={"count": count, **self.context})

    def _put_content(self, table, content: PartitionedContent[ObjT]) -> None:
        item_dict = clean_dict(content.item.dict())
        item_dict[INTERNAL_OBJECT_VERSION] = 1
        item_dict.update(**internal_timestamp())
        put_item: Dict[str, Union[str, int]] = {
            self._partition_key: self._partition_id(content.partition_ids),
            self._sort_key: self._content_id(content.content_ids),
            **item_dict,
        }
        if expiry := content.expiry:
            put_item[INTERNAL_TTL] = int(expiry.timestamp())
        table.put_item(Item=put_item)

    def update(
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
        try:
            update_kwargs = {
                "Key": key,
                "UpdateExpression": args.update_expression,
                "ExpressionAttributeNames": args.attribute_names,
                "ExpressionAttributeValues": args.attribute_values,
            }
            if args.condition_expression:
                update_kwargs["ConditionExpression"] = args.condition_expression
            self._table.update_item(**update_kwargs)
            logger.info("Finished updating item")
        except Exception as ex:
            code = get_error_code(ex)
            if code == "ConditionalCheckFailedException":
                raise RequestObjectStateError(
                    f"Object existence or version condition failed for: {str(key)}"
                ) from ex
            raise

    def get(
        self, partition_id: Optional[Sequence[str]], content_id: Optional[Sequence[str]]
    ) -> Optional[ObjT]:
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
        response = self._table.get_item(
            Key={
                self._partition_key: self._partition_id(partition_id),
                self._sort_key: self._content_id(content_id),
            }
        )
        db_item = response.get("Item")
        if db_item:
            logger.info("Found item from table by key", extra=log_context)
            item = self._item_class(**db_item)
        else:
            logger.info("No item found in table by key", extra=log_context)
            item = None
        return item

    def get_batch(
        self,
        request_ids: Sequence[Tuple[Optional[Sequence[str]], Optional[Sequence[str]]]],
    ) -> List[ObjT]:
        records: List[ObjT] = []
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
            batch_response: Dict[str, Any] = self._extend_batch(batch_keys, records)
            while unprocessed_keys := batch_response.get("UnprocessedKeys"):
                logger.info(
                    "Getting unprocessed keys",
                    extra={
                        "batch_number": batch_number,
                        "batch_size": len(unprocessed_keys),
                    },
                )
                batch_response = self._extend_batch(unprocessed_keys, records)
        return records

    def _extend_batch(self, request_keys: List[Dict[str, str]], records: List[ObjT]):
        batch_response = self._resource.batch_get_item(
            RequestItems={self._table_name: {"Keys": request_keys}}
        )
        records.extend(
            (self._item_class(**i) for i in batch_response["Responses"].get(self._table_name, []))
        )
        return batch_response

    def list(
        self,
        partition_id: Optional[Sequence[str]],
        content_prefix: Optional[Sequence[str]],
        sort_ascending: bool = True,
        limit: Optional[int] = None,
        filters: Optional[FilterCommand] = None,
    ) -> Iterator[ObjT]:
        if partition_id is None:
            partition_id = EMPTY_LIST
        if content_prefix is None:
            content_prefix = EMPTY_LIST

        condition = Key(self._partition_key).eq(self._partition_id(partition_id)) & Key(
            self._sort_key
        ).begins_with(self._content_id(content_prefix))
        logger.info("Starting query for content with prefix query")
        items = self._query_all_data(condition, sort_ascending, limit, filters)
        yield from (self._item_class(**item) for item in items)

    def list_between(
        self,
        partition_id: Optional[Sequence[str]],
        content_start: Optional[Sequence[str]],
        content_end: Optional[Sequence[str]],
        sort_ascending: bool = True,
        limit: Optional[int] = None,
        filters: Optional[FilterCommand] = None,
    ) -> Iterator[ObjT]:
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
            yield from self.list(partition_id, content_start)
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
            items = self._query_all_data(condition, sort_ascending, limit, filters)
            yield from (self._item_class(**item) for item in items)

    def delete(
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
        items = self._query_all_data(condition)
        with self._table.batch_writer() as writer:
            for item in items:
                writer.delete_item(
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

    def _query_all_data(
        self,
        key_condition_expression: Key,
        sort_ascending: bool = True,
        limit: Optional[int] = None,
        filters: Optional[FilterCommand] = None,
    ) -> Iterable[Dict]:
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

        response = self._table.query(**query_kwargs)
        total_count = response["Count"]
        items = response.get("Items", [])

        yield from items

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
            response = self._table.query(**query_kwargs)
            total_count += response["Count"]
            items = response.get("Items", [])
            yield from items
