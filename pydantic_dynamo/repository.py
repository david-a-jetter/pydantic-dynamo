from __future__ import annotations

import logging
from datetime import datetime, date, time
from enum import Enum
from io import StringIO

from boto3 import Session
from boto3.dynamodb.conditions import Key, Attr, ConditionBase
from typing import (
    Optional,
    Dict,
    Iterable,
    Type,
    List,
    Any,
    Union,
    Iterator,
    Set,
    Tuple,
    Sequence,
)

from pydantic import BaseModel

from pydantic_dynamo.exceptions import RequestObjectStateError
from pydantic_dynamo.utils import utc_now, chunks, get_error_code
from pydantic_dynamo.models import (
    UpdateCommand,
    FilterCommand,
    ObjT,
    PartitionedContent,
    AbstractRepository,
)

logger = logging.getLogger(__name__)


LAST_EVALUATED_KEY = "LastEvaluatedKey"
FILTER_EXPRESSION = "FilterExpression"
INTERNAL_TIMESTAMP_KEY = "_timestamp"
INTERNAL_OBJECT_VERSION = "_object_version"
INTERNAL_TTL = "_ttl"
EMPTY_LIST: List[str] = []


def _internal_timestamp() -> Dict[str, str]:
    return {INTERNAL_TIMESTAMP_KEY: utc_now().isoformat()}


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
        item_dict = _clean_dict(content.item.dict())
        item_dict[INTERNAL_OBJECT_VERSION] = 1
        item_dict.update(**_internal_timestamp())
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
        _validate_command_for_schema(self._item_schema, command)
        key = {
            self._partition_key: self._partition_id(partition_id),
            self._sort_key: self._content_id(content_id),
        }
        build_kwargs: Dict[str, Any] = {
            "command": command,
        }
        if require_exists:
            build_kwargs["key"] = key
        args = _build_update_args_for_command(**build_kwargs)  # type: ignore
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
            _validate_filters_for_schema(self._item_schema, filters)
            if filter_expression := _build_filter_expression(filters):
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


def _execute_update_item(table, key: Dict, args: UpdateItemArguments) -> None:
    try:
        update_kwargs = {
            "Key": key,
            "UpdateExpression": args.update_expression,
            "ExpressionAttributeNames": args.attribute_names,
            "ExpressionAttributeValues": args.attribute_values,
        }
        if args.condition_expression:
            update_kwargs["ConditionExpression"] = args.condition_expression
        table.update_item(**update_kwargs)
    except Exception as ex:
        code = get_error_code(ex)
        if code == "ConditionalCheckFailedException":
            raise RequestObjectStateError(
                f"Object version condition failed for: {str(key)}"
            ) from ex
        raise


def _clean_dict(item_dict: Dict) -> Dict:
    dicts = [item_dict]

    while len(dicts) > 0:
        current_dict = dicts.pop()
        for k, v in current_dict.items():
            if isinstance(v, Dict):
                dicts.append(v)
            elif isinstance(v, BaseModel):
                current_dict[k] = _clean_dict(v.dict())
            # TODO: Add Test for Set condition
            elif isinstance(v, (List, Set)):
                if len(v) > 0:
                    first = next(iter(v))
                    if isinstance(first, Dict):
                        for obj in v:
                            dicts.append(obj)
                    elif isinstance(first, BaseModel):
                        current_dict[k] = [_clean_dict(obj.dict()) for obj in v]
                    else:
                        current_dict[k] = [_clean_value(el) for el in v]
            else:
                current_dict[k] = _clean_value(v)
    return item_dict


def _clean_value(value: Any) -> Any:
    if isinstance(value, (date, time, datetime)):
        return value.isoformat()
    elif isinstance(value, Enum):
        return value.value
    elif isinstance(value, BaseModel):
        return value.dict()
    else:
        return value


class UpdateItemArguments(BaseModel):
    class Config:
        arbitrary_types_allowed = True

    update_expression: str
    condition_expression: Optional[ConditionBase]
    attribute_names: Dict[str, str]
    attribute_values: Dict[str, Any]


def _build_update_args_for_command(
    command: UpdateCommand,
    key: Optional[Dict[str, str]] = None,
) -> UpdateItemArguments:
    set_attrs = command.set_commands
    set_attrs[INTERNAL_TIMESTAMP_KEY] = utc_now()
    if expiry_dt := command.expiry:
        set_attrs[INTERNAL_TTL] = int(expiry_dt.timestamp())
    set_attrs.pop(INTERNAL_OBJECT_VERSION, None)

    update_expression = StringIO()
    update_expression.write("SET ")
    names = {}
    values: Dict[str, Any] = {":zero": 0}

    attr_count = 0
    val_count = 0
    for k, v in set_attrs.items():
        base_attr_id = f"#att{attr_count}"
        names[base_attr_id] = k

        if isinstance(v, Dict):
            for set_k, set_v in v.items():
                if attr_count > 0:
                    update_expression.write(", ")
                attr_count += 1
                prop_attr_id = f"#att{attr_count}"
                attr_id = f"{base_attr_id}.{prop_attr_id}"
                val_id = f":val{val_count}"
                update_expression.write(f"{attr_id} = {val_id}")

                names[prop_attr_id] = set_k
                values[val_id] = set_v

                val_count += 1
            attr_count += 1
        else:
            val_id = f":val{val_count}"

            if attr_count > 0:
                update_expression.write(", ")
            update_expression.write(f"{base_attr_id} = {val_id}")
            values[val_id] = v

            val_count += 1
            attr_count += 1

    add_attrs = command.increment_attrs
    add_attrs.update(**{INTERNAL_OBJECT_VERSION: 1})
    add_attrs.pop(INTERNAL_TIMESTAMP_KEY, None)

    for k, v in add_attrs.items():
        attr_id = f"#att{attr_count}"
        val_id = f":val{val_count}"
        if attr_count > 0:
            update_expression.write(", ")

        update_expression.write(f"{attr_id} = if_not_exists({attr_id}, :zero) + {val_id}")
        names[attr_id] = k
        values[val_id] = v

        attr_count += 1
        val_count += 1

    append_attrs = command.append_attrs
    append_attrs.pop(INTERNAL_TIMESTAMP_KEY, None)
    append_attrs.pop(INTERNAL_OBJECT_VERSION, None)

    if len(append_attrs) > 0:
        values[":empty_list"] = []
    for k, v in append_attrs.items():
        attr_id = f"#att{attr_count}"
        val_id = f":val{val_count}"
        if attr_count > 0:
            update_expression.write(", ")
        update_expression.write(
            f"{attr_id} = list_append(if_not_exists({attr_id}, :empty_list), {val_id})"
        )
        names[attr_id] = k
        values[val_id] = 1 if v is None else v

        attr_count += 1
        val_count += 1

    condition = None
    if key:
        for k, v in key.items():
            key_condition = Attr(k).eq(v)
            if condition:
                condition = condition & key_condition
            else:
                condition = key_condition
    if command.current_version:
        version_condition = Attr(INTERNAL_OBJECT_VERSION).eq(command.current_version)
        if condition:
            condition = condition & version_condition
        else:
            condition = version_condition
    clean_values = _clean_dict(values)
    arguments = UpdateItemArguments(
        update_expression=update_expression.getvalue(),
        condition_expression=condition,
        attribute_names=names,
        attribute_values=clean_values,
    )

    logger.info(
        "Generated update item argument expression",
        extra={"expression": arguments.update_expression},
    )

    return arguments


def _build_filter_expression(filters: FilterCommand) -> Optional[ConditionBase]:
    condition: Optional[ConditionBase] = None
    for attr in filters.not_exists:
        new_condition = Attr(attr).not_exists()
        if condition is None:
            condition = new_condition
        else:
            condition = condition & new_condition
    clean_equals = _clean_dict(filters.equals)
    for k, v in clean_equals.items():
        new_condition = Attr(k).eq(v)
        if condition is None:
            condition = new_condition
        else:
            condition = condition & new_condition
    clean_not_equals = _clean_dict(filters.not_equals)
    for k, v in clean_not_equals.items():
        new_condition = Attr(k).ne(v)
        if condition is None:
            condition = new_condition
        else:
            condition = condition & new_condition

    return condition


def _validate_command_for_schema(schema: Dict, command: UpdateCommand) -> None:
    schema_props: Dict = schema.get("properties")  # type: ignore[assignment]

    set_error_keys = []
    for k, v in command.set_commands.items():
        schema_prop = schema_props.get(k)
        if not schema_prop:
            set_error_keys.append(k)
            continue
        if isinstance(v, Dict):
            schema_prop_key = schema_prop["$ref"].split("/")[-1]
            nested_schema = schema["definitions"][schema_prop_key]
            nested_props: Dict = nested_schema.get("properties")  # type: ignore[assignment]
            for nested_k, nested_v in v.items():
                nested_prop = nested_props.get(nested_k)
                if not nested_prop:
                    set_error_keys.append(f"{v}.{nested_k}")

    if len(set_error_keys) > 0:
        raise ValueError(
            f"command contains set attrs not found in {schema.get('title')} type: "
            f"{','.join(set_error_keys)}"
        )

    increment_attrs = command.increment_attrs.keys()
    _validate_attrs_in_schema(
        schema,
        (
            *increment_attrs,
            *command.append_attrs.keys(),
        ),
    )

    incr_error_keys = []
    for k in increment_attrs:
        prop = schema_props[k]
        if prop.get("type") != "integer":
            incr_error_keys.append(k)
    if len(incr_error_keys) > 0:
        raise ValueError(
            "command contains increment_attrs that are not integers in "
            f"{schema.get('title')} type: "
            f"{','.join(incr_error_keys)}"
        )


def _validate_filters_for_schema(schema: Dict, filters: FilterCommand) -> None:
    _validate_attrs_in_schema(
        schema,
        (*filters.not_exists, *filters.equals.keys(), *filters.not_equals.keys()),
    )


def _validate_attrs_in_schema(schema: Dict, attrs: Iterable[str]) -> None:
    schema_props = schema["properties"]
    invalid_keys = [a for a in attrs if a not in schema_props]

    if len(invalid_keys) > 0:
        raise ValueError(
            f"command contains attrs not found in {schema.get('title')} type: "
            f"{','.join(invalid_keys)}"
        )
