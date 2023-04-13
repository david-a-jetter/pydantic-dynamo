import logging
from datetime import datetime, timezone, date, time
from enum import Enum
from io import StringIO
from typing import Optional, Sequence, Iterable, List, Dict, Any, Set

from boto3.dynamodb.conditions import ConditionBase, Attr
from pydantic import BaseModel

from pydantic_dynamo.constants import INTERNAL_TIMESTAMP_KEY, INTERNAL_TTL, INTERNAL_OBJECT_VERSION
from pydantic_dynamo.exceptions import RequestObjectStateError
from pydantic_dynamo.models import UpdateCommand, FilterCommand

logger = logging.getLogger(__name__)


def utc_now() -> datetime:
    return datetime.now(tz=timezone.utc)


def internal_timestamp() -> Dict[str, str]:
    return {INTERNAL_TIMESTAMP_KEY: utc_now().isoformat()}


def get_error_code(ex: Exception) -> Optional[str]:
    if hasattr(ex, "response"):
        return ex.response.get("Error", {}).get("Code")  # type: ignore[no-any-return,attr-defined]
    return None


def chunks(items: Sequence, size: int) -> Iterable[Sequence]:
    """Yield successive n-sized chunks from items."""
    for i in range(0, len(items), size):
        yield items[i : (i + size)]


class UpdateItemArguments(BaseModel):
    class Config:
        arbitrary_types_allowed = True

    update_expression: str
    condition_expression: Optional[ConditionBase]
    attribute_names: Dict[str, str]
    attribute_values: Dict[str, Any]


def execute_update_item(table, key: Dict, args: UpdateItemArguments) -> None:
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


def clean_dict(item_dict: Dict) -> Dict:
    dicts = [item_dict]

    while len(dicts) > 0:
        current_dict = dicts.pop()
        for k, v in current_dict.items():
            if isinstance(v, Dict):
                dicts.append(v)
            elif isinstance(v, BaseModel):
                current_dict[k] = clean_dict(v.dict())
            elif isinstance(v, (List, Set)):
                if len(v) > 0:
                    first = next(iter(v))
                    if isinstance(first, Dict):
                        for obj in v:
                            dicts.append(obj)
                    elif isinstance(first, BaseModel):
                        current_dict[k] = [clean_dict(obj.dict()) for obj in v]
                    else:
                        current_dict[k] = [clean_value(el) for el in v]
            else:
                current_dict[k] = clean_value(v)
    return item_dict


def clean_value(value: Any) -> Any:
    if isinstance(value, (date, time, datetime)):
        return value.isoformat()
    elif isinstance(value, Enum):
        return value.value
    elif isinstance(value, BaseModel):
        return value.dict()
    else:
        return value


def build_update_args_for_command(
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

    condition = build_update_condition(command, key)
    clean_values = clean_dict(values)
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


def build_update_condition(
    command: UpdateCommand,
    key: Optional[Dict[str, str]] = None,
) -> Optional[ConditionBase]:
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

    return condition


def build_filter_expression(filters: FilterCommand) -> Optional[ConditionBase]:
    condition: Optional[ConditionBase] = None
    for attr in filters.not_exists:
        new_condition = Attr(attr).not_exists()
        if condition is None:
            condition = new_condition
        else:
            condition = condition & new_condition
    clean_equals = clean_dict(filters.equals)
    for k, v in clean_equals.items():
        new_condition = Attr(k).eq(v)
        if condition is None:
            condition = new_condition
        else:
            condition = condition & new_condition
    clean_not_equals = clean_dict(filters.not_equals)
    for k, v in clean_not_equals.items():
        new_condition = Attr(k).ne(v)
        if condition is None:
            condition = new_condition
        else:
            condition = condition & new_condition

    return condition


def validate_command_for_schema(schema: Dict, command: UpdateCommand) -> None:
    schema_props: Dict = schema.get("properties")  # type: ignore[assignment]

    set_error_keys = []
    for k, v in command.set_commands.items():
        schema_prop = schema_props.get(k)
        if not schema_prop:
            set_error_keys.append(k)
            continue
        if isinstance(v, Dict):
            if schema_prop_ref := schema_prop.get("$ref"):
                schema_prop_key = schema_prop_ref.split("/")[-1]
                nested_schema = schema["definitions"][schema_prop_key]
                nested_props: Dict = nested_schema.get("properties")  # type: ignore[assignment]
                for nested_k, nested_v in v.items():
                    if nested_k not in nested_props:
                        set_error_keys.append(f"{k}.{nested_k}")
            elif schema_prop["type"] != "object":
                set_error_keys.append(k)

    if len(set_error_keys) > 0:
        raise ValueError(
            f"command contains set attrs not found in {schema.get('title')} type: "
            f"{','.join(set_error_keys)}"
        )

    increment_attrs = command.increment_attrs.keys()
    validate_attrs_in_schema(
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


def validate_filters_for_schema(schema: Dict, filters: FilterCommand) -> None:
    validate_attrs_in_schema(
        schema,
        (*filters.not_exists, *filters.equals.keys(), *filters.not_equals.keys()),
    )


def validate_attrs_in_schema(schema: Dict, attrs: Iterable[str]) -> None:
    schema_props = schema["properties"]
    invalid_keys = [a for a in attrs if a not in schema_props]

    if len(invalid_keys) > 0:
        raise ValueError(
            f"command contains attrs not found in {schema.get('title')} type: "
            f"{','.join(invalid_keys)}"
        )
