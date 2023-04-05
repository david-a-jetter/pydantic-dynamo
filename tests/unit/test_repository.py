import random
from datetime import datetime, timezone
from unittest.mock import patch, MagicMock

import pytest
from faker import Faker

from pydantic_dynamo.models import PartitionedContent, UpdateCommand, FilterCommand
from pydantic_dynamo.repository import (
    DynamoRepository,
)
from pydantic_dynamo.exceptions import RequestObjectStateError
from pydantic_dynamo.utils import clean_dict
from tests.models import ExtraModel, FieldModel, ComposedFieldModel, CountEnum
from tests.factories import UpdateCommandFactory, UpdateItemArgumentsFactory
from tests.factories import boto_exception

fake = Faker()


def _random_enum():
    return random.choice([s for s in CountEnum])


@patch("pydantic_dynamo.repository.Session")
def test_dynamo_repo_build(session_cls):
    table = MagicMock()
    session = MagicMock()
    session_cls.return_value = session
    resource = MagicMock()
    session.resource.return_value = resource
    resource.Table.return_value = table
    table_name = fake.bs()
    item_class = ExtraModel
    partition = fake.bs()
    partition_key = fake.bs()
    content_type = fake.bs()

    repo = DynamoRepository[ExtraModel].build(
        table_name, item_class, partition, partition_key, content_type
    )

    assert repo._item_class == item_class
    assert repo._partition_prefix == partition
    assert repo._partition_name == partition_key
    assert repo._content_type == content_type
    assert repo._table == table

    assert session.resource.call_args[0] == ("dynamodb",)
    assert resource.Table.call_args[0] == (f"{table_name}",)


@patch("pydantic_dynamo.repository.internal_timestamp")
def test_dynamo_repo_put(internal_timestamp):
    now = datetime.now(tz=timezone.utc)
    internal_timestamp.return_value = {"_timestamp": now.isoformat()}

    partition = fake.bs()
    content_type = fake.bs()
    partition_ids = [fake.bs()]
    partition_type = fake.bs()
    content_ids = [fake.bs()]
    partition_key = fake.bs()
    sort_key = fake.bs()
    table = MagicMock()

    repo = DynamoRepository[ExtraModel](
        item_class=ExtraModel,
        partition_prefix=partition,
        partition_name=partition_type,
        content_type=content_type,
        table_name=fake.bs(),
        partition_key=partition_key,
        sort_key=sort_key,
        table=table,
        resource=MagicMock(),
    )

    item_dict = fake.pydict()
    item = ExtraModel(**item_dict)
    content = PartitionedContent(partition_ids=partition_ids, content_ids=content_ids, item=item)
    repo.put(content)

    assert table.put_item.call_args[1] == {
        "Item": {
            partition_key: f"{partition}#{partition_type}#{partition_ids[0]}",
            sort_key: f"{content_type}#{content_ids[0]}",
            "_object_version": 1,
            "_timestamp": now.isoformat(),
            **clean_dict(item_dict),
        }
    }


def test_dynamo_repo_get():
    partition = fake.bs()
    partition_name = fake.bs()
    content_type = fake.bs()
    table = MagicMock()
    item_dict = fake.pydict()
    table.get_item.return_value = {"Item": item_dict}

    partition_id = [fake.bs()]
    content_id = [fake.bs(), fake.bs()]
    partition_key = fake.bs()
    sort_key = fake.bs()

    repo = DynamoRepository[ExtraModel](
        item_class=ExtraModel,
        partition_prefix=partition,
        partition_name=partition_name,
        content_type=content_type,
        table_name=fake.bs(),
        partition_key=partition_key,
        sort_key=sort_key,
        table=table,
        resource=MagicMock(),
    )
    actual = repo.get(partition_id, content_id)

    assert actual == ExtraModel(**item_dict)
    assert table.get_item.call_args == (
        (),
        {
            "Key": {
                partition_key: f"{partition}#{partition_name}#{partition_id[0]}",
                sort_key: f"{content_type}#{content_id[0]}#{content_id[1]}",
            }
        },
    )


def test_dynamo_repo_get_batch():
    partition = fake.bothify()
    partition_name = fake.bothify()
    content_type = fake.bothify()
    table_name = fake.bs()
    partition_key = fake.bs()
    sort_key = fake.bs()
    resource = MagicMock()
    item_1 = fake.pydict()
    item_2 = fake.pydict()
    item_3 = fake.pydict()

    unprocessed = [{fake.bothify(): fake.bothify()}]

    resource.batch_get_item.side_effect = [
        {"Responses": {table_name: [item_1]}, "UnprocessedKeys": unprocessed},
        {"Responses": {table_name: [item_2]}},
        {"Responses": {table_name: [item_3]}},
    ]

    repo = DynamoRepository[ExtraModel](
        item_class=ExtraModel,
        partition_prefix=partition,
        partition_name=partition_name,
        content_type=content_type,
        table_name=table_name,
        partition_key=partition_key,
        sort_key=sort_key,
        table=MagicMock(),
        resource=resource,
    )

    request_ids = [([fake.bothify()], [fake.bothify()]) for _ in range(120)]
    items = repo.get_batch(request_ids)

    assert items == [ExtraModel(**item_1), ExtraModel(**item_2), ExtraModel(**item_3)]
    assert resource.batch_get_item.call_args_list == [
        (
            (),
            {
                "RequestItems": {
                    table_name: {
                        "Keys": [
                            {
                                partition_key: f"{partition}#{partition_name}#{rid[0][0]}",
                                sort_key: f"{content_type}#{rid[1][0]}",
                            }
                            for rid in request_ids[:100]
                        ]
                    }
                }
            },
        ),
        ((), {"RequestItems": {table_name: {"Keys": unprocessed}}}),
        (
            (),
            {
                "RequestItems": {
                    table_name: {
                        "Keys": [
                            {
                                partition_key: f"{partition}#{partition_name}#{rid[0][0]}",
                                sort_key: f"{content_type}#{rid[1][0]}",
                            }
                            for rid in request_ids[100:]
                        ]
                    }
                }
            },
        ),
    ]


def test_dynamo_repo_get_none_inputs():
    partition = fake.bs()
    partition_name = fake.bs()
    content_type = fake.bs()
    partition_key = fake.bs()
    sort_key = fake.bs()
    table = MagicMock()
    item_dict = fake.pydict()
    table.get_item.return_value = {"Item": item_dict}

    repo = DynamoRepository[ExtraModel](
        item_class=ExtraModel,
        partition_prefix=partition,
        partition_name=partition_name,
        content_type=content_type,
        table_name=fake.bs(),
        partition_key=partition_key,
        sort_key=sort_key,
        table=table,
        resource=MagicMock(),
    )
    actual = repo.get(None, None)

    assert actual == ExtraModel(**item_dict)
    assert table.get_item.call_args == (
        (),
        {
            "Key": {
                partition_key: f"{partition}#{partition_name}#",
                sort_key: f"{content_type}#",
            }
        },
    )


def test_dynamo_repo_list():
    partition = fake.bs()
    partition_name = fake.bs()
    content_type = fake.bs()
    partition_key = fake.bs()
    sort_key = fake.bs()
    table = MagicMock()
    item_dict = fake.pydict()
    table.query.return_value = {"Items": [item_dict], "Count": fake.pyint()}

    partition_id = [fake.bs(), fake.bs()]
    content_id = [fake.bs(), fake.bs()]

    repo = DynamoRepository[ExtraModel](
        item_class=ExtraModel,
        partition_prefix=partition,
        partition_name=partition_name,
        content_type=content_type,
        table_name=fake.bs(),
        partition_key=partition_key,
        sort_key=sort_key,
        table=table,
        resource=MagicMock(),
    )
    ascending = random.choice((True, False))
    limit = fake.pyint()
    actual = list(repo.list(partition_id, content_id, ascending, limit))

    assert actual == [ExtraModel(**item_dict)]

    args, kwargs = table.query.call_args
    assert kwargs["ScanIndexForward"] == ascending
    assert kwargs["Limit"] == limit
    expression = kwargs["KeyConditionExpression"]
    assert expression.expression_operator == "AND"
    assert expression._values[0].expression_operator == "="
    assert expression._values[0]._values[0].name == partition_key
    assert (
        expression._values[0]._values[1]
        == f"{partition}#{partition_name}#{partition_id[0]}#{partition_id[1]}"
    )
    assert expression._values[1].expression_operator == "begins_with"
    assert expression._values[1]._values[0].name == sort_key
    assert expression._values[1]._values[1] == f"{content_type}#{content_id[0]}#{content_id[1]}"


def test_dynamo_repo_list_none_inputs():
    partition = fake.bs()
    partition_name = fake.bs()
    content_type = fake.bs()
    partition_key = fake.bs()
    sort_key = fake.bs()
    table = MagicMock()
    item_dict = fake.pydict()
    table.query.return_value = {"Items": [item_dict], "Count": fake.pyint()}

    repo = DynamoRepository[ExtraModel](
        item_class=ExtraModel,
        partition_prefix=partition,
        partition_name=partition_name,
        content_type=content_type,
        table_name=fake.bs(),
        partition_key=partition_key,
        sort_key=sort_key,
        table=table,
        resource=MagicMock(),
    )
    ascending = random.choice((True, False))
    limit = fake.pyint()
    actual = list(repo.list(None, None, ascending, limit))

    assert actual == [ExtraModel(**item_dict)]

    args, kwargs = table.query.call_args
    assert kwargs["ScanIndexForward"] == ascending
    assert kwargs["Limit"] == limit
    expression = kwargs["KeyConditionExpression"]
    assert expression.expression_operator == "AND"
    assert expression._values[0].expression_operator == "="
    assert expression._values[0]._values[0].name == partition_key
    assert expression._values[0]._values[1] == f"{partition}#{partition_name}#"
    assert expression._values[1].expression_operator == "begins_with"
    assert expression._values[1]._values[0].name == sort_key
    assert expression._values[1]._values[1] == f"{content_type}#"


def test_dynamo_repo_list_no_ids():
    partition = fake.bs()
    partition_name = fake.bs()
    content_type = fake.bs()
    partition_key = fake.bs()
    sort_key = fake.bs()
    table = MagicMock()
    item_dict = fake.pydict()
    table.query.return_value = {"Items": [item_dict], "Count": fake.pyint()}

    repo = DynamoRepository[ExtraModel](
        item_class=ExtraModel,
        partition_prefix=partition,
        partition_name=partition_name,
        content_type=content_type,
        table_name=fake.bs(),
        partition_key=partition_key,
        sort_key=sort_key,
        table=table,
        resource=MagicMock(),
    )
    ascending = random.choice((True, False))
    limit = fake.pyint()
    actual = list(repo.list([], [], ascending, limit))

    assert actual == [ExtraModel(**item_dict)]

    args, kwargs = table.query.call_args
    assert kwargs["ScanIndexForward"] == ascending
    assert kwargs["Limit"] == limit
    expression = kwargs["KeyConditionExpression"]
    assert expression.expression_operator == "AND"
    assert expression._values[0].expression_operator == "="
    assert expression._values[0]._values[0].name == partition_key
    assert expression._values[0]._values[1] == f"{partition}#{partition_name}#"
    assert expression._values[1].expression_operator == "begins_with"
    assert expression._values[1]._values[0].name == sort_key
    assert expression._values[1]._values[1] == f"{content_type}#"


def test_dynamo_repo_list_with_filter():
    partition = fake.bs()
    partition_name = fake.bs()
    content_type = fake.bs()
    partition_key = fake.bs()
    sort_key = fake.bs()
    table = MagicMock()
    item_dict = {"test_field": fake.bs()}
    table.query.return_value = {"Items": [item_dict], "Count": fake.pyint()}

    partition_id = [fake.bs()]
    content_id = [fake.bs()]

    repo = DynamoRepository[FieldModel](
        item_class=FieldModel,
        partition_prefix=partition,
        partition_name=partition_name,
        content_type=content_type,
        table_name=fake.bs(),
        partition_key=partition_key,
        sort_key=sort_key,
        table=table,
        resource=MagicMock(),
    )
    ascending = random.choice((True, False))
    limit = fake.pyint()
    filters = FilterCommand(not_exists={"failures"})
    actual = list(repo.list(partition_id, content_id, ascending, limit, filters))

    assert actual == [FieldModel(**item_dict)]

    args, kwargs = table.query.call_args
    assert kwargs["ScanIndexForward"] == ascending
    assert "Limit" not in kwargs
    filter_expression = kwargs["FilterExpression"]
    assert filter_expression.expression_operator == "attribute_not_exists"
    assert filter_expression._values[0].name == "failures"
    expression = kwargs["KeyConditionExpression"]
    assert expression.expression_operator == "AND"
    assert expression._values[0].expression_operator == "="
    assert expression._values[0]._values[0].name == partition_key
    assert expression._values[0]._values[1] == f"{partition}#{partition_name}#{partition_id[0]}"
    assert expression._values[1].expression_operator == "begins_with"
    assert expression._values[1]._values[0].name == sort_key
    assert expression._values[1]._values[1] == f"{content_type}#{content_id[0]}"


def test_dynamo_repo_list_between():
    partition = fake.bs()
    partition_name = fake.bs()
    content_type = fake.bs()
    partition_key = fake.bs()
    sort_key = fake.bs()
    table = MagicMock()
    item_dict = fake.pydict()
    table.query.return_value = {"Items": [item_dict], "Count": fake.pyint()}

    partition_id = [fake.bs(), fake.bs()]
    content_start = [fake.bs(), fake.bs()]
    content_end = [fake.bs(), fake.bs()]

    repo = DynamoRepository[ExtraModel](
        item_class=ExtraModel,
        partition_prefix=partition,
        partition_name=partition_name,
        content_type=content_type,
        table_name=fake.bs(),
        partition_key=partition_key,
        sort_key=sort_key,
        table=table,
        resource=MagicMock(),
    )
    actual = list(repo.list_between(partition_id, content_start, content_end))

    assert actual == [ExtraModel(**item_dict)]

    args, kwargs = table.query.call_args
    expression = kwargs["KeyConditionExpression"]
    assert expression.expression_operator == "AND"
    assert expression._values[0].expression_operator == "="
    assert expression._values[0]._values[0].name == partition_key
    assert (
        expression._values[0]._values[1]
        == f"{partition}#{partition_name}#{partition_id[0]}#{partition_id[1]}"
    )
    assert expression._values[1].expression_operator == "BETWEEN"
    assert expression._values[1]._values[0].name == sort_key
    assert (
        expression._values[1]._values[1] == f"{content_type}#{content_start[0]}#{content_start[1]}"
    )
    assert expression._values[1]._values[2] == f"{content_type}#{content_end[0]}#{content_end[1]}"


def test_dynamo_repo_list_between_none_inputs():
    partition = fake.bs()
    partition_name = fake.bs()
    content_type = fake.bs()
    partition_key = fake.bs()
    sort_key = fake.bs()
    table = MagicMock()
    item_dict = fake.pydict()
    table.query.return_value = {"Items": [item_dict], "Count": fake.pyint()}

    repo = DynamoRepository[ExtraModel](
        item_class=ExtraModel,
        partition_prefix=partition,
        partition_name=partition_name,
        content_type=content_type,
        table_name=fake.bs(),
        partition_key=partition_key,
        sort_key=sort_key,
        table=table,
        resource=MagicMock(),
    )
    actual = list(repo.list_between(None, None, None))

    assert actual == [ExtraModel(**item_dict)]

    args, kwargs = table.query.call_args
    expression = kwargs["KeyConditionExpression"]
    assert expression.expression_operator == "AND"
    assert expression._values[0].expression_operator == "="
    assert expression._values[0]._values[0].name == partition_key
    assert expression._values[0]._values[1] == f"{partition}#{partition_name}#"
    assert expression._values[1].expression_operator == "begins_with"
    assert expression._values[1]._values[0].name == sort_key
    assert expression._values[1]._values[1] == f"{content_type}#"


def test_dynamo_repo_list_between_with_filter():
    partition = fake.bs()
    partition_name = fake.bs()
    content_type = fake.bs()
    partition_key = fake.bs()
    sort_key = fake.bs()
    table = MagicMock()
    item_dict = {"test_field": fake.bs()}
    table.query.return_value = {"Items": [item_dict], "Count": fake.pyint()}

    partition_id = [fake.bs()]
    content_start = [fake.bs()]
    content_end = [fake.bs()]

    repo = DynamoRepository[FieldModel](
        item_class=FieldModel,
        partition_prefix=partition,
        partition_name=partition_name,
        content_type=content_type,
        table_name=fake.bs(),
        partition_key=partition_key,
        sort_key=sort_key,
        table=table,
        resource=MagicMock(),
    )
    ascending = random.choice((True, False))
    limit = fake.pyint()
    filters = FilterCommand(not_exists={"failures"})
    actual = list(
        repo.list_between(partition_id, content_start, content_end, ascending, limit, filters)
    )

    assert actual == [FieldModel(**item_dict)]

    args, kwargs = table.query.call_args
    assert kwargs["ScanIndexForward"] == ascending
    assert "Limit" not in kwargs
    filter_expression = kwargs["FilterExpression"]
    assert filter_expression.expression_operator == "attribute_not_exists"
    assert filter_expression._values[0].name == "failures"
    expression = kwargs["KeyConditionExpression"]
    assert expression.expression_operator == "AND"
    assert expression._values[0].expression_operator == "="
    assert expression._values[0]._values[0].name == partition_key
    assert expression._values[0]._values[1] == f"{partition}#{partition_name}#{partition_id[0]}"
    assert expression._values[1].expression_operator == "BETWEEN"
    assert expression._values[1]._values[0].name == sort_key
    assert expression._values[1]._values[1] == f"{content_type}#{content_start[0]}"
    assert expression._values[1]._values[2] == f"{content_type}#{content_end[0]}"


def test_content_get_repo_no_items():
    table = MagicMock()
    table.get_item.return_value = {"Not_Items": []}

    repo = DynamoRepository[ExtraModel](
        item_class=ExtraModel,
        partition_prefix=fake.bs(),
        partition_name=fake.bs(),
        content_type=fake.bs(),
        table_name=fake.bs(),
        partition_key=fake.bs(),
        sort_key=fake.bs(),
        table=table,
        resource=MagicMock(),
    )
    actual = repo.get(fake.bs(), fake.bs())

    assert actual is None


@patch("pydantic_dynamo.repository.build_update_args_for_command")
def test_dynamo_repo_update(build_update_args):
    update_args = UpdateItemArgumentsFactory()
    build_update_args.return_value = update_args
    partition = fake.bothify()
    partition_name = fake.bothify()
    content_type = fake.bothify()
    partition_key = fake.bs()
    sort_key = fake.bs()
    table = MagicMock()
    repo = DynamoRepository[ComposedFieldModel](
        item_class=ComposedFieldModel,
        partition_prefix=partition,
        partition_name=partition_name,
        content_type=content_type,
        table_name=fake.bs(),
        partition_key=partition_key,
        sort_key=sort_key,
        table=table,
        resource=MagicMock(),
    )
    partition_id = [fake.bothify()]
    content_id = [fake.bothify(), fake.bothify()]
    current_version = fake.pyint()
    command = UpdateCommand(
        set_commands={
            "test_field": fake.bs(),
            "composed": {"test_field": fake.bs(), "failures": None},
        },
        increment_attrs={"failures": 1},
        current_version=current_version,
    )

    repo.update(partition_id, content_id, command)

    update_a, update_k = table.update_item.call_args

    assert update_k.pop("Key") == {
        partition_key: f"{partition}#{partition_name}#{partition_id[0]}",
        sort_key: f"{content_type}#{content_id[0]}#{content_id[1]}",
    }

    assert update_k.pop("ConditionExpression") == update_args.condition_expression
    assert update_k.pop("UpdateExpression") == update_args.update_expression
    assert update_k.pop("ExpressionAttributeNames") == update_args.attribute_names
    assert update_k.pop("ExpressionAttributeValues") == update_args.attribute_values
    assert len(update_k) == 0


def test_dynamo_repo_update_condition_check_failed():
    item_id = fake.bs()
    content_id = fake.bs()
    table = MagicMock()
    get_resp = {"Items": [{"test_field": fake.bs()}]}
    table.query.return_value = get_resp
    ex = boto_exception("ConditionalCheckFailedException")
    table.update_item.side_effect = ex

    repo = DynamoRepository[FieldModel](
        item_class=FieldModel,
        partition_prefix=fake.bs(),
        partition_name=fake.bs(),
        content_type=fake.bs(),
        table_name=fake.bs(),
        partition_key=fake.bs(),
        sort_key=fake.bs(),
        table=table,
        resource=MagicMock(),
    )
    command = UpdateCommandFactory(set_commands={"test_field": fake.bs()}, increment_attrs=set())

    with pytest.raises(RequestObjectStateError) as ex:
        repo.update(item_id, content_id, command)

    assert item_id in str(ex.value)
    assert content_id in str(ex.value)


def test_dynamo_repo_update_item_invalid_set_command():
    item_id = fake.bs()
    content_id = fake.bs()
    table = MagicMock()

    repo = DynamoRepository[FieldModel](
        item_class=FieldModel,
        partition_prefix=fake.bs(),
        partition_name=fake.bs(),
        content_type=fake.bs(),
        table_name=fake.bs(),
        partition_key=fake.bs(),
        sort_key=fake.bs(),
        table=table,
        resource=MagicMock(),
    )
    command = UpdateCommandFactory()

    with pytest.raises(ValueError) as ex:
        repo.update(item_id, content_id, command)

    assert table.update_item.call_count == 0
    exception_value = str(ex.value)
    assert "FieldModel" in exception_value
    for attr in command.set_commands.keys():
        assert attr in exception_value


def test_dynamo_repo_update_item_invalid_incr_command():
    item_id = fake.bs()
    content_id = fake.bs()
    table = MagicMock()

    repo = DynamoRepository[FieldModel](
        item_class=FieldModel,
        partition_prefix=fake.bs(),
        partition_name=fake.bs(),
        content_type=fake.bs(),
        table_name=fake.bs(),
        partition_key=fake.bs(),
        sort_key=fake.bs(),
        table=table,
        resource=MagicMock(),
    )
    command = UpdateCommandFactory(set_commands={})

    with pytest.raises(ValueError) as ex:
        repo.update(item_id, content_id, command)

    assert table.update_item.call_count == 0
    exception_value = str(ex.value)
    assert "FieldModel" in exception_value
    for attr in command.increment_attrs:
        assert attr in exception_value


def test_dynamo_repo_list_invalid_filter():
    item_id = fake.bs()
    content_id = fake.bs()
    table = MagicMock()

    repo = DynamoRepository[FieldModel](
        item_class=FieldModel,
        partition_prefix=fake.bs(),
        partition_name=fake.bs(),
        content_type=fake.bs(),
        table_name=fake.bs(),
        partition_key=fake.bs(),
        sort_key=fake.bs(),
        table=table,
        resource=MagicMock(),
    )
    filters = FilterCommand(not_exists={fake.bs()})

    with pytest.raises(ValueError) as ex:
        list(repo.list(item_id, content_id, filters=filters))

    assert table.query.call_count == 0
    exception_value = str(ex.value)
    assert "FieldModel" in exception_value
    for attr in filters.not_exists:
        assert attr in exception_value


def test_dynamo_repo_delete():
    partition = fake.bs()
    partition_name = fake.bs()
    content_type = fake.bs()
    partition_key = fake.bs()
    sort_key = fake.bs()
    table = MagicMock()
    items = [
        ExtraModel(**{partition_key: fake.bs(), sort_key: fake.bs()}).dict() for _ in range(11)
    ]
    table.query.return_value = {"Items": items, "Count": fake.pyint()}
    writer = MagicMock()
    table.batch_writer.return_value.__enter__.return_value = writer

    partition_id = [fake.bs(), fake.bs()]
    content_id = [fake.bs(), fake.bs()]

    repo = DynamoRepository[ExtraModel](
        item_class=FieldModel,
        partition_prefix=partition,
        partition_name=partition_name,
        content_type=content_type,
        table_name=fake.bs(),
        partition_key=partition_key,
        sort_key=sort_key,
        table=table,
        resource=MagicMock(),
    )
    repo.delete(partition_id, content_id)

    args, kwargs = table.query.call_args
    expression = kwargs["KeyConditionExpression"]
    assert expression.expression_operator == "AND"
    assert expression._values[0].expression_operator == "="
    assert expression._values[0]._values[0].name == partition_key
    assert (
        expression._values[0]._values[1]
        == f"{partition}#{partition_name}#{partition_id[0]}#{partition_id[1]}"
    )
    assert expression._values[1].expression_operator == "begins_with"
    assert expression._values[1]._values[0].name == sort_key
    assert expression._values[1]._values[1] == f"{content_type}#{content_id[0]}#{content_id[1]}"
    assert writer.delete_item.call_args_list == [
        (
            (),
            {
                "Key": {
                    partition_key: item[partition_key],
                    sort_key: item[sort_key],
                }
            },
        )
        for item in items
    ]


def test_dynamo_repo_delete_none_inputs():
    partition = fake.bs()
    partition_name = fake.bs()
    content_type = fake.bs()
    partition_key = fake.bs()
    sort_key = fake.bs()
    table = MagicMock()
    items = [
        ExtraModel(**{partition_key: fake.bs(), sort_key: fake.bs()}).dict() for _ in range(11)
    ]
    table.query.return_value = {"Items": items, "Count": fake.pyint()}
    writer = MagicMock()
    table.batch_writer.return_value.__enter__.return_value = writer

    repo = DynamoRepository[ExtraModel](
        item_class=FieldModel,
        partition_prefix=partition,
        partition_name=partition_name,
        content_type=content_type,
        table_name=fake.bs(),
        partition_key=partition_key,
        sort_key=sort_key,
        table=table,
        resource=MagicMock(),
    )
    repo.delete(None, None)

    args, kwargs = table.query.call_args
    expression = kwargs["KeyConditionExpression"]
    assert expression.expression_operator == "AND"
    assert expression._values[0].expression_operator == "="
    assert expression._values[0]._values[0].name == partition_key
    assert expression._values[0]._values[1] == f"{partition}#{partition_name}#"
    assert expression._values[1].expression_operator == "begins_with"
    assert expression._values[1]._values[0].name == sort_key
    assert expression._values[1]._values[1] == f"{content_type}#"
    assert writer.delete_item.call_args_list == [
        (
            (),
            {
                "Key": {
                    partition_key: item[partition_key],
                    sort_key: item[sort_key],
                }
            },
        )
        for item in items
    ]
