import random
from copy import deepcopy
from datetime import datetime, date, timezone, time
from unittest.mock import patch, MagicMock

import pytest
from faker import Faker

from pydantic_dynamo.models import PartitionedContent, UpdateCommand, FilterCommand
from pydantic_dynamo.repository import (
    _clean_dict,
    _build_update_args_for_command,
    DynamoRepository,
)
from pydantic_dynamo.exceptions import RequestObjectStateError
from tests.models import ExtraModel, FieldModel, ComposedFieldModel, TestEnum
from tests.factories import UpdateCommandFactory
from tests.factories import boto_exception

fake = Faker()


def _random_enum():
    return random.choice([s for s in TestEnum])


def test_clean_dict():
    original = {
        "nested": {
            "nested_dict": fake.pydict(),
            "nested_enum": _random_enum(),
            "nested_list_of_dict": [fake.pydict() for _ in range(3)],
            "nested_datetime": datetime.now(tz=timezone.utc),
            "nested_list_of_dt": [datetime.now(tz=timezone.utc) for _ in range(3)],
            "nested_list_of_date": [date.today() for _ in range(3)],
            "nested_list_of_time": [time() for _ in range(3)],
        },
        "dict": fake.pydict(),
        "enum": _random_enum(),
        "list_of_dict": [fake.pydict() for _ in range(3)],
        "datetime": datetime.now(tz=timezone.utc),
        "list_of_dt": [datetime.now(tz=timezone.utc) for _ in range(3)],
        "list_of_date": [date.today() for _ in range(3)],
        "list_of_time": [time() for _ in range(3)],
    }
    og = deepcopy(original)
    cleaned = _clean_dict(original)
    assert cleaned["nested"]["nested_dict"].keys() == og["nested"]["nested_dict"].keys()
    assert cleaned["nested"]["nested_enum"] == og["nested"]["nested_enum"].value
    assert len(cleaned["nested"]["nested_list_of_dict"]) == len(og["nested"]["nested_list_of_dict"])
    assert cleaned["nested"]["nested_datetime"] == og["nested"]["nested_datetime"].isoformat()
    assert cleaned["nested"]["nested_list_of_dt"] == [
        dt.isoformat() for dt in og["nested"]["nested_list_of_dt"]
    ]
    assert cleaned["nested"]["nested_list_of_date"] == [
        dt.isoformat() for dt in og["nested"]["nested_list_of_date"]
    ]
    assert cleaned["nested"]["nested_list_of_time"] == [
        t.isoformat() for t in og["nested"]["nested_list_of_time"]
    ]
    assert cleaned["dict"].keys() == og["dict"].keys()
    assert cleaned["enum"] == og["enum"].value
    assert len(cleaned["list_of_dict"]) == len(og["list_of_dict"])
    assert cleaned["datetime"] == og["datetime"].isoformat()
    assert cleaned["list_of_dt"] == [dt.isoformat() for dt in og["list_of_dt"]]
    assert cleaned["list_of_date"] == [dt.isoformat() for dt in og["list_of_date"]]
    assert cleaned["list_of_time"] == [t.isoformat() for t in og["list_of_time"]]


def test_clean_dict_pydantic_base_model():
    unclean = {
        "items": [ExtraModel(a="a", b="b"), ExtraModel(c=ExtraModel(c="c"), d=[ExtraModel(d="d")])]
    }
    cleaned = _clean_dict(unclean)
    assert cleaned["items"] == [{"a": "a", "b": "b"}, {"c": {"c": "c"}, "d": [{"d": "d"}]}]


@patch("pydantic_dynamo.repository.utc_now")
def test_build_update_item_arguments(utc_now):
    now = datetime.now(tz=timezone.utc)
    utc_now.return_value = now
    some_string = fake.bs()
    some_enum = _random_enum()
    incr_attr = fake.bs()
    incr_attr2 = fake.bs()
    incr = fake.pyint()
    current_version = fake.pyint()
    el1 = fake.bs()
    el2 = fake.bs()
    el3 = fake.bs()
    command = UpdateCommand(
        current_version=current_version,
        set_commands={"some_attr": some_string, "some_enum": some_enum},
        increment_attrs={incr_attr: incr, incr_attr2: 1},
        append_attrs={"some_list": [el1, el2], "some_list_2": [el3]},
    )
    key = {"partition_key": fake.bs(), "sort_key": fake.bs()}

    update_args = _build_update_args_for_command(command, key=key)

    actual_condition = update_args.condition_expression
    assert actual_condition.expression_operator == "AND"
    assert actual_condition._values[1]._values[0].name == "_object_version"
    assert actual_condition._values[1]._values[1] == current_version
    assert actual_condition._values[0]._values[1].expression_operator == "="
    assert actual_condition._values[0]._values[1]._values[0].name == "sort_key"
    assert actual_condition._values[0]._values[1]._values[1] == key["sort_key"]
    assert actual_condition._values[0]._values[0].expression_operator == "="
    assert actual_condition._values[0]._values[0]._values[0].name == "partition_key"
    assert actual_condition._values[0]._values[0]._values[1] == key["partition_key"]

    assert (
        update_args.update_expression == "SET #att0 = :val0, #att1 = :val1, #att2 = :val2, "
        "#att3 = if_not_exists(#att3, :zero) + :val3, "
        "#att4 = if_not_exists(#att4, :zero) + :val4, "
        "#att5 = if_not_exists(#att5, :zero) + :val5, "
        "#att6 = list_append(if_not_exists(#att6, :empty_list), :val6), "
        "#att7 = list_append(if_not_exists(#att7, :empty_list), :val7)"
    )
    assert update_args.attribute_names == {
        "#att0": "some_attr",
        "#att1": "some_enum",
        "#att2": "_timestamp",
        "#att3": incr_attr,
        "#att4": incr_attr2,
        "#att5": "_object_version",
        "#att6": "some_list",
        "#att7": "some_list_2",
    }
    assert update_args.attribute_values == {
        ":zero": 0,
        ":empty_list": [],
        ":val0": some_string,
        ":val1": some_enum.value,
        ":val2": now.isoformat(),
        ":val3": incr,
        ":val4": 1,
        ":val5": 1,
        ":val6": [el1, el2],
        ":val7": [el3],
    }


def test_build_update_item_arguments_null_version():
    command = UpdateCommandFactory(current_version=None)

    update_args = _build_update_args_for_command(command)

    assert update_args.condition_expression is None


# TODO: Fix this flaky test
# def test_build_filter_expression():
#     filters = FilterCommand(
#         not_exists={"ne_a", "ne_b"},
#         equals={"eq_a": True, "eq_b": 1},
#         not_equals={"dne_a": "dne_b", "dne_c": False},
#     )
#     expression = _build_filter_expression(filters)
#     assert expression == (
#         Attr("ne_a").not_exists()
#         & Attr("ne_b").not_exists()
#         & Attr("eq_a").eq(True)
#         & Attr("eq_b").eq(1)
#         & Attr("dne_a").ne("dne_b")
#         & Attr("dne_c").ne(False)
#     )


@patch("pydantic_dynamo.repository.Session")
def test_content_repo_build(session_cls):
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


@patch("pydantic_dynamo.repository.utc_now")
def test_content_repo_put(utc_now):
    now = datetime.now(tz=timezone.utc)
    utc_now.return_value = now

    partition = fake.bs()
    content_type = fake.bs()
    partition_ids = [fake.bs()]
    partition_type = fake.bs()
    content_ids = [fake.bs()]
    table = MagicMock()

    repo = DynamoRepository[ExtraModel](
        item_class=ExtraModel,
        partition_prefix=partition,
        partition_name=partition_type,
        content_type=content_type,
        table_name=fake.bs(),
        table=table,
        resource=MagicMock(),
    )

    item_dict = fake.pydict()
    item = ExtraModel(**item_dict)
    content = PartitionedContent(partition_ids=partition_ids, content_ids=content_ids, item=item)
    repo.put(content)

    assert table.put_item.call_args[1] == {
        "Item": {
            "_table_item_id": f"{partition}#{partition_type}#{partition_ids[0]}",
            "_table_content_id": f"{content_type}#{content_ids[0]}",
            "_object_version": 1,
            "_timestamp": now.isoformat(),
            **_clean_dict(item_dict),
        }
    }


def test_content_repo_get():
    partition = fake.bs()
    partition_key = fake.bs()
    content_type = fake.bs()
    table = MagicMock()
    item_dict = fake.pydict()
    table.get_item.return_value = {"Item": item_dict}

    partition_id = [fake.bs()]
    content_id = [fake.bs(), fake.bs()]

    repo = DynamoRepository[ExtraModel](
        item_class=ExtraModel,
        partition_prefix=partition,
        partition_name=partition_key,
        content_type=content_type,
        table_name=fake.bs(),
        table=table,
        resource=MagicMock(),
    )
    actual = repo.get(partition_id, content_id)

    assert actual == ExtraModel(**item_dict)
    assert table.get_item.call_args == (
        (),
        {
            "Key": {
                "_table_item_id": f"{partition}#{partition_key}#{partition_id[0]}",
                "_table_content_id": f"{content_type}#{content_id[0]}#{content_id[1]}",
            }
        },
    )


def test_content_repo_get_batch():
    partition = fake.bothify()
    partition_key = fake.bothify()
    content_type = fake.bothify()
    table_name = fake.bs()
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
        partition_name=partition_key,
        content_type=content_type,
        table_name=table_name,
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
                                "_table_item_id": f"{partition}#{partition_key}#{rid[0][0]}",
                                "_table_content_id": f"{content_type}#{rid[1][0]}",
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
                                "_table_item_id": f"{partition}#{partition_key}#{rid[0][0]}",
                                "_table_content_id": f"{content_type}#{rid[1][0]}",
                            }
                            for rid in request_ids[100:]
                        ]
                    }
                }
            },
        ),
    ]


def test_content_repo_get_none_inputs():
    partition = fake.bs()
    partition_key = fake.bs()
    content_type = fake.bs()
    table = MagicMock()
    item_dict = fake.pydict()
    table.get_item.return_value = {"Item": item_dict}

    repo = DynamoRepository[ExtraModel](
        item_class=ExtraModel,
        partition_prefix=partition,
        partition_name=partition_key,
        content_type=content_type,
        table_name=fake.bs(),
        table=table,
        resource=MagicMock(),
    )
    actual = repo.get(None, None)

    assert actual == ExtraModel(**item_dict)
    assert table.get_item.call_args == (
        (),
        {
            "Key": {
                "_table_item_id": f"{partition}#{partition_key}#",
                "_table_content_id": f"{content_type}#",
            }
        },
    )


def test_content_repo_list():
    partition = fake.bs()
    partition_key = fake.bs()
    content_type = fake.bs()
    table = MagicMock()
    item_dict = fake.pydict()
    table.query.return_value = {"Items": [item_dict], "Count": fake.pyint()}

    partition_id = [fake.bs(), fake.bs()]
    content_id = [fake.bs(), fake.bs()]

    repo = DynamoRepository[ExtraModel](
        item_class=ExtraModel,
        partition_prefix=partition,
        partition_name=partition_key,
        content_type=content_type,
        table_name=fake.bs(),
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
    assert expression._values[0]._values[0].name == "_table_item_id"
    assert (
        expression._values[0]._values[1]
        == f"{partition}#{partition_key}#{partition_id[0]}#{partition_id[1]}"
    )
    assert expression._values[1].expression_operator == "begins_with"
    assert expression._values[1]._values[0].name == "_table_content_id"
    assert expression._values[1]._values[1] == f"{content_type}#{content_id[0]}#{content_id[1]}"


def test_content_repo_list_none_inputs():
    partition = fake.bs()
    partition_key = fake.bs()
    content_type = fake.bs()
    table = MagicMock()
    item_dict = fake.pydict()
    table.query.return_value = {"Items": [item_dict], "Count": fake.pyint()}

    repo = DynamoRepository[ExtraModel](
        item_class=ExtraModel,
        partition_prefix=partition,
        partition_name=partition_key,
        content_type=content_type,
        table_name=fake.bs(),
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
    assert expression._values[0]._values[0].name == "_table_item_id"
    assert expression._values[0]._values[1] == f"{partition}#{partition_key}#"
    assert expression._values[1].expression_operator == "begins_with"
    assert expression._values[1]._values[0].name == "_table_content_id"
    assert expression._values[1]._values[1] == f"{content_type}#"


def test_content_repo_list_no_ids():
    partition = fake.bs()
    partition_key = fake.bs()
    content_type = fake.bs()
    table = MagicMock()
    item_dict = fake.pydict()
    table.query.return_value = {"Items": [item_dict], "Count": fake.pyint()}

    repo = DynamoRepository[ExtraModel](
        item_class=ExtraModel,
        partition_prefix=partition,
        partition_name=partition_key,
        content_type=content_type,
        table_name=fake.bs(),
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
    assert expression._values[0]._values[0].name == "_table_item_id"
    assert expression._values[0]._values[1] == f"{partition}#{partition_key}#"
    assert expression._values[1].expression_operator == "begins_with"
    assert expression._values[1]._values[0].name == "_table_content_id"
    assert expression._values[1]._values[1] == f"{content_type}#"


def test_content_repo_list_with_filter():
    partition = fake.bs()
    partition_key = fake.bs()
    content_type = fake.bs()
    table = MagicMock()
    item_dict = {"test_field": fake.bs()}
    table.query.return_value = {"Items": [item_dict], "Count": fake.pyint()}

    partition_id = [fake.bs()]
    content_id = [fake.bs()]

    repo = DynamoRepository[FieldModel](
        item_class=FieldModel,
        partition_prefix=partition,
        partition_name=partition_key,
        content_type=content_type,
        table_name=fake.bs(),
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
    assert expression._values[0]._values[0].name == "_table_item_id"
    assert expression._values[0]._values[1] == f"{partition}#{partition_key}#{partition_id[0]}"
    assert expression._values[1].expression_operator == "begins_with"
    assert expression._values[1]._values[0].name == "_table_content_id"
    assert expression._values[1]._values[1] == f"{content_type}#{content_id[0]}"


def test_content_repo_list_between():
    partition = fake.bs()
    partition_key = fake.bs()
    content_type = fake.bs()
    table = MagicMock()
    item_dict = fake.pydict()
    table.query.return_value = {"Items": [item_dict], "Count": fake.pyint()}

    partition_id = [fake.bs(), fake.bs()]
    content_start = [fake.bs(), fake.bs()]
    content_end = [fake.bs(), fake.bs()]

    repo = DynamoRepository[ExtraModel](
        item_class=ExtraModel,
        partition_prefix=partition,
        partition_name=partition_key,
        content_type=content_type,
        table_name=fake.bs(),
        table=table,
        resource=MagicMock(),
    )
    actual = list(repo.list_between(partition_id, content_start, content_end))

    assert actual == [ExtraModel(**item_dict)]

    args, kwargs = table.query.call_args
    expression = kwargs["KeyConditionExpression"]
    assert expression.expression_operator == "AND"
    assert expression._values[0].expression_operator == "="
    assert expression._values[0]._values[0].name == "_table_item_id"
    assert (
        expression._values[0]._values[1]
        == f"{partition}#{partition_key}#{partition_id[0]}#{partition_id[1]}"
    )
    assert expression._values[1].expression_operator == "BETWEEN"
    assert expression._values[1]._values[0].name == "_table_content_id"
    assert (
        expression._values[1]._values[1] == f"{content_type}#{content_start[0]}#{content_start[1]}"
    )
    assert expression._values[1]._values[2] == f"{content_type}#{content_end[0]}#{content_end[1]}"


def test_content_repo_list_between_none_inputs():
    partition = fake.bs()
    partition_key = fake.bs()
    content_type = fake.bs()
    table = MagicMock()
    item_dict = fake.pydict()
    table.query.return_value = {"Items": [item_dict], "Count": fake.pyint()}

    repo = DynamoRepository[ExtraModel](
        item_class=ExtraModel,
        partition_prefix=partition,
        partition_name=partition_key,
        content_type=content_type,
        table_name=fake.bs(),
        table=table,
        resource=MagicMock(),
    )
    actual = list(repo.list_between(None, None, None))

    assert actual == [ExtraModel(**item_dict)]

    args, kwargs = table.query.call_args
    expression = kwargs["KeyConditionExpression"]
    assert expression.expression_operator == "AND"
    assert expression._values[0].expression_operator == "="
    assert expression._values[0]._values[0].name == "_table_item_id"
    assert expression._values[0]._values[1] == f"{partition}#{partition_key}#"
    assert expression._values[1].expression_operator == "begins_with"
    assert expression._values[1]._values[0].name == "_table_content_id"
    assert expression._values[1]._values[1] == f"{content_type}#"


def test_content_repo_list_between_with_filter():
    partition = fake.bs()
    partition_key = fake.bs()
    content_type = fake.bs()
    table = MagicMock()
    item_dict = {"test_field": fake.bs()}
    table.query.return_value = {"Items": [item_dict], "Count": fake.pyint()}

    partition_id = [fake.bs()]
    content_start = [fake.bs()]
    content_end = [fake.bs()]

    repo = DynamoRepository[FieldModel](
        item_class=FieldModel,
        partition_prefix=partition,
        partition_name=partition_key,
        content_type=content_type,
        table_name=fake.bs(),
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
    assert expression._values[0]._values[0].name == "_table_item_id"
    assert expression._values[0]._values[1] == f"{partition}#{partition_key}#{partition_id[0]}"
    assert expression._values[1].expression_operator == "BETWEEN"
    assert expression._values[1]._values[0].name == "_table_content_id"
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
        table=table,
        resource=MagicMock(),
    )
    actual = repo.get(fake.bs(), fake.bs())

    assert actual is None


@patch("pydantic_dynamo.repository.utc_now")
def test_content_repo_update(utc_now):
    now = datetime.now(tz=timezone.utc)
    utc_now.return_value = now
    partition = fake.bothify()
    partition_key = fake.bothify()
    content_type = fake.bothify()
    table = MagicMock()
    repo = DynamoRepository[ComposedFieldModel](
        item_class=ComposedFieldModel,
        partition_prefix=partition,
        partition_name=partition_key,
        content_type=content_type,
        table_name=fake.bs(),
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
        "_table_item_id": f"{partition}#{partition_key}#{partition_id[0]}",
        "_table_content_id": f"{content_type}#{content_id[0]}#{content_id[1]}",
    }

    actual_condition = update_k.pop("ConditionExpression")

    assert actual_condition.expression_operator == "AND"
    assert actual_condition._values[1]._values[0].name == "_object_version"
    assert actual_condition._values[1]._values[1] == current_version
    assert actual_condition._values[0]._values[1].expression_operator == "="
    assert actual_condition._values[0]._values[1]._values[0].name == "_table_content_id"
    assert (
        actual_condition._values[0]._values[1]._values[1]
        == f"{content_type}#{content_id[0]}#{content_id[1]}"
    )
    assert actual_condition._values[0]._values[0].expression_operator == "="
    assert actual_condition._values[0]._values[0]._values[0].name == "_table_item_id"
    assert (
        actual_condition._values[0]._values[0]._values[1]
        == f"{partition}#{partition_key}#{partition_id[0]}"
    )

    assert (
        update_k.pop("UpdateExpression")
        == "SET #att0 = :val0, #att1.#att2 = :val1, #att1.#att3 = :val2, #att4 = :val3, "
        "#att5 = if_not_exists(#att5, :zero) + :val4, "
        "#att6 = if_not_exists(#att6, :zero) + :val5"
    )
    assert update_k.pop("ExpressionAttributeNames") == {
        "#att0": "test_field",
        "#att1": "composed",
        "#att2": "test_field",
        "#att3": "failures",
        "#att4": "_timestamp",
        "#att5": "failures",
        "#att6": "_object_version",
    }
    assert update_k.pop("ExpressionAttributeValues") == {
        ":zero": 0,
        ":val0": command.set_commands["test_field"],
        ":val1": command.set_commands["composed"]["test_field"],
        ":val2": command.set_commands["composed"]["failures"],
        ":val3": now.isoformat(),
        ":val4": 1,
        ":val5": 1,
    }
    assert len(update_k) == 0


def test_content_repo_update_condition_check_failed():
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
        table=table,
        resource=MagicMock(),
    )
    command = UpdateCommandFactory(set_commands={"test_field": fake.bs()}, increment_attrs=set())

    with pytest.raises(RequestObjectStateError) as ex:
        repo.update(item_id, content_id, command)

    assert item_id in str(ex.value)
    assert content_id in str(ex.value)


def test_content_repo_update_item_invalid_set_command():
    item_id = fake.bs()
    content_id = fake.bs()
    table = MagicMock()

    repo = DynamoRepository[FieldModel](
        item_class=FieldModel,
        partition_prefix=fake.bs(),
        partition_name=fake.bs(),
        content_type=fake.bs(),
        table_name=fake.bs(),
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


def test_content_repo_update_item_invalid_incr_command():
    item_id = fake.bs()
    content_id = fake.bs()
    table = MagicMock()

    repo = DynamoRepository[FieldModel](
        item_class=FieldModel,
        partition_prefix=fake.bs(),
        partition_name=fake.bs(),
        content_type=fake.bs(),
        table_name=fake.bs(),
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


def test_content_repo_list_invalid_filter():
    item_id = fake.bs()
    content_id = fake.bs()
    table = MagicMock()

    repo = DynamoRepository[FieldModel](
        item_class=FieldModel,
        partition_prefix=fake.bs(),
        partition_name=fake.bs(),
        content_type=fake.bs(),
        table_name=fake.bs(),
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


def test_content_repo_delete():
    partition = fake.bs()
    partition_key = fake.bs()
    content_type = fake.bs()
    table = MagicMock()
    items = [
        ExtraModel(**{"_table_item_id": fake.bs(), "_table_content_id": fake.bs()}).dict()
        for _ in range(11)
    ]
    table.query.return_value = {"Items": items, "Count": fake.pyint()}
    writer = MagicMock()
    table.batch_writer.return_value.__enter__.return_value = writer

    partition_id = [fake.bs(), fake.bs()]
    content_id = [fake.bs(), fake.bs()]

    repo = DynamoRepository[ExtraModel](
        item_class=FieldModel,
        partition_prefix=partition,
        partition_name=partition_key,
        content_type=content_type,
        table_name=fake.bs(),
        table=table,
        resource=MagicMock(),
    )
    repo.delete(partition_id, content_id)

    args, kwargs = table.query.call_args
    expression = kwargs["KeyConditionExpression"]
    assert expression.expression_operator == "AND"
    assert expression._values[0].expression_operator == "="
    assert expression._values[0]._values[0].name == "_table_item_id"
    assert (
        expression._values[0]._values[1]
        == f"{partition}#{partition_key}#{partition_id[0]}#{partition_id[1]}"
    )
    assert expression._values[1].expression_operator == "begins_with"
    assert expression._values[1]._values[0].name == "_table_content_id"
    assert expression._values[1]._values[1] == f"{content_type}#{content_id[0]}#{content_id[1]}"
    assert writer.delete_item.call_args_list == [
        (
            (),
            {
                "Key": {
                    "_table_item_id": item["_table_item_id"],
                    "_table_content_id": item["_table_content_id"],
                }
            },
        )
        for item in items
    ]


def test_content_repo_delete_none_inputs():
    partition = fake.bs()
    partition_key = fake.bs()
    content_type = fake.bs()
    table = MagicMock()
    items = [
        ExtraModel(**{"_table_item_id": fake.bs(), "_table_content_id": fake.bs()}).dict()
        for _ in range(11)
    ]
    table.query.return_value = {"Items": items, "Count": fake.pyint()}
    writer = MagicMock()
    table.batch_writer.return_value.__enter__.return_value = writer

    repo = DynamoRepository[ExtraModel](
        item_class=FieldModel,
        partition_prefix=partition,
        partition_name=partition_key,
        content_type=content_type,
        table_name=fake.bs(),
        table=table,
        resource=MagicMock(),
    )
    repo.delete(None, None)

    args, kwargs = table.query.call_args
    expression = kwargs["KeyConditionExpression"]
    assert expression.expression_operator == "AND"
    assert expression._values[0].expression_operator == "="
    assert expression._values[0]._values[0].name == "_table_item_id"
    assert expression._values[0]._values[1] == f"{partition}#{partition_key}#"
    assert expression._values[1].expression_operator == "begins_with"
    assert expression._values[1]._values[0].name == "_table_content_id"
    assert expression._values[1]._values[1] == f"{content_type}#"
    assert writer.delete_item.call_args_list == [
        (
            (),
            {
                "Key": {
                    "_table_item_id": item["_table_item_id"],
                    "_table_content_id": item["_table_content_id"],
                }
            },
        )
        for item in items
    ]
