import random
from datetime import datetime, timezone
from unittest.mock import patch, MagicMock

from faker import Faker

from pydantic_dynamo.models import PartitionedContent, UpdateCommand, FilterCommand
from pydantic_dynamo.v2.repository import (
    DynamoRepository,
)
from pydantic_dynamo.utils import clean_dict
from tests.models import FieldModel, ComposedFieldModel, Example
from tests.factories import (
    UpdateItemArgumentsFactory,
    ExamplePartitionedContentFactory,
    example_content_to_db_item,
)
from pydantic_dynamo.v2.models import GetResponse, BatchResponse

fake = Faker()


@patch("pydantic_dynamo.v2.repository.internal_timestamp")
def test_dynamo_repo_put(internal_timestamp):
    now = datetime.now(tz=timezone.utc)
    internal_timestamp.return_value = {"_timestamp": now.isoformat()}

    partition = fake.bs()
    content_type = fake.bs()
    partition_type = fake.bs()
    partition_key = fake.bs()
    sort_key = fake.bs()
    table = MagicMock()

    repo = DynamoRepository[Example](
        item_class=Example,
        partition_prefix=partition,
        partition_name=partition_type,
        content_type=content_type,
        table_name=fake.bs(),
        partition_key=partition_key,
        sort_key=sort_key,
        table=table,
        resource=MagicMock(),
    )

    expiry = fake.date_time()
    content = ExamplePartitionedContentFactory(expiry=expiry)
    repo.put(content)

    assert table.put_item.call_args[1] == {
        "Item": {
            partition_key: f"{partition}#{partition_type}#" + "#".join(content.partition_ids),
            sort_key: f"{content_type}#" + "#".join(content.content_ids),
            "_object_version": 1,
            "_timestamp": now.isoformat(),
            "_ttl": int(expiry.timestamp()),
            **clean_dict(content.item.dict()),
        }
    }


@patch("pydantic_dynamo.v2.repository.internal_timestamp")
def test_dynamo_repo_put_batch(internal_timestamp):
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
    writer = MagicMock()
    table.batch_writer.return_value.__enter__.return_value = writer

    repo = DynamoRepository[Example](
        item_class=Example,
        partition_prefix=partition,
        partition_name=partition_type,
        content_type=content_type,
        table_name=fake.bs(),
        partition_key=partition_key,
        sort_key=sort_key,
        table=table,
        resource=MagicMock(),
    )
    expiry = fake.date_time()
    contents = [
        ExamplePartitionedContentFactory(
            partition_ids=partition_ids, content_ids=content_ids, expiry=expiry
        ),
        ExamplePartitionedContentFactory(
            partition_ids=partition_ids, content_ids=content_ids, expiry=expiry
        ),
    ]
    repo.put_batch(contents)

    assert writer.put_item.call_args_list == [
        (
            (),
            {
                "Item": {
                    partition_key: f"{partition}#{partition_type}#"
                    + "#".join(contents[0].partition_ids),
                    sort_key: f"{content_type}#" + "#".join(contents[0].content_ids),
                    "_object_version": 1,
                    "_timestamp": now.isoformat(),
                    "_ttl": int(expiry.timestamp()),
                    **clean_dict(contents[0].item.dict()),
                }
            },
        ),
        (
            (),
            {
                "Item": {
                    partition_key: f"{partition}#{partition_type}#"
                    + "#".join(contents[1].partition_ids),
                    sort_key: f"{content_type}#" + "#".join(contents[1].content_ids),
                    "_object_version": 1,
                    "_timestamp": now.isoformat(),
                    "_ttl": int(expiry.timestamp()),
                    **clean_dict(contents[1].item.dict()),
                }
            },
        ),
    ]


def test_dynamo_repo_get():
    partition_prefix = fake.bs()
    partition_name = fake.bs()
    content_type = fake.bs()
    table = MagicMock()

    partition_id = [fake.bs()]
    content_id = [fake.bs(), fake.bs()]
    partition_key = fake.bs()
    sort_key = fake.bs()
    content = ExamplePartitionedContentFactory()
    table.get_item.return_value = {
        "Item": example_content_to_db_item(
            partition_key, partition_prefix, partition_name, sort_key, content_type, content
        )
    }

    repo = DynamoRepository[Example](
        item_class=Example,
        partition_prefix=partition_prefix,
        partition_name=partition_name,
        content_type=content_type,
        table_name=fake.bs(),
        partition_key=partition_key,
        sort_key=sort_key,
        table=table,
        resource=MagicMock(),
    )
    actual = repo.get(partition_id, content_id)

    assert actual == GetResponse(content=content)
    assert table.get_item.call_args == (
        (),
        {
            "Key": {
                partition_key: f"{partition_prefix}#{partition_name}#{partition_id[0]}",
                sort_key: f"{content_type}#{content_id[0]}#{content_id[1]}",
            }
        },
    )


def test_dynamo_repo_get_none_inputs():
    partition_prefix = fake.bs()
    partition_name = fake.bs()
    content_type = fake.bs()
    partition_key = fake.bs()
    sort_key = fake.bs()
    content = ExamplePartitionedContentFactory()
    table = MagicMock()
    table.get_item.return_value = {
        "Item": example_content_to_db_item(
            partition_key, partition_prefix, partition_name, sort_key, content_type, content
        )
    }

    repo = DynamoRepository[Example](
        item_class=Example,
        partition_prefix=partition_prefix,
        partition_name=partition_name,
        content_type=content_type,
        table_name=fake.bs(),
        partition_key=partition_key,
        sort_key=sort_key,
        table=table,
        resource=MagicMock(),
    )
    actual = repo.get(None, None)

    assert actual == GetResponse(content=content)
    assert table.get_item.call_args == (
        (),
        {
            "Key": {
                partition_key: f"{partition_prefix}#{partition_name}#",
                sort_key: f"{content_type}#",
            }
        },
    )


def test_content_get_repo_no_items():
    table = MagicMock()
    table.get_item.return_value = {"Not_Items": []}

    repo = DynamoRepository[Example](
        item_class=Example,
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

    assert actual == GetResponse(content=None)


def test_dynamo_repo_get_batch():
    partition_prefix = fake.bothify()
    partition_name = fake.bothify()
    content_type = fake.bothify()
    table_name = fake.bs()
    partition_key = fake.bs()
    sort_key = fake.bs()
    resource = MagicMock()
    items = [
        example_content_to_db_item(
            partition_key,
            partition_prefix,
            partition_name,
            sort_key,
            content_type,
            ExamplePartitionedContentFactory(partition_ids=[str(i)], content_ids=[str(i)]),
        )
        for i in range(5)
    ]

    unprocessed = [{fake.bothify(): fake.bothify()}]

    resource.batch_get_item.side_effect = [
        {
            "Responses": {table_name: items[:2]},
            "UnprocessedKeys": unprocessed,
        },
        {"Responses": {table_name: items[2:4]}},
        {"Responses": {table_name: [items[-1]]}},
    ]

    repo = DynamoRepository[Example](
        item_class=Example,
        partition_prefix=partition_prefix,
        partition_name=partition_name,
        content_type=content_type,
        table_name=table_name,
        partition_key=partition_key,
        sort_key=sort_key,
        table=MagicMock(),
        resource=resource,
    )

    # Max key count for each request is 100
    request_ids = [([fake.bothify()], [fake.bothify()]) for _ in range(120)]
    actual = repo.get_batch(request_ids)

    expected = BatchResponse(
        contents=[
            PartitionedContent[Example](
                partition_ids=[str(i)], content_ids=[str(i)], item=Example(**item)
            )
            for i, item in enumerate(items)
        ]
    )

    assert actual == expected
    assert resource.batch_get_item.call_args_list == [
        (
            (),
            {
                "RequestItems": {
                    table_name: {
                        "Keys": [
                            {
                                partition_key: f"{partition_prefix}#{partition_name}#{rid[0][0]}",
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
                                partition_key: f"{partition_prefix}#{partition_name}#{rid[0][0]}",
                                sort_key: f"{content_type}#{rid[1][0]}",
                            }
                            for rid in request_ids[100:]
                        ]
                    }
                }
            },
        ),
    ]


def test_dynamo_repo_list():
    partition_prefix = fake.bs()
    partition_name = fake.bs()
    content_type = fake.bs()
    partition_key = fake.bs()
    sort_key = fake.bs()
    table = MagicMock()
    contents = ExamplePartitionedContentFactory.build_batch(3)
    table.query.return_value = {
        "Items": [
            example_content_to_db_item(
                partition_key, partition_prefix, partition_name, sort_key, content_type, content
            )
            for content in contents
        ],
        "Count": fake.pyint(),
    }

    partition_id = [fake.bs(), fake.bs()]
    content_id = [fake.bs(), fake.bs()]

    repo = DynamoRepository[Example](
        item_class=Example,
        partition_prefix=partition_prefix,
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
    actual = repo.list(partition_id, content_id, ascending, limit)

    assert sorted(actual.contents) == sorted(contents)

    args, kwargs = table.query.call_args
    assert kwargs["ScanIndexForward"] == ascending
    assert kwargs["Limit"] == limit
    expression = kwargs["KeyConditionExpression"]
    assert expression.expression_operator == "AND"
    assert expression._values[0].expression_operator == "="
    assert expression._values[0]._values[0].name == partition_key
    assert (
        expression._values[0]._values[1]
        == f"{partition_prefix}#{partition_name}#{partition_id[0]}#{partition_id[1]}"
    )
    assert expression._values[1].expression_operator == "begins_with"
    assert expression._values[1]._values[0].name == sort_key
    assert expression._values[1]._values[1] == f"{content_type}#{content_id[0]}#{content_id[1]}"


def test_dynamo_repo_list_last_evaluated_under_limit():
    partition_prefix = fake.bs()
    partition_name = fake.bs()
    content_type = fake.bs()
    partition_key = fake.bs()
    sort_key = fake.bs()
    table = MagicMock()
    contents = ExamplePartitionedContentFactory.build_batch(4)
    start_key = fake.bothify()
    table.query.side_effect = [
        {
            "Items": [
                example_content_to_db_item(
                    partition_key, partition_prefix, partition_name, sort_key, content_type, content
                )
                for content in contents[:3]
            ],
            "LastEvaluatedKey": start_key,
            "Count": 3,
        },
        {
            "Items": [
                example_content_to_db_item(
                    partition_key, partition_prefix, partition_name, sort_key, content_type, content
                )
                for content in contents[3:4]
            ],
            "Count": 1,
        },
    ]

    repo = DynamoRepository[Example](
        item_class=Example,
        partition_prefix=partition_prefix,
        partition_name=partition_name,
        content_type=content_type,
        table_name=fake.bs(),
        partition_key=partition_key,
        sort_key=sort_key,
        table=table,
        resource=MagicMock(),
    )
    partition_id = [fake.bs(), fake.bs()]
    content_id = [fake.bs(), fake.bs()]
    ascending = random.choice((True, False))
    limit = 5
    actual = repo.list(partition_id, content_id, ascending, limit)

    assert sorted(actual.contents) == sorted(contents)
    assert len(table.query.call_args_list) == 2
    _, kwargs2 = table.query.call_args_list[1]
    assert kwargs2["ExclusiveStartKey"] == start_key


def test_dynamo_repo_list_last_evaluated_over_limit():
    partition_prefix = fake.bs()
    partition_name = fake.bs()
    content_type = fake.bs()
    partition_key = fake.bs()
    sort_key = fake.bs()
    table = MagicMock()
    contents = ExamplePartitionedContentFactory.build_batch(6)
    start_key = fake.bothify()
    table.query.side_effect = [
        {
            "Items": [
                example_content_to_db_item(
                    partition_key, partition_prefix, partition_name, sort_key, content_type, content
                )
                for content in contents[:3]
            ],
            "LastEvaluatedKey": start_key,
            "Count": 3,
        },
        {
            "Items": [
                example_content_to_db_item(
                    partition_key, partition_prefix, partition_name, sort_key, content_type, content
                )
                for content in contents[3:6]
            ],
            "Count": 3,
        },
    ]

    repo = DynamoRepository[Example](
        item_class=Example,
        partition_prefix=partition_prefix,
        partition_name=partition_name,
        content_type=content_type,
        table_name=fake.bs(),
        partition_key=partition_key,
        sort_key=sort_key,
        table=table,
        resource=MagicMock(),
    )
    partition_id = [fake.bs(), fake.bs()]
    content_id = [fake.bs(), fake.bs()]
    ascending = random.choice((True, False))
    limit = 5
    actual = repo.list(partition_id, content_id, ascending, limit)

    assert sorted(actual.contents) == sorted(contents)
    assert len(table.query.call_args_list) == 2
    _, kwargs2 = table.query.call_args_list[1]
    assert kwargs2["ExclusiveStartKey"] == start_key


def test_dynamo_repo_list_last_evaluated_over_limit_after_evaluated_key():
    partition_prefix = fake.bs()
    partition_name = fake.bs()
    content_type = fake.bs()
    partition_key = fake.bs()
    sort_key = fake.bs()
    table = MagicMock()
    contents = ExamplePartitionedContentFactory.build_batch(8)
    start_key1 = fake.bothify()
    start_key2 = fake.bothify()
    table.query.side_effect = [
        {
            "Items": [
                example_content_to_db_item(
                    partition_key, partition_prefix, partition_name, sort_key, content_type, content
                )
                for content in contents[:3]
            ],
            "LastEvaluatedKey": start_key1,
            "Count": 3,
        },
        {
            "Items": [
                example_content_to_db_item(
                    partition_key, partition_prefix, partition_name, sort_key, content_type, content
                )
                for content in contents[3:6]
            ],
            "LastEvaluatedKey": start_key2,
            "Count": 3,
        },
    ]

    repo = DynamoRepository[Example](
        item_class=Example,
        partition_prefix=partition_prefix,
        partition_name=partition_name,
        content_type=content_type,
        table_name=fake.bs(),
        partition_key=partition_key,
        sort_key=sort_key,
        table=table,
        resource=MagicMock(),
    )
    partition_id = [fake.bs(), fake.bs()]
    content_id = [fake.bs(), fake.bs()]
    ascending = random.choice((True, False))
    limit = 5
    actual = repo.list(partition_id, content_id, ascending, limit)

    assert sorted(actual.contents) == sorted(contents[:6])
    assert len(table.query.call_args_list) == 2
    _, kwargs2 = table.query.call_args_list[1]
    assert kwargs2["ExclusiveStartKey"] == start_key1


def test_dynamo_repo_list_none_inputs():
    partition_prefix = fake.bs()
    partition_name = fake.bs()
    content_type = fake.bs()
    partition_key = fake.bs()
    sort_key = fake.bs()
    table = MagicMock()
    contents = ExamplePartitionedContentFactory.build_batch(3)
    table.query.return_value = {
        "Items": [
            example_content_to_db_item(
                partition_key, partition_prefix, partition_name, sort_key, content_type, content
            )
            for content in contents
        ],
        "Count": fake.pyint(),
    }

    repo = DynamoRepository[Example](
        item_class=Example,
        partition_prefix=partition_prefix,
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
    actual = repo.list(None, None, ascending, limit)

    assert sorted(actual.contents) == sorted(contents)

    args, kwargs = table.query.call_args
    assert kwargs["ScanIndexForward"] == ascending
    assert kwargs["Limit"] == limit
    expression = kwargs["KeyConditionExpression"]
    assert expression.expression_operator == "AND"
    assert expression._values[0].expression_operator == "="
    assert expression._values[0]._values[0].name == partition_key
    assert expression._values[0]._values[1] == f"{partition_prefix}#{partition_name}#"
    assert expression._values[1].expression_operator == "begins_with"
    assert expression._values[1]._values[0].name == sort_key
    assert expression._values[1]._values[1] == f"{content_type}#"


def test_dynamo_repo_list_no_ids():
    partition_prefix = fake.bs()
    partition_name = fake.bs()
    content_type = fake.bs()
    partition_key = fake.bs()
    sort_key = fake.bs()
    table = MagicMock()
    contents = ExamplePartitionedContentFactory.build_batch(3)
    table.query.return_value = {
        "Items": [
            example_content_to_db_item(
                partition_key, partition_prefix, partition_name, sort_key, content_type, content
            )
            for content in contents
        ],
        "Count": fake.pyint(),
    }

    repo = DynamoRepository[Example](
        item_class=Example,
        partition_prefix=partition_prefix,
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
    actual = repo.list([], [], ascending, limit)

    assert sorted(actual.contents) == sorted(contents)

    args, kwargs = table.query.call_args
    assert kwargs["ScanIndexForward"] == ascending
    assert kwargs["Limit"] == limit
    expression = kwargs["KeyConditionExpression"]
    assert expression.expression_operator == "AND"
    assert expression._values[0].expression_operator == "="
    assert expression._values[0]._values[0].name == partition_key
    assert expression._values[0]._values[1] == f"{partition_prefix}#{partition_name}#"
    assert expression._values[1].expression_operator == "begins_with"
    assert expression._values[1]._values[0].name == sort_key
    assert expression._values[1]._values[1] == f"{content_type}#"


def test_dynamo_repo_list_with_filter():
    partition_prefix = fake.bs()
    partition_name = fake.bs()
    content_type = fake.bs()
    partition_key = fake.bs()
    sort_key = fake.bs()
    table = MagicMock()
    contents = ExamplePartitionedContentFactory.build_batch(3)
    table.query.return_value = {
        "Items": [
            example_content_to_db_item(
                partition_key, partition_prefix, partition_name, sort_key, content_type, content
            )
            for content in contents
        ],
        "Count": fake.pyint(),
    }

    partition_id = [fake.bs()]
    content_id = [fake.bs()]

    repo = DynamoRepository[Example](
        item_class=Example,
        partition_prefix=partition_prefix,
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
    filters = FilterCommand(not_exists={"optional_field"})
    actual = repo.list(partition_id, content_id, ascending, limit, filters)

    assert sorted(actual.contents) == sorted(contents)

    args, kwargs = table.query.call_args
    assert kwargs["ScanIndexForward"] == ascending
    assert "Limit" not in kwargs
    filter_expression = kwargs["FilterExpression"]
    assert filter_expression.expression_operator == "attribute_not_exists"
    assert filter_expression._values[0].name == "optional_field"
    expression = kwargs["KeyConditionExpression"]
    assert expression.expression_operator == "AND"
    assert expression._values[0].expression_operator == "="
    assert expression._values[0]._values[0].name == partition_key
    assert (
        expression._values[0]._values[1] == f"{partition_prefix}#{partition_name}#{partition_id[0]}"
    )
    assert expression._values[1].expression_operator == "begins_with"
    assert expression._values[1]._values[0].name == sort_key
    assert expression._values[1]._values[1] == f"{content_type}#{content_id[0]}"


def test_dynamo_repo_list_between():
    partition_prefix = fake.bs()
    partition_name = fake.bs()
    content_type = fake.bs()
    partition_key = fake.bs()
    sort_key = fake.bs()
    table = MagicMock()
    contents = ExamplePartitionedContentFactory.build_batch(3)
    table.query.return_value = {
        "Items": [
            example_content_to_db_item(
                partition_key, partition_prefix, partition_name, sort_key, content_type, content
            )
            for content in contents
        ],
        "Count": fake.pyint(),
    }

    partition_id = [fake.bs(), fake.bs()]
    content_start = [fake.bs(), fake.bs()]
    content_end = [fake.bs(), fake.bs()]

    repo = DynamoRepository[Example](
        item_class=Example,
        partition_prefix=partition_prefix,
        partition_name=partition_name,
        content_type=content_type,
        table_name=fake.bs(),
        partition_key=partition_key,
        sort_key=sort_key,
        table=table,
        resource=MagicMock(),
    )
    actual = repo.list_between(partition_id, content_start, content_end)

    assert sorted(actual.contents) == sorted(contents)

    args, kwargs = table.query.call_args
    expression = kwargs["KeyConditionExpression"]
    assert expression.expression_operator == "AND"
    assert expression._values[0].expression_operator == "="
    assert expression._values[0]._values[0].name == partition_key
    assert (
        expression._values[0]._values[1]
        == f"{partition_prefix}#{partition_name}#{partition_id[0]}#{partition_id[1]}"
    )
    assert expression._values[1].expression_operator == "BETWEEN"
    assert expression._values[1]._values[0].name == sort_key
    assert (
        expression._values[1]._values[1] == f"{content_type}#{content_start[0]}#{content_start[1]}"
    )
    assert expression._values[1]._values[2] == f"{content_type}#{content_end[0]}#{content_end[1]}"


def test_dynamo_repo_list_between_none_inputs():
    partition_prefix = fake.bs()
    partition_name = fake.bs()
    content_type = fake.bs()
    partition_key = fake.bs()
    sort_key = fake.bs()
    table = MagicMock()
    contents = ExamplePartitionedContentFactory.build_batch(3)
    table.query.return_value = {
        "Items": [
            example_content_to_db_item(
                partition_key, partition_prefix, partition_name, sort_key, content_type, content
            )
            for content in contents
        ],
        "Count": fake.pyint(),
    }

    repo = DynamoRepository[Example](
        item_class=Example,
        partition_prefix=partition_prefix,
        partition_name=partition_name,
        content_type=content_type,
        table_name=fake.bs(),
        partition_key=partition_key,
        sort_key=sort_key,
        table=table,
        resource=MagicMock(),
    )
    actual = repo.list_between(None, None, None)

    assert sorted(actual.contents) == sorted(contents)

    args, kwargs = table.query.call_args
    expression = kwargs["KeyConditionExpression"]
    assert expression.expression_operator == "AND"
    assert expression._values[0].expression_operator == "="
    assert expression._values[0]._values[0].name == partition_key
    assert expression._values[0]._values[1] == f"{partition_prefix}#{partition_name}#"
    assert expression._values[1].expression_operator == "begins_with"
    assert expression._values[1]._values[0].name == sort_key
    assert expression._values[1]._values[1] == f"{content_type}#"


def test_dynamo_repo_list_between_with_filter():
    partition_prefix = fake.bs()
    partition_name = fake.bs()
    content_type = fake.bs()
    partition_key = fake.bs()
    sort_key = fake.bs()
    table = MagicMock()
    contents = ExamplePartitionedContentFactory.build_batch(3)
    table.query.return_value = {
        "Items": [
            example_content_to_db_item(
                partition_key, partition_prefix, partition_name, sort_key, content_type, content
            )
            for content in contents
        ],
        "Count": fake.pyint(),
    }

    partition_id = [fake.bs()]
    content_start = [fake.bs()]
    content_end = [fake.bs()]

    repo = DynamoRepository[Example](
        item_class=Example,
        partition_prefix=partition_prefix,
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
    filters = FilterCommand(not_exists={"optional_field"})
    actual = repo.list_between(partition_id, content_start, content_end, ascending, limit, filters)

    assert sorted(actual.contents) == sorted(contents)

    args, kwargs = table.query.call_args
    assert kwargs["ScanIndexForward"] == ascending
    assert "Limit" not in kwargs
    filter_expression = kwargs["FilterExpression"]
    assert filter_expression.expression_operator == "attribute_not_exists"
    assert filter_expression._values[0].name == "optional_field"
    expression = kwargs["KeyConditionExpression"]
    assert expression.expression_operator == "AND"
    assert expression._values[0].expression_operator == "="
    assert expression._values[0]._values[0].name == partition_key
    assert (
        expression._values[0]._values[1] == f"{partition_prefix}#{partition_name}#{partition_id[0]}"
    )
    assert expression._values[1].expression_operator == "BETWEEN"
    assert expression._values[1]._values[0].name == sort_key
    assert expression._values[1]._values[1] == f"{content_type}#{content_start[0]}"
    assert expression._values[1]._values[2] == f"{content_type}#{content_end[0]}"


@patch("pydantic_dynamo.v2.repository.build_update_args_for_command")
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


def test_dynamo_repo_delete():
    partition_prefix = fake.bs()
    partition_name = fake.bs()
    content_type = fake.bs()
    partition_key = fake.bs()
    sort_key = fake.bs()
    table = MagicMock()
    contents = ExamplePartitionedContentFactory.build_batch(11)
    items = [
        example_content_to_db_item(
            partition_key, partition_prefix, partition_name, sort_key, content_type, c
        )
        for c in contents
    ]
    table.query.return_value = {"Items": items, "Count": fake.pyint()}
    writer = MagicMock()
    table.batch_writer.return_value.__enter__.return_value = writer

    partition_id = [fake.bs(), fake.bs()]
    content_id = [fake.bs(), fake.bs()]

    repo = DynamoRepository[Example](
        item_class=FieldModel,
        partition_prefix=partition_prefix,
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
        == f"{partition_prefix}#{partition_name}#{partition_id[0]}#{partition_id[1]}"
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
    partition_prefix = fake.bs()
    partition_name = fake.bs()
    content_type = fake.bs()
    partition_key = fake.bs()
    sort_key = fake.bs()
    table = MagicMock()
    contents = ExamplePartitionedContentFactory.build_batch(11)
    items = [
        example_content_to_db_item(
            partition_key, partition_prefix, partition_name, sort_key, content_type, c
        )
        for c in contents
    ]
    table.query.return_value = {"Items": items, "Count": fake.pyint()}
    writer = MagicMock()
    table.batch_writer.return_value.__enter__.return_value = writer

    repo = DynamoRepository[Example](
        item_class=FieldModel,
        partition_prefix=partition_prefix,
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
    assert expression._values[0]._values[1] == f"{partition_prefix}#{partition_name}#"
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
