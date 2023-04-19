from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo

import pytest
from faker import Faker

from pydantic_dynamo.exceptions import RequestObjectStateError
from pydantic_dynamo.models import FilterCommand, UpdateCommand, PartitionedContent
from pydantic_dynamo.v2.models import GetResponse
from tests.factories import ExamplePartitionedContentFactory, ExampleFactory
from tests.models import CountEnum, Example, FieldModel

fake = Faker()


async def test_get_none(v2_example_repo):
    actual = await v2_example_repo.get(None, None)
    expected = GetResponse()
    assert actual == expected


async def test_put_then_get_item(v2_example_repo):
    partition_ids = [fake.bothify()]
    content_ids = [fake.bothify()]
    content = ExamplePartitionedContentFactory(partition_ids=partition_ids, content_ids=content_ids)
    await v2_example_repo.put(content)
    actual = await v2_example_repo.get(partition_ids, content_ids)
    expected = GetResponse(content=content)
    assert actual == expected


async def test_put_batch_then_get_batch(v2_example_repo):
    partition_ids = [fake.bothify()]
    content_ids_list = [[str(i).rjust(3, "0")] for i in range(125)]
    contents = [
        ExamplePartitionedContentFactory(partition_ids=partition_ids, content_ids=content_ids)
        for content_ids in content_ids_list
    ]
    await v2_example_repo.put_batch(contents)
    get_ids = [(partition_ids, content_ids) for content_ids in content_ids_list]
    actual = [
        content
        async for response in v2_example_repo.get_batch(get_ids)
        for content in response.contents
    ]
    actual.sort()
    contents.sort()
    assert actual == contents


async def test_put_batch_then_list_no_prefix_filter_with_sorting(v2_example_repo):
    partition_ids = [fake.bothify()]
    content_ids_list = [[str(i).rjust(3, "0")] for i in range(2250)]
    contents = [
        ExamplePartitionedContentFactory(partition_ids=partition_ids, content_ids=content_ids)
        for content_ids in content_ids_list
    ]
    await v2_example_repo.put_batch(contents)
    ascending_actual = [
        content
        async for response in v2_example_repo.list(
            partition_ids, content_prefix=None, sort_ascending=True
        )
        for content in response.contents
    ]
    contents.sort()
    assert ascending_actual == contents

    descending_actual = [
        content
        async for response in v2_example_repo.list(
            partition_ids, content_prefix=None, sort_ascending=False
        )
        for content in response.contents
    ]
    contents.sort(reverse=True)
    assert descending_actual == contents


async def test_put_batch_then_list_with_prefix_filter_and_sorting(v2_example_repo):
    partition_ids = [fake.bothify()]
    content_ids_list = [[str(i).rjust(3, "0")] for i in range(137)]
    contents = [
        ExamplePartitionedContentFactory(partition_ids=partition_ids, content_ids=content_ids)
        for content_ids in content_ids_list
    ]
    await v2_example_repo.put_batch(contents)
    ascending_actual = [
        content
        async for response in v2_example_repo.list(
            partition_ids, content_prefix=["0"], sort_ascending=True
        )
        for content in response.contents
    ]
    expected = contents[:100]
    expected.sort()
    assert ascending_actual == expected

    descending_actual = [
        content
        async for response in v2_example_repo.list(
            partition_ids, content_prefix=["0"], sort_ascending=False
        )
        for content in response.contents
    ]
    expected.sort(reverse=True)
    assert descending_actual == expected


async def test_put_batch_then_list_limit(v2_example_repo):
    partition_ids = [fake.bothify()]
    content_ids_list = [[str(i).rjust(3, "0")] for i in range(10)]
    contents = [
        ExamplePartitionedContentFactory(partition_ids=partition_ids, content_ids=content_ids)
        for content_ids in content_ids_list
    ]
    await v2_example_repo.put_batch(contents)
    actual = [
        content
        async for response in v2_example_repo.list(partition_ids, content_prefix=None, limit=2)
        for content in response.contents
    ]
    expected = contents[:2]
    assert actual == expected


async def test_put_batch_then_list_filter(v2_example_repo):
    partition_ids = [fake.bothify()]
    content_ids_list = [[str(i).rjust(3, "0")] for i in range(10)]
    contents = [
        ExamplePartitionedContentFactory(partition_ids=partition_ids, content_ids=content_ids)
        for content_ids in content_ids_list
    ]
    await v2_example_repo.put_batch(contents)
    actual = [
        content
        async for response in v2_example_repo.list(
            partition_ids,
            content_prefix=None,
            filters=FilterCommand(equals={"enum_field": "one"}),
        )
        for content in response.contents
    ]
    actual.sort()
    expected = list(filter(lambda c: c.item.enum_field == CountEnum.One, contents))
    expected.sort()
    assert actual == expected


async def test_put_batch_then_list_between_sorting(v2_example_repo):
    partition_ids = [fake.bothify()]
    now = datetime.now(tz=timezone.utc)
    content_ids_list = [[(now + timedelta(seconds=i)).isoformat()] for i in range(10)]
    contents = [
        ExamplePartitionedContentFactory(partition_ids=partition_ids, content_ids=content_ids)
        for content_ids in content_ids_list
    ]
    await v2_example_repo.put_batch(contents)
    ascending_actual = [
        content
        async for response in v2_example_repo.list_between(
            partition_id=partition_ids,
            content_start=[(now + timedelta(seconds=1)).isoformat()],
            content_end=[(now + timedelta(seconds=8)).isoformat()],
            sort_ascending=True,
        )
        for content in response.contents
    ]
    expected = contents[1:9]
    assert ascending_actual == expected
    descending_actual = [
        content
        async for response in v2_example_repo.list_between(
            partition_id=partition_ids,
            content_start=[(now + timedelta(seconds=1)).isoformat()],
            content_end=[(now + timedelta(seconds=8)).isoformat()],
            sort_ascending=False,
        )
        for content in response.contents
    ]
    expected.sort(key=lambda c: c.content_ids[0], reverse=True)
    assert descending_actual == expected


async def test_put_batch_then_list_between_limit(v2_example_repo):
    partition_ids = [fake.bothify()]
    now = datetime.now(tz=timezone.utc)
    content_ids_list = [[(now + timedelta(seconds=i)).isoformat()] for i in range(10)]
    contents = [
        ExamplePartitionedContentFactory(partition_ids=partition_ids, content_ids=content_ids)
        for content_ids in content_ids_list
    ]
    await v2_example_repo.put_batch(contents)
    ascending_actual = [
        content
        async for response in v2_example_repo.list_between(
            partition_id=partition_ids,
            content_start=[(now + timedelta(seconds=1)).isoformat()],
            content_end=[(now + timedelta(seconds=8)).isoformat()],
            limit=5,
            sort_ascending=True,
        )
        for content in response.contents
    ]
    expected = contents[1:6]
    assert ascending_actual == expected
    descending_actual = [
        content
        async for response in v2_example_repo.list_between(
            partition_id=partition_ids,
            content_start=[(now + timedelta(seconds=1)).isoformat()],
            content_end=[(now + timedelta(seconds=8)).isoformat()],
            limit=5,
            sort_ascending=False,
        )
        for content in response.contents
    ]
    descending_expected = sorted(contents, key=lambda c: c.content_ids[0], reverse=True)[1:6]
    assert descending_actual == descending_expected


async def test_put_batch_then_list_between_filter(v2_example_repo):
    partition_ids = [fake.bothify()]
    now = datetime.now(tz=timezone.utc)
    content_ids_list = [[(now + timedelta(seconds=i)).isoformat()] for i in range(10)]
    contents = [
        ExamplePartitionedContentFactory(partition_ids=partition_ids, content_ids=content_ids)
        for content_ids in content_ids_list
    ]
    await v2_example_repo.put_batch(contents)
    ascending_actual = [
        content
        async for response in v2_example_repo.list_between(
            partition_id=partition_ids,
            content_start=[(now + timedelta(seconds=1)).isoformat()],
            content_end=[(now + timedelta(seconds=8)).isoformat()],
            filters=FilterCommand(equals={"enum_field": "one"}),
        )
        for content in response.contents
    ]
    expected = list(filter(lambda c: c.item.enum_field == CountEnum.One, contents[1:9]))
    assert ascending_actual == expected


async def test_update_happy_path(v2_example_repo):
    partition_id = [fake.bothify()]
    content_id = [fake.bothify()]
    content = ExamplePartitionedContentFactory(partition_ids=partition_id, content_ids=content_id)
    await v2_example_repo.put(content)

    new_dt = datetime.now(tz=ZoneInfo("America/New_York"))
    new_str = fake.bs()

    # TTL stored as integer where we lose microsecond granularity
    new_expiry = (new_dt + timedelta(days=99)).replace(microsecond=0)
    await v2_example_repo.update(
        partition_id,
        content_id,
        UpdateCommand(
            current_version=1,
            set_commands={"datetime_field": new_dt},
            increment_attrs={"int_field": 2},
            append_attrs={"list_field": [new_str]},
            expiry=new_expiry,
        ),
    )

    updated = await v2_example_repo.get(partition_id, content_id)

    og_dict = content.item.dict()
    og_dict.pop("datetime_field")
    expected = PartitionedContent[Example](
        partition_ids=partition_id,
        content_ids=content_id,
        item=Example(
            datetime_field=new_dt,
            int_field=og_dict.pop("int_field") + 2,
            list_field=og_dict.pop("list_field") + [new_str],
            **og_dict
        ),
        current_version=2,
        expiry=new_expiry,
    )

    assert updated.content == expected


async def test_update_nested_model_field(v2_example_repo):
    partition_id = [fake.bothify()]
    content_id = [fake.bothify()]
    init_value = fake.bs()
    content = ExamplePartitionedContentFactory(
        partition_ids=partition_id,
        content_ids=content_id,
        item=ExampleFactory(model_field=FieldModel(test_field=init_value, failures=1)),
    )
    await v2_example_repo.put(content)
    await v2_example_repo.update(
        partition_id,
        content_id,
        UpdateCommand(set_commands={"model_field": {"failures": 2}}),
    )
    updated = await v2_example_repo.get(partition_id, content_id)
    expected = GetResponse(
        content=PartitionedContent[Example](
            partition_ids=partition_id,
            content_ids=content_id,
            item=Example(
                model_field=FieldModel(test_field=init_value, failures=2),
                **content.item.dict(exclude={"model_field"})
            ),
            current_version=2,
        )
    )
    assert updated == expected


async def test_update_nested_dict_field(v2_example_repo):
    partition_id = [fake.bothify()]
    content_id = [fake.bothify()]
    init_value = fake.bs()
    content = ExamplePartitionedContentFactory(
        partition_ids=partition_id,
        content_ids=content_id,
        item=ExampleFactory(dict_field={"one": init_value, "two": init_value}),
    )
    await v2_example_repo.put(content)
    new_value = fake.bs()
    await v2_example_repo.update(
        partition_id,
        content_id,
        UpdateCommand(set_commands={"dict_field": {"two": new_value}}),
    )
    updated = await v2_example_repo.get(partition_id, content_id)
    expected = GetResponse(
        content=PartitionedContent[Example](
            partition_ids=partition_id,
            content_ids=content_id,
            item=Example(
                dict_field={"one": init_value, "two": new_value},
                **content.item.dict(exclude={"dict_field"})
            ),
            current_version=2,
        )
    )
    assert updated == expected


async def test_update_requires_exists_but_doesnt(v2_example_repo):
    partition_id = [fake.bothify()]
    content_id = [fake.bothify()]
    with pytest.raises(RequestObjectStateError) as ex:
        await v2_example_repo.update(
            partition_id=partition_id, content_id=content_id, command=UpdateCommand()
        )

    assert partition_id[0] in str(ex)
    assert content_id[0] in str(ex)


async def test_update_mismatched_version(v2_example_repo):
    partition_id = [fake.bothify()]
    content_id = [fake.bothify()]
    content = ExamplePartitionedContentFactory(partition_ids=partition_id, content_ids=content_id)
    await v2_example_repo.put(content)
    new_expiry = datetime.now(tz=ZoneInfo("America/New_York"))

    # First update should succeed and increment version to 2
    await v2_example_repo.update(
        partition_id,
        content_id,
        UpdateCommand(
            current_version=1,
            expiry=new_expiry,
        ),
    )

    # Second update should fail because db has current_version of 2
    with pytest.raises(RequestObjectStateError) as ex:
        await v2_example_repo.update(
            partition_id,
            content_id,
            UpdateCommand(
                current_version=1,
                expiry=new_expiry,
            ),
        )

    assert partition_id[0] in str(ex)
    assert content_id[0] in str(ex)


async def test_put_batch_then_delete(v2_example_repo):
    partition_ids = [fake.bothify()]
    # 25 seems to be the boto3 default size for batch writes, 100 is the limit for batch gets
    # 137 ensures we span both of those limits and do so with partial size batches
    content_ids_list = [[str(i).rjust(3, "0")] for i in range(137)]
    contents = [
        ExamplePartitionedContentFactory(partition_ids=partition_ids, content_ids=content_ids)
        for content_ids in content_ids_list
    ]
    await v2_example_repo.put_batch(contents)
    await v2_example_repo.delete(partition_ids, ["0"])

    remaining = [
        content
        async for response in v2_example_repo.list(
            partition_ids,
            None,
        )
        for content in response.contents
    ]

    assert remaining == contents[100:]
