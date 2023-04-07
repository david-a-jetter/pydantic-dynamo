from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo

import pytest
from faker import Faker

from pydantic_dynamo.exceptions import RequestObjectStateError
from pydantic_dynamo.models import FilterCommand, UpdateCommand, PartitionedContent
from pydantic_dynamo.v2.models import GetResponse, BatchResponse
from tests.factories import ExamplePartitionedContentFactory
from tests.models import CountEnum, Example

fake = Faker()


def test_get_none(v2_example_repo):
    actual = v2_example_repo.get(None, None)
    expected = GetResponse()
    assert actual == expected


def test_put_then_get_item(v2_example_repo):
    partition_ids = [fake.bothify()]
    content_ids = [fake.bothify()]
    content = ExamplePartitionedContentFactory(partition_ids=partition_ids, content_ids=content_ids)
    v2_example_repo.put(content)
    actual = v2_example_repo.get(partition_ids, content_ids)
    expected = GetResponse(content=content)
    assert actual == expected


def test_put_batch_then_get_batch(v2_example_repo):
    partition_ids = [fake.bothify()]
    # 25 seems to be the boto3 default size for batch writes, 100 is the limit for batch gets
    # 137 ensures we span both of those limits and do so with partial size batches
    content_ids_list = [[str(i).rjust(3, "0")] for i in range(137)]
    contents = [
        ExamplePartitionedContentFactory(partition_ids=partition_ids, content_ids=content_ids)
        for content_ids in content_ids_list
    ]
    v2_example_repo.put_batch(contents)
    get_ids = [(partition_ids, content_ids) for content_ids in content_ids_list]
    actual = v2_example_repo.get_batch(get_ids)
    actual.contents.sort(key=lambda c: c.content_ids)
    expected = BatchResponse(contents=contents)
    expected.contents.sort(key=lambda c: c.content_ids)
    assert actual == expected


def test_put_batch_then_list_sorting(v2_example_repo):
    partition_ids = [fake.bothify()]
    # 25 seems to be the boto3 default size for batch writes, 100 is the limit for batch gets
    # 137 ensures we span both of those limits and do so with partial size batches
    content_ids_list = [[str(i).rjust(3, "0")] for i in range(137)]
    contents = [
        ExamplePartitionedContentFactory(partition_ids=partition_ids, content_ids=content_ids)
        for content_ids in content_ids_list
    ]
    v2_example_repo.put_batch(contents)
    ascending_actual = v2_example_repo.list(
        partition_ids, content_prefix=["0"], sort_ascending=True
    )
    expected = contents[:100]
    expected.sort(key=lambda c: c.content_ids[0])
    assert list(ascending_actual.contents) == expected

    ascending_actual = v2_example_repo.list(
        partition_ids, content_prefix=["0"], sort_ascending=False
    )
    expected.sort(key=lambda c: c.content_ids[0], reverse=True)
    assert list(ascending_actual.contents) == expected


def test_put_batch_then_list_limit(v2_example_repo):
    partition_ids = [fake.bothify()]
    content_ids_list = [[str(i).rjust(3, "0")] for i in range(10)]
    contents = [
        ExamplePartitionedContentFactory(partition_ids=partition_ids, content_ids=content_ids)
        for content_ids in content_ids_list
    ]
    v2_example_repo.put_batch(contents)
    ascending_actual = v2_example_repo.list(partition_ids, content_prefix=None, limit=2)
    expected = contents[:2]
    assert list(ascending_actual.contents) == expected


def test_put_batch_then_list_filter(v2_example_repo):
    partition_ids = [fake.bothify()]
    content_ids_list = [[str(i).rjust(3, "0")] for i in range(10)]
    contents = [
        ExamplePartitionedContentFactory(partition_ids=partition_ids, content_ids=content_ids)
        for content_ids in content_ids_list
    ]
    v2_example_repo.put_batch(contents)
    ascending_actual = v2_example_repo.list(
        partition_ids,
        content_prefix=None,
        filters=FilterCommand(equals={"enum_field": "one"}),
    )
    expected = list(filter(lambda c: c.item.enum_field == CountEnum.One, contents))
    assert list(ascending_actual.contents) == expected


def test_put_batch_then_list_between_sorting(v2_example_repo):
    partition_ids = [fake.bothify()]
    now = datetime.now(tz=timezone.utc)
    content_ids_list = [[(now + timedelta(seconds=i)).isoformat()] for i in range(10)]
    contents = [
        ExamplePartitionedContentFactory(partition_ids=partition_ids, content_ids=content_ids)
        for content_ids in content_ids_list
    ]
    v2_example_repo.put_batch(contents)
    ascending_actual = v2_example_repo.list_between(
        partition_id=partition_ids,
        content_start=[(now + timedelta(seconds=1)).isoformat()],
        content_end=[(now + timedelta(seconds=8)).isoformat()],
        sort_ascending=True,
    )
    expected = contents[1:9]
    assert list(ascending_actual.contents) == expected
    descending_actual = v2_example_repo.list_between(
        partition_id=partition_ids,
        content_start=[(now + timedelta(seconds=1)).isoformat()],
        content_end=[(now + timedelta(seconds=8)).isoformat()],
        sort_ascending=False,
    )
    expected.sort(key=lambda c: c.content_ids[0], reverse=True)
    assert list(descending_actual.contents) == expected


def test_put_batch_then_list_between_limit(v2_example_repo):
    partition_ids = [fake.bothify()]
    now = datetime.now(tz=timezone.utc)
    content_ids_list = [[(now + timedelta(seconds=i)).isoformat()] for i in range(10)]
    contents = [
        ExamplePartitionedContentFactory(partition_ids=partition_ids, content_ids=content_ids)
        for content_ids in content_ids_list
    ]
    v2_example_repo.put_batch(contents)
    ascending_actual = v2_example_repo.list_between(
        partition_id=partition_ids,
        content_start=[(now + timedelta(seconds=1)).isoformat()],
        content_end=[(now + timedelta(seconds=8)).isoformat()],
        limit=5,
        sort_ascending=True,
    )
    expected = contents[1:6]
    assert list(ascending_actual.contents) == expected
    descending_actual = v2_example_repo.list_between(
        partition_id=partition_ids,
        content_start=[(now + timedelta(seconds=1)).isoformat()],
        content_end=[(now + timedelta(seconds=8)).isoformat()],
        limit=5,
        sort_ascending=False,
    )
    descending_expected = sorted(contents, key=lambda c: c.content_ids[0], reverse=True)[1:6]
    assert list(descending_actual.contents) == descending_expected


def test_put_batch_then_list_between_filter(v2_example_repo):
    partition_ids = [fake.bothify()]
    now = datetime.now(tz=timezone.utc)
    content_ids_list = [[(now + timedelta(seconds=i)).isoformat()] for i in range(10)]
    contents = [
        ExamplePartitionedContentFactory(partition_ids=partition_ids, content_ids=content_ids)
        for content_ids in content_ids_list
    ]
    v2_example_repo.put_batch(contents)
    ascending_actual = v2_example_repo.list_between(
        partition_id=partition_ids,
        content_start=[(now + timedelta(seconds=1)).isoformat()],
        content_end=[(now + timedelta(seconds=8)).isoformat()],
        filters=FilterCommand(equals={"enum_field": "one"}),
    )
    expected = list(filter(lambda c: c.item.enum_field == CountEnum.One, contents[1:9]))
    assert list(ascending_actual.contents) == expected


def test_update_happy_path(v2_example_repo):
    partition_id = [fake.bothify()]
    content_id = [fake.bothify()]
    content = ExamplePartitionedContentFactory(partition_ids=partition_id, content_ids=content_id)
    v2_example_repo.put(content)

    new_dt = datetime.now(tz=ZoneInfo("America/New_York"))
    new_str = fake.bs()

    # TTL stored as integer where we lose microsecond granularity
    new_expiry = (new_dt + timedelta(days=99)).replace(microsecond=0)
    v2_example_repo.update(
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

    updated = v2_example_repo.get(partition_id, content_id)

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


def test_update_requires_exists_but_doesnt(v2_example_repo):
    partition_id = [fake.bothify()]
    content_id = [fake.bothify()]
    with pytest.raises(RequestObjectStateError) as ex:
        v2_example_repo.update(
            partition_id=partition_id, content_id=content_id, command=UpdateCommand()
        )

    assert partition_id[0] in str(ex)
    assert content_id[0] in str(ex)


def test_update_mismatched_version(v2_example_repo):
    partition_id = [fake.bothify()]
    content_id = [fake.bothify()]
    content = ExamplePartitionedContentFactory(partition_ids=partition_id, content_ids=content_id)
    v2_example_repo.put(content)
    new_expiry = datetime.now(tz=ZoneInfo("America/New_York"))

    # First update should succeed and increment version to 2
    v2_example_repo.update(
        partition_id,
        content_id,
        UpdateCommand(
            current_version=1,
            expiry=new_expiry,
        ),
    )

    # Second update should fail because db has current_version of 2
    with pytest.raises(RequestObjectStateError) as ex:
        v2_example_repo.update(
            partition_id,
            content_id,
            UpdateCommand(
                current_version=1,
                expiry=new_expiry,
            ),
        )

    assert partition_id[0] in str(ex)
    assert content_id[0] in str(ex)


def test_put_batch_then_delete(v2_example_repo):
    partition_ids = [fake.bothify()]
    # 25 seems to be the boto3 default size for batch writes, 100 is the limit for batch gets
    # 137 ensures we span both of those limits and do so with partial size batches
    content_ids_list = [[str(i).rjust(3, "0")] for i in range(137)]
    contents = [
        ExamplePartitionedContentFactory(partition_ids=partition_ids, content_ids=content_ids)
        for content_ids in content_ids_list
    ]
    v2_example_repo.put_batch(contents)
    v2_example_repo.delete(partition_ids, ["0"])

    remaining = v2_example_repo.list(
        partition_ids,
        None,
    )

    assert list(remaining.contents) == contents[100:]
