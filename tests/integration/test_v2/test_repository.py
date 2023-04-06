from datetime import date, time, timezone

from faker import Faker

from pydantic_dynamo.models import PartitionedContent, FilterCommand
from pydantic_dynamo.v2.models import GetResponse, BatchResponse, QueryResponse
from tests.factories import random_element, ExamplePartitionedContentFactory
from tests.models import FieldModel, Example, CountEnum

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
