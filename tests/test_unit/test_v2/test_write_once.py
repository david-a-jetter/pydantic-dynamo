from unittest.mock import AsyncMock, MagicMock

from faker import Faker

from tests.factories import ExamplePartitionedContentFactory
from pydantic_dynamo.v2.models import BatchResponse
from pydantic_dynamo.v2.write_once import WriteOnceRepository


fake = Faker()


async def test_write_once_empty_input():
    core = MagicMock()
    repo = WriteOnceRepository(core)

    new = await repo.write([])
    assert new == []
    assert core.list_between.call_count == 0
    assert core.put_batch.call_count == 0


async def test_write_once_no_existing():
    core = MagicMock()
    core.list_between.return_value.__aiter__.return_value = []
    put_batch = AsyncMock()
    core.put_batch = put_batch
    partition_ids = [[fake.bothify(), fake.bothify()] for _ in range(2)]
    repo = WriteOnceRepository(core)
    content_ids = [["Z"], ["B"], ["A"], ["D"]]
    data = [
        ExamplePartitionedContentFactory(partition_ids=p, content_ids=c)
        for p in partition_ids
        for c in content_ids
    ]
    new = await repo.write(data)

    assert core.list_between.call_args_list == [
        ((partition_ids[0], ["A"], ["Z"]), {}),
        ((partition_ids[1], ["A"], ["Z"]), {}),
    ]
    actual_put = [c for args, kwargs in core.put_batch.call_args_list for c in args[0]]
    sorted_expected = sorted(data)
    assert sorted(actual_put) == sorted_expected
    assert sorted(new) == sorted_expected


async def test_write_once_some_existing():
    partition_ids = [[fake.bothify(), fake.bothify()] for _ in range(2)]
    content_ids = [["Z"], ["B"], ["A"], ["D"]]
    data = [
        ExamplePartitionedContentFactory(partition_ids=p, content_ids=c)
        for p in partition_ids
        for c in content_ids
    ]
    core = MagicMock()
    response_one = MagicMock()
    response_one.__aiter__.return_value = [BatchResponse(contents=[data[0]])]
    response_two = MagicMock()
    response_two.__aiter__.return_value = [BatchResponse(contents=[data[-1]])]
    core.list_between.side_effect = [response_one, response_two]
    put_batch = AsyncMock()
    core.put_batch = put_batch
    repo = WriteOnceRepository(core)
    new = await repo.write(data)

    assert core.list_between.call_args_list == [
        ((partition_ids[0], ["A"], ["Z"]), {}),
        ((partition_ids[1], ["A"], ["Z"]), {}),
    ]
    actual_put = [c for args, kwargs in core.put_batch.call_args_list for c in args[0]]
    sorted_expected = sorted(data[i] for i in range(8) if i not in (0, 7))
    assert sorted(actual_put) == sorted_expected
    assert sorted(new) == sorted_expected


async def test_write_once_all_existing():
    partition_ids = [[fake.bothify(), fake.bothify()] for _ in range(2)]
    content_ids = [["Z"], ["B"], ["A"], ["D"]]
    data = [
        ExamplePartitionedContentFactory(partition_ids=p, content_ids=c)
        for p in partition_ids
        for c in content_ids
    ]
    core = MagicMock()
    response_one = MagicMock()
    response_one.__aiter__.return_value = [
        BatchResponse(contents=data[0:2]),
        BatchResponse(contents=data[2:4]),
    ]
    response_two = MagicMock()
    response_two.__aiter__.return_value = [
        BatchResponse(contents=data[4:6]),
        BatchResponse(contents=data[6:8]),
    ]
    core.list_between.side_effect = [response_one, response_two]
    put_batch = AsyncMock()
    core.put_batch = put_batch
    repo = WriteOnceRepository(core)
    new = await repo.write(data)

    assert core.list_between.call_args_list == [
        ((partition_ids[0], ["A"], ["Z"]), {}),
        ((partition_ids[1], ["A"], ["Z"]), {}),
    ]
    assert core.put_batch.call_count == 0
    assert new == []
