from faker import Faker

from tests.factories import ExamplePartitionedContentFactory
from tests.models import Example
from pydantic_dynamo.v2.repository import DynamoRepository
from pydantic_dynamo.v2.write_once import WriteOnceRepository

fake = Faker()


async def test_write_once_repo(v2_example_repo: DynamoRepository[Example]):
    repo = WriteOnceRepository[Example](async_repo=v2_example_repo)
    partition_ids = [[fake.bothify(), fake.bothify()] for _ in range(2)]
    content_ids = [["Z"], ["B"], ["A"], ["D"]]
    data = [
        ExamplePartitionedContentFactory(partition_ids=p, content_ids=c)
        for p in partition_ids
        for c in content_ids
    ]

    written = await repo.write(data)

    actual = [
        c
        for partition_id in partition_ids
        async for response in v2_example_repo.list(partition_id, None)
        for c in response.contents
    ]

    assert sorted(actual) == sorted(data)
    assert sorted(actual) == sorted(written)

    written_again = await repo.write(data)

    assert len(written_again) == 0
