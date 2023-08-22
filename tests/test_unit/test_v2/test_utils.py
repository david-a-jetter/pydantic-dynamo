from pydantic_dynamo.v2.models import BatchResponse
from pydantic_dynamo.v2.utils import AIter, list_contents_from_batches, list_items_from_batches
from tests.factories import ExampleFactory, PartitionedContentFactory


async def test_list_contents_from_batches():
    items = ExampleFactory.build_batch(3)
    contents = [PartitionedContentFactory(item=i) for i in items]
    responses = AIter(data=[BatchResponse(contents=contents)])

    actual = await list_contents_from_batches(responses)

    assert actual == contents


async def test_list_items_from_batches():
    items = ExampleFactory.build_batch(3)
    contents = [PartitionedContentFactory(item=i) for i in items]
    responses = AIter(data=[BatchResponse(contents=contents)])

    actual = await list_items_from_batches(responses)

    assert actual == items
