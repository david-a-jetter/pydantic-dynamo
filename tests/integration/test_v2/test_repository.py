from datetime import date, time, timezone

from faker import Faker

from pydantic_dynamo.models import PartitionedContent
from tests.factories import random_element
from tests.models import FieldModel, Example, CountEnum

fake = Faker()


def test_get_none(v2_example_repo):
    actual = v2_example_repo.get(None, None)
    assert actual.content is None


def test_get_item(v2_example_repo):
    partition_ids = [fake.bothify()]
    content_ids = [fake.bothify()]
    content = PartitionedContent(
        partition_ids=partition_ids,
        content_ids=content_ids,
        item=Example(
            dict_field={fake.bothify(): FieldModel(test_field=fake.bs())},
            model_field=FieldModel(test_field=fake.bs(), failures=fake.pyint()),
            list_field=[fake.bs(), fake.bs()],
            set_field={fake.bs(), fake.bs()},
            date_field=date.fromisoformat(fake.date()),
            time_field=time.fromisoformat(fake.time()),
            datetime_field=fake.date_time(tzinfo=timezone.utc),
            enum_field=random_element(CountEnum),
        ),
    )
    v2_example_repo.put(content)
    actual = v2_example_repo.get(partition_ids, content_ids)
    assert actual.content.item == content.item
