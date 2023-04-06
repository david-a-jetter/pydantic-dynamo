import random
from datetime import date, time, timezone
from typing import Iterable, TypeVar

import factory
from boto3.dynamodb.conditions import Attr

from pydantic_dynamo.models import UpdateCommand, PartitionedContent
from faker import Faker

from pydantic_dynamo.utils import UpdateItemArguments
from tests.models import Example, FieldModel, CountEnum

fake = Faker()

T = TypeVar("T")


def random_element(items: Iterable[T]) -> T:
    return random.choice([el for el in items])


def boto_exception(code: str) -> Exception:
    response = {"Error": {"Code": code}}
    ex = Exception()
    ex.response = response  # type: ignore[attr-defined]
    return ex


class UpdateCommandFactory(factory.Factory):
    class Meta:
        model = UpdateCommand

    set_commands = factory.Faker("pydict")
    increment_attrs = factory.LazyFunction(lambda: {fake.bs(): _ for _ in range(5)})


class UpdateItemArgumentsFactory(factory.Factory):
    class Meta:
        model = UpdateItemArguments

    update_expression = factory.Faker("bs")
    condition_expression = factory.LazyFunction(lambda: Attr(fake.bothify()).eq(fake.bothify()))
    attribute_names = factory.LazyFunction(
        lambda: {fake.bothify(): fake.bothify() for _ in range(3)}
    )
    attribute_values = factory.LazyFunction(lambda: fake.pydict())


class FieldModelFactory(factory.Factory):
    class Meta:
        model = FieldModel

    test_field = factory.Faker("bs")
    failures = factory.Faker("pyint")


class ExampleFactory(factory.Factory):
    class Meta:
        model = Example

    dict_field = factory.LazyFunction(lambda: {fake.bothify(): FieldModel(test_field=fake.bs())})
    model_field = factory.SubFactory(FieldModelFactory)
    list_field = factory.List((fake.bs() for _ in range(2)))
    set_field = factory.LazyFunction(lambda: {fake.bs() for _ in range(2)})
    date_field = factory.LazyFunction(lambda: date.fromisoformat(fake.date()))
    time_field = factory.LazyFunction(lambda: time.fromisoformat(fake.time()))
    datetime_field = factory.Faker("date_time", tzinfo=timezone.utc)
    enum_field = factory.LazyFunction(lambda: random_element(CountEnum))


class ExamplePartitionedContentFactory(factory.Factory):
    class Meta:
        model = PartitionedContent[Example]

    partition_ids = factory.List((fake.bothify() for _ in range(2)))
    content_ids = factory.List((fake.bothify() for _ in range(2)))
    item = factory.SubFactory(ExampleFactory)
