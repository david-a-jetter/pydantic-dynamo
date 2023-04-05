import random
from typing import Iterable, TypeVar

import factory
from boto3.dynamodb.conditions import Attr

from pydantic_dynamo.models import UpdateCommand
from faker import Faker

from pydantic_dynamo.utils import UpdateItemArguments

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
