import factory

from pydantic_dynamo.models import UpdateCommand
from faker import Faker

fake = Faker()


class UpdateCommandFactory(factory.Factory):
    class Meta:
        model = UpdateCommand

    set_commands = factory.Faker("pydict")
    increment_attrs = factory.LazyFunction(lambda: {fake.bs(): _ for _ in range(5)})


def boto_exception(code: str) -> Exception:
    response = {"Error": {"Code": code}}
    ex = Exception()
    ex.response = response  # type: ignore[attr-defined]
    return ex
