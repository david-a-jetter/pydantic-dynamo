from uuid import uuid4

import pytest
import pytest_asyncio
from aioboto3 import Session
from testcontainers.core.container import DockerContainer

from pydantic_dynamo.v2.repository import DynamoRepository
from pydantic_dynamo.v2.sync_repository import SyncDynamoRepository
from tests.models import Example

PARTITION_KEY = "_table_item_id"
SORT_KEY = "_table_content_id"


@pytest.fixture(scope="session")
def local_db():
    with DockerContainer("amazon/dynamodb-local").with_bind_ports(8000, 8000).with_command(
        "-jar DynamoDBLocal.jar"
    ).with_exposed_ports(8080) as local_db:
        yield local_db


@pytest_asyncio.fixture
async def local_db_resource(local_db):
    session = Session()

    boto3_kwargs = {
        "service_name": "dynamodb",
        "endpoint_url": "http://localhost:8000",
        "region_name": "local",
        "aws_access_key_id": "key",
        "aws_secret_access_key": "secret",
    }
    table_name = str(uuid4())
    async with session.resource(**boto3_kwargs) as resource:
        await resource.create_table(
            TableName=table_name,
            KeySchema=[
                {"AttributeName": PARTITION_KEY, "KeyType": "HASH"},
                {"AttributeName": SORT_KEY, "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": PARTITION_KEY, "AttributeType": "S"},
                {"AttributeName": SORT_KEY, "AttributeType": "S"},
            ],
            ProvisionedThroughput={"ReadCapacityUnits": 10, "WriteCapacityUnits": 10},
        )

        yield resource, table_name
        table = await resource.Table(table_name)
        await table.delete()


@pytest_asyncio.fixture
async def v2_example_repo(local_db_resource):
    resource, table_name = local_db_resource
    yield DynamoRepository[Example](
        item_class=Example,
        partition_prefix="test",
        partition_name="integration",
        content_type="example",
        table_name=table_name,
        partition_key=PARTITION_KEY,
        sort_key=SORT_KEY,
        table=await resource.Table(table_name),
        resource=resource,
    )


@pytest.fixture
def sync_v2_example_repo(v2_example_repo):
    yield SyncDynamoRepository[Example](async_repo=v2_example_repo)
