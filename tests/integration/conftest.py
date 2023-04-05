import pytest
from boto3 import Session
from testcontainers.core.container import DockerContainer

from pydantic_dynamo.v2.repository import DynamoRepository
from tests.models import Example

TABLE_NAME = "integration-tests"
PARTITION_KEY = "_table_item_id"
SORT_KEY = "_table_content_id"


@pytest.fixture(scope="session")
def local_db():
    with DockerContainer("amazon/dynamodb-local").with_bind_ports(8000, 8000).with_command(
        "-jar DynamoDBLocal.jar"
    ).with_exposed_ports(8080) as local_db:
        yield local_db


@pytest.fixture
def local_db_resource(local_db):
    session = Session()

    boto3_kwargs = {
        "service_name": "dynamodb",
        "endpoint_url": "http://localhost:8000",
        "region_name": "local",
        "aws_access_key_id": "key",
        "aws_secret_access_key": "secret",
    }
    resource = session.resource(**boto3_kwargs)

    resource.create_table(
        TableName=TABLE_NAME,
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

    yield resource

    resource.Table(TABLE_NAME).delete()


@pytest.fixture
def v2_example_repo(local_db_resource):
    return DynamoRepository(
        item_class=Example,
        partition_prefix="test",
        partition_name="integration",
        content_type="example",
        table_name=TABLE_NAME,
        partition_key=PARTITION_KEY,
        sort_key=SORT_KEY,
        table=local_db_resource.Table(TABLE_NAME),
        resource=local_db_resource,
    )
