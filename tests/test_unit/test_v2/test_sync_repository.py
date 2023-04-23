from unittest.mock import MagicMock

from tests.models import Example
from pydantic_dynamo.v2.sync_repository import SyncDynamoRepository


def test_context_manager():
    with SyncDynamoRepository[Example](async_repo=MagicMock()) as repo:
        assert isinstance(repo, SyncDynamoRepository)
