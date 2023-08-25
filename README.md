# pydantic-dynamo
A Python repository over DynamoDB, leveraging the excellent 
[Pydantic](https://docs.pydantic.dev/) library to model records.

[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

## Contributing

Please see [the contributing guide](./CONTRIBUTING.md).

## Installation
Install from PyPI:
```
pip install pydantic-dynamo
```

Or even better, use [Poetry](https://python-poetry.org/docs/):
```
poetry add pydantic-dynamo
```

## Usage

All of the goodness of pydantic data modeling, pointed at your DynamoDB table instead of your REST API.

The ideal consumer is a developer of a low-to-medium complexity application. You will lose
some benefits of the single-table pattern, specifically the ability to query
and retrieve objects of different types in a single connection. Except the most complex single-table examples, 
access patterns can be implemented to utilize the `list`/`list_between`
and `get_batch` functions, documented below, to prevent N+1 queries and provide sufficient performance.


### Table Creation

This package assumes the specified table already exists and the application it is
running from has sufficient access to the table.

The following IAM permissions are required:

```yaml
- dynamodb:BatchGetItem
- dynamodb:BatchWriteItem
- dynamodb:GetItem
- dynamodb:PutItem
- dynamodb:Query
- dynamodb:UpdateItem
```

### Modeling
Create a Pydantic model specifically for storage. This should generally not be shared
in API contracts or other external interfaces to adhere to single-responsibility principle.

```python
from pydantic import BaseModel
from typing import Optional

class FilmActor(BaseModel):
    id: str
    name: str
    review: Optional[str]
    
```

#### Reserved Attribute Names
This package implicitly uses the following attributes, and therefore they should not be used in any
object model classes.

| Attribute            | Usage                                                                                                                   |
|----------------------|-------------------------------------------------------------------------------------------------------------------------|
| `_table_item_id`     | Default Partition Key name, can be overridden in repository instantiation. *Not a default in v2*                        |
| `_table_content_id`  | Default Sort Key name, can be overridden in repository instantiation. *Not a default in v2*                             |
| `_timestamp`         | Automatic ISO-formatted timestamp for when record was created or updated                                                |
| `_object_version`    | Internal object versioning integer, automatically incremented on update, and can be used to enforce conditional updates |
| `_ttl`               | Optionally used to store the DynamoDB TTL expiry value, which must be declared in the table's `TimeToLiveSpecification` |


## Version 1 (Deprecated)
The original repository implementation that still exists but will no longer receive updates.
This will be removed in a future release.

[V1 Documentation](./docs/v1.md) has been moved to keep the main README a reasonable size.

## Version 2 (Experimental)
This is the implementation that is intended for longer-term maintenance and will receive updates.

### Instantiation
V2 removes the `build` convenience function found in V1 and provides only a `__init__` constructor. Since V2 is async-first
the table and resource objects need to be constructed from [aioboto3](https://aioboto3.readthedocs.io/en/latest/usage.html#dynamodb-examples)

A working example can be found in the [integration tests conftest.py](./tests/test_integration/conftest.py)

```python
from aioboto3 import Session
from pydantic_dynamo.v2.repository import DynamoRepository
from tests.models import Example # Use your record model instead

session = Session()
boto3_kwargs = {"service_name": "dynamodb"} # endpoint_url, region_name, etc.
async with session.resource(**boto3_kwargs) as resource:
    repo = DynamoRepository[Example](
        item_class=Example,
        partition_prefix="test",
        partition_name="integration",
        content_type="example",
        table_name="table_name",
        partition_key="PARTITION_KEY",
        sort_key="SORT_KEY",
        table=await resource.Table("table_name"),
        resource=resource,
        consistent_reads=False,  # default
    )
```

The repository class also implements `AsyncContextManager` so it can be readily used in `async with...` statements.
This may make it easier to properly manage other object dependencies in factory/builder functions.
```python
from contextlib import asynccontextmanager
from aioboto3 import Session
from pydantic_dynamo.v2.repository import DynamoRepository
from tests.models import Example  # Use your record model instead

@asynccontextmanager
async def build_repo() -> DynamoRepository[Example]:
    session = Session()
    boto3_kwargs = {"service_name": "dynamodb"}  # endpoint_url, region_name, etc.
    async with session.resource(**boto3_kwargs) as resource, DynamoRepository[Example](
        item_class=Example,
        partition_prefix="test",
        partition_name="integration",
        content_type="example",
        table_name="table_name",
        partition_key="PARTITION_KEY",
        sort_key="SORT_KEY",
        table=await resource.Table("table_name"),
        resource=resource,
        consistent_reads=False,  # default
    ) as repo:
        yield repo
```

There is also a synchronous variant that can be used if you don't want to work with async/await and async generators
in your business code. Most of the subsequent documentation is focused on the async variant.

```python
from aioboto3 import Session
from pydantic_dynamo.v2.repository import DynamoRepository
from pydantic_dynamo.v2.sync_repository import SyncDynamoRepository
from tests.models import Example # Use your record model instead

session = Session()
boto3_kwargs = {"service_name": "dynamodb"} # endpoint_url, region_name, etc.
async with session.resource(**boto3_kwargs) as resource:
    repo = DynamoRepository[Example](
        item_class=Example,
        partition_prefix="test",
        partition_name="integration",
        content_type="example",
        table_name="table_name",
        partition_key="PARTITION_KEY",
        sort_key="SORT_KEY",
        table=await resource.Table("table_name"),
        resource=resource,
        consistent_reads=False,  # default
    )

    sync_repo = SyncDynamoRepository[Example](async_repo=repo)
```

#### Consistent Reads
By default, both boto3 and this library use eventually consistent read operations.
You can configure a repository instance to use strongly consistent reads by passing the
`consistent_reads` kwarg as `True` during instantiation.

You can [read more about read consistency in AWS's documentation.](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.ReadConsistency.html)

### Saving Data

Data is saved using an instance of the generic `PartitionedContent[ObjT]` class found in 
[models.py](./pydantic_dynamo/models.py). The `partition_ids` and `content_ids` are `List[str]`. 
Each value in the list is concatenated before saving, and prefixed with the repository's configured values.

For the `content_ids` field, you can leverage this to achieve degrees of query-ability for
more complex use cases, eg: `content_ids=["usa", "ny", "saratoga", "12020"]` will result in a 
sort key value of `usa#ny#saratoga#12020` that can be efficiently queried with DynamoDB's `begins_with`
condition, utilized in this library's `list` function.

It's wise to ensure that any values being used in the partition and content IDs are also retained as 
fields on the model object as well, which will make updates easier to perform. *This is less critical now
since the V2 responses include reified `PartitionedContent[ObjT]` objects instead of just the modeled records in V1.*

#### Put Single Item

This leverages the DynamoDB Put operation, and will overwrite an existing item with identical partition and content IDs.

```python
from pydantic_dynamo.models import PartitionedContent
from uuid import uuid4

id1 = str(uuid4())
actor1 = FilmActor(id=id1, name="Daniel Day-Lewis")

await repo.put(
    PartitionedContent[FilmActor](
        partition_ids=[], content_ids=[id1], item=actor1
    )
)
```

#### Put Multiple Items

When saving more than one item, you can use a batch operation that will utilize DynamoDB's `write_batch` 
operation, which will more efficiently buffer data and minimize the total number of network calls compared to calling
`put` in a loop.

```python
from pydantic_dynamo.models import PartitionedContent
from uuid import uuid4

id1 = str(uuid4())
actor1 = FilmActor(id=id1, name="Daniel Day-Lewis")
id2 = str(uuid4())
actor2 = FilmActor(id=id2, name="Steve Buscemi")


await repo.put_batch(
    (
        PartitionedContent[FilmActor](
            partition_ids=[], content_ids=[id1], item=actor1
        ),
        PartitionedContent[FilmActor](
            partition_ids=[], content_ids=[id2], item=actor2
        ),
    )
)
```

#### Update an item

NB: Please review the limitation in [issue #1](https://github.com/david-a-jetter/pydantic-dynamo/issues/1)

Updates are handled in a somewhat more complex and manual manner using an `UpdateCommand` object. 
Since this is constructed by sending `Dict[str, Any]`, dictionary entries are validated against
the pydantic model's schema before sending data to DynamoDB.

`set_commands` can be used to map attributes' names to a new value.
`increment_attrs` can be used to increment attributes' current values by some integer.
`append_attrs` can be used to extend a `List` attribute's values

`current_version` can be used to enforce a check on the object's version number to
adhere to an object versioning pattern. 


```python
from pydantic_dynamo.models import UpdateCommand

await repo.update(
    partition_id=None,
    content_id=[id1],
    command=UpdateCommand(
        set_commands={"review": "Talented, but unfriendly in Gangs of New York"}
    )
)

```


### Reading Data

#### Get Item

Finally, something simple to document. This gets a single item by its partition and content IDs,
returning a `GetResponse[FilmActor]` with a `content` value of `None` if no item is found.

This example would retrieve just the first actor item.
```python
from typing import Optional
from pydantic_dynamo.models import PartitionedContent
from pydantic_dynamo.v2.models import GetResponse

response: GetResponse = await repo.get(partition_id=None, content_id=[id1])
item: Optional[PartitionedContent[FilmActor]] = response.content
```

#### Reading Multiple Items

This and all subsequent read operations return `AsyncIterable[BatchResponse[ObjT]]` references. These cannot be directly awaited,
but rather need to be iterated in an async loop. See [PEP-525](https://peps.python.org/pep-0525/) for details and this
repository's tests for complete examples.

Most DynamoDB batch operations implement a continuation token pattern, where the API will return a subset of the data
that matches the parameters up to some stored size (eg. 1MB) and then includes a continuation token to use in a subsequent
API call.

A single `BatchResponse[ObjT]` represents one API response from DynamoDB, and contains an `Iterable[ObjT]` reference. This leaves
you as the consumer in complete control whether you should lazily stream the records or project them all into a list.

#### Get Batch by Key

This leverages DynamoDB's `batch_get_item` API to collect multiple items by their partition and content IDs.
This is often useful after having collected a previous set of records that have potentially related
items that you want to retrieve, and then associate the two in a subsequent mapping logic layer.

This example would retrieve both actor items in a single network request.
```python
from typing import List
from pydantic_dynamo.models import PartitionedContent

items: List[PartitionedContent[FilmActor]] = [
    content 
    async for response in repo.get_batch([(None, [id1]), (None, [id2])])
    for content in response.contents                      
]
```

#### Listing Items
The following two functions leverage DynamoDB's `query` API and offers the ability 
to filter on partial content ID values, change sort order, limit the quantity of items.

These operations support optional kwargs: 
* `limit` integer to cap the number of records returned
* `sort_ascending` boolean to change sort order based on the sort key value, which defaults to `True`

##### Filtering

You may also pass an optional `FilterCommand` to filter on non-key attributes. All fields
on this object are optional, and are applied utilizing `and` logic.

```python
from pydantic_dynamo.models import FilterCommand

# Find actors without a `review` attribute
filter1 = FilterCommand(
    not_exists={"review"}
)

# Find actors who are talented but unfriendly in Gangs of New York
filter2 = FilterCommand(
    equals={"review": "Talented, but unfriendly in Gangs of New York"}
)

# Find actors who are not talented but unfriendly in Gangs of New York
filter3 = FilterCommand(
    not_equals={"review": "Talented, but unfriendly in Gangs of New York"}
)

```

These operations are more interesting with a more complex data model, so let's pretend there might be 
more than one opinion on a given actor, so we'll store many reviews instead of just my own.

Let's define a review model that can be related to an actor:
```python
from pydantic import BaseModel
from datetime import datetime

class ActorReview(BaseModel):
    id: str
    actor_id: str
    created: datetime
    review: str
```

and then pretend we save a few of them:

```python
from datetime import datetime, timezone
from uuid import uuid4

now = datetime.now(tz=timezone.utc)

await repo.put_batch(
    (
        PartitionedContent[ActorReview](
            partition_ids=[actor1.id],
            content_ids=[now.isoformat()],
            item=ActorReview(
                id=str(uuid4()),
                actor_id=actor1.id,
                created=now,
                review="I really thought he was the hero of this movie"
            )
        ),
        PartitionedContent[ActorReview](
            partition_ids=[actor1.id],
            content_ids=[now.isoformat()],
            item=ActorReview(
                id=str(uuid4()),
                actor_id=actor1.id,
                created=now,
                review="He really embodies the New York state of mind"
            )
        )
    )
)
```

##### List
This function supports filter items with a `begins_with` filter on their content IDs.

This example would retrieve all review items for `actor1` that we previously saved.
```python
from typing import List
from pydantic_dynamo.models import PartitionedContent

items: List[PartitionedContent[ActorReview]] = [
    content
    async for response in repo.list(
        partition_id=[actor1.id],
        content_prefix=None,
        sort_ascending=True,
        limit=None,
        filters=None
    )
    for content in response.contents
]
```

This example would retrieve all of the reviews created on a given year, because ISO-formatted datetime
values are lexicographically sortable.
```python
from typing import List
from pydantic_dynamo.models import PartitionedContent

items: List[PartitionedContent[ActorReview]] = [
    content
    async for response in repo.list(
        partition_id=[actor1.id],
        content_prefix=["2023"],
        sort_ascending=True,
        limit=None,
        filters=None
    )
    for content in response.contents
]
```

##### List Between
This function supports filter items with a `between` filter on their content IDs.

NB: If `content_start == content_end` this will revert to calling `list` using `begins_with`. This prevents
unexpected behavior of returning no records due to how Dynamo's Query API handles `between` conditions.

This example would retrieve all reviews for actor1 that were created in or after January 2023 *and* in or before March 2023.
```python
from typing import List
from pydantic_dynamo.models import PartitionedContent

items: List[PartitionedContent[[FilmActor]] = [
    content
    async for response in repo.list_between(
        partition_id=[actor1.id],
        content_start=["2023-01"],
        content_end=["2023-04"],
        sort_ascending=True,
        limit=None,
        filters=None
    )
    for content in response.contents
]
```

### Write Once Repository
This utility grew out of a need to minimize duplicate data being saved to the database, because new
was going to be broadcast to data warehouse for wider analysis. 
This wrapper around the v2 `DynamoRepository` will return a `List[PartitionedContent[ObjT]]` 
for all records actually written to the database. The database will be queried, and each item
will be compared to an existing record, if one exists, using Python equivalency.

Assuming the underlying repository is using strongly consistent reads, this should minimize the
number of duplicate writes, although there is no statistical evaluation to prove its efficacy.

**A note on costs:**

Cost optimization is not the goal of this feature, but rather achieving a reasonable likelihood of
saving any given record only once.
This actually queries the database to compare input data to the existing data. This may cost more
than just blindly putting the content. However, since write units cost significantly more than read units
(maybe 4x? the pricing is confusing), if you have a high likelihood of duplicate data being saved, this
*may* actually be able to save you some money. As with all things MIT licensed... no guarantees :)

```python
from pydantic_dynamo.v2.write_once import WriteOnceRepository

write_once = WriteOnceRepository[Example](async_repo=repo)

count = 10000
data = [PartitionedContent(...) for _ in range(count)]

actually_written: List[PartitionedContent[ObjT]] = await write_once.write(data)
assert len(actually_written) == count

actually_written_again: List[PartitionedContent[ObjT]] = await write_once.write(data)
assert len(actually_written_again) == 0

```
