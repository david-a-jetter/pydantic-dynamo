# pydantic-dynamo
A Python repository over DynamoDB leveraging the excellent 
[Pydantic](https://docs.pydantic.dev/) library to model records.

## Contributing

TODO

## Installation
Install from PyPI `pip install pydantic-dynamo`

Or even better, use [Poetry](https://python-poetry.org/docs/) `poetry add pydantic-dynamo`

## Usage

The intended usage of this package is a low-to-medium complexity application. You will lose
some benefits of the single-table pattern, specifically the ability to query
and retrieve objects of different types in one connection. For most use cases except
the most complex examples, access patterns can be implemented to utilize the `list`/`list_between`
and `get_batch` functions, documented below, to prevent N+1 queries.

This package assumes the specified table already exists and the application it is
running from has sufficient access to the table.

Currently, the package requires the table be created with a partition key named 
`_table_item_id` and a sort key named `_table_content_id`.

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

### Instantiation
The repository configuration will dictate the prefix values used for the partition and sort
key attributes. Once data is saved, these values cannot be changed without losing
access to previously saved data.

`partition_prefix` can be used to categorize data and potentially implement
row-based access control. See [DynamoDB docs](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/specifying-conditions.html)
and [IAM Condition docs](https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_elements_condition.html).
**Access control in this manner is only theoretical right now and has not been fully tested.**

`partition_name` is used in conjunction with the `partition_prefix`, with a `#` delimiter,
to form the partition key value of items within the repository. Individual items can then be
saved with their own partition ID value to allow querying based on the sort key values only,
or they can be saved without their own partition ID to allow querying across the entire 
repository.

`content_type` is used as the prefix of the sort key value. Typically, this should be set
to the snake-cased version of the pydantic class name, eg: `movie_character`.

If all repositories are sharing a single table, it's important that each repository 
has a different combination of the above three values to ensure that the data is segmented.
You can choose to point repositories to different tables, but managing capacity becomes
a more complicated problem, which is outside the scope of this library.

There are two ways to instantiate a repository instance:

Through the `build` method that will generate the boto3 session and table objects:

```python
from pydantic_dynamo.repository import DynamoRepository
repo = DynamoRepository[FilmActor].build(
    table_name="dynamodb-table-name",
    item_class=FilmActor,
    partition_prefix="content",
    partition_name="movies",
    content_type="character",
)
```

Or directly to the `__init__` if you want control over how the boto3 objects are created:

```python
from pydantic_dynamo.repository import DynamoRepository
from boto3 import Session

resource = Session().resource("dynamodb")
table = resource.Table("dynamodb-table-name")

repo = DynamoRepository[FilmActor](
    item_class=FilmActor,
    partition_prefix="content",
    partition_name="movies",
    content_type="character",
    table=table,
    resource=resource
)
```
### Saving Data

Data is saved using an instance of the generic `PartitionedContent[ObjT]` class found in 
[models.py](./pydantic_dynamo/models.py). The `partition_ids` and `content_ids` are `List[str]`. 
Each value in the list is eventually concatenated, and prefixed with the repository's configured values.

Particularly for the `content_ids` field, you can leverage this to achieve degrees of query-ability for
more complex use cases, eg: `content_ids=["usa", "ny", "saratoga", "12020"]` will result in a 
sort key value of `usa#ny#saratoga#12020` that can be efficiently queried with DynamoDB's `begins_with`
condition, utilized in this library's `list` function.

It's wise to ensure that any values being used in the partition and content IDs are also retained as 
fields on the model object as well, which will make updates easier to perform.

#### Put Single Item

This is logically similar to the DynamoDB Put operation, and will overwrite an existing item with 
identical partition and content IDs.

```python
from pydantic_dynamo.models import PartitionedContent
from uuid import uuid4

id1 = str(uuid4())
actor1 = FilmActor(id=id1, name="Daniel Day-Lewis")

repo.put(
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


repo.put_batch(
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
adhere to an object versioning pattern. Since there isn't a way the repo currently returns
and object's version, this is not useful at the moment but is an experiment in progress.


```python
from pydantic_dynamo.models import UpdateCommand

repo.update(
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
returning `None` if no item is found.

This example would retrieve just the first actor item.
```python
from typing import Optional

item: Optional[FilmActor] = repo.get(partition_id=None, content_id=[id1])
```

#### Get Multiple Items

This leverages DynamoDB's `batch_get_item` API to collect multiple items by their partition and content IDs.
This is often useful after having collected a previous set of records that have potentially related
items that you want to retrieve, and then associate the two in a subsequent mapping logic layer.

This example would retrieve both actor items in a single network request.
```python
from typing import List

items: List[FilmActor] = repo.get_batch([(None, [id1]), (None, [id2])])

```

#### Listing Items
The following two functions leverage DynamoDB's `query` API and offers the ability 
to filter on content ID values, change sort order, limit the quantity of items. 

NB: These returns an `Iterator` type, which will not execute any query until it begins iteration.

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

##### List
This function supports filter items with a `begins_with` filter on their content IDs.

This example would retrieve all actor items.
```python
from typing import Iterator

items: Iterator[FilmActor] = repo.list(
    partition_id=None,
    content_prefix=None,
    sort_ascending=True, # default order by sort key value
    limit=None,
    filters=None
)
```

##### List Between
This function supports filter items with a `between` filter on their content IDs.

NB: If `content_start == content_end` this will revert to calling `list` using `begins_with`.

This example would retrieve all actor items. It's a lame example and should be updated
with something more interesting. A common use case is to include an ISO-formatted datetime
value at the end of a content ID, and you can retrieve all values in a given partition
between two specified datetimes.
```python
from typing import Iterator

items: Iterator[FilmActor] = repo.list_between(
    partition_id=None,
    content_start=None,
    content_end=None,
    sort_ascending=True, # default order by sort key value
    limit=None,
    filters=None
)

```
