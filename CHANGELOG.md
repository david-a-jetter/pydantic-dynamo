# Versions

# 0.2.2
* Add support for strongly consistent reads by setting the `consistent_reads` optional kwarg in repository constructor
* Add missing generic argument `ObjT` type annotations to repositories


# 0.2.1
* Add implementation to support `AbstractAsyncContextManager` to `DynamoRepository`
  * Supports `async with DynamoRepository(...) as repo:` usage
* Add implementation nto support `AbstractContextManager` to `SyncDynamoRepository`
  * Supports `with SyncDynamoRepository(...) as repo:` usage


## 0.2.0

* Introduce `v2` `DynamoRepository`
  * Adds dependency to `aioboto3` to support async/await operations for all repository functions
  * Read operations return instances of `PartitionedContent` to support object versioning and easier updates
* Introduce `v2` `SyncDynamoRepository`
  * Wraps the asynchronous `DynamoRepository` implementation to provide a synchronous API

## 0.1.5 and below

The original release of this package that contained only V1 functionality.
Usage is documented in [v1.md](./docs/v1.md)
