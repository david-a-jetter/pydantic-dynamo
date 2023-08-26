# Versions

## 0.4.1 + 0.4.2
* Loosen the dependency ranges by requiring only `aioboto3`

## 0.4.0
* Add `AIter`, `list_contents_from_batches`, and `list_items_from_batches` convenience functions
in the `pydantic_dynamo.v2.utils` module to reduce consumer boilerplate

## 0.3.2
* Fix bug where v2 repository would raise KeyError when reading items saved without 
an `_object_version` attribute. Expected behavior is to use default version of `1`.

## 0.3.1
* Add ability to send `AsyncIterable` input_content to `WriteOnceRepository.write` method

## 0.3.0
* Add `WriteOnceRepository` implementation
  * A utility that minimizes, but does not guarantee to prevent, writing duplicate data

## 0.2.2
* Add support for strongly consistent reads by setting the `consistent_reads` optional kwarg in repository constructor
* Add missing generic argument `ObjT` type annotations to repositories


## 0.2.1
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
