Release Notes
=============

## 0.76

Backward-incompatible change to the `DataStore.getTimeline()` REST API and Java API.  The previous version, designed
for debugging use only, was inherently unscalable and could cause EmoDB to fail with OutOfMemory errors when objects
in the System of Record had too many very large uncompacted deltas.  The new version resolves this by using a streaming
approach that never loads all deltas into memory at once, although working with objects with many uncompacted deltas
will be very slow and is *strongly* discouraged.

The `DataStore.getTimeline()` method now takes optional `start`, `end`, `reversed` and `limit` parameters to allow
retrieving subsets of record history and/or paging through history using multiple requests.  The default `limit` is 10.

## 0.73

Added a new utility class `com.bazaarvoice.emodb.sor.api.Coordinate` that clients may use to simplify their code.

## 0.72

Extended `DataStoreStreaming` to wrap the DataStore `updateAll` method to reduce the amount of retry work done when
the connection to the server is lost unexpectedly so as to support essentially unlimited #s of records streamed in a
single call.

## 0.71

Added the following HTTP client library helper classes that wrap the DataStore and BlobStore streaming read methods
`listTables`, `scan`, `getSplit` and `scanMetadata` to automatically detect and recover from interruptions in the
streaming due to unexpected server connection loss so as to support essentially unlimited #s of records streamed in
a single call.

    com.bazaarvoice.emodb.sor.client.DataStoreStreaming
    com.bazaarvoice.emodb.blob.client.BlobStoreStreaming

## 0.70

Upgraded EmoDB client and server to Dropwizard 0.6.1, Jackson 2.1.x and the open source Ostrich 1.5.2.

The REST API is unchanged.  You may continue to use pre-0.70 clients with 0.70+ servers and vice-versa. However,
upgrading client code to the 0.70+ client libraries will likely require upgrading your clients to Dropwizard 0.6.1
and the open source Ostrich.  This will involve code changes since Dropwizard and Ostrich both have significant API
changes.

## 0.69

The System of Record `Conditions.in()` clause will now perform in O(1) instead of O(n). In combination with the new
method `Conditions.intrinsic(String, Condition)`, it should be reasonably efficient to subscribe to a large set of
enumerated SoR tables. Databus subscriptions that previously used `or(intrinsic(~table:..),intrinsic(~table:..),..)`
(like Polloi) will be converted automatically to the more efficient O(1) `intrisic(~table:..,..,..,..)` form when
possible--clients should not need to make any changes.

## 0.67

If using the EmoDB Java HTTP client libraries, the `createTable()` and `dropTable()` REST APIs on `DataStore`
and `BlobStore` may now be called from any AWS region, not just `us-east-1`.

Previously, these specific APIs would fail with an exception in any AWS region other than `us-east-1`. Now the
servers return an HTTP 301 redirect instead, redirecting the client to the `us-east-1` ELB. The EmoDB Java clients
will follow the redirect automatically but, because `createTable()` and `dropTable()` use the HTTP `PUT` and `DELETE`
methods, most other HTTP clients will not. Those clients will need Emo-specific software updates.

## 0.66

Added `Date BlobMetadata.getTimestamp()` to the Blob Store API. It returns the date/time when a blob was uploaded.
