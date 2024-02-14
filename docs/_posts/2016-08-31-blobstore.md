---
layout: post
title: "Blob Store"
date: 2016-05-20
excerpt: "Setting up EmoDB"
tags: [Blob Store, EmoDB]
type: [blog]
---

BlobStore API
=============

Use the EmoDB BlobStore to store large binary objects.  In contrast to the System of Record which expects JSON, allows
updates and integrates with the Databus, the BlobStore makes no assumptions about its content, works best with
immutable blobs, and does not trigger Databus events.  Like the System of Record, it is designed to work well in
cross-data center environments.

The BlobStore exposes a RESTful API.  You can access the API directly over HTTP or via a Java client library.

BlobStore IDs must be ASCII alphanumeric with a few punctuation characters such as '-', '_'.  It is often convenient to
pick IDs by generating random 128-bit random UUIDs (`java.util.UUID.randomUUID().toString()`).

All data for a single blob must fit (on disk) on a single machine in the Cassandra cluster.

Java Client Library
-------------------

Add the following to your Maven POM (set the `<emo-version>` to the current version of EmoDB):

```xml
<dependency>
    <groupId>com.bazaarvoice.emodb</groupId>
    <artifactId>emodb-blob-client</artifactId>
    <version>${emo-version}</version>
</dependency>
```

Minimal Java client without ZooKeeper or Dropwizard:

```java
String emodbHost = "localhost:8080";  // Adjust to point to the EmoDB server.
String apiKey = "xyz";  // Use the API key provided by EmoDB
MetricRegistry metricRegistry = new MetricRegistry(); // This is usually a singleton passed

Client jerseyClient = new JerseyClientBuilder(metricRegistry)
        .using(Executors.newSingleThreadExecutor())
        .using(Jackson.newObjectMapper())
        .build("BlobClient");
ServiceFactory<BlobStore> blobStoreFactory =
        BlobStoreClientFactory.forClusterAndHttpClient("local_default", jerseyClient).usingCredentials(apiKey);

BlobStore blobStore = ServicePoolBuilder.create(BlobStore.class)
        .withHostDiscoverySource(new BlobStoreFixedHostDiscoverySource(emodbHost))
        .withServiceFactory(blobStoreFactory)
        .withMetricRegistry(metricRegistry)
        .buildProxy(new ExponentialBackoffRetry(5, 50, 1000, TimeUnit.MILLISECONDS));

// ... use "blobStore" to access the BlobStore ...

ServicePoolProxies.close(blobStore);
```

Robust Java client using ZooKeeper, [SOA](https://github.com/bazaarvoice/ostrich) and [Dropwizard](http://dropwizard.codahale.com/):

```java
@Override
protected void initialize(Configuration configuration, Environment environment) throws Exception {
    // YAML-friendly configuration objects.
    ZooKeeperConfiguration zooKeeperConfig = configuration.getZooKeeperConfiguration();
    JerseyClientConfiguration jerseyClientConfig = configuration.getHttpClientConfiguration();
    BlobStoreFixedHostDiscoverySource blobStoreEndPointOverrides = configuration.getBlobStoreEndPointOverrides();

    // Connect to ZooKeeper.
    CuratorFramework curator = zooKeeperConfig.newManagedCurator(environment);
    curator.start();

    // Configure the Jersey HTTP client library.
    Client jerseyClient = new JerseyClientFactory(jerseyClientConfig).build(environment);

    String apiKey = "xyz";  // Use the API key provided by EmoDB

    // Connect to the BlobStore using ZooKeeper (Ostrich) host discovery.
    ServiceFactory<BlobStore> blobStoreFactory =
        BlobStoreClientFactory.forClusterAndHttpClient("local_default", jerseyClient).usingCredentials(apiKey);
    BlobStore blobStore = ServicePoolBuilder.create(BlobStore.class)
            .withHostDiscoverySource(blobStoreEndPointOverrides)
            .withHostDiscovery(new ZooKeeperHostDiscovery(curator, blobStoreFactory.getServiceName()))
            .withServiceFactory(blobStoreFactory)
            .buildProxy(new ExponentialBackoffRetry(5, 50, 1000, TimeUnit.MILLISECONDS));
    environment.manage(new ManagedServicePoolProxy(blobStore));

    // ... use "blobStore" to access the BlobStore ...
}
```

REST calls
----------

As with all parts of EmoDB the REST API requires [API keys]({{ site.baseurl }}/security) and the Java client includes these
in all requests automatically.  For clarity the API key header is not included each REST example below, but in a
properly secured system you would need to add it to each request.

Quick Start
-----------

### Installation

#### Using binaries:

1. Download the [EmoDB binaries](https://github.com/bazaarvoice/emodb/releases).

2. Run the EmoDB server locally. This will start zookeeper and cassandra locally.

        $ bin/start-local.sh
        ...
        INFO  [2012-05-14 19:12:19,802] org.eclipse.jetty.server.AbstractConnector: Started InstrumentedBlockingChannelConnector@0.0.0.0:8080
        INFO  [2012-05-14 19:12:19,805] org.eclipse.jetty.server.AbstractConnector: Started SocketConnector@0.0.0.0:8081
        # Use Ctrl-C to kill the server when you are done.

3.  Check that the server responds to requests (from another window):

        $ curl -s "http://localhost:8081/ping"
        pong

        $ curl -s "http://localhost:8081/healthcheck"
        {"deadlocks":{"healthy":true},"media_global-cassandra":{"healthy":true,"message":"127.0.0.1(127.0.0.1):9160 124us"},...}

4.  To erase the EmoDB data, simply delete the data folder:

        $ rm -rf bin/data/
        $ bin/start-local.sh
{:.workflow}

#### Using source code:

1. Download the [EmoDB source code](https://github.com/bazaarvoice/emodb):

        $ git clone git@github.com:bazaarvoice/emodb.git emodb

2.  Build the source and run the tests:

        $ cd emodb
        $ mvn clean install

3.  Run the EmoDB server locally:

        $ cd web-local
        $ ./start.sh
        ...
        INFO  [2012-05-14 19:12:19,802] org.eclipse.jetty.server.AbstractConnector: Started InstrumentedBlockingChannelConnector@0.0.0.0:8080
        INFO  [2012-05-14 19:12:19,805] org.eclipse.jetty.server.AbstractConnector: Started SocketConnector@0.0.0.0:8081
        # Use Ctrl-C to kill the server when you are done.

4.  Check that the server responds to requests (from another window):

        $ curl -s "http://localhost:8081/ping"
        pong

        $ curl -s "http://localhost:8081/healthcheck"
        {"deadlocks":{"healthy":true},"media_global-cassandra":{"healthy":true,"message":"127.0.0.1(127.0.0.1):9160 124us"},...}

5.  To erase the EmoDB data and restart with a clean slate:

        $ cd web-local
        $ ./start-clean.sh
{:.workflow}

### Examples

The following examples assume you have [jq](https://stedolan.github.io/jq/) or have aliased `alias jq='python -mjson.tool'`.
It is optional--`jq .` just formats the JSON responses to make them easier to read.

1.  Create a table in the BlobStore.  Specify "table attributes" with properties that will be returned with every
    object in the table:

        $ curl -s -XPUT -H "Content-Type: application/json" \
            "http://localhost:8080/blob/1/_table/photo:testcustomer?options=placement:'media_global:ugc'&audit=comment:'initial+provisioning',host:aws-tools-02" \
            --data-binary '{"type":"photo","client":"TestCustomer"}' | jq .
        {
          "success": true
        }

2.  Verify that the table was created as expected.  The result should be the table attributes.

        $ curl -s "http://localhost:8080/blob/1/_table/photo:testcustomer" | jq .
        {
          "client": "TestCustomer",
          "type": "photo"
        }

3.  Store an image in the BlobStore.  Custom attributes can be specified with the "X-BVA-*" HTTP headers:

        $ curl -s -XPUT -H "X-BVA-contentType: image/png" -H "X-BVA-width: 200" -H "X-BVA-height: 200" \
            "http://localhost:8080/blob/1/photo:testcustomer/logo.png" \
            --upload-file docs/bazaarvoice-logo-green.png | jq .                  
        {
          "success": true
        }

4.  Check that the image exists and view its attributes.  Note that Content-MD5 contains the base-64 MD5 hash of the
    image and ETag contains the base-16 SHA1 hash of the image:

        $ curl -s -I "http://localhost:8080/blob/1/photo:testcustomer/logo.png"
        HTTP/1.1 200 OK
        Date: Thu, 02 Aug 2012 03:08:45 GMT
        Last-Modified: Thu, 02 Aug 2012 03:08:10 GMT
        Content-Length: 4758
        Content-MD5: nvnjGvWboB02ncDdXWfRAA==
        ETag: "ad76f54fda208edf6693927b6b427bd4a3687f68"
        Content-Type: image/png
        X-BV-Length: 4758
        X-BVA-client: TestCustomer
        X-BVA-contentType: image/png
        X-BVA-height: 200
        X-BVA-type: photo
        X-BVA-width: 200

5.  Download the image and view it in a web browser:

        $ open "http://localhost:8080/blob/1/photo:testcustomer/logo.png"

6.  Scan through the metadata of the first ten images in the table:

        $ curl -s "http://localhost:8080/blob/1/photo:testcustomer" | jq .
        [
            {
                "attributes": {
                    "client": "TestCustomer",
                    "contentType": "image/png",
                    "height": "200",
                    "type": "photo",
                    "width": "200"
                },
                "id": "logo.png",
                "length": 4758,
                "md5": "9ef9e31af59ba01d369dc0dd5d67d100",
                "sha1": "ad76f54fda208edf6693927b6b427bd4a3687f68"
            }
        ]
{:.workflow}

Table Management
----------------

### Create Table

Create a new table.

HTTP:

    PUT /blob/1/_table/<table>?options=placement:<placement>&audit=<o-rison-map>

    <json>

Java:

```java
void createTable(String table, TableOptions options, Map<String, String> attributes, Audit audit);
```

A table is a bucket containing blobs.  Creating a table is relatively cheap, and you can create as many tables as you
want.  In general, pick the granularity of your tables to match the granularity of your Hadoop jobs.  Each Hadoop job
will scan every blob in a table.  For example, a good starting recommendation is to create one table per type per
client, eg. "photo:testcustomer".

Tables may only be created from the one "system" data center.  Attempts to create a table from another data center will
be rejected.  All data centers must up and available when tables are created so the data store can ensure that table
metadata has replicated to all servers before the table can be used.

Request Body:

*   The body of the request must be a valid JSON object where all keys and values are strings.  Every blob in the table
    will include the attributes specified in this JSON object.  It's OK to pass an empty JSON object: `{}`.

Request Parameters:

*   `table` - required - The name of the table to create.  The table name must pass the check implemented by
    `com.bazaarvoice.emodb.blob.api.Names.isLegalTableName()`: lowercase, [a-z0-9-.:@_], 255 characters or less.
    Choose the granularity of your tables carefully since it determines the size of your EmoDB Hadoop-based map/reduce
    jobs.  Each map/reduce job takes as input one or more entire table.  The BlobStore does not provide a
    mechanism for iterating over a smaller defined slice of a table.  In general, good practice is to create a table
    for every combination of data type and client, for example "photo:testcustomer".  By convention, use colons `:`
    to separate fields in your table names.
*   `audit` - required - An [O-Rison-encoded](https://github.com/Nanonid/rison) map containing
    information that can be used to trace changes to an object and debug applications that use EmoDB.
    If your client is written in Java, you may use the [rison](https://github.com/bazaarvoice/rison)
    project to implement the O-Rison encoding.  For other languages, see [here](https://github.com/Nanonid/rison).
    There are a few [pre-defined keys](https://github.com/bazaarvoice/emodb/blob/main/common/api/src/main/java/com/bazaarvoice/emodb/common/api/Audit.java)
    in Audit.java that clients are encouraged to use.  You may pass an empty map of audit information
    (encoded as an empty string), but this is discouraged.  After applying the O-Rison encoding, don't
    forget that, as with all url query parameters, the audit argument must be UTF-8 URI-encoded.  There
    are no intrinsic limits on the size of the audit map, but in practice it is limited by the maximum
    length of the URL.
*   `options` - required - An [O-Rison-encoded](https://github.com/Nanonid/rison) map containing options
    that affect the internal storage of documents in the table.  For now, the only option is "placement" which
    must be a valid blob placement.

Example:

    $ curl -s -XPUT -H "Content-Type: application/json" \
        "http://localhost:8080/blob/1/_table/photo:testcustomer?options=placement:'media_global:ugc'&audit=comment:'initial+provisioning',host:aws-tools-02" \
        --data-binary '{"type":"photo","client":"TestCustomer"}' | jq .
    {
      "success": true
    }

Java Example:

```java
Map<String, String> template = ImmutableMap.of("type", "photo", "client", "testcustomer");
TableOptions options = new TableOptionsBuilder().setPlacement("media_global:ugc").build();
Audit audit = new AuditBuilder().setProgram("example-app").setLocalHost().build();
dataStore.createTable("photo:testcustomer", options, template, audit);
```

### Get Table:

Retrieve the JSON attributes specified when a table was created.

HTTP:

    GET /blob/1/_table/<table>

Java:

```java
Map<String, String> getTableAttributes(String table);
```

Example:

    $ curl -s "http://localhost:8080/blob/1/_table/photo:testcustomer" | jq .
    {
      "client": "TestCustomer",
      "type": "photo"
    }

### Drop Table

Drop a table and all data it contains.

HTTP:

    DELETE /blob/1/_table/<table>?audit=<o-rison-map>

Java:

    // No Java client library support.

Tables may only be dropped from the one "system" data center.  Attempts to drop a table from another data center will
be rejected.  All data centers must up and available when tables are created so the data store can ensure that table
metadata has replicated to all servers before the table can be used.

Request Parameters:

*   `audit` - required - An [O-Rison-encoded](https://github.com/Nanonid/rison) map containing
    information that can be used to trace changes to an object and debug applications that use EmoDB.
    If your client is written in Java, you may use the [rison](https://github.com/bazaarvoice/rison)
    project to implement the O-Rison encoding.  For other languages, see [here](https://github.com/Nanonid/rison).
    There are a few [pre-defined keys](https://github.com/bazaarvoice/emodb/blob/main/common/api/src/main/java/com/bazaarvoice/emodb/common/api/Audit.java)
    in Audit.java that clients are encouraged to use.  You may pass an empty map of audit information
    (encoded as an empty string), but this is discouraged.  After applying the O-Rison encoding, don't
    forget that, as with all url query parameters, the audit argument must be UTF-8 URI-encoded.  There
    are no intrinsic limits on the size of the audit map, but in practice it is limited by the maximum
    length of the URL.

Example:

    $ curl -s -XDELETE --user drop:local \
        "http://localhost:8080/blob/1/_table/photo:testcustomer?audit=comment:'termination',host:aws-tools-02" | jq .
    {
      "success": true
    }

### List Tables

List all tables in the Blob Store.

HTTP:

    GET /blob/1/_table

Java:

```java
Iterator<Table> listTables(@Nullable String fromTableExclusive, long limit);
```

URL Parameters:

*   `limit=10` - optional - Maximum number of tables to return.  Defaults to 10.  Set to a very large value
    (eg. `Long.MAX_VALUE`) to stream all tables.
*   `from=<table>` - optional - Begin scanning at the first table that follows the specified table name.  No default.

Example:

    $ curl -s "http://localhost:8080/blob/1/_table
    [
      {
        "name": "photo:testcustomer",
        "options": {
          "placement": "media_global:ugc"
        },
        "attributes": {
          "type": "photo",
          "client": "TestCustomer"
        }
      }
    ]

BlobStore API
-------------

### Put

Store a binary object.  The object can have a set of associated string attributes.

Because of eventual consistency and cross-data center replication, the BlobStore does not check to see whether the blob
already exists--it just overwrites any existing blob with the same ID.  The last writer wins.  It is up to the
application to avoid write conflicts.  This can be convenient because, if an application fails while uploading a blob,
it can simply retry the upload operation and safely overwrite the previous partially-uploaded contents.

HTTP:

    PUT /blob/1/<table>/<blobId>

    <data>

Java:

```java
void put(String table, String blobId, InputStream in, Map<String, String> attributes);
```

### Get

Retrieve a binary object.

HTTP:

    GET /blob/1/<table>/<blobId>

Java:

```java
Blob get(String table, String blobId) throws BlobNotFoundException;
```

### Head

Retrieve metadata about a binary object.

HTTP:

    HEAD /blob/1/<table>/<blobId>

Java:

```java
BlobMetadata getMetadata(String table, String blobId) throws BlobNotFoundException;
```

### Delete

Delete a binary object.

HTTP:

    DELETE /blob/1/<table>/<blobId>

Java:

```java
void delete(String table, String blobId);
```

### Scan

Return metadata for the first `N` objects in a table, sorted arbitrarily.  Or, if a `from` parameter is specified,
return the next `N` non-deleted objects that follow the specified blob ID (exclusive).  This can be used to iterate
over all objects in a particular table, `N` at a time.

While the sort order is unspecified, it is deterministic such that if you repeatedly scan the blob store, setting
`from` in each scan operation to the value of `~id` from the last record from the last scan, you'll iterate over all
entities in a table without omissions or duplicates, subject to concurrent writers adding and deleting objects.

HTTP:

    GET /blob/1/<table>

Java:

```java
Iterator<BlobMetadata> scanMetadata(String table, @Nullable String fromBlobIdExclusive, long limit);
```

URL Parameters:

*   `limit=10` - optional - Maximum number of objects to return.  Defaults to 10.  Set to a very large value
    (eg. `Long.MAX_VALUE`) to stream all records.
*   `from=<blobId>` - optional - Begin scanning at the first blob that follows the specified ID.  No default.

Example:

    $ curl -s "http://localhost:8080/blob/1/photo:testcustomer?limit=20"
    [
      {
        "id": "logo.png",
        "timestamp": "2013-02-21T20:46:37.000+0000",
        "length": 4758,
        "md5": "9ef9e31af59ba01d369dc0dd5d67d100",
        "sha1": "ad76f54fda208edf6693927b6b427bd4a3687f68",
        "attributes": {
          "client": "TestCustomer",
          "contentType": "image/png",
          "height": "200",
          "type": "photo",
          "width": "200"
        }
      }
    ]

Performance note: there is a substantial performance overhead to performing a scan.  It was designed to support
occasional bulk extract of all data in a table.

A scan may fail if the client loses its connection to the EmoDB server before all results have been returned.  To
work around this issue and automatically re-create the connection to the server when it gets lost, use the methods in
the `com.bazaarvoice.emodb.blob.client.BlobStoreStreaming` class.  For example:

```java
// Stream all blob metadata objects from an EmoDB table and process them one-by-one.
for (BlobMetadata row : BlobStoreStreaming.scan(blobStore, table)) {
    // process row
}

// OR

// Stream all rows from an EmoDB table and process them in batches.
int batchSize = 100;
for (List<BlobMetadata> batch : Iterables.partition(BlobStoreStreaming.scan(blobStore, table), batchSize)) {
    // process batch of rows
}
```
