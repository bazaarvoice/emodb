System of Record API
====================

The EmoDB System of Record (SoR) exposes a RESTful API.  You can access the API directly over HTTP or via a Java client
library.

Java Client Library
-------------------

Add the following to your Maven POM (set the `<emo-version>` to the current version of EmoDB):

```xml
<dependency>
    <groupId>com.bazaarvoice.emodb</groupId>
    <artifactId>emodb-sor-client</artifactId>
    <version>${emo-version}</version>
</dependency>
```

Minimal Java client without ZooKeeper or Dropwizard:

```java
String emodbHost = "localhost:8080";  // Adjust to point to the EmoDB server.
String apiKey = "xyz";  // Use the API key provided by EmoDB
MetricRegistry metricRegistry = new MetricRegistry(); // This is usually a singleton passed
DataStore dataStore = ServicePoolBuilder.create(DataStore.class)
                .withHostDiscoverySource(new DataStoreFixedHostDiscoverySource(emodbHost))
                .withServiceFactory(DataStoreClientFactory.forCluster("local_default", new MetricRegistry()).usingCredentials(apiKey))
                .withMetricRegistry(metricRegistry)
                .buildProxy(new ExponentialBackoffRetry(5, 50, 1000, TimeUnit.MILLISECONDS));

... use "dataStore" to access the System of Record ...

ServicePoolProxies.close(dataStore);
```

Robust Java client using ZooKeeper, [SOA] (https://github.com/bazaarvoice/ostrich) and [Dropwizard]
(http://www.dropwizard.io):

```java
@Override
protected void initialize(Configuration configuration, Environment environment) throws Exception {
    // YAML-friendly configuration objects.
    ZooKeeperConfiguration zooKeeperConfig = configuration.getZooKeeper();
    JerseyClientConfiguration jerseyClientConfig = configuration.getHttpClientConfiguration();
    DataStoreFixedHostDiscoverySource sorEndPointOverrides = configuration.getSorEndPointOverrides();

    // Connect to ZooKeeper.
    CuratorFramework curator = zooKeeperConfig.newManagedCurator(environment);
    curator.start();

    // Configure the Jersey HTTP client library.
    Client jerseyClient = new JerseyClientFactory(jerseyClientConfig).build(environment);

    String apiKey = "xyz";  // Use the API key provided by EmoDB

    // Connect to the DataStore using ZooKeeper (Ostrich) host discovery.
    ServiceFactory<DataStore> dataStoreFactory =
        DataStoreClientFactory.forClusterAndHttpClient("local_default", jerseyClient).usingCredentials(apiKey);
    DataStore dataStore = ServicePoolBuilder.create(DataStore.class)
            .withHostDiscoverySource(sorEndPointOverrides)
            .withHostDiscovery(new ZooKeeperHostDiscovery(curator, dataStoreFactory.getServiceName()))
            .withServiceFactory(dataStoreFactory)
            .buildProxy(new ExponentialBackoffRetry(5, 50, 1000, TimeUnit.MILLISECONDS));
    environment.addHealthCheck(new DataStoreHealthCheck(dataStore));
    environment.manage(new ManagedServicePoolProxy(dataStore));

    ... use "dataStore" to access the System of Record ...
}
```

Table Management
----------------

### Create Table

Create a new table.

HTTP:

    PUT /sor/1/_table/<table>?options=placement:<placement>&audit=<o-rison-map>

    <json>

Java:

    void createTable(String table, TableOptions options, Map<String, ?> template, Audit audit);

A table is a bucket containing JSON documents.  Creating a table is relatively cheap, and you can create as many tables
as you want.  In general, pick the granularity of your tables to match the granularity of your Hadoop jobs.  Each
Hadoop job will scan every document in a table.  For example, a good starting recommendation is to create one table per
type per client, eg. "review:testcustomer".

Tables may only be created from the one "system" data center.  Attempts to create a table from another data center will
be rejected.  All data centers must up and available when tables are created so the data store can ensure that table
metadata has replicated to all servers before the table can be used.

Request Body:

*   The body of the request must be a valid JSON object.  Every object in the table (even deleted objects) will
    contain the properties specified in this JSON object.  It's OK to pass an empty JSON object: `{}`.

Request Parameters:

*   `table` - required - The name of the table to create.  The table name must pass the check implemented by
    `com.bazaarvoice.emodb.sor.api.Names.isLegalTableName()`: lowercase, [a-z0-9-.:@_], 255 characters or less.
    Choose the granularity of your tables carefully since it determines the size of your EmoDB Hadoop-based map/reduce
    jobs.  Each map/reduce job takes as input one or more entire table.  The System of Record does not provide a
    mechanism for iterating over a smaller defined slice of a table.  In general, good practice is to create a table
    for every combination of data type and client, for example "review:testcustomer".  By convention, use colons `:`
    to separate fields in your table names.

*   `audit` - required - An [O-Rison-encoded] (http://mjtemplate.org/examples/rison.html) map containing
    information that can be used to trace changes to an object and debug applications that use EmoDB.
    If your client is written in Java, you may use the [rison] (https://github.com/bazaarvoice/rison)
    project to implement the O-Rison encoding.  For other languages, see http://mjtemplate.org/examples/rison.html.
    There are a few [pre-defined keys] (https://github.com/bazaarvoice/emodb/blob/master/common/api/src/main/java/com/bazaarvoice/emodb/sor/api/Audit.java)
    in Audit.java that clients are encouraged to use.  You may pass an empty map of audit information
    (encoded as an empty string), but this is discouraged.  After applying the O-Rison encoding, don't
    forget that, as with all url query parameters, the audit argument must be UTF-8 URI-encoded.  There
    are no intrinsic limits on the size of the audit map, but in practice it is limited by the maximum
    length of the URL.
*   `options` - required - An [O-Rison-encoded] (http://mjtemplate.org/examples/rison.html) map containing options
    that affect the internal storage of documents in the table.  For now, the only option is "placement" which
    must be either "ugc_global:ugc" (for user generated data) or "catalog_global:cat" (for client metadata and product
    catalog data).

Placement Values:

*   `ugc_global:ugc` - Use this placement for user generated data.

*   `catalog_global:cat` - Use this placement for client metadata and product catalog data.

*   `app_global:default` - Use this placement for arbitrary data from generally low traffic applications.

Example:

    $ curl -s -XPUT -H "Content-Type: application/json" \
        "http://localhost:8080/sor/1/_table/review:testcustomer?options=placement:'ugc_global:ugc'&audit=comment:'initial+provisioning',host:aws-tools-02" \
        --data-binary '{"type":"review","client":"TestCustomer"}' | jsonpp
    {
      "success": true
    }

Java Example:

    Map<String, Object> template = ImmutableMap.of("type", "review", "client", "testcustomer");
    TableOptions options = new TableOptionsBuilder().setPlacement("ugc_global:ugc").build();
    Audit audit = new AuditBuilder().setProgram("example-app").setLocalHost().build();
    dataStore.createTable("review:testcustomer", options, template, audit);


### Get Table:

Retrieve the JSON object template specified when a table was created.

HTTP:

    GET /sor/1/_table/<table>

Java:

    Map<String, Object> getTableTemplate(String table);

Request URL Parameters:

*   `debug=true` - optional - Sort the JSON to make it easier to read.

Example:

    $ curl -s "http://localhost:8080/sor/1/_table/review:testcustomer?debug=true" | jsonpp
    {
      "client": "TestCustomer",
      "type": "review"
    }

### Drop Table

Drop a table and all data it contains.

HTTP:

    DELETE /sor/1/_table/<table>?audit=<o-rison-map>

Java:

    // No Java client library support.

Tables may only be dropped from the one "system" data center.  Attempts to drop a table from another data center will
be rejected.  All data centers must up and available when tables are created so the data store can ensure that table
metadata has replicated to all servers before the table can be used.

Authorization:

See [API Keys](Security.md) documentation for information on authorizing this request.

Request Parameters:

*   `audit` - required - An [O-Rison-encoded] (http://mjtemplate.org/examples/rison.html) map containing
    information that can be used to trace changes to an object and debug applications that use EmoDB.
    If your client is written in Java, you may use the [rison] (https://github.com/bazaarvoice/rison)
    project to implement the O-Rison encoding.  For other languages, see http://mjtemplate.org/examples/rison.html.
    There are a few [pre-defined keys] (https://github.com/bazaarvoice/emodb/blob/master/common/api/src/main/java/com/bazaarvoice/emodb/common/api/Audit.java)
    in Audit.java that clients are encouraged to use.  You may pass an empty map of audit information
    (encoded as an empty string), but this is discouraged.  After applying the O-Rison encoding, don't
    forget that, as with all url query parameters, the audit argument must be UTF-8 URI-encoded.  There
    are no intrinsic limits on the size of the audit map, but in practice it is limited by the maximum
    length of the URL.

Example:

    $ curl -s -XDELETE --user drop:local \
        "http://localhost:8080/sor/1/_table/review:testcustomer?audit=comment:'termination',host:aws-tools-02" | jsonpp
    {
      "success": true
    }

### List Tables

List all tables in the System of Record.

HTTP:

    GET /sor/1/_table

Java:

    Iterator<Table> listTables(@Nullable String fromTableExclusive, long limit);

URL Parameters:

*   `limit=10` - optional - Maximum number of tables to return.  Defaults to 10.  Set to a very large value
    (eg. `Long.MAX_VALUE`) to stream all tables.
*   `from=<table>` - optional - Begin scanning at the first table that follows the specified table name.  No default.

Example:

    $ curl -s "http://localhost:8080/sor/1/_table
    [
      {
        "name": "review:testcustomer",
        "options": {
          "placement": "ugc_global:ugc"
        },
        "template": {
          "type": "review",
          "client": "TestCustomer"
        }
      }
    ]

### Size of Table

Get the approximate number of documents in a table.  Getting the exact count is expensive and discouraged
if an exact count is not required.  The `limit` parameter will guarantee an exact count up to the provided limit plus
an approximate count of remaining documents records if greater.  For example, consider the following scenarios:

Request limit attribute | Response | Wall clock time | Breakdown
----------------------: | -------: | --------------: | ---------
100                     | 350949   |     37 ms       | 100 exact + 350849 approximate remaining records
1000                    | 350825   |     97 ms       | 1000 exact + 349825 approximate remaining records
10000                   | 350353   |    679 ms       | 10000 exact + 340353 approximate remaining records
null                    | 349154   | 16,895 ms       | Exactly 349154 records

HTTP:

    GET /sor/1/_table/<table>/size?limit=<limit>

Java:

    long getTableApproximateSize(String table, int limit)

URL Parameters:

*   `limit=<limit>` - optional - Size up to which an exact record count is made and after which
                      the number of remaining records is approximated

Example:

    $ curl -s /sor/1/_table/review:testcustomer/size?limit=100
    350949

Document API
------------

### Create / Update / Delete

Create or replace a document in the system of record:

HTTP:

    PUT /sor/1/<table>/<key>?audit=<o-rison-map>
    Content-Type: application/json

    <json>

    void update(String table, String key, UUID changeId, Delta delta, Audit audit);

Create or modify or delete a document in the system of record:

HTTP:

    POST /sor/1/<table>/<key>?audit=<o-rison-map>
    Content-Type: application/x.json-delta

    <json-delta>

Delete a document in the system of record:

HTTP:

    DELETE /sor/1/<table>/<key>?audit=<o-rison-map>

All three operations use the same Java API.  The create/update/delete operation is selected by using a different
instance of Delta.  See [Deltas] (Deltas.md).

Java:

    void update(String table, String key, UUID changeId, Delta delta, Audit audit);

Request Body:

*   `PUT` - The body of the request must be a valid JSON object.

*   `POST` - The body of the request must be a valid JSON delta string in the format generated by
    `Delta.toString()`.  From Java, use the [Deltas] (https://github.com/bazaarvoice/emodb/blob/master/sor-api/src/main/java/com/bazaarvoice/emodb/sor/delta/Deltas.java)
    class to create instances of `Delta`.

Request HTTP Headers:

*   `Content-Type: application/json` - required for PUT
*   `Content-Type: application/x.json-delta` - required for POST

Request URL Parameters:

*   `audit` - required - An [O-Rison-encoded] (http://mjtemplate.org/examples/rison.html) map containing
    information that can be used to trace changes to an object and debug applications that use EmoDB.
    If your client is written in Java, you may use the [rison] (https://github.com/bazaarvoice/rison)
    project to implement the O-Rison encoding.  For other languages, see http://mjtemplate.org/examples/rison.html.
    There are a few [pre-defined keys] (https://github.com/bazaarvoice/emodb/blob/master/common/api/src/main/java/com/bazaarvoice/emodb/common/api/Audit.java)
    in Audit.java that clients are encouraged to use.  You may pass an empty map of audit information
    (encoded as an empty string), but this is discouraged.  After applying the O-Rison encoding, don't
    forget that, as with all url query parameters, the audit argument must be UTF-8 URI-encoded.  There
    are no intrinsic limits on the size of the audit map, but in practice it is limited by the maximum
    length of the URL.

*   `changeId` - optional - A time UUID corresponding to when the change should take effect, formatted
    as a string.  If `changeId` is not provided, a time UUID will be generated as of the time of the HTTP
    request.  Use the [`TimeUUIDs.newUUID()`] (https://github.com/bazaarvoice/emodb/tree/master/sor-api/src/main/java/com/bazaarvoice/emodb/sor/uuid/TimeUUIDs.java)
    method to generate time UUIDs.

*   `tags=re-etl&tags=alpine...` - optional - Tags to attach to this update. These tags describe and give a context to the changes you are making. Readers can then
     filter on the desired tags. Tags should be no longer than 8 characters and no more than 3 tags are allowed.  
     In practice, this is useful if the databus listeners would like to prioritize events from the same table based on various tags.

*   `debug=true` - optional - Include in the response the time UUID of the saved delta.

Examples:

    $ curl -s -XPUT -H "Content-Type: application/json" \
        "http://localhost:8080/sor/1/review:testcustomer/demo1?audit=comment:'initial+submission',host:aws-submit-09" \
        --data-binary '{"author":"Bob","title":"Best Ever!","rating":5}' | jsonpp
    {
      "success": true
    }

    $ curl -s -H "Content-Type: application/x.json-delta" \
        "http://localhost:8080/sor/1/review:testcustomer/demo1?audit=comment:'moderation+complete',host:aws-cms-01" \
        --data-binary '{..,"status":"APPROVED"}' | jsonpp
    {
      "success": true
    }

    $ curl -s -H "Content-Type: application/x.json-delta" \
        "http://localhost:8080/sor/1/review:testcustomer/demo1?tags=re-etl&tags=alpine&audit=comment:'moderation+complete',host:aws-cms-01" \
        --data-binary '{..,"status":"APPROVED"}' | jsonpp
    {
      "success": true
    }

    $ curl -s -XDELETE "http://localhost:8080/sor/1/review:testcustomer/demo1?audit=comment:'purge+client',host:aws-tools-02" \
        | jsonpp
    {
      "success": true
    }

Java Examples:

    // Create
    Map<String, Object> json = ImmutableMap.<String, Object>builder()
            .put("author", "Bob")
            .put("title", "Best Ever!")
            .put("rating", 5)
            .build();
    Audit audit = new AuditBuilder()
            .setProgram("example-app")
            .setComment("initial submission")
            .setLocalHost()
            .build();
    dataStore.update("review:testcustomer", "demo1", TimeUUIDs.newUUID(), Deltas.literal(json), audit);

    // Update
    Delta delta = Deltas.mapBuilder()
            .put("status", "APPROVED")
            .build();
    Audit audit = new AuditBuilder()
            .setProgram("example-app")
            .setComment("moderation complete")
            .setLocalHost()
            .build();
    dataStore.update("review:testcustomer", "demo1", TimeUUIDs.newUUID(), delta, audit);

    // Delete
    Audit audit = new AuditBuilder()
            .setProgram("example-app")
            .setComment("purge client")
            .setLocalHost()
            .build();
    dataStore.update("review:testcustomer", "demo1", TimeUUIDs.newUUID(), Deltas.delete(), audit);

### Updates and Event Tags:

EmoDB allows for tagging updates (including deletes) with up to 3 event tags of up to 8 chars each. Event tags are primarily used for adding tags to databus events
generated by the updates. Databus listeners may subscribe to these event tags.
Please note that these tags are only available on the corresponding databus events generated, and are not a part of the document.

### Get

Get a resolved entity from the system of record as JSON:

HTTP:

    GET /sor/1/<table>/<key>

Java:

    Map<String, Object> get(String table, String key);

URL Parameters:

*   `debug=true` - optional - Sort the JSON to make it easier to read.

Example:

    $ curl -s "http://localhost:8080/sor/1/review:testcustomer/demo1?debug=true" | jsonpp
    {
      "~deleted": false,
      "~firstUpdateAt": "2012-06-22T20:11:53.473Z",
      "~id": "demo1",
      "~lastUpdateAt": "2012-06-22T20:12:09.679Z",
      "~lastMutateAt": "2012-06-22T20:12:09.679Z",
      "~signature": "7db2ef78f7830acaaa53f242a5e5ffa1",
      "~table": "review:testcustomer",
      "~version": 2,
      "author": "Bob",
      "client": "TestCustomer",
      "rating": 5,
      "status": "APPROVED",
      "title": "Best Ever!",
      "type": "review"
    }

Java Example:

    Map<String, Object> json = dataStore.get("review:testcustomer", "demo1");

### Multi-Get

Get multiple records from the specified list of coordinates. Coordinate format is `<table>/<id>`.
Note that the records will *not* be returned in the order it was sent, and may not have a deterministic order. 

HTTP:

    GET /sor/1/_multiget?id=coordinate1&id=coordinate2

Java:

    Map<String, Object> get(List<Coordinate> coordinates, ReadConsistency consistency);

URL Parameters:

*   `coordinates` - List of coordinates, sent in the form of `id=<coordinate1>&id=<coordinate2>`.
*   `consistency` - optional - ReadConsistency.STRONG by default

Example:

    $ curl -s "http://localhost:8080/sor/1/_multiget?&APIKey=<ApiKey>&id=review:testcustomer/demo1&id=review:testcustomer" | jsonpp
    [{
      "~deleted": false,
      "~firstUpdateAt": "2012-06-22T20:11:53.473Z",
      "~id": "demo1",
      "~lastUpdateAt": "2012-06-22T20:12:09.679Z",
      "~lastMutateAt": "2012-06-22T20:12:09.679Z",
      "~signature": "7db2ef78f7830acaaa53f242a5e5ffa1",
      "~table": "review:testcustomer",
      "~version": 2,
      "author": "Bob",
      "client": "TestCustomer",
      "rating": 5,
      "status": "APPROVED",
      "title": "Best Ever!",
      "type": "review"
    },
    {
      "~deleted": false,
      "~firstUpdateAt": "2012-06-22T20:11:53.473Z",
      "~id": "demo1",
      "~lastUpdateAt": "2012-06-22T20:12:09.679Z",
      "~lastMutateAt": "2012-06-22T20:12:09.679Z",
      "~signature": "7db2ef78f7830acaaa53f242a5e5ffa1",
      "~table": "review:testcustomer",
      "~version": 2,
      "author": "Bob",
      "client": "TestCustomer",
      "rating": 5,
      "status": "APPROVED",
      "title": "Best Ever!",
      "type": "review"
    }]

Java Example:

    Iterator<Map<String, Object>> jsonIterator = dataStore.multiget(List<Coordinate> coordinates);

### Get Timeline

Get a historical view of an entity from the system of record in reverse chronological order, for debugging.  Note that
deltas will be compacted together over time, so do not rely on individual deltas always being available.  Audit records
are not compacted, but access to audit records may be slow.

    GET /sor/1/<table>/<key>/timeline

URL Parameters:

*   `data=false` - optional - Omit delta and compaction information.
*   `audit=true` - optional - Include audit information.
*   `start=<uuid>|<iso-8601-timestamp>` - optional - A time UUID or timestamp of the latest (if reversed) or earliest
                                                     (if not reversed) record to return.
*   `end=<uuid>|<iso-8601-timestamp>` - optional - A time UUID or timestamp of the earliest (if reversed) or latest
                                                   (if not reversed) record to return.
*   `reversed=false` - optional - Return history from oldest to newest.
*   `limit=10` - optional - Maximum number of records to return.  Defaults to 10.  Set to a very large value
        (eg. `Long.MAX_VALUE`) to stream all records.

Example:

    $ curl -s "http://localhost:8080/sor/1/review:testcustomer/demo1/timeline?audit=true"
    [
      {
        "timestamp": "2012-06-22T20:12:09.679+0000",
        "id": "8bf94df0-bca6-11e1-87ef-001c42000009",
        "delta": "{..,\"status\":\"APPROVED\"}",
        "audit": {
          "comment": "moderation complete",
          "host": "aws-cms-01",
          "~sha1": "4507332be7b42bd100a233be3847e5df99fbeb2d"
        }
      },
      {
        "timestamp": "2012-06-22T20:11:53.473+0000",
        "id": "82507710-bca6-11e1-87ef-001c42000009",
        "delta": "{\"author\":\"Bob\",\"rating\":5,\"title\":\"Best Ever!\"}",
        "audit": {
          "comment": "initial submission",
          "host": "aws-submit-09",
          "~sha1": "33aef50cae4e44cc7be803054335bafdd375644b"
        }
      }
    ]

### Scan

Return the first `N` non-deleted entities in a table, sorted arbitrarily.  Or, if a `from` parameter is specified,
return the next `N` non-deleted entities that follow the specified document key (exclusive).  This can be used to
iterate over all documents in a particular table, `N` entities at a time.

While the sort order is unspecified, it is deterministic such that if you repeatedly scan the system of record,
setting `from` in each scan operation to the value of `~id` from the last record from the last scan, you'll iterate
over all entities in a table without omissions or duplicates, subject to concurrent writers adding and deleting
documents.

HTTP:

    GET /sor/1/<table>

Java:

    Iterator<Map<String, Object>> scan(String table, @Nullable String fromKeyExclusive, long limit, ReadConsistency consistency);

URL Parameters:

*   `limit=10` - optional - Maximum number of entities to return.  Defaults to 10.  Set to a very large value
    (eg. `Long.MAX_VALUE`) to stream all records.
*   `from=<key>` - optional - Begin scanning at the first key that follows the specified key.  No default.

Example:

    $ curl -s "http://localhost:8080/sor/1/review:testcustomer?from=demo1&limit=20"
    [
      {
        "author": "Tom",
        "title": "Could be better.",
        "rating": 3,
        "status": "APPROVED",
        "type": "review",
        "client": "TestCustomer",
        "~id": "demo2",
        "~table": "review:testcustomer",
        "~version": 1,
        "~signature": "a5a611fbc9399a27c6098f460ddd3402",
        "~deleted": false,
        "~firstUpdateAt": "2012-07-30T21:33:28.908Z",
        "~lastUpdateAt": "2012-07-30T21:33:33.194Z"
        "~lastMutateAt": "2012-07-30T21:33:33.194Z"
      }
    ]

In combination with the [Databus API] (DatabusApi.md), the scan operation can be used to seed and update an external
replica of a table in the system of record:

1.  Create a databus subscription for the table.

1.  Scan all rows in the table, copying the data to the external replica.

1.  Process databus events starting from when the scan was initiated.

Performance note: there is a substantial performance overhead to performing a scan.  It was designed to support
occasional bulk extract of all data in a table.  For efficient search across entities it's usually a better idea to
query a secondary index such as Polloi.

A scan may fail if the client loses its connection to the EmoDB server before all results have been returned.  To
work around this issue and automatically re-create the connection to the server when it gets lost, use the methods in
the `com.bazaarvoice.emodb.sor.client.DataStoreStreaming` class.  For example:

```java
// Stream all rows from an EmoDB table and process them one-by-one.
for (Map<String, Object> row : DataStoreStreaming.scan(dataStore, table, ReadConsistency.STRONG)) {
    // process row
}

// Stream all rows from an EmoDB table and process them in batches.
int batchSize = 100;
for (List<Map<String, Object>> batch : Iterables.partition(
        DataStoreStreaming.scan(dataStore, table, ReadConsistency.STRONG), batchSize)) {
    // process batch of rows
}
```

If you're writing your client in a language other than Java, the chances are your HTTP client library doesn't stream
JavaScript results in an efficient way out-of-the-box.  In that case, set the scan limit to a number that won't blow
out memory (eg. 1000) and perform repeated scans, setting the "from key" to the ID of the last row in each batch.

### Parallel Scan

To scan the system of record in parallel, call the `getSplits()` method to get a list of split identifiers.  Then, in
parallel, scan the data in each split by calling the `getSplit()` method repeatedly.

HTTP:

    GET /sor/1/_split/<table>

    GET /sor/1/_split/<table>/<split>

Java:

    Collection<String> getSplits(String table, int desiredRecordsPerSplit);

    Iterator<Map<String, Object>> getSplit(String table, String split, @Nullable String fromKeyExclusive, long limit, ReadConsistency consistency);

URL Parameters for `getSplits()`:

*   `size=10000` - optional - Desired number of entities per split.  Defaults to 10,000.

URL Parameters for `getSplit()`:

*   `limit=10` - optional - Maximum number of entities to return.  Defaults to 10.  Set to a very large value
    (eg. `Long.MAX_VALUE`) to stream all records in the split.
*   `from=<key>` - optional - Begin scanning at the first key that follows the specified key.  No default.

A split fetch may fail if the client loses its connection to the EmoDB server before all results have been returned.
To work around this issue and automatically re-create the connection to the server when it gets lost, use the methods
in the `com.bazaarvoice.emodb.sor.client.DataStoreStreaming` class.  See the following example:

```java
final DataStore dataStore = ...;
ExecutorService executor = ...;
final String table = "review:testcustomer";

// Split up the job of fetching all the data in a table into large tasks by fetching a list of
// "split" identifiers where each split contains approximately 10,000 documents, more or less.
Collection<String> splits = dataStore.getSplits(table, 10000);

// Execute each large task.  Typically this will be spread across multiple machines.  This
// example uses multiple threads and an ExecutorService for the purpose of illustration.
for (final String split : splits) {
    executor.submit(new Runnable() {
        @Override
        public void run() {
            // Stream all rows from an EmoDB table split and process them one-by-one.
            for (Map<String, Object> row : DataStoreStreaming.getSplit(dataStore, table, split, ReadConsistency.STRONG)) {
                // process row
            }
        }
    });
}
```

If you're writing your client in a language other than Java, the chances are your HTTP client library doesn't stream
JavaScript results in an efficient way out-of-the-box.  In that case, set the getSplit limit to a number that won't blow
out memory (eg. 1000) and call getSplit repeatedly, setting the "from key" to the ID of the last row in each batch.

### Compact

Force compaction of a document in the system of record, for debugging:

    POST /sor/1/<table>/<key>/compact

URL Parameters:

*   `ttl=<seconds>` - optional - Assume updates older than the specified number of seconds are fully consistent
    across all data centers.
    

Facades
----------------

An EmoDB facade is a special table that has the same name as an existing table, but in a different placement. For example, if a table "review:eu-customer" exists in ugc_eu keyspace, then Emodb allows for creating a `facade` in ugc_us keyspace with the same table name. Now, when emodb gets a request for this table in US datacenter, it will be directed to the facade of the table available in ugc_us keyspace.  Note that Emodb does not replicate the data for these facades. It is up to the consumer to populate their facades with whatever data they deem fit. The current user for this feature is EmoDB-shovel that replicates and anonymizes data from EU keyspace into US keyspace. Facades are read-only, and writes using `_table` api will fail.

### Create Facade

Create a new facade.

HTTP:

    PUT /sor/1/_facade/<table>?options=placement:<placement>&audit=<o-rison-map>

Java:

    void createTable(String table, FacadeOptions options, Audit audit);  

Example:

    $ curl -s -XPUT -H "Content-Type: application/json" \
        "http://localhost:8080/sor/1/_facade/review:testcustomer?options=placement:'ugc_global:ugc'&audit=comment:'initial+provisioning',host:aws-tools-02" | jsonpp
    {
      "success": true
    }

### Get

Getting a resolved document from a facade works the exact same way as it does from a table.

### Update documents in a facade

Create/update/delete documents in a facade.

HTTP:

    POST /sor/1/_facade/<table>/<key>?audit=<o-rison-map>
    Content-Type: application/x.json-delta

    <json-delta>

Java:

    void updateAllForFacade(Iterable<Update> updates);

Example:

    $ curl -s -XPUT --user "facade:local" -H "Content-Type: application/json" \
        "http://localhost:8080/sor/1/_facade/review:testcustomer/demo1?audit=comment:'initial+submission',host:aws-submit-09" \
        --data-binary '{"author":"Bob","~sourceVersion":100,"title":"Anonymized Best Ever!","rating":5}'
    {
      "success": true
    }

Authorization:

See [API Keys](Security.md) documentation for information on authorizing this request.

### Drop a facade

Drop and delete all data in a facade.

HTTP:

    DELETE /sor/1/_facade/<table>/<key>?placement=<placement>&audit=<o-rison-map>

Java:

    // No support for dropping a facade

Authorization:

See [API Keys](Security.md) documentation for information on authorizing this request.