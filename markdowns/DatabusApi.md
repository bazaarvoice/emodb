Databus API
============

The EmoDB Databus allows applications to get notified of updates as they are made in the System of Record.

A client application must create a persistent subscription to a set of tables in the System of Record.  The System of
Record will start "DVRing" updates for that subscription.  The application consumes the events by polling, retrieving
outstanding events, processing them, and acknowledging them.

The Databus is designed to support multiple concurrent writers updating the System of Record and multiple concurrent
readers consuming and processing events for a particular subscription.

The Databus guarantees that, for any single subscription, all updates will eventually trigger a Databus event.  It does
*not* make any guarantees about event order or duplicate events.  There are many failure scenarios where the Databus
will deliver events out-of-order or in duplicate.  It is up to the consumer to deal with this.  To help, the System of
Record provides two "intrinsic" properties on every object:

*   `~version` - A data center-specific monotonically increasing version number for every object.  Every update
    increases the version number by one.  A Databus reader can compare version numbers to determine if events arrive
    out-of-order.  Version numbers should not be compared between data centers--due to weak consistency, objects in data
    center A and B can both have the same version number but represent different content.

*   `~signature` - A 128-bit hash of a sequence of updates (technically, a hash of the sequence of time UUIDs associated
    with the updates).  Every update results in a new object signature.  There is no intrinsic ordering of signature
    hash values--they can't be used to detect out-of-order events.  They can be compared across data centers.  If two
    versions of the same object have the same signature hash, an application can be confident they represent the same
    data.

The Databus exposes a RESTful API.  You can access the API directly over HTTP or via a Java client library.

Java Client Library
-------------------

Add the following to your Maven POM (set the `<emo-version>` to the current version of EmoDB):

```xml
<dependency>
    <groupId>com.bazaarvoice.emodb</groupId>
    <artifactId>emodb-databus-client</artifactId>
    <version>${emo-version}</version>
</dependency>
```

Minimal Java client without ZooKeeper or Dropwizard:

```java
String emodbHost = "localhost:8080";  // Adjust to point to the EmoDB server.
String apiKey = "xyz";  // Use the API key provided by EmoDB
MetricRegistry metricRegistry = new MetricRegistry(); // This is usually a singleton passed
Databus databus = ServicePoolBuilder.create(Databus.class)
                .withHostDiscoverySource(new DatabusFixedHostDiscoverySource(emodbHost))
                .withServiceFactory(DatabusClientFactory.forCluster("local_default", metricRegistry).usingCredentials(apiKey))
                .withMetricRegistry(metricRegistry)
                .buildProxy(new ExponentialBackoffRetry(5, 50, 1000, TimeUnit.MILLISECONDS));

... use "databus" to access the Databus ...

ServicePoolProxies.close(databus);
```

Robust Java client using ZooKeeper, [SOA] (https://github.com/bazaarvoice/ostrich) and [Dropwizard]
(http://dropwizard.codahale.com/):

```java
@Override
protected void initialize(Configuration configuration, Environment environment) throws Exception {
    // YAML-friendly configuration objects.
    ZooKeeperConfiguration zooKeeperConfig = configuration.getZooKeeper();
    JerseyClientConfiguration jerseyClientConfig = configuration.getHttpClientConfiguration();
    DatabusFixedHostDiscoverySource databusEndPointOverrides = configuration.getDatabusEndPointOverrides();

    // Connect to ZooKeeper.
    CuratorFramework curator = zooKeeperConfig.newManagedCurator(environment);
    curator.start();

    // Configure the Jersey HTTP client library.
    Client jerseyClient = new JerseyClientFactory(jerseyClientConfig).build(environment);

    String apiKey = "xyz";  // Use the API key provided by EmoDB

    // Connect to the Databus using ZooKeeper (Ostrich) host discovery.
    ServiceFactory<Databus> databusFactory =
        DatabusClientFactory.forClusterAndHttpClient("local_default", jerseyClient).usingCredentials(apiKey);
    Databus databus = ServicePoolBuilder.create(Databus.class)
            .withHostDiscoverySource(databusEndPointOverrides)
            .withHostDiscovery(new ZooKeeperHostDiscovery(curator, databusFactory.getServiceName()))
            .withServiceFactory(databusFactory)
            .buildProxy(new ExponentialBackoffRetry(5, 50, 1000, TimeUnit.MILLISECONDS));
    environment.addHealthCheck(new DatabusHealthCheck(databus));
    environment.manage(new ManagedServicePoolProxy(databus));

    ... use "databus" to access the Databus ...
}
```

Subscription Management
-----------------------

### Subscribe

Subscribe to changes to a set of tables in the System of Record.  A Databus event will be generated for every SoR update.

HTTP:

    PUT /bus/1/<subscription>?ttl=<seconds>&eventTtl=<seconds>

    <table-filter-condition>

Java:

    void subscribe(String subscription, Condition tableFilter, Duration subscriptionTtl, Duration eventTtl);

Request Body:

*   `PUT` - The body of the request is optional.  If specified, it must be a valid JSON condition string in the format
    generated by `Condition.toString()`.  See the [Conditions] (Deltas.md#conditional)
    section of the Delta documentation.  From Java, use the [Conditions] (https://github.com/bazaarvoice/emodb/blob/master/sor-api/src/main/java/com/bazaarvoice/emodb/sor/condition/Conditions.java)
    class to create instances of `Condition`.  The subscription will follow events on all tables for which the
    condition evaluates to true.  The condition is evaluated against the table template and with the `~table` intrinsic.
    It is *not* evaluated against the specific content of the document being updated.

Request HTTP Headers:

*   `Content-Type: application/x.json-condition` - required if PUT body is specified

Request URL Parameters:

*   `ttl` - optional - The number of seconds before this subscription expires, unless the subscription is renewed.  To
    renew the subscription, simply invoke this method again.  The default `ttl` is 86400 seconds, equal to 24 hours.
    In general, applications should specify a TTL between 1 day and 1 week and should renew the subscription every few
    hours.  By specifying a TTL when a subscription is created, application developers and administrators are relieved
    from the burden of cleaning up old subscriptions manually.

*   `eventTtl` - optional - The number of seconds before any specific event followed by this subscription expires.  The
    default `ttl` is 86400 seconds, equal to 24 hours.  In general, applications should specify a TTL between 1 day and
    1 week.  The TTL should be long enough that the application will process events before they expire.

To subscribe to *all* tables in the System of Record, omit the condition from the body of the post, or pass the condition `alwaysTrue()':

    $ curl -s -XPUT -H "Content-Type: application/x.json-condition" \
        "http://localhost:8080/bus/1/demo-app" \
        --data-binary 'alwaysTrue()'
    {
      "success": true
    }

To subscribe to a single table, subscribe with a condition against the `~table` intrinsic:

    $ curl -s -XPUT -H "Content-Type: application/x.json-condition" \
        "http://localhost:8080/bus/1/demo-app" \
        --data-binary 'intrinsic("~table":"review:testcustomer")'
    {
      "success": true
    }

To subscribe to multiple tables, subscribe with a condition that checks fields of the table's template (specified when
the table was created).  For example if a typical template looks like `{"type":"review","client":"TestCustomer"}` you
can subscribe to all review tables with this:

    $ curl -s -XPUT -H "Content-Type: application/x.json-condition" \
        "http://localhost:8080/bus/1/demo-app" \
        --data-binary '{..,"type":"review"}'
    {
      "success": true
    }

See the [Conditions] (Deltas.md#conditional) section of the Delta
documentation for more about conditions.

### Unsubscribe

Unsubscribe to a set of tables in the System of Record.  It's usually not necessary to unsubscribe explicitly since
subscriptions expire automatically after the TTL specified when they were created.

HTTP:

    DELETE /bus/1/<subscription>

Java:

    void unsubscribe(String subscription);

### Count Events

Get an estimate of the number of unacknowledged events pending for a particular subscription.  This is most useful for
debugging.

HTTP:

    GET /bus/1/<subscription>/size

Java:

    int getEventCount(String subscription);

### Poll for Events

Check to see if there are any unclaimed, unacknowledged events pending for a subscription and, if there are, claim them
temporarily.  During the claim period, subsequent calls to poll will not return the events.  Once the claim period
expires, if the events have not been acknowledged, they may be returned again in another poll.

HTTP:

    GET /bus/1/<subscription>/poll?ttl=<seconds>&limit=<number>

Java:

    List<Event> poll(String subscription, Duration claimTtl, int limit);

Request URL Parameters:

*   `subscription` - required - The name of the subscription to poll.

*   `ttl` - optional - The number of seconds of the claim period.  The default `ttl` is 30 seconds.  Applications
    should choose a time period long enough that they are confident they can process and acknowledge the returned
    events before the claim expires.  But if the claim period is too long, it may take a while for events to be
    re-processed if an application dies while holding claims.

*   `limit` - optional - The maximum number of events to return.  The default `limit` is 10.

### Renew Claims

The application may renew a claim if it is close to expiring and the application believes it is still making progress
on processing the claim.  Most applications won't use this API, but it is available if necessary.

HTTP:

    POST /bus/1/<subscription>/renew?ttl=<seconds>

    <JSON list of event keys>

Java:

    void renew(String subscription, Collection<String> eventKeys, Duration claimTtl);

Request HTTP Headers:

*   `Content-Type: application/json` - required

### Acknowledge Claims

The application must acknowledge claims after processing them or else the Databus will assume the application died
and hand them out in future calls to poll.

HTTP:

    POST /bus/1/<subscription>/ack

    <JSON array of event keys>

Java:

    void acknowledge(String subscription, Collection<String> eventKeys);

Request HTTP Headers:

*   `Content-Type: application/json` - required

### Databus Replay

Asynchronously replay all events since a given timestamp within the past two days for an existing subscription.
A replay reference id is returned, which can be used to check the status of the replay operation.
If no timestamp is provided it will replay the past two days of events:

HTTP:

    POST /bus/1/<subscription>/replay # replay entire two days
    POST /bus/1/<subscription>/replay?since=2015-07-20T12:00:00.000Z # replay since the given timestamp
    GET /bus/1/_replay/<reference> # Check the status of Replay operation

Java:

    String replayAsync(String subscription);
    String replayAsyncSince(String subscription, Date since)
    ReplaySubscriptionStatus getReplayStatus(String reference) // Check the status of Replay operation    

Request URL Parameters:

*   `subscription` - required - The name of the subscription to replay.
*   `since` - optional - To replay events since a given timestamp within the past 2 days. If not provided, then past two days will be replayed.
