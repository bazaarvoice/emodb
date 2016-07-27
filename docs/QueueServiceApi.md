QueueService API
============

The EmoDB QueueService provides a set of highly available distributed queue-like data structures.

The QueueService guarantees every message sent will eventually be delivered.  It does *not* make any guarantees about
message order or duplicate messages.  In general, do not make any assumptions about the order that messages will be
delivered.  And there are many failure scenarios where the QueueService will deliver the same message multiple times.
It is up to the consumer to deal with this.

The QueueService is very similar to Amazon's Simple Queue Service and implements similar guarantees (or lack of them)
with regard to message order and duplicates. 

### DedupQueueService
DedupQueueService will dedup all messages in between polls currently in the queue with matching identifiers. The interface is identical to the QueueService, but when polling, duplicate entries will collapse into a single entry. Said another way, if two identical messages arrive in the queue between two polls, then they will be dedup'd. For example, if the order of events is: M1 -> M1 -> P1 (where Mx is a message and Px is a poll), then P1 will only see M1 once. However, if the order of events is M1 -> P1 -> M1 -> P2, then both P1 and P2 polls will see the message M1. 

Peek operations will, however, still show duplicates (it is only when you poll that duplicates are deduped). The API endpoint is `/dedupq/1` instead of `/queue/1`, but all the REST API calls are the same otherwise.

The QueueService exposes a RESTful API.  You can access the API directly over HTTP or via a Java client library.

Java Client Library
-------------------

Add the following to your Maven POM (set the `<emodb.version>` to the current version of EmoDB):

```xml
<dependency>
    <groupId>com.bazaarvoice.emodb</groupId>
    <artifactId>emodb-queue-client</artifactId>
    <version>${emodb.version}</version>
</dependency>
```

Minimal Java client without ZooKeeper or Dropwizard:

```java
String emodbHost = "localhost:8080";  // Adjust to point to the EmoDB server.
String apiKey = "xyz";  // Use the API key provided by EmoDB
MetricRegistry metricRegistry = new MetricRegistry(); // This is usually a singleton passed
QueueService queueService = ServicePoolBuilder.create(QueueService.class)
                .withHostDiscoverySource(new QueueFixedHostDiscoverySource(emodbHost))
                .withServiceFactory(
                        QueueClientFactory.forCluster("local_default", metricRegistry)
                                .usingCredentials(apiKey))
                .withMetricRegistry(metricRegistry)
                .buildProxy(new ExponentialBackoffRetry(5, 50, 1000, TimeUnit.MILLISECONDS));

... use "queueService" to access the QueueService ...

ServicePoolProxies.close(queueService);
```

Robust Java client using ZooKeeper, [SOA] (https://github.com/bazaarvoice/ostrich) and [Dropwizard]
(http://dropwizard.codahale.com/):

```java
@Override
protected void initialize(Configuration configuration, Environment environment) throws Exception {
    // YAML-friendly configuration objects.
    ZooKeeperConfiguration zooKeeperConfig = configuration.getZooKeeper();
    JerseyClientConfiguration jerseyClientConfig = configuration.getHttpClientConfiguration();
    QueueFixedHostDiscoverySource queueEndPointOverrides = configuration.getQueueEndPointOverrides();

    // Connect to ZooKeeper.
    CuratorFramework curator = zooKeeperConfig.newManagedCurator(environment);
    curator.start();

    // Configure the Jersey HTTP client library.
    Client jerseyClient = new JerseyClientFactory(jerseyClientConfig).build(environment);

    String apiKey = "xyz";  // Use the API key provided by EmoDB

    // Connect to the QueueService using ZooKeeper (Ostrich) host discovery.
    ServiceFactory<QueueService> queueServiceFactory =
        QueueClientFactory.forClusterAndHttpClient("local_default", jerseyClient).usingCredentials(apiKey);
    QueueService queueService = ServicePoolBuilder.create(QueueService.class)
            .withHostDiscoverySource(queueEndPointOverrides)
            .withHostDiscovery(new ZooKeeperHostDiscovery(curator, queueServiceFactory.getServiceName()))
            .withServiceFactory(queueServiceFactory)
            .buildProxy(new ExponentialBackoffRetry(5, 50, 1000, TimeUnit.MILLISECONDS));
    environment.addHealthCheck(new QueueServiceHealthCheck(queueService));
    environment.manage(new ManagedServicePoolProxy(queueService));

    ... use "queueService" to access the QueueService ...
}
```

Queue Operations
----------------

All queue operations should include the caller's API key.  When making direct HTTP requests the API key should be
included as an "X-BV-API-Key" header.  The Java client will automatically include this header with the API key provided
by the client factory's configuration.

### Send a single message

Post a single message into a queue.  There is no need to create the queue in advance.  The message must be a valid JSON
object (a combination of `null`, `Boolean`, `Number`, `String`, `List`, and `Map` objects).

HTTP:

    POST /queue/1/<queue>/send
    Content-Type: application/json

    <json-message>

Java:

    void send(String queue, Object message);

Request Body:

*   `POST` - The body of the request must be valid JSON.  All valid JSON types are accepted: `null`, booleans, numbers,
    strings, arrays, objects.

Request HTTP Headers:

*   `Content-Type: application/json` - required

Example:

    $ curl -s -XPOST -H "Content-Type: application/json" -H "X-BV-API-Key: api_key" \
        "http://localhost:8080/queue/1/demo-app_provision-q/send" \
        --data-binary '{"provision":"TestCustomer"}'
    {
      "success": true
    }

When choosing a queue name, you can specify any name that is unique for your application. However, some commonly used
formats are:  
- `{application}-{queueName}`
- `{application}-{universe}-{queueName}`
- `{application}-{universe}-{cluster}-{queueName}` 

... or using underscores instead of hyphens.

### Send a batch of messages

Post a set of messages into one or more queue in a single request.  Posting messages in batch is much more efficient
than posting one message at a time.  This is not an atomic operation--if the operation fails it is possible that some
messages in the batch were posted and other weren't.

HTTP:

    POST /queue/1/_sendbatch
    Content-Type: application/json

    {"<queue-1>":[<json-message-1>, <json-message-2>, ...], "<queue-2>":[<json-message-3>,, ...], ...}

Java:

    void sendAll(Map<String, ? extends Collection<?>) messagesByQueue);

Request Body:

*   `POST` - The body of the request must be valid JSON.  All valid JSON types are accepted: `null`, booleans, numbers,
    strings, arrays, objects.

Request HTTP Headers:

*   `Content-Type: application/json` - required

Example:

    $ curl -s -XPOST -H "Content-Type: application/json" -H "X-BV-API-Key: api_key" \
        "http://localhost:8080/queue/1/_sendbatch" \
        --data-binary '{"demo-app_provision-q":[{"provision":"TestCustomer"}]}'
    {
      "success": true
    }

### Poll for Messages

Check to see if there are any unclaimed, unacknowledged messages pending for a queue and, if there are, claim them
temporarily.  During the claim period, subsequent calls to poll will not return the messages.  Once the claim period
expires, if the messages have not been acknowledged, they may be returned again in another poll.

Messages are not returned in any particular order.  The Queue Service does _not_ implement strict first-in-first-out
semantics.

HTTP:

    GET /queue/1/<queue>/poll?ttl=<seconds>&limit=<number>

Java:

    List<Message> poll(String queue, Duration claimTtl, int limit);

Request URL Parameters:

*   `queue` - required - The name of the queue to poll.

*   `ttl` - optional - The number of seconds of the claim period.  The default `ttl` is 30 seconds.  Applications
    should choose a time period long enough that they are confident they can process and acknowledge the returned
    messages before the claim expires.  But if the claim period is too long, it may take a while for messages to be
    re-processed if an application dies while holding claims.  The ttl may range from 0 seconds to 1 hour.

*   `limit` - optional - The maximum number of messages to return.  The default `limit` is 10.  The limit may range
    from 1 to 1000.

There are certain failure scenarios in which claims may get ignored, and multiple calls to poll will return the same
message within the claim TTL window.  These failure scenarios should be occur infrequently, but the application must
handle them gracefully.

### Acknowledge Claims

The application must acknowledge claims after processing them or else the QueueService will assume the application died
and hand them out in future calls to poll.

HTTP:

    POST /queue/1/<queue>/ack
    Content-Type: application/json

    <JSON array of message IDs>

Java:

    void acknowledge(String queue, Collection<String> messageIds);

Request HTTP Headers:

*   `Content-Type: application/json` - required

### Renew Claims

The application may renew a claim if it is close to expiring and the application believes it is still making progress
on processing the claim.  Most applications won't use this API, but it is available if necessary.

HTTP:

    POST /queue/1/<queue>/renew?ttl=<seconds>
    Content-Type: application/json

    <JSON list of message keys>

Java:

    void renew(String queue, Collection<String> messageIds, Duration claimTtl);

Request HTTP Headers:

*   `Content-Type: application/json` - required

Request URL Parameters:

*   `ttl` - optional - The number of seconds of the claim period.  The default `ttl` is 30 seconds.  The ttl may range
    from 0 seconds to 1 hour.

If a claim has expired, renew will recreate the claim with the new TTL.

Renewing a claim will never cause a claim to expire early.  For example, if an application polls and creates a 60
second claim then immediately attempts to renew the claim for 30 seconds, the original 60 second claim will take
priority because it has the later expiration time.

Queue Administration
--------------------

### Peek into a Queue

Fetch `limit` messages from the queue, chosen arbitrarily, ignoring any pending claims.  This is mainly useful for
debugging.

HTTP:

    GET /queue/1/<queue>/peek?limit=<number>

Java:

    List<Message> peek(String queue, int limit);

Request URL Parameters:

*   `queue` - required - The name of the queue to peek into.

*   `limit` - optional - The maximum number of messages to return.  The default `limit` is 10.  It must be between 1
    and 1,000.


There is currently no way to iterate through more items than can be fetched in a single request.

### Count Messages

Get an estimate of the number of unacknowledged messages pending for a queue.  The returned value is an estimate because
it may or may not include messages whose status' change during the message counting process, such as by being created or
acknowledged.

The time it takes to perform a full count of the queue size scales linearly with the current size of the queue.  For
particularly large queues this can result in long response times when getting a count.  Frequently the caller is only
interested in an estimate up to a certain point.  For example, an alert monitor may only take action if there are more
than 1,000 records in the queue and waiting for a complete total beyond 1,000 records provides little additional
benefit.  To support this the caller can optionally pass in a limit parameter.  This will perform a full count up
to the limit then use a faster heuristic approximation to count the remaining messages.  For example, with a limit
of 1,000 a return value of 20,000 means the count found 1,000 messages and approximated there were an additional 19,000
messages.


HTTP:

    GET /queue/1/<queue>/size?limit=<number>

Java:

    long getMessageCount(String queue);
    long getMessageCountUpTo(String queue, long limit);

Request URL Parameters:

*   `queue` - required - The name of the queue for querying the size.

*   `limit` - optional - If provided the count returned will heuristically approximate the number of messages beyond
    the first `limit` messages.

### Count Outstanding Claims

Get an estimate of the number of claimed messages for a queue.

HTTP:

    GET /queue/1/<queue>/claimcount

Java:

    int getClaimCount(String queue);

### Purge Messages

Delete all messages in the queue.

HTTP:

    DELETE /queue/1/<queue>

Java:

    void purge(String queue);

### Unclaim All Messages

Release all outstanding claims on messages.  All messages immediately become available to the `poll` method.

HTTP:

    POST /queue/1/<queue>/unclaimall

Java:

    void unclaimAll(String queue);

### Unclaim

There's no direct API for unclaiming a specific message or collection of messages. However, you can achieve the same effect by renewing the claims for your message(s) with a duration of zero, which will essentially cause the message(s) to appear in the queue for the next `poll` operation immediately.

HTTP:

    POST /queue/1/<queue>/renew?ttl=0
    Content-Type: application/json

    <JSON list of message keys>

Java:

    void renew(String queue, Collection<String> messageIds, Duration.ZERO);

### Move Queue

Asynchronously move items from one queue to another. A queue move reference `id` is returned, which can be used to check the status of the move operation.

HTTP:

    POST /queue/1/_move?from=<from_queue>&to=<to_queue>

Java:

    String moveAsync(String from, String to);

### Check Queue Move Operation Status

Check the status of a move operation.

HTTP:

    GET /queue/1/_move/<reference>

Java:

    MoveQueueStatus getMoveStatus(String reference)
