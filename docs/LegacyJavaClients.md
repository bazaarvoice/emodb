Legacy Java Clients
===================

The current version of EmoDB uses DropWizard 7.1.  The Java clients returned by the
[Data Store] (https://github.com/bazaarvoice/emodb/blob/master/sor-client/src/main/java/com/bazaarvoice/emodb/sor/client/DataStoreClientFactory.java),
[Blob Store] (https://github.com/bazaarvoice/emodb/blob/master/blob-client/src/main/java/com/bazaarvoice/emodb/blob/client/BlobStoreClientFactory.java),
[Databus] (https://github.com/bazaarvoice/emodb/blob/master/databus-client/src/main/java/com/bazaarvoice/emodb/databus/client/DatabusClientFactory.java),
[Queue] (https://github.com/bazaarvoice/emodb/blob/master/queue-client/src/main/java/com/bazaarvoice/emodb/queue/client/QueueClientFactory.java),
and [Dedup Queue] (https://github.com/bazaarvoice/emodb/blob/master/queue-client/src/main/java/com/bazaarvoice/emodb/queue/client/DedupQueueClientFactory.java)
client factories use dependencies matching that version of DropWizard.

Historically EmoDB version up to and including 3.33 used DropWizard 6.2.  There have been numerous API updates and
bug fixes since that release and all projects using older versions of EmoDB are encouraged to upgrade.  However, this
would also require those projects to upgrade to DropWizard 7 -- or at least upgrade to using
the same shared dependencies.  As an intermediate solution EmoDB provides a set of legacy clients.  These clients
receive the same updates as the standard EmoDB clients and use a nearly identical API (the differences will be
explained shortly).

Shading
-------

The legacy clients are a slightly-modified version of the standard EmoDB clients with all dependencies shaded into
a single uber-jar.  The package names in the shaded jar have been relocated so they should not conflict with your
project's declared dependencies.  For the most part these relocated classes are used only internally by EmoDB; the
few places they are exposed through the EmoDB client API are addressed later in this document.

Maven Dependency
----------------

Unlike with the standard EmoDB client libraries all shaded clients are included in a single Maven dependency.

```xml
<dependency>
    <groupId>com.bazaarvoice.emodb</groupId>
    <artifactId>emodb-shaded-clients-dropwizard6</artifactId>
    <!-- Replace the following with the actual latest version -->
    <version>4.14</version>
</dependency>
```

This project includes clients for the system of record, blob store, databus, and queues.

Creating a Client
-----------------

Creating a client using the shaded project is nearly identical to the process for creating a standard client.
For example considering the following example for creating a system of record client:

```java
import com.bazaarvoice.shaded.emodb.sor.client.DataStoreClientFactory;
import com.bazaarvoice.shaded.ostrich.EmoServicePoolBuilder;
import com.bazaarvoice.shaded.ostrich.ServiceFactory;
import com.bazaarvoice.shaded.ostrich.EmoZooKeeperHostDiscovery;
import com.bazaarvoice.shaded.ostrich.pool.ServiceCachingPolicyBuilder;
import com.bazaarvoice.shaded.ostrich.retry.ExponentialBackoffRetry;


ServiceFactory<DataStore> dataStoreFactory =
    DataStoreClientFactory.forClusterAndHttpClient("local_default", jerseyClient).usingCredentials(apiKey);
DataStore dataStore = EmoServicePoolBuilder.create(DataStore.class)
        .withHostDiscovery(new EmoZooKeeperHostDiscovery(curator, dataStoreFactory.getServiceName()))
        .withServiceFactory(dataStoreFactory)
        .buildProxy(new ExponentialBackoffRetry(5, 50, 1000, TimeUnit.MILLISECONDS));
```

Notice the key differences from using the standard client:

* `DataStoreClientFactory` is in a different package that starts with `com.bazaarvoice.shaded.emodb`.
* Ostrich classes such as `ServiceFactory` are unchanged but have been relocated to `com.bazaarvoice.shaded.ostrich`.
* `EmoServicePoolBuilder` is used instead of `ServicePoolBuilder`.  This isn't required but it helpfully initializes several required attributes that you will not use, such as the metric registry.
* `EmoZooKeeperHostDiscovery` is used instead of `ZooKeeperHostDiscovery` for the same reason as above.

Using a Client
--------------

From this point forward the API for each client matches that for the standard client.  However, due to shading the
following classes have been relocated:

Original Class | Shaded Class | Usages
-------------- | ------------ | ------
`com.google.common.io.InputSupplier` | `com.google.shaded.common.io.InputSupplier` | `BlobStore.put()`

