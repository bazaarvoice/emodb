Security
========

EmoDB is a shared service that is designed for multi-tenancy.  While this is a work in progress (for example, all EmoDB
tables currently share a common namespace) there need to be security protections to ensure the integrity of the system.

These protections include:

1. Authentication
  * Prevent system access by unknown entities
  * Prevent a team from accidentally performing updates intended for one environment (e.g.; QA) in another environment
    (e.g; production) by assigning different credentials per environment
  * Provide an audit trail for all operations
2. Authorization
  * Prevent a team from accessing, mutating, or otherwise disturbing resources managed by another team
  * Allow a team to define the rules for protecting and sharing their resources

API Keys
--------

To provide these protections EmoDB uses API keys.  Each individual or team can be granted an API key which can be
narrowly permitted to perform only the actions required by the grantee.  This includes restricting access to specific
resources (e.g.; permitting access only to tables matching "my_team_prefix:*") as well as restricting access levels to
those resources (e.g.; read-only vs. read-write).


How to get an API key
---------------------

We recommend creating a protocol for the creation and distribution of API keys.  Each API key request should include
the following:

1. The individual or team email address
2. A description of who will own the key (typically your team name)
3. What type of access the key needs (specific resources, read-only vs. read-write, and so on)
4. What environments will be accessed (QA, integration, production).  If the requester needs access to multiple
   environments they should get a separate key for each environment.

The actual process of managing API keys is described [below] (#managing-api-keys).

Using your API key
---------------------

### Direct access

If you are making API calls directly against the EmoDB API, such as through `curl`, then you can pass your API key
in either of the following ways:

1. Create a `X-BV-API-Key` header with your key's value:

```
$ curl -s -H "X-BV-API-Key: <your_api_key_>" http://localhost:8080/sor/1/review:testcustomer/demo1"
```

2. Add a `APIKey` query parameter to your call:

```
$ curl -s http://localhost:8080/sor/1/review:testcustomer/demo1?APIKey=<your_api_key>"
```

### Java client

For the simple case where your entire application uses a single API key use the `usingCredentials()` method in the
client factory.  For example, the following creates a `DataStore` client using a single API key:


```java
String emodbHost = "localhost:8080";  // Adjust to point to the EmoDB server.
String apiKey = "<your_api_key>";
DataStore dataStore = ServicePoolBuilder.create(DataStore.class)
        .withHostDiscoverySource(new DataStoreFixedHostDiscoverySource(emodbHost))
        .withServiceFactory(DataStoreClientFactory.forCluster("local_default").usingCredentials(apiKey))
        .buildProxy(new ExponentialBackoffRetry(5, 50, 1000, TimeUnit.MILLISECONDS));
```

If you application uses multiple API keys then it would be inefficient to create a separate service pool for each data
store.  Instead you can use a `DataStoreAuthenticator` to re-use a single data store pool for multiple API keys.

To do this change `DataStore.class` to `AuthDataStore.class` and wrap it using a `DataStoreAuthenticator`.
This time the previous example would be updated to the following:

```java
String emodbHost = "localhost:8080";  // Adjust to point to the EmoDB server.
// Use AuthDataStore
AuthDataStore authDataStore = ServicePoolBuilder.create(AuthDataStore.class)
        .withHostDiscoverySource(new DataStoreFixedHostDiscoverySource(emodbHost))
        .withServiceFactory(DataStoreClientFactory.forCluster("local_default"))
        .buildProxy(new ExponentialBackoffRetry(5, 50, 1000, TimeUnit.MILLISECONDS));

// Create a DataStoreAuthenticator from the AuthDataStore service pool
DataStoreAuthenticator dataStoreAuthenticator = DataStoreAuthenticator.proxied(authDataStore)

String apiKey1 = "<your_first_api_key>";
String apiKey2 = "<your_second_api_key>";

// Convert back to a DataStore view
DataStore dataStore1 = dataStoreAuthenticator.usingCredentials(apiKey1);
DataStore dataStore2 = dataStoreAuthenticator.usingCredentials(apiKey2);
```

Both `dataStore1` and `dataStore2` use the same underlying client but calls to each will be authenticated using
their respective API keys.

Although not presented here `BlobStore`, `DataBus`, `QueueService`, and `DedupQueueService` all have a corresponding
`usingCredentials()` method, `Auth` and `Authenticator` classes and can be used following the same pattern.


Anonymous Access
----------------

If you choose you can configure EmoDB to allow anonymous access.  An anonymous user has full permission to perform
most standard operations in the data store, blob, databus, and queue services (see
[DefaultRoles.java] (https://github.com/bazaarvoice/emodb/blob/master/web/src/main/java/com/bazaarvoice/emodb/web/auth/DefaultRoles.java#L126)
for full anonymous permissions).  While we don't recommend running EmoDB with anonymous access enabled it does
lower the bar for quickly getting going with EmoDB.  To enable anonymous access set the following attribute in
your `config.yaml` file:

```
auth:
  allowAnonymousAccess: true
```

Anonymous access enables the following behavior:

* A client can explicitly authenticate as an anonymous user using the reserved API key "anonymous".
* All REST calls with no API key provided are automatically authenticated as anonymous.

Managing API Keys
-----------------

Typically API keys are managed by a core trusted group and protocol.  If you are an EmoDB user and need a key you should
follow your EmoDB administrator's protocol to receive one.  If you are an EmoDB administrator or you are running EmoDB
locally and need to create keys on your local system then please see the
[Key management documentation] (https://github.com/bazaarvoice/emodb/blob/master/docs/SecurityManagement.md)
