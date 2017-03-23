---
layout: post
title: "User Access Control"
date: 2017-03-22
excerpt: "EmoDB User Access Control"
tags: [User Access Control, EmoDB]
type: [blog]
---

User Access Control
===================

As mentioned in the [API key](https://bazaarvoice.github.io/emodb/security/) documentation EmoDB uses API keys to
authenticate an authorize client requests. Each API key can have zero or more roles associated with it, and each of
those roles contains permissions which grant access to different parts of the system.

User access control is the system in EmoDB for managing API keys, permissions and roles.  The following sections
describe the rules and APIs for user access control.

Java Client Library
-------------------

To use the user access control API you can either call the service directly or use the Java client library.
To use the Java client add the following to your Maven POM (set the `<emo-version>` to the current version of EmoDB):

```xml
<dependency>
    <groupId>com.bazaarvoice.emodb</groupId>
    <artifactId>emodb-uac-client</artifactId>
    <version>${emo-version}</version>
</dependency>
```

Minimal Java client without ZooKeeper or Dropwizard:

```java
String emodbHost = "localhost:8080";  // Adjust to point to the EmoDB server.
String apiKey = "xyz";  // Use the API key provided by EmoDB
MetricRegistry metricRegistry = new MetricRegistry(); // This is usually a singleton passed
UserAccessControl userAccessControl = ServicePoolBuilder.create(UserAccessControl.class)
                .withHostDiscoverySource(new UserAccessControlFixedHostDiscoverySource(emodbHost))
                .withServiceFactory(UserAccessControlClientFactory.forCluster("local_default", new MetricRegistry()).usingCredentials(apiKey))
                .withMetricRegistry(metricRegistry)
                .buildProxy(new ExponentialBackoffRetry(5, 50, 1000, TimeUnit.MILLISECONDS));

... use "userAccessControl" to access the service ...

ServicePoolProxies.close(userAccessControl);
```

Robust Java client using ZooKeeper, [SOA](https://github.com/bazaarvoice/ostrich) and [Dropwizard](http://www.dropwizard.io):

```java
@Override
protected void initialize(Configuration configuration, Environment environment) throws Exception {
    // YAML-friendly configuration objects.
    ZooKeeperConfiguration zooKeeperConfig = configuration.getZooKeeper();
    JerseyClientConfiguration jerseyClientConfig = configuration.getHttpClientConfiguration();
    UserAccessControlFixedHostDiscoverySource sorEndPointOverrides = configuration.getSorEndPointOverrides();

    // Connect to ZooKeeper.
    CuratorFramework curator = zooKeeperConfig.newManagedCurator(environment);
    curator.start();

    // Configure the Jersey HTTP client library.
    Client jerseyClient = new JerseyClientFactory(jerseyClientConfig).build(environment);

    String apiKey = "xyz";  // Use the API key provided by EmoDB

    // Connect to the UserAccessControl using ZooKeeper (Ostrich) host discovery.
    ServiceFactory<UserAccessControl> userAccessControlFactory =
        UserAccessControlClientFactory.forClusterAndHttpClient("local_default", jerseyClient).usingCredentials(apiKey);
    UserAccessControl userAccessControl = ServicePoolBuilder.create(UserAccessControl.class)
            .withHostDiscoverySource(sorEndPointOverrides)
            .withHostDiscovery(new ZooKeeperHostDiscovery(curator, userAccessControlFactory.getServiceName()))
            .withServiceFactory(userAccessControlFactory)
            .buildProxy(new ExponentialBackoffRetry(5, 50, 1000, TimeUnit.MILLISECONDS));
    environment.addHealthCheck(new UserAccessControlHealthCheck(userAccessControl));
    environment.manage(new ManagedServicePoolProxy(userAccessControl));

    ... use "userAccessControl" to access the service ...
}
```

Permissions
-----------

Permissions are used to control access to EmoDB's resources.  For example, if team A creates a queue for its own
project's internal use it would be harmful if team B were to poll and ack messages from that queue without team A's
knowledge or consent.  Permissions can be used to restrict the capabilities of an individual role, and assigning the
role to one or more API keys transitively limits the capabilities of those API keys.

A full list of possible permissions can be found in [Permissions.java](https://github.com/bazaarvoice/emodb/blob/master/web/src/main/java/com/bazaarvoice/emodb/web/auth/Permissions.java).
The following section highlights the general format and nuances around SoR and Blob permissions.

### Permission format

In general permissions follow the format of "_context_|_action_|_resource_".  For example `databus|poll|subscription1`
indicates permission to perform the "poll" action on the databus subscription "subscription1".

For _actions_ and _resources_ the value can be one of the following:

* A single value (e.g: `update`)
* A wildcard value.  This can indicate either the entire value, `*`, or a portion, such as `get*`.
* A conditional value (more on this later)


#### Context

The currently supported contexts are `sor`, `blob`, `queue`, `databus`, `facade` and `system`.  The context portion
of the permission must start with one of these values.  (It is technically possible to use a pure wildcard, `*`, although
this is discouraged.  The existing "admin" role already provides this capability.)

#### Action

The action restricts what the user can do within the context.  As such each context typically has its own set of actions
which may not have meaning in other contexts.  For example, `databus|poll` makes sense but `blob|poll` does not and
therefore is never utilized by EmoDB.

#### Resource

The resource restricts what the user can do within the context and action.  For example, `databus|poll|*` indicates
that polling all databus subscriptions is permitted, while `databus|poll|subscription1` indicates that polling is
only permitted for a single subscription, `subscription1`.

### Conditionals

Permitting activities using only single values and wildcard values is limiting.  For example, assume there is a role that
should have permission to perform all actions in the `sor` context except `drop_table`.  The only way to do this is to
create separate permissions for all possible actions _except_ `drop_table` (`sor|update|*`, `sor|create_table|*`,
and so on).  If the user were further restricted by a subset of tables instead of simply `*` this now requires a
complicated cartesian product of all possible combinations.

For this reason an action or resource can use a conditional to determine matching values.  The
[conditional]({{ site.baseurl }}/deltas/#conditional) is exactly the same format
as used by deltas and databus subscriptions.  To create a conditional surround the condition string in an `if()`
statement.

#### Examples

Permission                                           | Effect
----------                                           | ------
`sor|if(in("update","create_table"))|*`              | Equivalent to having both `sor|update|*` and `sor|create_table|*`
`sor|if(not("drop_table"))|*`                        | User can perform all actions in the `sor` context except `drop_table`
`queue|*|if(and(like("team:*"),not("team:edward")))` | User can perform all actions on all queues matching `team:*` except `team:edward`
{:.chart}

### Table conditionals

In all contexts except `sor` and `blob` a conditional in the _resource_ is evaluated using the resource name, such a the
name of a queue or databus subscription.  In `sor` and `blob` it is evaluated as a table conditional in exactly the
same way as when creating a databus subscription.

#### Examples

For the follow examples assume the SoR table "ermacs_data" has been created in placement "ugc_global:ugc" and was
created with template `{"team": "ermacs"}`.  (The common prefix of `sor|update|` has been removed from the first
column and in some cases whitespace added for readability.)


Resource in "sor" context                                                            | Matches table?   | Why
--------------------------------------------------------------                       | --------------   | ---
`ermacs_*`                                                                           | Yes              | Table name starts with prefix "ermacs_"
`if(intrinsic("~table":"ermacs_data"))`                                              | Yes              | Table name is an exact match
`if(intrinsic("~table":in("ermacs_data","ermacs_logs")))`                            | Yes              | Table name is in the "in" condition
`if(intrinsic("~placement":'ugc_global:ugc'))`                                       | Yes              | Table placement is an exact match
`if(intrinsic("~placement":like("*:ugc")))`                                          | Yes              | Table placement is a like match
`if({..,"team":"ermacs"})`                                                           | Yes              | Table has attribute "team" set to "ermacs"
`if({..,"team":"ermacs","other":"attr"))`                                            | No               | Only one matching attribute is present on the table
`if(and(intrinsic("~table":like("ermacs_*")),` `intrinsic("~placement":like("*:ugc"))))` | Yes              | Table name and placement both match the respective like conditions
`if(and(intrinsic("~table":like("ermacs_*")),` `intrinsic("~placement":like("*:cat"))))` | No               | Only one of the conditions is met; placement does not end with "cat"
{:.chart}

Role Administration API
-----------------------

A role consists of the following attributes:

* Group
* ID
* Name
* Description
* Permission set

The "group" is effectively a namespace for grouping related roles, and the "ID" is a unique identifier for the role
within its group.  Collectively [group, ID] is a unique identifier for each role.  A group must be a sequence of at most
255 characters, each of which must be either alpha-numeric or one of [-.:_].  Additionally, the group name "\_" is reserved.
IDs are similarly constrained except there is no restriction on creating IDs named "\_".  Role names and descriptions are
optional, although providing descriptive values for each is recommended.

EmoDB has several pre-defined roles that are always available.  You can see these roles and what permissions they
have in [DefaultRoles.java](https://github.com/bazaarvoice/emodb/blob/master/web/src/main/java/com/bazaarvoice/emodb/web/auth/DefaultRoles.java)

The role administration API allows you to create new roles with custom permissions.  These roles can then be associated
with one or more API keys to provide fine controls over what actions the API key can perform.  Note that each of these
role operations requires an authenticating API key which itself has specific permissions.  The permissions necessary are
documented with each operation below.

### Create a role

Permissions required:

* `role|create|{group}|{id}`

HTTP:

    POST /uac/1/role/{group}/{id}

    Content-Type: "application/x.json-create-role"

Java:

```java
void createRole(CreateEmoRoleRequest request)
```

When creating a role you only need to provide those attrbutes which are being initialized.  For example, if you leave
out any permissions the role will be created but it will initially have no permissions associated with it.

Example:

    $ curl -s -XPOST 'http://localhost:8080/uac/1/role/sample_group/sample_id' \
          -H "Content-Type: application/x.json-create-role" \
          -d '{"name":"Sample role","description":"A sample role","permissions":["sor|read|*","blob|read|*"]}'
    {"success":true}
    
Java Example:

```java
uac.createRole(
        new CreateEmoRoleRequest(new EmoRoleKey("sample_group", "sample_id"))
                .setName("Sample role")
                .setDescription("A sample role")
                .setPermissions(ImmutableSet.of("sor|read|*", "blob|read|*")))
```

### View all roles or view all roles in one group

Permissions required:

* None, although only roles for which you have `role|read|{group}|{id}` permission will be returned

HTTP:

    GET /uac/1/role
    GET /uac/1/role/{group}

Java:

```java
Iterator<EmoRole> getAllRoles()
Iterator<EmoRole> getAllRolesInGroup(String group)
```

Example:

    $ curl -s "http://localhost:8080/uac/1/role" | jq .
    [
      {
        "group": "sample_group",
        "id": "sample_id",
        "name": "Sample role",
        "description": "A sample role",
        "permissions": [
          "blob|read|*",
          "sor|read|*"
        ]
    }
   
    $ curl -s "http://localhost:8080/uac/1/role/sample_group" | jq .
    [
      {
        "group": "sample_group",
        "id": "sample_id",
        "name": "Sample role",
        "description": "A sample role",
        "permissions": [
          "blob|read|*",
          "sor|read|*"
        ]
      }
    ]

### View a role

Permissions required:

* `role|read|{group}|{id}`

HTTP:

    GET /uac/1/role/{group}/{id}
    
Java

```java
EmoRole getRole(EmoRoleKey roleKey)
```
    
Example:

    $ curl -s "http://localhost:8080/uac/1/role/sample_group/sample_id" | jq .
    {
      "group": "sample_group",
      "id": "sample_id",
      "name": "Sample role",
      "description": "A sample role",
      "permissions": [
        "blob|read|*",
        "sor|read|*"
      ]
    }
    
Java Example:

```java
EmoRole role = uac.getRole(new EmoRoleKey("sample_group", "sample_id"));
```

# Update a role
            
Permissions required:

* `role|update|{group}|{id}`

HTTP:

    PUT /uac/1/role/{group}/{id}

    Content-Type: "application/x.json-update-role"
    
Java:

```java
void updateRole(UpdateEmoRoleRequest request)
```
   
When updating a role you only need to provide those attributes which you want changed.  Additionally, you can incrementally
add and remove individual permissions without passing back the entire permission set on each call.  The following
examples revoke permission for `blob|read|*` and add permission for `databus|*|subscription1`.

Example:

    $ curl -s -XPUT "http://localhost:8080/uac/1/role/sample_group/sample_id' \
          -H "Content-Type: application/x.json-update-role" \
          -d '{"name":"A new name","revokePermissions":["blob|read|*"],"grantPermissions":["databus|*|subscription1"]}'
    {"success":true}
    
Java Example:

```java
uac.updateRole(        
        new UpdateEmoRoleRequest(new EmoRoleKey("sample_group", "sample_id"))
                .setName("A new name")
                .revokePermissions(ImmutableSet.of("blob|read|*"))
                .grantPermissions(ImmutableSet.of("databus|*|subscription1")))
```

### Delete a role

Permissions required:

* `role|delete|{group}|{id}`

HTTP:

    DELETE /uac/1/role/{group}/{id}
    
Java:

```java
void deleteRole(EmoRoleKey roleKey)
```

Example:

    $ curl -s -XDELETE "http://localhost:8080/uac/1/role/sample_group/sample_id"
    {"success":true}
    
Java Example:

```java
uac.deleteRole(new EmoRoleKey("sample_group", "sample_id"))

```

API Key Administration API
--------------------------

An API key consists of the following attributes:

* key (private unique identifier used for authentication, such as in the "X-BV-API-Key" header)
* ID (public unique identifier)
* Owner
* Description
* Role set

The "owner" is the only required settable attribute.  No specific format is required, although we recommend an email
address or some similar identifier for contacting the API key's owner in case there is an issue.

It is possible to assign an API key a role that does not exist.  If this happens there is no error, the role simply
doesn't provide the API key with any additional permissions.  If and when the role is created the API key will gain
the permissions associated with that role.

As with roles there are specific permissions which are required for each API key operation.  Note that the permissions
to modify an API key's roles are different than the permissions for the API key's other attributes.  For example,
a user may have permission to add and remove roles in the group "sample_group" for an API key but not to perform any
other API key updates.

The following documentation is not exhaustive; there are more API key operations available, and for those described some
have additional parameters not presented here.  This documentation includes the most common uses for API key administration;
please refer to the Java client classes for a full list of operations and parameters.

### Create an API key

Permissions required:

* `apikey|create`
* `role|grant|{group}|{id}` for each role the request assigns to the API key

HTTP:

    POST /uac/1/api-key
   
    Content-Type: "application/x.json-create-api-key"
    
Java

```java
CreateEmoApiKeyResponse createApiKey(CreateEmoApiKeyRequest request)
```

Note that both the private key and public ID are determined by Emo and returned in the response.

Example:

    curl -s -XPOST "http://localhost:8080/uac/1/api-key" \
        -H "Content-Type: application/x.json-create-api-key" \
        -d '{"owner":"owner@example.com","description":"Sample API key","roles":[{"group":"sample_group","id":"sample_id"}]}' | jq .
    {
      "id": "MEBZF4AP3YI6PMX7F22LNQUCKI",
      "key": "um3affkxtsuzehmgmrlfnh2f2wis3bwvwshsfsztdkxymkjc"
    }

Java Example:

```java
CreateEmoApiKeyResponse response = uac.createApiKey(
        new CreateEmoApiKeyRequest()
                .setOwner("owner@example.com")
                .setDescription("Sample API key")
                .setRoles(ImmutableSet.of(new EmoRoleKey("sample_group", "sample_id"))))
```

### View an API key

Permissions required:

* `apikey|read` unless the API key requested matches the authenticating API key; every user has permission to view himself

HTTP:

    GET /uac/1/api-key/{id}

Java:

```java
EmoApiKey getApiKey(String id)
```

Example:

    curl -s "http://localhost:8080/uac/1/api-key/MEBZF4AP3YI6PMX7F22LNQUCKI' | jq .
    {
      "description": "Sample API key",
      "roles": [
        {
          "id": "sample_id",
          "group": "sample_group"
        }
      ],
      "owner": "owner@example.com",
      "issued": "2017-03-23T15:35:44.287Z",
      "maskedKey": "um3a****************************************mkjc",
      "id": "MEBZF4AP3YI6PMX7F22LNQUCKI"
    }
   
Java Example:

```java
EmoApiKey apiKey = uac.getApiKey("MEBZF4AP3YI6PMX7F22LNQUCKI")
```

### Update an API key

Permissions required:

* `apikey|update` if either the owner or description is updated by the request
* `role|grant|{group}|{id}` for each role the request assigns to or unassigns from the API key

HTTP:

    PUT /uac/1/api-key/{id}

    Content-Type: "application/x.json-update-api-key
    
Java:
    
```java
updateApiKey(UpdateEmoApiKeyRequest request)
```

When updating an API key you only need to provide those attributes which you want changed.  Additionally, you can
incrementally add and remove individual roles without passing back the entire role set on each call.  The following
examples change the owner, unassign role "sample_id" and assign role "new_sample_id", both in group "sample_group".

As noted previously, the permissions required for updating an API depend on what attributes the request is updating.
For example, an update which only assigns a single new role only requires the `role|grant` permission for that role.
Also note that this same "grant" permission is used to both assign and unassign the role.  It is not possible for
a user to have permission to assign a role but not to unassign it, or vice versa.

Example:

    $ curl -s -XPUT "http://localhost:8080/uac/1/api-key/MEBZF4AP3YI6PMX7F22LNQUCKI" \
        -H "Content-Type: application/x.json-update-api-key" \
        -d '{"owner":"new_owner@example.com","unassignRoles":[{"group":"sample_group","id":"sample_id"}],"assignRoles":[{"group":"sample_group","id":"new_sample_id"}]}'
    {"success":true}
    
Java Example:

```java
uac.updateApiKey(
        new UpdateEmoApiKeyRequest("MEBZF4AP3YI6PMX7F22LNQUCKI")
                .setOwner("new_owner@example.com")
                .unassignRoles(ImmutableSet.of(new EmoRoleKey("sample_group", "sample_id")))
                .assignRoles(ImmutableSet.of(new EmoRoleKey("sample_group", "new_sample_id"))))
```

### Migrate an API key

Permissions required:

* `apikey|update`

HTTP:

    POST /uac/1/api-key/{id}/migrate

Java:

```java
String migrateApiKey(MigrateEmoApiKeyRequest request)
```

Migrating an API key maintains its ID and all associated attributes and roles but invalidates the existing private key
and issues a new one.  This is typically done either when a caller lost his private key or when the key has definitively
or potentially been leaked.

Example:

    $ curl -s -XPOST "http://localhost:8080/uac/1/api-key/MEBZF4AP3YI6PMX7F22LNQUCKI/migrate" | jq .
    {
      "id": "MEBZF4AP3YI6PMX7F22LNQUCKI",
      "key": "hyvdfvbfdmj6jvzsegp3yukmyw43e6f50tzfyyi3e3yawdct"
    }

Java Example:

```java
String newPrivateKey = uac.migrateApiKey(new MigrateEmoApiKeyRequest("MEBZF4AP3YI6PMX7F22LNQUCKI"))
```

### Delete an API key 

Permissions required:

* `apikey|delete`
* `role|grant|{group}|{id}` for each role currently assigned to the API key
 
HTTP:

    DELETE /uac/1/api-key/{id}
    
Java:

```java
void deleteApiKey(String id)
```

Example:

    $ curl -s -XDELETE "http://localhost:8080/uac/1/api-key/MEBZF4AP3YI6PMX7F22LNQUCKI"
    {"successs":true}
    
Java Example:

```java
deleteApiKey("MEBZF4AP3YI6PMX7F22LNQUCKI")
```
