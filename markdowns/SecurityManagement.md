Security Management
===================

Prerequisites
-------------

If you are running Emo locally, using the provided binaries, the default admin API key is `local_admin`.

Before EmoDB can manage API keys it needs to be configured with a pair of reserved API keys:

1. Administrator: This API key has full admin access to EmoDB.
2. Replication: This API key is used to authenticate internal databus replication calls made by EmoDB between data centers.

To configure EmoDB with these keys follow the following steps:

1. Update the EmoDB `config.yaml` with temporary values for these keys:

```
auth:
  adminApiKey:       "dummy"
  replicationApiKey: "dummy"
```

2. Choose two keys.  The keys can be any valid string with no white space.  For this example we'll choose
   _pebbles_ and _bambam_ for the administration and replication keys respectively.

3. (Optional) Secure the keys.  Run the following DropWizard command.  If the cluster will be different than the one in `config.yaml`
   then specify a `cluster` option like in the examples below:

```
$ java -jar emodb-web-x.x.jar encrypt-configuration-api-key config.yaml --api-key pebbles --cluster local_cluster
RS9uq2Ukyj5WDijFLvWc/L2YYz6/MugvyAUfRknzhgJNyqe94IPU1wNpMF5WmXRrT1qEUDmVYoDE9Ku7NPmLGg

$ java -jar emodb-web-x.x.jar encrypt-configuration-api-key config.yaml --api-key bambam --cluster local_cluster
h6jqPR3/sMoY59wwUZaaJTWobLzqyqQhN0zPX69F7JE29flOaJj0kYBKZDH+mZJGP7M87ZUOcP7JVf8l+tMkmA
```

4. Update `config.yaml` with the actual values:

```
auth:
  adminApiKey:       "RS9uq2Ukyj5WDijFLvWc/L2YYz6/MugvyAUfRknzhgJNyqe94IPU1wNpMF5WmXRrT1qEUDmVYoDE9Ku7NPmLGg"
  replicationApiKey: "h6jqPR3/sMoY59wwUZaaJTWobLzqyqQhN0zPX69F7JE29flOaJj0kYBKZDH+mZJGP7M87ZUOcP7JVf8l+tMkmA"
```

At this point _pebbles_ has administrative access to EmoDB.  From this point onward either _pebbles_ or other API keys
with administrative access can manage API keys.


### Optionally Securing the Keys in config.yaml

As noted above securing the admin and replication API keys in `config.yaml` is optional; EmoDB would work just as well were
_pebbles_ and _bambam_ written in plaintext.  The risk in storing these keys in plaintext is that anyone on the EmoDB instance
with access to the configuration file can read the admin API key and therefore have full administrative access.  Additionally, you likely
will have a system in place for deploying EmoDB and that system will require `config.yaml` or some other dependency to contain
the API keys, such a file in a Puppet module or a deployment package stored in S3, and each one of these introduces a new vulnerability
for your admin API key.

Having said that, be aware that the cryptographic strength of the encrypted API keys is pretty weak.  With the current implementation
decrypting the keys can be done with only your configuration file and the source code for encrypting and decrypting the keys.  Therefore,
encrypting the keys should be considered only a base level encryption to prevent casual viewers and resource crawlers from reading the
configuration file's API keys.  You should still treat the encrypted API keys with the same level of protection as you would with any
other sensitive credential.

API Key Administration Task
---------------------------

All API Key management is performed using the DropWizard `api-key` task.  Each operation requires the API key of
an EmoDB administrator.

### Create API key

When creating an API key you can assign it one or more roles.  Each role determines what permissions the API key will
have.  See the [Role AdministrationTask] (#role-administration-task) for more details.

The following example creates a new API key with standard record and databus access:

```
$ curl -XPOST 'localhost:8081/tasks/api-key?action=create&APIKey=pebbles&owner=ermacs-dev@bazaarvoice.com&description=Ermacs+application&role=record_standard&role=databus_standard'
API key: bd1crrrqxt4rs8ktaezpripnseeron5rispeyjpr48u7bltg

Warning:  This is your only chance to see this key.  Save it somewhere now.
```

### View API key

You can view the metadata for an API key with the _view_ action as in the following example:

```
$ curl -XPOST 'localhost:8081/tasks/api-key?action=view&APIKey=pebbles&key=bd1crrrqxt4rs8ktaezpripnseeron5rispeyjpr48u7bltg'
owner: ermacs-dev@bazaarvoice.com
description: Ermacs application
roles: record_standard, databus_standard
issued: 11/13/14 11:55 AM
```

### Update API key

You can change the roles associated with an API key with the _update_ action as in the following example:

```
$ curl -XPOST 'localhost:8081/tasks/api-key?action=update&APIKey=pebbles&key=bd1crrrqxt4rs8ktaezpripnseeron5rispeyjpr48u7bltg&removeRole=record_standard&addRole=record_update'
API key updated

$ curl -XPOST 'localhost:8081/tasks/api-key?action=view&APIKey=pebbles&key=bd1crrrqxt4rs8ktaezpripnseeron5rispeyjpr48u7bltg'
owner: ermacs-dev@bazaarvoice.com
description: Ermacs application
roles: record_update, databus_standard
issued: 11/13/14 11:59 AM
```

### Migrate API key

If an API key is compromised you can migrate the key while retaining all metadata with the _migrate_ action as in the following example:

```
$ curl -XPOST 'localhost:8081/tasks/api-key?action=migrate&APIKey=pebbles&key=bd1crrrqxt4rs8ktaezpripnseeron5rispeyjpr48u7bltg'
Migrated API key: kp7w6odzin5zki7riqhduadisi7a6wa7cobbfbb379e3z6q5

Warning:  This is your only chance to see this key.  Save it somewhere now

$ curl -XPOST 'localhost:8081/tasks/api-key?action=view&APIKey=pebbles&key=kp7w6odzin5zki7riqhduadisi7a6wa7cobbfbb379e3z6q5'
owner: ermacs-dev@bazaarvoice.com
description: Ermacs application
roles: record_update, databus_standard
issued: 11/13/14 12:01 PM
```

### Delete API key

You can delete an API key with the _delete_ action as in the following example:

```
$ curl -XPOST 'localhost:8081/tasks/api-key?action=delete&APIKey=pebbles&key=kp7w6odzin5zki7riqhduadisi7a6wa7cobbfbb379e3z6q5'
API key deleted
```

Permissions
-----------

Permissions are used to control access to EmoDB's resources.  For example, if team A creates a queue for its own
project's internal use it would be harmful if team B were to poll and ack messages from that queue without team A's
knowledge or consent.  Permissions can be used to restrict the capabilities of an individual role, and assigning the
role to one or more API keys transitively limits the capabilities of those API keys.

A full list of possible permissions can be found in [Permissions.java] (https://github.com/bazaarvoice/emodb/blob/master/web/src/main/java/com/bazaarvoice/emodb/web/auth/Permissions.java).
The following section highlights the general format and nuances around SoR and Blob permissions.

### Permission format

In general permissions follow the format of "_context_|_action_|_resource_".  For example `databus|poll|subscription1`
indicates permission to perform the "poll" action on the databus subscription "subscription1".

For _actions_ and _resources_ the value can be one of the following:

1. A single value (e.g: `update`)
2. A wildcard value.  This can indicate either the entire value, `*`, or a portion, such as `get*`.
3. A conditional value (more on this later)

#### Context

The currently supported contexts are `sor`, `blob`, `queue`, `databus`, `facade` and `system`.  The context portion
of the permission must start with one of these values.  (It is technically possible to use a pure wildcard, `*`, although
this is discouraged.  The existing "admin" role already provides this capability.)

#### Action

The action restricts what the user can do within the context.  As such each context typically has its own set of actions
which may not have meaning in other contexts.  For example, `databus|poll` make sense but `blob|poll` does not and
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
[conditional] (Deltas.md#conditional) is exactly the same format
as used by deltas and databus subscriptions.  To create a conditional surround the condition string in an `if()`
statement.

#### Examples

Permission                                           | Effect
----------                                           | ------
`sor|if(in("update","create_table"))|*`              | Equivalent to having both `sor|update|*` and `sor|create_table|*`
`sor|if(not("drop_table"))|*`                        | User can perform all actions in the `sor` context except `drop_table`
`queue|*|if(and(like("team:*"),not("team:edward")))` | User can perform all actions on all queues matching `team:*` except `team:edward`

### Table conditionals

In all contexts except `sor` and `blob` a conditional in the _resource_ is evaluated using the resource name, such a the
name of a queue or databus subscription.  In `sor` and `blob` it is evaluated as a table conditional in exactly the
same way as when creating a databus subscription.

#### Examples

For the follow examples assume the SoR table "ermacs_data" has been created in placement "ugc_global:ugc" and was
created with template `{"team": "ermacs"}`.  (The common prefix of `sor|update|` has been removed from the first
column for readability.)


Resource in "sor" context                                                            | Matches table?   | Why
--------------------------------------------------------------                       | --------------   | ---
`ermacs_*`                                                                           | Yes              | Table name starts with prefix "ermacs_"
`if(intrinsic("~table":"ermacs_data"))`                                              | Yes              | Table name is an exact match
`if(intrinsic("~table":in("ermacs_data","ermacs_products")))`                        | Yes              | Table name is in the "in" condition
`if(intrinsic("~placement":'ugc_global:ugc'))`                                       | Yes              | Table placement is an exact match
`if(intrinsic("~placement":like("*:ugc")))`                                          | Yes              | Table placement is a like match
`if({..,"team":"ermacs"})`                                                           | Yes              | Table has attribute "team" set to "ermacs"
`if({..,"team":"ermacs","other":"attr"))`                                            | No               | Only one matching attribute is present on the table
`if(and(intrinsic("~table":like("ermacs_*")),intrinsic("~placement":like("*:ugc"))))` | Yes              | Table name and placement both match the respective like conditions
`if(and(intrinsic("~table":like("ermacs_*")),intrinsic("~placement":like("*:cat"))))` | No               | Only one of the conditions is met; placement does not end with "cat"


Role Administration Task
------------------------

EmoDB has several pre-defined roles that are always available.  You can see these roles and what permissions they
have in [DefaultRoles.java] (https://github.com/bazaarvoice/emodb/blob/master/web/src/main/java/com/bazaarvoice/emodb/web/auth/DefaultRoles.java)

The role administration task allows you to create new roles with custom permissions.  These roles can then be associated
with one or more API keys to provide fine controls over what actions the API key can perform.

As with the API keys, role administration requires an API key which has administrative access.  Continuing from the
previous example, _pebbles_ is an administrator and therefore can run this task.

### Create or update custom role

EmoDB does not distinguish between creating a new role and updating an existing role.  An API key can be associated with
any role name, even one that has not been created.  However, until an administrator explicitly assigns permissions
to the role assigning the role to an API key grants it no additional permissions.

The following example creates a role called "ermacs" with the following permissions:

* Full permissions on SoR tables with a template attribute "team" of "ermacs" and are in the "ugc_global:ugc" placement
* Full databus permissions for any subscription that starts with "ermacs_"
* Permission to poll any queue that starts with "ermacs_"

```
$ curl -XPOST 'localhost:8081/tasks/role?action=update&APIKey=pebbles&role=ermacs&permit=sor|*|if(and({..,"team":"ermacs"},intrinsic("~placement","ugc_global:ugc")))&permit=databus|*|ermacs_*&permit=queue|poll|ermacs_*'
Role updated.
ermacs has 3 permissions
- databus|*|ermacs_*
- queue|poll|ermacs_*
- sor|*|if(and({..,"team":"ermacs"},intrinsic("~placement","ugc_global:ugc")))
```

The following example demonstrates using a "revoke" parameter to remove a previously granted permission.  Note that the
permission must match exactly, no wildcards or conditionals are evaluated when permitting or revoking permissions:

```
$ curl -XPOST 'localhost:8081/tasks/role?action=update&APIKey=pebbles&role=ermacs&revoke=sor|*|if(and({..,"team":"ermacs"},intrinsic("~placement","ugc_global:ugc")))'
Role updated.
ermacs has 2 permissions
- databus|*|ermacs_*
- queue|poll|ermacs_*
```

### View custom role

You can view the permissions for a role with the _view_ action as in the following example:

```
$ curl -XPOST "localhost:8081/tasks/role?action=view&APIKey=pebbles&role=ermacs"
ermacs has 2 permissions
- databus|*|ermacs_*
- queue|poll|ermacs_*
```

### Check permission

You can test whether a role has a specific permission using the _check_ action as in the following examples:

```
$ curl -XPOST "localhost:8081/tasks/role?action=check&APIKey=pebbles&role=ermacs&permission=queue|poll|ermacs_queue1"
ermacs is permitted queue|poll|ermacs_queue1 by the following:
- queue|poll|ermacs_*

$ curl -XPOST "localhost:8081/tasks/role?action=check&APIKey=pebbles&role=ermacs&permission=databus|subscribe|ermacs_subscription1"
ermacs is permitted databus|subscribe|ermacs_subscription1 by the following:
- databus|*|ermacs_*

$ curl -XPOST "localhost:8081/tasks/role?action=check&APIKey=pebbles&role=ermacs&permission=databus|subscribe|inaccessible"
ermacs is not permitted databus|subscribe|inaccessible
```

### Delete custom role

As previously stated EmoDB does not distinguish between a non-existent role and a role with no permissions.  To remove
all permissions from a role use the _delete_ action as in the following example:

```
$ curl -XPOST "localhost:8081/tasks/role?action=delete&APIKey=pebbles&role=ermacs"
Role deleted

$ curl -XPOST "localhost:8081/tasks/role?action=view&APIKey=pebbles&role=ermacs"
ermacs has 0 permissions
```

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
