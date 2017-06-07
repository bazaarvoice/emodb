---
layout: post
title: "Security Management"
date: 2016-05-17
excerpt: "EmoDB Security Administration"
tags: [Security Administration, EmoDB]
type: [blog]
---

Security Management
===================

Prerequisites
-------------

If you are running Emo locally, using the provided binaries, the default admin API key is `local_admin`.

Before EmoDB can manage API keys it needs to be configured with a pair of reserved API keys:

1. Administrator: This API key has full admin access to EmoDB.
2. Replication: This API key is used to authenticate internal databus replication calls made by EmoDB between data centers.
{:.workflow}

To configure EmoDB with these keys follow the following steps:

1.  Update the EmoDB `config.yaml` with temporary values for these keys:

        auth:
          adminApiKey:       "dummy"
          replicationApiKey: "dummy"

2. Choose two keys.  The keys can be any valid string with no white space.  For this example we'll choose
   __pebbles__ and __bambam__ for the administration and replication keys respectively.

3. (Optional) Secure the keys.  Run the following DropWizard command.  If the cluster will be different than the one in `config.yaml`
   then specify a `cluster` option like in the examples below:

       $ java -jar emodb-web-x.x.jar encrypt-configuration-api-key config.yaml --api-key pebbles --cluster local_cluster
        RS9uq2Ukyj5WDijFLvWc/L2YYz6/MugvyAUfRknzhgJNyqe94IPU1wNpMF5WmXRrT1qEUDmVYoDE9Ku7NPmLGg

       $ java -jar emodb-web-x.x.jar encrypt-configuration-api-key config.yaml --api-key bambam --cluster local_cluster
       h6jqPR3/sMoY59wwUZaaJTWobLzqyqQhN0zPX69F7JE29flOaJj0kYBKZDH+mZJGP7M87ZUOcP7JVf8l+tMkmA

4. Update `config.yaml` with the actual values:

       auth:
         adminApiKey:       "RS9uq2Ukyj5WDijFLvWc/L2YYz6/MugvyAUfRknzhgJNyqe94IPU1wNpMF5WmXRrT1qEUDmVYoDE9Ku7NPmLGg"
         replicationApiKey: "h6jqPR3/sMoY59wwUZaaJTWobLzqyqQhN0zPX69F7JE29flOaJj0kYBKZDH+mZJGP7M87ZUOcP7JVf8l+tMkmA"
{:.workflow}

At this point __pebbles__ has administrative access to EmoDB.  From this point onward either __pebbles__ or other API keys
with administrative access can manage API keys.


### Optionally Securing the Keys in config.yaml

As noted above securing the admin and replication API keys in `config.yaml` is optional; EmoDB would work just as well were
__pebbles__ and __bambam__ written in plaintext.  The risk in storing these keys in plaintext is that anyone on the EmoDB instance
with access to the configuration file can read the admin API key and therefore have full administrative access.  Additionally, you likely
will have a system in place for deploying EmoDB and that system will require `config.yaml` or some other dependency to contain
the API keys, such as a file in a Puppet module or a deployment package stored in S3, and each one of these introduces a new vulnerability
for your admin API key.

Having said that, be aware that the cryptographic strength of the encrypted API keys is pretty weak.  With the current implementation
decrypting the keys can be done with only your configuration file and the source code for encrypting and decrypting the keys.  Therefore,
encrypting the keys should be considered only a base level encryption to prevent casual viewers and resource crawlers from reading the
configuration file's API keys.  You should still treat the encrypted API keys with the same level of protection as you would with any
other sensitive credential.

Anonymous Access
----------------

If you choose you can configure EmoDB to allow anonymous access.  An anonymous user has full permission to perform
most standard operations in the data store, blob, databus, and queue services (see
[DefaultRoles.java](https://github.com/bazaarvoice/emodb/blob/master/web/src/main/java/com/bazaarvoice/emodb/web/auth/DefaultRoles.java#L126)
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
