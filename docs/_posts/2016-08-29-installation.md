---
layout: post
title: "Installation"
date: 2016-05-22
excerpt: "Setting up EmoDB"
tags: [Installation, EmoDB]
type: [blog]
---

Installation
============

So you've gone through the [Quick Start](../quickstart), run EmoDB locally,
and are ready to try it out in a small-scale environment.  This document describes the minimum amount of
setup required and some guidelines on setting up your first "real world" EmoDB cluster.

### Caveats

* As much as we'd like to help you all the way from start to finish there are several steps that are either already well
  documented elsewhere or simply too unique to each environment to create a one-size-fits-all document.  This document
  will refer you to much better documentation for the former and provide general guidelines for the latter.
* There are a great many possible configurations possible with EmoDB, such as the number of data centers and placements
  you choose to start with, and which is the "right" configuration really depends on your situation.
  This document describes just one possible configuration, but you are encouraged to expand from there to whatever best
  fits your needs.

### Prerequisites

Before you can run EmoDB there are two other dependencies that must be deployed first.

#### [Apache ZooKeeper](https://zookeeper.apache.org/)

ZooKeeper is a commonly used service that provides services such as service location and distributed state
synchronization.  EmoDB is dependent on there being a ZooKeeper cluster available, so if you don't already have one
standing you'll need to do that first.  You can learn more about ZooKeeper and administering your own cluster
[here](https://zookeeper.apache.org/doc/trunk/).

Note that in a multi-data center deployment you should have a unique ZooKeeper cluster in each data center for the
respective EmoDB cluster to use.  Routing all EmoDB clusters through a single ZooKeeper is not necessary and would
be detrimental due to the latency each non-local EmoDB cluster would incur.

### [Cassandra](https://cassandra.apache.org/)

EmoDB uses Cassandra to provide a robust data store with high availability, reliability, and cross-data center
replication.  There are myriad ways of configuring and deploying Cassandra, and what's worked best for us may not
be what works best for you.  Whether you already have a Cassandra infrastructure in place, deploy from open
source, or go with an enterprise provider like DataStax, you'll have to go with the solution which makes the most
sense for you.


With that in mind, there are a few suggestions and requirements for your Cassandra stack to work with EmoDB.

* EmoDB is only compatible with the 2.x versions of Cassandra.  Cassandra 3.0 changed the way it stores hinted-handoffs
  and the new method is incompatible with the way EmoDB determines full-consistency lag.
* Additionally, while EmoDB works with the latest 2.2 Cassandra we've seen performance issues running EmoDB with
  Cassandra 2.1+ which only showed up at high scale.  We haven't completed our investigation into root cause and it's
  possible that EmoDB and Cassandra 2.2 can be configured to work well together.  However, we found that by using
  Cassandra 2.0 we saw no similar performance degradation under comparable high EmoDB load.  We currently use
  Cassandra 2.0.17 and recommend you do the same, but feel free to try any 2.x Cassandra as your mileage may vary.
* EmoDB uses a custom key format that provides a contiguous layout of records by table for efficient scanning
  while also balancing records around the ring to prevent hot spots.  For this reason the Cassandra ring used by the
  System of Record (SoR) and Blob stores must be configured with a byte-ordered partitioner,
  `org.apache.cassandra.dht.ByteOrderedPartitioner`.  If you use a separate Databus and Queue ring (which we do) then
  that ring should use a standard random partitioner.
* Using v-nodes with EmoDB is a topic of some debate.  On the one hand token maintenance is greatly simplified by
  using v-nodes.  On the other hand the custom partitioning performed by EmoDB assumes the keys are laid out in a physical
  ring which roughly follows the token ring, and the unpredictable layout of v-nodes in the cluster could result in there
  being hot spots when scanning a table, getting table splits, or performing an Emo Stash extract.  In the end it's up
  to you whether to use v-nodes and monitor what load imbalances result in your case, if any.
* The data in EmoDB is only as good as the underlying data in Cassandra.  We recommend following the Cassandra
  maintenance guidelines as closely as possible, such as monitoring your ring's health and performing regular repairs.
  
  
### Setting Up Your Cassandra Rings

A typical starter EmoDB deployment uses two Cassandra rings:  one for the SoR and Blob Store, and one for the Databus
and Queue services.  As previously noted, the SoR and Blob Store Cassandra ring must use a byte-ordered partitioner.
If your EmoDB cluster spans multiple data centers then each data center should have its own unique
Databus and Queue ring, since Databus and Queue contents are specific to the local EmoDB cluster.

Although EmoDB only requires one SoR and Blob Store ring there are cases where multiple rings are desirable.
Here are some examples:

* A simple separation is to create one ring for the SoR and a separate ring for the Blob store.  Your
  application may have different usage patterns for these areas.  For example, a high frequency of large blob traffic
  may become a thread and/or network bottleneck for simultaneous SoR requests.  Separating the two allows
  for tuning each to the guarantees your use case requires.
* You may be able to lower costs by separating rings according to record access patterns.  For example,
  one class of records (class-A) may have a small number of records and high read/write frequency, while a second class
  (class-B) may have a much larger number of records but overall the frequency of read/write operations is much lower.
  The Cassandra ring for class-A records can use a small number of expensive nodes tuned for its traffic needs while
  a separate Cassandra ring for class-B records can be much larger but use cheaper, less powerful nodes while still
  providing the required service level.

Note that you don't have to make these decisions from the beginning and there is no penalty for not getting it right.
If after a while you find that splitting or joining SoR and/or Blob rings makes sense then EmoDB provides a seamless
path for migrating your data to a new ring with full availability and no downtime for read or write operations
while the records are migrated.

Once you've chosen your initial Cassandra topology and all of your rings are available then they are EmoDB ready.
EmoDB will automatically create any keyspaces and tables it requires based in its configuration files.

### Choosing Your Placements

The way EmoDB knows where to write records is determined by *placements*.  A placement is basically a logical grouping
of tables.  All tables in the same placement share the same Cassandra ring and replication rules.  For example,
consider the common case where similar data is collected globally but, due to local privacy rules, cannot be
geographically replicated outside of the source region.  For simplicity's sake, assume your ring has two data centers,
one in the United States and one in Europe, and some of your data legally must remain in each of these regions.
You could create a single Cassandra ring and use three placements: "us_only:dat", "eu_only:dat", and "global:dat",
configured such that "us_only:dat" and "eu_only:dat" have a replication strategy that only replicates in their
corresponding regions while the replication strategy for "global:dat" replicates to both data centers.  The
creation of a table in one of these three placements will determine how and where the data is replicated, but in all
three cases there is only a single Cassandra ring to maintain.

There is no set limit to the number of Cassandra rings your EmoDB cluster uses or the number of placements in each cluster,
although there are practical limits preventing, say, creating hundreds of distinct Cassandra rings for a single
EmoDB cluster.  We advise starting simple with sufficient rings and placements to cover your known use cases, such as
with a single Databus/Queue ring and a single SoR/Blob ring with a simple collection of placements, then growing from
there if necessary.

### Configuring Placements

Once you've chosen your Cassandra ring topology and placements it's time to configure them in EmoDB.  There are two files
that need to be configured, `config-ddl.yaml` and `config.yaml`.  The former gives a Cassandra-centric description your
placement's toplogy.  The latter is a general DropWizard configuration file for the EmoDB application as a whole.
There are many configuration options here, but for now we're going to focus only on the portions related to configuring
the Cassandra placements in the SoR.  There are similar sections for configuring the Blob Store, Databus, and Queue
services which largely mirror the SoR in terms of Cassandra configuration.

For this example we're going to use the following topology:

<img src="{{ site.baseurl }}{{ site.images_url }}/placements.svg">

In this example there are two data centers, US and EU, and two SoR Cassandra rings, "data" and "social".  The "data"
ring has three keyspaces:  "data_global", "data_us", and "data_eu", each of which has a replication strategy which
determines which data centers the placements are available in.  For example, tables placed in "data_global:app"
will have their content available in and replicated to both data centers, while tables placed in "data_us:app" will
only have their content available in the US data center.  The "social" ring is similarly configured.

Note that "data_global" contains two placements, "data_global:logs" and "data_global:app".  Both placements are in the
same Cassandra ring and have the same replication strategy, so why not combine them into a single placement?
There are several reasons why you may create multiple placements in the same keyspace.

* If you choose to enable Emo Stash then which tables are exported is configured at the placement level.  You may
  choose to only export some tables in the "data_global" keyspace, so separating them by placement makes that possible.
* EmoDB table permissions can be set at the placement level.  For example, you could create an API key that has
  read permission for all tables in "data_global:logs" but not "data_global:app".

#### `config-ddl.yaml`

The following example demonstrates the relevant section of `config-ddl.yaml` for configuring the SoR for the EmoDB
cluster in the US data center.

```
systemOfRecord:
   keyspaces:
      data_global:
         replicationFactor: 3
         tables:
            logs:
               keyspace: data_global
               table.delta_v2: logs_delta_v2
               table.history: logs_history
            app:
               keyspace: data_global
               table.delta_v2: app_delta_v2
               table.history: app_history
      data_us:
         replicationFactor: 3
         tables:
            app:
               keyspace: data_us
               table.delta_v2: app_delta_v2
               table.history: app_history
      social_global:
         replicationFactor: 3
         tables:
            app:
               keyspace: social_global
               table.delta_v2: social_delta_v2
               table.history: social_history
      social_us:
         replicationFactor: 3
         tables:
            app:
               keyspace: social_us
               table.delta_v2: social_delta_v2
               table.history: social_history
```

Note the following:

* The keyspace name and table prefixes must match the first and second parts of the placement name around the colon.
  For example, "data_global:logs" is in keyspace "data_global" and all the tables associated with it start
  with "logs".
* Because this is the configuration file for the US data center only placements which are visible in that data center
  should be included.  The EmoDB cluster in the EU data center should have a similar configuration file but with the
  US placements removed and the EU placements added.
* There is no indication at this level of which ring each placement is on.  Rather, this configuration file only
  focuses on how each placement's keyspaces and tables are configured.  The association of keyspaces to rings is done
  in `config.yaml`.

#### `config.yaml`

The following example demonstrates the relevant section of `config.yaml` for configuring the SoR for the EmoDB
cluster in the US data center.  Portions of the configuration not directly related to our topology have been removed
for readability.

```
systemOfRecord:

  validTablePlacements:
  - "data_global:logs"
  - "data_global:app"
  - "data_us:app"
  - "data_eu:app"
  - "social_global:posts"
  - "social_us:posts"
  - "social_eu:posts"

  cassandraClusters:
    data:
      cluster: data
      dataCenter: us
      seeds: 10.0.0.1,10.0.0.2
      thriftPort: 9160
      cqlPort: 9164
      maxConnectionsPerHost: 30
      latencyAware: true
      partitioner: bop
      keyspaces:
        data_global:
          healthCheckColumnFamily: app_delta
        data_us:
          healthCheckColumnFamily: app_delta

    social:
      cluster: social
      dataCenter: us
      seeds: 10.0.0.51,10.0.0.52
      thriftPort: 9160
      cqlPort: 9164
      maxConnectionsPerHost: 30
      latencyAware: true
      partitioner: bop
      keyspaces:
        social_global:
          healthCheckColumnFamily: social_delta
        social_us:
          healthCheckColumnFamily: social_delta
```

Note the following:

* As with the previous configuration file only clusters and keyspaces which should be visible in the local data center
  should be included.  In the EU the `dataCenter` attributes would be changed to the local Cassandra data center name
  in the EU, such as "eu", and the `keyspaces` maps would omit the US keyspaces and include the EU keyspaces.
* The `healthCheckColumnFamily` just needs to be any table from any of the placements in the keyspace.  EmoDB will
  occasionally run small queries against this column family as a connectivity health check.
* The `validTablePlacements` attribute includes all placements, including those not visible in the local data center.  This is
  necessary to provide each data center a total view of all possible placements, even those not available locally.
* You will likely need to come up with your own solution for initializing the Cassandra seeds.  This example demonstrates
  explicitly setting the seeds in the configuration file.  While EmoDB also supports Cassandra seed location using
  ZooKeeper service discovery that procedure is a little to involved for this document, but it should be covered in a
  future post.

#### System Placement

In addition to the placements you've created for your data there is an additional placement required for storing
system data, such as table metadata and authentication information.  The only requirement for this placement is that it
must be replicated to all data centers.  So technically, from our example above we could designate any one of
"data_global:logs", "data_global:app", or "social_global:posts" as the system data center.  Although there are
built-in safeguards against non-administrators accessing system tables we recommend completely separating them
by creating a placement specifically for system data.  For example, you could create a new placement on the "data"
ring called "data_global:sys" which would reside in the "data_global" keyspace.  This would satisfy the requirement
to be globally replicated and require no additional Cassandra upkeep beyond the existing infrastructure.


#### System Data Center

While most operations can be performed in any data center this is not the case for operations concerning table metadata,
what in a traditional database would be considered DDL operations.  There needs to be global synchronization on these
changes to prevent internal inconsistencies, such as could happen if two users attempt to create a table with the same
name at the same time.

To support this EmoDB requires one data center is designated as the system data center.  Most local requests are served
by the local EmoDB cluster.  However, any requests involving table metadata are forwarded to the system data center.
The system data center then uses its local ZooKeeper to serialize these operations within the local cluster, with the
end result being that they are serialized globally.

If your EmoDB cluster only has one data center then your choice is easy.  If you have more than one data center then
arbitrarily pick one as the system data center.  You need to ensure that there is a URL available for the non-system
data centers to make API calls to the system data center.  In `config.yaml` the `dataCenter` portion needs to be
configured with information about both the local and system data centers.  For example, the following are sample
configurations from our previous example, assuming the US data center is chosen as the system data center:

In the US data center:

```
dataCenter:
  currentDataCenter: emodb-us
  cassandraDataCenter: us
  systemDataCenter: emodb-us
  dataCenterServiceUri: http://emodb.us.mydomain.com:80
  dataCenterAdminUri: http://emodb.us.mydomain.com:81
  systemDataCenterServiceUri: http://emodb.us.mydomain.com:80
```

In the EU data center:

```
dataCenter:
  currentDataCenter: emodb-eu
  cassandraDataCenter: eu
  systemDataCenter: emodb-us
  dataCenterServiceUri: http://emodb.eu.mydomain.com:80
  dataCenterAdminUri: http://emodb.eu.mydomain.com:81
  systemDataCenterServiceUri: http://emodb.us.mydomain.com:80
```

#### EmoDB Cluster Name

The entire EmoDB cluster needs to have a name.  The actual value is open-ended and you can create your own naming
convention.  The cluster name is used so each instance in the EmoDB cluster can discover each other.  The cluster
name is set in `config.yaml`, such as in this example:

```
cluster: emodb_qa
```

### Step 3... Success!

... almost!  Obviously you are not quite done at this point.  There is still more configuration and installation to be
done before you are up and running.  However, most of the major configuration is done at this point.  There are examples
of `config.yaml` from the EmoDB code [here](https://github.com/bazaarvoice/emodb/blob/master/web-local/config-local.yaml)
and [here](https://github.com/bazaarvoice/emodb/blob/master/sdk/src/main/resources/emodb-default-config.yaml), and
although these are geared toward running EmoDB locally they should provide a decent starting point for configuring
the rest of your EmoDB cluster.

Once your configuration is ready you need to get the configuration files and the main emodb-web jar installed on
your EmoDB instances however best makes sense for you (Puppet, instance image, and so on).  From there you just need
to start the application.  For example:

```
/usr/bin/java -Xmx3G -jar /path/to/emodb-web.jar server /path/to/config.yaml /path/to/config-ddl.yaml
```