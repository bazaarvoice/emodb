[![Build Status](https://travis-ci.com/bazaarvoice/emodb.svg?token=AwBevKKpyg5x21QsyKTV&branch=master)](https://travis-ci.com/bazaarvoice/emodb)

EmoDB
=====

Store your feelings here.

Written by [Bazaarvoice](http://www.bazaarvoice.com): see [the credits page](https://github.com/bazaarvoice/emodb/blob/master/Credits.md) for more details.

Introduction
------------

EmoDB is a RESTful HTTP server for storing JSON objects and for watching for changes
to those events.

It is designed to span multiple data centers, using eventual consistency (AP) and multi-master
conflict resolution.  It relies on [Apache Cassandra] (http://cassandra.apache.org/) for
persistence and cross-data center replication.

Documentation
-------------

[Release Notes] (markdowns/ReleaseNotes.md)

[System of Record API] (markdowns/SystemOfRecordApi.md)

[System of Record Deltas] (markdowns/Deltas.md)

[Databus API] (markdowns/DatabusApi.md)

[BlobStore API] (markdowns/BlobStoreApi.md)

[Queue API] (markdowns/QueueServiceApi.md)

[Stash API] (markdowns/StashApi.md)

[Hadoop and Hive Support] (markdowns/MapReduce.md)

[Operations] (markdowns/Operations.md)

[EmoDB SDK] (markdowns/Maven.md)

[API Keys] (markdowns/Security.md)

[Legacy Java Client Support] (markdowns/LegacyJavaClients.md)

Quick Start
-----------

### Installation

#### Using binaries:

1. Download the [EmoDB binaries] (https://github.com/bazaarvoice/emodb/releases)

2. Run the Emodb server locally. This will start zookeeper and cassandra locally.
    
        $ bin/start-local.sh
        ...
        INFO  [2012-05-14 19:12:19,802] org.eclipse.jetty.server.AbstractConnector: Started InstrumentedBlockingChannelConnector@0.0.0.0:8080
        INFO  [2012-05-14 19:12:19,805] org.eclipse.jetty.server.AbstractConnector: Started SocketConnector@0.0.0.0:8081
        # Use Ctrl-C to kill the server when you are done.

3.  Check that the server responds to requests (from another window):

        $ curl -s "http://localhost:8081/ping"
        pong

        $ curl -s "http://localhost:8081/healthcheck"
        * deadlocks: OK
        * emo-cassandra: OK
          127.0.0.1(127.0.0.1):9160 879us

4.  To erase the EmoDB data, simply delete the data folder:

        $ rm -rf bin/data/
        $ bin/start-local.sh

#### Using source code:

1. Download the [EmoDB source code] (https://github.com/bazaarvoice/emodb):

        $ git clone git@github.com:bazaarvoice/emodb.git emodb

2.  Build the source and run the tests:

        $ cd emodb
        $ mvn clean install

3.  Run the EmoDB server locally:

        $   cd web-local
        $ ./start.sh
        ...
        INFO  [2012-05-14 19:12:19,802] org.eclipse.jetty.server.AbstractConnector: Started InstrumentedBlockingChannelConnector@0.0.0.0:8080
        INFO  [2012-05-14 19:12:19,805] org.eclipse.jetty.server.AbstractConnector: Started SocketConnector@0.0.0.0:8081
        # Use Ctrl-C to kill the server when you are done.

4.  Check that the server responds to requests (from another window):

        $ curl -s "http://localhost:8081/ping"
        pong

        $ curl -s "http://localhost:8081/healthcheck"
        * deadlocks: OK
        * emo-cassandra: OK
          127.0.0.1(127.0.0.1):9160 879us

5.  To erase the EmoDB data and restart with a clean slate:

        $ cd web-local
        $ ./start-clean.sh

### Examples

The following examples assume you have [jq] (https://stedolan.github.io/jq/) or an equivalent (see Recommended
Software below).  It is optional-- `jq .` just formats the JSON responses to make them easier to read.

1.  Create a table in the System of Record.  Specify a "table template" with properties that will be returned with
    every object in the table:

        $ curl -s -XPUT -H "Content-Type: application/json" \
                "http://localhost:8080/sor/1/_table/review:testcustomer?options=placement:'ugc_global:ugc'&audit=comment:'initial+provisioning',host:aws-tools-02" \
                --data-binary '{"type":"review","client":"TestCustomer"}' | jq .
        {
          "success": true
        }

2.  Verify that the table was created as expected.  The result should be the table template.

        $ curl -s "http://localhost:8080/sor/1/_table/review:testcustomer" | jq .
        {
          "client": "TestCustomer",
          "type": "review"
        }

3.  Via the Databus, subscribe to changes on all tables containing reviews:

        $ curl -s -XPUT -H "Content-Type: application/x.json-condition" \
            "http://localhost:8080/bus/1/demo-app" \
            --data-binary '{..,"type":"review"}' | jq .
        {
          "success": true
        }

4.  Store a document in the System of Record:

        $ curl -s -XPUT -H "Content-Type: application/json" \
            "http://localhost:8080/sor/1/review:testcustomer/demo1?audit=comment:'initial+submission',host:aws-submit-09" \
            --data-binary '{"author":"Bob","title":"Best Ever!","rating":5}' | jq .
        {
          "success": true
        }

5.  Update the document in the System of Record:

        $ curl -s -H "Content-Type: application/x.json-delta" \
            "http://localhost:8080/sor/1/review:testcustomer/demo1?audit=comment:'moderation+complete',host:aws-cms-01" \
            --data-binary '{..,"status":"APPROVED"}' | jq .
        {
          "success": true
        }

6.  See what the document looks like after the update:

        $ curl -s "http://localhost:8080/sor/1/review:testcustomer/demo1" | jq .
        {
          "~deleted": false,
          "~firstUpdateAt": "2012-06-22T20:11:53.473Z",
          "~id": "demo1",
          "~lastUpdateAt": "2012-06-22T20:12:09.679Z",
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

7.  Look at the first 10 documents in the table, sorted arbitrarily:

        $ curl -s "http://localhost:8080/sor/1/review:testcustomer" | jq .
        [
          {
            "~deleted": false,
            "~firstUpdateAt": "2012-06-22T20:11:53.473Z",
            "~id": "demo1",
            "~lastUpdateAt": "2012-06-22T20:12:09.679Z",
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
        ]

8.  Poll the Databus to see the pending change events.  Note that generally there will be one event per update, but multiple
    updates to the same entity may be consolidated.  The current complete object is returned with each event.  The result will
    look something like:

        $ curl -s "http://localhost:8080/bus/1/demo-app/poll?ttl=30" | jq .
        [
          {
            "eventKey":"e9f5b640-caf8-11e1-96fe-0013e8cdbb13#review:testcustomer#demo1",
            "content":{
              "~deleted":false,"~lastUpdateAt":"2012-07-11T01:37:02.372Z","~id":"demo1",
              "~lastUpdateAt":"2012-07-11T01:37:13.351Z","~table":"review:testcustomer",
              "~signature": "7db2ef78f7830acaaa53f242a5e5ffa1","~version":2,
              "author":"Bob","client":"TestCustomer","rating":5,
              "status":"APPROVED", "title":"Best Ever!","type":"review"
            }
          }
        ]

9.  Acknowledge one of the Databus events to indicate we don't need it any more (copy the ID from the previous response):

        $ curl -s -XPOST -H "Content-Type: application/json" \
            "http://localhost:8080/bus/1/demo-app/ack" \
            --data-binary '["e9f5b640-caf8-11e1-96fe-0013e8cdbb13#review:testcustomer#demo1"]' | jq .
        {
          "success": true
        }

10.  Look at the timeline showing all System of Record changes to the object (modulo compaction):

        $ curl -s "http://localhost:8080/sor/1/review:testcustomer/demo1/timeline?audit=true" | jq .
        [
          {
            "timestamp": "2012-07-11T01:37:13.351+0000",
            "id": "f080f970-caf8-11e1-96fe-0013e8cdbb13",
            "delta": "{..,\"status\":\"APPROVED\"}",
            "audit": {
              "comment": "moderation complete",
              "host": "aws-cms-01",
              "~sha1": "4507332be7b42bd100a233be3847e5df99fbeb2d"
            }
          },
          {
            "timestamp": "2012-07-11T01:37:02.372+0000",
            "id": "e9f5b640-caf8-11e1-96fe-0013e8cdbb13",
            "delta": "{\"author\":\"Bob\",\"rating\":5,\"title\":\"Best Ever!\"}",
            "audit": {
              "comment": "initial submission",
              "host": "aws-submit-09",
              "~sha1": "33aef50cae4e44cc7be803054335bafdd375644b"
            }
          }
        ]

Recommended Software
--------------------

For debugging, it's useful to have a JSON pretty printer.  On a Mac with [Homebrew] (http://mxcl.github.com/homebrew/)
installed:

    brew install jq
    
Alternatively, you can use jsonpp
    
    brew install jsonpp

Alternatively, use Python's `json.tool`:

    alias jsonpp='python -mjson.tool'

Many of the examples include `jq` or `jsonpp`.  Running the examples without `jsonpp` will work just fine, but the results may
be more difficult to read.
