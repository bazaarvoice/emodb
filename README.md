[![Build Status](https://travis-ci.org/bazaarvoice/emodb.svg?branch=master)](https://travis-ci.org/bazaarvoice/emodb)

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

[System of Record API] (https://bazaarvoice.github.io/emodb/sor/)

[System of Record Deltas] (https://bazaarvoice.github.io/emodb/deltas/)

[Databus API] (https://bazaarvoice.github.io/emodb/databus/)

[BlobStore API] (https://bazaarvoice.github.io/emodb/blobstore/)

[Queue API] (https://bazaarvoice.github.io/emodb/queue/)

[Stash API] (https://bazaarvoice.github.io/emodb/stash/)

[Hadoop and Hive Support] (https://bazaarvoice.github.io/emodb/mapreduce/)

[Operations] (markdowns/Operations.md)

[EmoDB SDK] (https://bazaarvoice.github.io/emodb/maven/)

[API Keys] (https://bazaarvoice.github.io/emodb/security/)

[API Key Management] (https://bazaarvoice.github.io/emodb/securityadmin/)

[Legacy Java Client Support] (markdowns/LegacyJavaClients.md)

Quick Start
-----------

[Quick Start Guide] (https://bazaarvoice.github.io/emodb/quickstart/)
