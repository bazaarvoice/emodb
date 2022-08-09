Java Clients
===================

The current version of EmoDB uses DropWizard 7.1.  The Java clients returned by the
[Data Store] (https://github.com/bazaarvoice/emodb/blob/main/sor-client/src/main/java/com/bazaarvoice/emodb/sor/client/DataStoreClientFactory.java),
[Blob Store] (https://github.com/bazaarvoice/emodb/blob/main/blob-client/src/main/java/com/bazaarvoice/emodb/blob/client/BlobStoreClientFactory.java),
[Databus] (https://github.com/bazaarvoice/emodb/blob/main/databus-client/src/main/java/com/bazaarvoice/emodb/databus/client/DatabusClientFactory.java),
[Queue] (https://github.com/bazaarvoice/emodb/blob/main/queue-client/src/main/java/com/bazaarvoice/emodb/queue/client/QueueClientFactory.java),
and [Dedup Queue] (https://github.com/bazaarvoice/emodb/blob/main/queue-client/src/main/java/com/bazaarvoice/emodb/queue/client/DedupQueueClientFactory.java)
client factories use dependencies matching that version of DropWizard.

Historically EmoDB version up to and including 3.33 used DropWizard 6.2.  There have been numerous API updates and
bug fixes since that release and all projects using older versions of EmoDB are encouraged to upgrade.  However, this
would also require those projects to upgrade to DropWizard 7 -- or at least upgrade to using
the same shared dependencies.
