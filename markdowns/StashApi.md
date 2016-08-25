Stash
=====

What Stash Is
-------------

EmoDB provides APIs for querying individual records or scanning all records for a particular table.  This is useful if
your application requires the latest possible version of all records.  However, for large operations such as map/reduce
jobs that will scan the entire contents of one or more tables this process can be slow and, in numbers, put a good
deal of strain on the EmoDB servers.

EmoDB Stash is a process which takes a daily view of all EmoDB tables and places them in S3.  If your operation meets
the following criteria then Stash may be a good fit for you:

1. You don't need up-to-the-second data and can tolerate data that is several hours to a day stale.
2. You scan every record in one or more tables.
3. You want fast access to the data without risk of being rate-limited by EmoDB.

Where Stash Is
--------------

Stash data lives in S3 in the following buckets:

Region    | Bucket
------    | ------
us-east-1 | emodb-us-east-1
eu-west-1 | emodb-eu-west-1

Each region contains data from the corresponding region's EmoDB server.  For example, if a table is available in
eu-west-1 but not in us-east-1 then similarly it will only be in the eu-west-1 Stash version of the table.

Within each bucket the stash for each universe is prefixed by `stash/{universe}`.  For example, the cert stash
in us-east-1 is located under the S3 prefix `s3://emodb-us-east-1/stash/cert`.

What Stash Is Not
-----------------

#### A point-in-time snapshot

The stash process for scanning and writing data to S3 can take hours.  As such Stash should not be considered a
point-in-time snapshot of EmoDB data.  Your application needs to be prepared to handle potential inconsistencies caused
by related records being stashed at different times. 

A recommended approach to bootstrap from Stash that gets you up to date with EmoDB is described in the section 
[Bootstrapping Using Stash] (StashApi.md#bootstrapping-using-stash) below.

#### A historical daily record

Stash runs daily and as such there can be numerous copies of Stash in S3 at any given time.  However, each stash is
automatically deleted after seven days.  You should not use Stash as long-term daily records of EmoDB
content.  Additionally, if you have an operation can run for more than seven days then you will either have
to switch to a newer Stash copy mid-operation or copy the data to secondary storage before the Stash you are using
is more than seven days old.

Pre-requisites
--------------

Since the Stash data lives in S3 your resource must have access to the appropriate bucket and objects:

Action        | Resource
--------      | ---------
s3:ListBucket | arn:aws:s3:::{bucket}
s3:GetObject  | arn:aws:s3:::{bucket}/stash/{universe}/*

For example, if you need to access the cert Stash in us-east-1 then your resource could have the following permissions
in its policy:

```json
{
    "Effect": "Allow",
    "Sid": "StashS3ListBucket",
    "Action": [
        "s3:ListBucket"
    ],
    "Resource": [
        "arn:aws:s3:::emodb-us-east-1"
    ]
},
{
    "Effect": "Allow",
    "Sid": "StashS3GetObject",
    "Action": [
        "s3:GetObject"
    ],
    "Resource": [
        "arn:aws:s3:::emodb-us-east-1/stash/cert/*"
    ]
}
```

Java Stash Library
------------------

If your application uses Java then the simplest way to access Stash data is with the Stash API.  The Stash API is
included in the [System of Record API] (SystemOfRecordApi.md) and
requires a `DataStore` instance for Stash location; follow the instructions on the System of Record page to include it
in your project and create a `DataStore`.

Stash access is provided using `DataStoreStash`:

```java
DataStore dataStore = createDataStore();

DataStoreStash stash = DataStoreStash.getInstance(dataStore);
```

By default DataStoreStash uses your instance's credentials for S3 access; if necessary there is an alternate
method to explicitly pass in the S3 access and secret keys.

### Get the Stash Time

You can find out when the latest Stash was taken using `getStashTime()`:

```java
Date stashTime = stash.getStashTime();
```

This date signifies the time the stash was started.  It is guaranteed that any updates in EmoDB before this time are
present in the stash; updates in EmoDB after this time may or may not be in the stash.

### Locking the Stash Time

Since Stash is created daily it is possible if you have a long running operation that a newer Stash will become
available while your operation is still running.  By default the Stash API always uses the latest version of Stash
available.  To keep a consistent view of Stash you can lock the stash time:

```java
stash.lockStashTime();
```

After locking the stash time all future operations on this instance will use the most recently available stash at the
time it was locked.  However, as previously stated be aware that stash data is deleted after seven days.  Locking can
be reversed by calling `unlockStashTime()`.

### Getting and Reading Splits

As with the System or Record Stash data is returned as splits.  The following example compares getting and reading
splits using the System of Record versus Stash:

#### Using System of Record

```java
Collection<String> splits = dataStore.getSplits(table, 10000);

for (final String split : splits) {
    // Single-threaded for simplicity
    for (Map<String, Object> row : DataStoreStreaming.getSplit(dataStore, table, split, ReadConsistency.STRONG)) {
        // process row
    }
}
```

#### Using Stash

```java
Collection<String> splits = stash.getSplits(table);

for (final String split : splits) {
    // Single-threaded for simplicity
    try (StashRowIterable rows = stash.getSplit(table, split)) {
        for (Map<String, Object> row : rows) {
            // process row
        }
    }
}
```

Key differences:

* The System of Record allows the caller to request a split size. Splits in Stash are precomputed and therefore the API
  does not support a requested split size.

* Stash returns a `StashRowIterable` which should be closed once the caller is done using it.  This is done to close
  the streaming S3 connection after reading the split.

### Scanning a table

As with the System of Record the Stash API also provides a single call synchronously to scan all records in a table.
The following example compares scanning using the System of Record versus Stash:

#### Using System of Record

```java
for (Map<String, Object> row : DataStoreStreaming.scan(dataStore, table, ReadConsistency.STRONG)) {
    // process row
}
```

#### Using Stash

```java
try (StashRowIterable rows = stash.scan(table)) {
    for (Map<String, Object> row : rows) {
        // process row
    }
}
```

Key differences:

* As with a Stash split the Stash scan returns an `StashRowIterable` which should be closed when no longer needed.

* Unlike with System of Record there is no performance difference between splits and scans; the Stash scan call is a
  convenience method for sequentially reading all splits from S3.

Bootstrapping Using Stash
-------------------------

Services such as Polloi can use Stash to bootstrap tables.  Bootstrapping from Stash does not use any EmoDB resources
and is therefore limited only by access to S3.  To bootstrap using Stash follow these steps:

1. Create a databus subscription for the tables you are bootstrapping.  See the
   [Databus API] (DatabusApi.md) for details.

2. Request a replay of the past two days for the subscription.  This will pick up any updates that took place
   since the Stash start time.  See the "Databus Replay" section in the
   [Databus API] (DatabusApi.md).

3. Use the Stash splits to bootstrap your tables.

**Caution**

If you start processing the subscription while reading the Stash splits it is possible that you will receive a
record on the databus and then at a later time read the same record from Stash.  The Stash record will be
*older* than the databus record and therefore should not replace the databus version.  Here are two suggested
methods to prepare for this circumstance:

* Perform a version check on the Stash record against any existing record prior to persisting it.
* Defer processing the databus until all Stash tables have been bootstrapped.

Rolling Your Own Access
-----------------------

If you are not using Java or choose not to use the Stash API you can access the Stash files directly.  The following
section details how.

### Stash S3 Layout

The top level S3 directory for a Stash consists of individual Stash root directories named for the Stash time plus
a special file called `_LATEST`.  For example, the following may represent the directory contents of
`s3://emodb-us-east-1/stash/cert`:

```
(dir)  2015-02-21-00-00-00
(dir)  2015-02-22-00-00-00
(dir)  2015-02-23-00-00-00
(dir)  2015-02-24-00-00-00
(file) _LATEST
```

Each Stash root contains one directory for each table where the following characters have been substituted for
wider compatibility:

Original table name character | Stash directory character
----------------------------- | -------------------------
: | ~

Note that since EmoDB enforces table names contain only lower-case characters the above substitutions are unambiguous.

For example, the following is a possible subset of directories under a Stash root for client "testclient1":

```
(dir) answer~testclient1
(dir) catalog~testclient1~
(dir) review~testclient
```

Each table directory contains one or more gzipped files.  Each file contains one or more rows of JSON separated by
newlines, and each JSON row is an EmoDB document from the table.  For example, the following may represent the contents
of the table directory for "answer~testclient1':

```
(file) answer~testclient1-0d-bdd29a20d201740e-1294.json.gz
(file) answer~testclient1-4d-bdd29a20d201740e-1417.json.gz
(file) answer~testclient1-8d-bdd29a20d201740e-870.json.gz
(file) answer~testclient1-cd-bdd29a20d201740e-1057.json.gz
```

The following example demonstrates the contents of one of these files once uncompressed:

```json
{"lastPublishTime":"2012-03-08T23:30:06.000Z","campaignId":"BV_QA_PORTAL_ANSWER_MANAGEMENT","sendEmailAlertWhenPublished":false,"about":{"~id":"65bdfafd-a06c-5d5f-9098-757c1e0ec0e8","~table":"question:testclient1"},"displayLocale":"en_US","featured":false,"contentCodes":["ABC"],"submissionId":"5z32qyiaguj0ofhg2ziz89i0x","contributor":{"~id":"b1c5010a-bebe-5d24-870f-acf305e587eb","~table":"contributor:testclient1"},"legacyInternalId":675689,"displayCode":"123","firstPublishTime":"2012-03-08T23:30:06.000Z","text":"Sample answer\nnumber 1","clientComments":{"c8df6800-6975-11e1-8001-000000000000":{"comment":"Approved by client","name":"Emily"}},"status":"APPROVED","agreedToTermsAndConditions":false,"displayAsAnonymous":false,"submissionTime":"2012-03-08T23:23:55.000Z","browserLocale":"en_GB","language":"en","space":"testclient1","type":"answer","client":"testclient1","~id":"117b20c5-67ce-5445-ab01-e3a9e1da59e1","~table":"answer:testclient1","~version":2,"~signature":"d9bbf05e1e537bc53ca6a253ffdc1d7b","~deleted":false,"~firstUpdateAt":"2013-01-25T19:20:09.413Z","~lastUpdateAt":"2013-10-21T12:02:33.280Z"}
{"lastPublishTime":"2011-01-21T00:15:10.000Z","productReferences":[{"~id":"product::testclient1_woman_testclient1gt150_gt_ss_dash_crewe","~table":"catalog:testclient1:"},{"~id":"product::testclient1_woman_testclient1gt260_baselayer_express_legging","~table":"catalog:testclient1:"},{"~id":"product::testclient1_woman_socksrun_run_ultralite_lowcut","~table":"catalog:testclient1:"}],"campaignId":"BV_QA_PORTAL_ANSWER_MANAGEMENT","sendEmailAlertWhenPublished":true,"about":{"~id":"09bd4cd5-bdad-58e5-949e-b01813544067","~table":"question:testclient1"},"displayLocale":"en_US","featured":false,"contentCodes":["ABC"],"submissionId":"4grfr4z3sjiacg3r0ye782e6r","contributor":{"~id":"4448d5a3-6325-5e31-9e5c-eeef54e91751","~table":"contributor:testclient1"},"displayCode":"123","legacyInternalId":270378,"firstPublishTime":"2011-01-21T00:15:10.000Z","text":"Sample answer\nnumber 2","clientComments":{"72a23200-24f3-11e0-8001-000000000000":{"comment":"Approved by client","name":"Emily"}},"status":"APPROVED","agreedToTermsAndConditions":false,"contributorEmailAddress":"f9e016cff4bca8b4eacade710e162552a30e595f54fe9f299d294dbcb12a87c6","displayAsAnonymous":false,"submissionTime":"2011-01-20T19:15:15.000Z","browserLocale":"en_GB","language":"en","space":"testclient1","type":"answer","client":"testclient1","~id":"1431f7b5-16b3-53d0-9a01-1885e1f37a59","~table":"answer:testclient1","~version":4,"~signature":"f2306387e15a8506dc507b232b701ded","~deleted":false,"~firstUpdateAt":"2012-12-04T08:45:24.261Z","~lastUpdateAt":"2013-10-21T12:02:33.280Z"}
{"lastPublishTime":"2011-11-22T23:00:05.000Z","campaignId":"BV_QA_REVIEW_THANKYOU","sendEmailAlertWhenPublished":true,"displayLocale":"en_US","about":{"~id":"54ef3c16-d5e1-500a-b3cc-0c2abb735ecf","~table":"question:testclient1"},"featured":false,"contentCodes":["ABC"],"submissionId":"f1e84nza2e916pvn51blve21w","contributor":{"~id":"46bf6cf1-cf4d-570e-ba39-d0bab30c92ff","~table":"contributor:testclient1"},"displayCode":"123","legacyInternalId":529383,"firstPublishTime":"2011-11-22T23:00:05.000Z","cdv-aAge":"35to44","text":"Sample answer\nnumber 3","clientComments":{"93954280-155d-11e1-8001-000000000000":{"comment":"Approved by client","name":"Emily"}},"status":"APPROVED","agreedToTermsAndConditions":false,"contributorEmailAddress":"04207f89c5b7dca6a54564a8cf5f3dfc51f9e7ad90802c73fd74f9d6cdc20fe7","displayAsAnonymous":false,"submissionTime":"2011-11-16T00:36:34.000Z","browserLocale":"en_US","language":"en","cdv-aGender":"Female","contributorLocation":"Oakland, CA","cdvOrder":["cdv-aAge","cdv-aGender"],"space":"testclient1","type":"answer","client":"testclient1","~id":"2c11d9f6-48f9-55d3-9708-ee6efec25146","~table":"answer:testclient1","~version":2,"~signature":"d9bbf05e1e537bc53ca6a253ffdc1d7b","~deleted":false,"~firstUpdateAt":"2013-01-25T19:20:09.413Z","~lastUpdateAt":"2013-10-21T12:02:33.280Z"}
```

### Choosing Which Stash to Use

The most recent Stash root directory available is not necessarily the most recent complete Stash.  If a Stash is
currently being written then the directory will exist but its contents will be incomplete.  There are two ways to
identify the latest complete Stash:

1. The `_LATEST` file always contains the name of the Stash root directory which was most recently completed.
2. Each Stash root directory will contain a file called `_SUCCESS` if it is complete.  Choose the most recently named
   Stash root directory that contains a `_SUCCESS` file.

### Empty Tables

Stash only uploads content for non-empty tables.  If a table exists but contains no rows then it will not have a Stash
presence.

### A Word of Caution about Concatenated GZIP Files

Stash uses concatenated gzip files.  If you are using Java the included `java.util.zip.GZIPInputStream` does not
properly handle concatenated gzip files.  We recommend using an alternative gzip decompressor such as the
`GzipCompressorInputStream` class provided by Apache's commons-compress library.

The Java Stash API will correctly handle concatenated gzip files.  Additionally, if you are using Hadoop 0.22.0 or
greater then you are also in good shape.
