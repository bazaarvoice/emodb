---
layout: post
title: "Hadoop and Hive Support"
date: 2016-05-16
excerpt: "Map Reduced, Hadoop and Hive Support"
tags: [Map Reduce, Hadoop, Hive, EmoStash, EmoDB]
type: [blog]
---

Hadoop and Hive Support
=======================

This document outlines how to incorporate EmoDB data into Hadoop jobs or as a Hive backend.

Before you read any further...
------------------------------

### Use Stash when possible

The EmoDB map-reduce libraries can source from either the EmoDB application or from Stash (S3 or HDFS, depending where your stash is exported to). Although both are documented
here you are _strongly_ encouraged to use Stash when possible for the following reasons:

* It helps by reducing load on EmoDb servers that are serving real-time consumers.
* It helps your hadoop jobs because S3 access is faster and not subject to potential rate limiting by EmoDB.


### Compatibility caveats

It is possible that these libraries will work on Hadoop- and Hive- compatible alternatives.  However, it has
only been successfully tested on these two technologies.  Additionally it has been proven incompatible with the
current versions of Impala and Presto at the time of this writing.

Hadoop
------

When writing map-reduce job using Hadoop it is possible to read EmoDB directly as an input source, either from the
EmoDB application or from Stash.  The following steps demonstrate how to do this.

### Include the `emo-sor-hadoop` module in your project.

Assuming your project uses Maven then it would look something like this:

```xml
<dependency>
    <groupId>com.bazaarvoice.emodb</groupId>
    <artifactId>emodb-sor-hadoop</artifactId>
    <version>2.41</version>
</dependency>
```

### Add the appropriate FileSystem to your job configuration

Add `fs.emodb.impl` to your job configuration for sourcing from EmoDB, or `fs.emostash.impl` for sourcing from Stash:

```java
import com.bazaarvoice.emodb.hadoop.io.EmoFileSystem;
import com.bazaarvoice.emodb.hadoop.io.StashFileSystem;
import org.apache.hadoop.fs.FileSystem;

//...

Configuration config = new Configuration();
// Only required if sourcing from EmoDB
config.setClass("fs.emodb.impl", EmoFileSystem.class, FileSystem.class);
// Only required if sourcing from Stash
config.setClass("fs.emostash.impl", StashFileSystem.class, FileSystem.class);
```

### Add S3 credentials for Stash if necessary

When using Stash the default credential provider chain is used to get S3 access credentials.  If you are running on an
EC2 instance then granting the necessary permissions to the instance profile role is the best method.  If you are
running on your laptop then you can add your credentials to your environment or pass them in as Java system properties.
However, if absolutely necessary you can provide them explicitly in your job configuration:

```java
config.set(ConfigurationParameters.ACCESS_KEY_PARAM, "yourAccessKey");
config.set(ConfigurationParameters.SECRET_KEY_PARAM, "yourSecretKey");
```

### (Optional) Set the job's InputFormatClass

`EmoFileSystem` and `StashFileSystem` can be read using the standard `TextFileFormat`, where keys are line numbers and
values are JSON lines of text.  For a more efficient and approachable interface you can use `EmoInputFormat` instead:

```java
Job job = Job.getInstance(config, "My Job");
job.setInputFormatClass(EmoInputFormat.class);
```

When using EmoInputFormat keys are `Text` containing EmoDB coordinates and values are of type `Row`.  A `Row` contains
methods such as:

* `getJson()` - Returns the row as a JSON string
* `getMap()` - Returns the row as a Java `Map<String, Object>`; this is identical to values returned by the EmoDB DataStore client
* Methods for getting intrinsics such as `getTable()`, `getId()`, and so on.

### Add the source tables

For each source table use `EmoInputFormat.addInputTable()`.  Table names are URIs built the following way:

Part        | Value
----        | -----
scheme      | "emodb" or "emostash" for EmoDB application or Stash source respectively
host        | Combination of universe and region, such as "ci.us-east-1" for the us-east-1 ci universe.
path        | Table name

For example, the following adds the review Stash tables for several customers:

```java
EmoInputFormat.addInputTable(job, "emostash://cert.us-east-1/review:testcustomer1");
EmoInputFormat.addInputTable(job, "emostash://cert.us-east-1/review:testcustomer2");
EmoInputFormat.addInputTable(job, "emostash://cert.us-east-1/review:testcustomer3");
```

For short and simple examples see the following classes:

* [RowCount.java] (https://github.com/bazaarvoice/emodb/blob/master/mapreduce/sor-hadoop/src/main/java/com/bazaarvoice/emodb/hadoop/examples/RowCount.java)
  * Counts the number of records in each input table
* [RowDump.java] (https://github.com/bazaarvoice/emodb/blob/master/mapreduce/sor-hadoop/src/main/java/com/bazaarvoice/emodb/hadoop/examples/RowDump.java)
  * Dumps the JSON contents of all input tables


Hive
----

You can query EmoDB and Stash data in Hive by creating external tables.  Since each Hive installation is different
the following is a general guide for configuring Hive, but you will likely need to tailor some of these steps to your
situation:

Get the latest emodb-sor-hive jar
---------------------------------

You will need the jar from the `emo-sor-hive` module.  Download the artifact or build it locally.  Add it to the
Hadoop classpath on your hive cluster and place it in a known location in HDFS.

Update hive-site.xml
--------------------

Add the following to your `hive-site.xml` configuration file:

```xml
<!-- Replace the value with the actual location where you placed the jar in HDFS -->
<property>
  <name>hive.aux.jars.path</name>
  <value>/path/to/hdfs/copy/of/emodb-sor-hive-2.41.jar</value>
</property>

<!-- Only required if sourcing from EmoDB -->
<property>
  <name>fs.emodb.impl</name>
  <value>com.bazaarvoice.emodb.hadoop.io.EmoFileSystem</value>
</property>

<!-- Only required if sourcing from Stash -->
<property>
  <name>fs.emostash.impl</name>
  <value>com.bazaarvoice.emodb.hadoop.io.StashFileSystem</value>
</property>

<!-- Only required if using Stash and the credentials aren't available in the default credential provider chain -->
<property>
  <name>com.bazaarvoice.emodb.stash.s3.accessKey</name>
  <value>your_access_key</value>
</property>
<property>
  <name>com.bazaarvoice.emodb.stash.s3.secretKey</name>
  <value>your_secret_key</value>
</property>
```

Add the emodb-sor-hive jar to Hive
----------------------------------

Either run this from the Hive command prompt or add it to your `.hiverc` file:

```
add jar hdfs:///path/to/hdfs/copy/of/emodb-sor-hive-2.41.jar
```

Create an external table
------------------------

EmoDB and Stash data can now be queried using external tables.  The URLs for the tables follow the same conventions
as when using Hadoop except colons must be URL encoded as `%3A`.  Tables can be created either with or without a
custom serializer.

### No custom serializer

The following creates a Hive table for "review:testcustomer":

```
hive> CREATE EXTERNAL TABLE review_testcustomer
    > (json string)
    > ROW FORMAT DELIMITED LOCATION 'emostash://cert.us-east-1/review%3Atestcustomer';
OK
Time taken: 1.227 seconds

hive> SELECT json FROM review_testcustomer LIMIT 1;
OK
{"lastPublishTime":"2014-05-28T19:00:19.000Z","sendEmailAlertWhenCommented":false,"sourceType":"NATIVE","ratingsOnly":false,"text":"Great!","status":"APPROVED","sendEmailAlertWhenPublished":false,"externalReviewId":"23458018","displayLocale":"en_US","about":{"~id":"product::5033789","~table":"catalog:testcustomer:"},"featured":false,"matchingStrategies":"EXTERNAL_ID","externalProductId":"5033789","title":"TEST>> Arthur looked up and squinting","cdv-rcvdProductSample":"false","contributor":{"~id":"8ae68550-c5bd-518f-8455-703660f53504","~table":"contributor:testcustomer"},"sourceName":"TestCustomer","submissionTime":"2013-05-07T00:06:52.000Z","legacyInternalId":23639195,"language":"en","rating":4,"reviewUrl":"http://reviews.testcustomer.bazaarvoice.com/0001/5033789/ratings/reviews.htm?reviewID=23458018","firstPublishTime":"2014-05-28T19:00:19.000Z","cdvOrder":["cdv-rcvdProductSample"],"space":"testcustomer","type":"review","client":"testcustomer","~id":"00085abc-c696-50b2-89e3-669be81172fa","~table":"review:testcustomer","~version":1,"~signature":"b6ccdb9c654009d2ba5cfc9599f68881","~deleted":false,"~firstUpdateAt":"2014-05-28T18:59:20.916Z","~lastUpdateAt":"2014-05-28T18:59:20.916Z"}
```

A line of JSON is good, but being able to extract data from it is better.  To support this you can create the following
Hive functions:

```
CREATE FUNCTION emo_id AS 'com.bazaarvoice.emodb.hive.udf.EmoId';
CREATE FUNCTION emo_table AS 'com.bazaarvoice.emodb.hive.udf.EmoTable';
CREATE FUNCTION emo_coordinate AS 'com.bazaarvoice.emodb.hive.udf.EmoCoordinate';
CREATE FUNCTION emo_first_update_at AS 'com.bazaarvoice.emodb.hive.udf.EmoFirstUpdateAt';
CREATE FUNCTION emo_last_update_at AS 'com.bazaarvoice.emodb.hive.udf.EmoLastUpdateAt';
CREATE FUNCTION emo_text AS 'com.bazaarvoice.emodb.hive.udf.EmoText';
CREATE FUNCTION emo_boolean AS 'com.bazaarvoice.emodb.hive.udf.EmoBoolean';
CREATE FUNCTION emo_int AS 'com.bazaarvoice.emodb.hive.udf.EmoInt';
CREATE FUNCTION emo_long AS 'com.bazaarvoice.emodb.hive.udf.EmoLong';
CREATE FUNCTION emo_float AS 'com.bazaarvoice.emodb.hive.udf.EmoFloat';
CREATE FUNCTION emo_timestamp AS 'com.bazaarvoice.emodb.hive.udf.EmoTimestamp';
```

Now you can make queries such as the following:

```
hive> SELECT emo_coordinate(json, 'about') AS product, count(*) AS num_reviews,
    >         avg(emo_int(json, 'rating')) AS average_rating
    > FROM review_testcustomer
    > WHERE emo_text(json, 'status') = 'APPROVED'
    > GROUP BY emo_coordinate(json, 'about')
    > ORDER BY num_reviews DESC
    > LIMIT 10;

<snip>

OK
catalog:testcustomer:/product::thingx	2148	3.090782122905028
catalog:testcustomer:/product::thing3	1489	2.9731363331094696
catalog:testcustomer:/product::thing2	1483	3.0128118678354685
catalog:testcustomer:/product::thinga	1397	3.0164638511095205
catalog:testcustomer:/product::thing4	1389	3.002159827213823
catalog:testcustomer:/product::thingy	1365	2.9956043956043956
catalog:testcustomer:/product::thingz	1354	2.9704579025110784
catalog:testcustomer:/product::thing5	1342	3.031296572280179
catalog:testcustomer:/product::thingb	1341	2.942580164056674
catalog:testcustomer:/product::product4	639	    3.043818466353678
Time taken: 10.714 seconds, Fetched: 10 row(s)
```

JSON paths can be traversed using dot notation, as shown in the following example:

```
hive> SELECT emo_text(json, 'about.~table'), emo_text(json, 'about.~id') FROM review_testcustomer LIMIT 5;
OK
catalog:testcustomer:	product::5033789
catalog:testcustomer:	product::xyz123-product-externalid-ccc-ykoum-bqvuk
catalog:testcustomer:	product::thingy
catalog:testcustomer:	product::gggg99-prod-99-externalid
catalog:testcustomer:	product::gggg99-prod-85-externalid
Time taken: 0.219 seconds, Fetched: 5 row(s)
```

### With a custom serializer

If your use case has well defined attributes from each row then you can use a custom serializer
to create a more columnar table:

```
hive> CREATE EXTERNAL TABLE review_testcustomer
    > (id STRING, submissionTime TIMESTAMP, rating INT, `about/~id` STRING)
    > ROW FORMAT SERDE 'com.bazaarvoice.emodb.hive.EmoSerDe'
    > STORED AS INPUTFORMAT 'com.bazaarvoice.emodb.hadoop.mapred.EmoInputFormat'
    >         OUTPUTFORMAT 'org.apache.hadoop.mapred.SequenceFileOutputFormat'
    > LOCATION 'emostash://cert.us-east-1/review%3Atestcustomer';
OK
Time taken: 0.029 seconds

hive> SELECT * FROM review_testcustomer LIMIT 5;
OK
00085abc-c696-50b2-89e3-669be81172fa	2013-05-06 19:06:52	4	product::5033789
000f00e1-04cc-53b7-b0cd-e2941e580935	2014-10-03 18:35:50	4	product::xyz123-product-externalid-ccc-ykoum-bqvuk
001f60ad-c9ff-5c0d-8928-336530817f0c	2013-05-22 18:19:54	3	product::thingy
002d77fa-17a2-5f65-adae-951cffd56e2f	2014-01-01 19:50:05	2	product::gggg99-prod-99-externalid
0030d880-94b2-5541-b1d5-fa83045337e1	2014-03-16 19:02:49	2	product::gggg99-prod-85-externalid

hive> SELECT `about/~id` AS product_id, max(submissionTime) AS latest_submission
    > FROM review_testcustomer
    > GROUP BY `about/~id`
    > ORDER BY latest_submission DESC
    > LIMIT 10;

<snip>

OK
product::produc33t21221	            2015-03-23 20:11:30
product::gggg99-prod-38-externalid	2015-03-23 20:10:31
product::gggg99-prod-82-externalid	2015-03-23 20:09:31
product::gggg99-prod-65-externalid	2015-03-23 20:08:30
product::product3223311aa	        2015-03-23 19:59:31
product::gggg99-prod-90-externalid	2015-03-23 19:47:58
product::gggg99-prod-62-externalid	2015-03-23 19:47:57
product::product1111223311	        2015-03-23 19:47:56
product::gggg99-prod-9-externalid	2015-03-23 19:47:56
product::product33622bbbaa	        2015-03-23 19:47:54
Time taken: 10.698 seconds, Fetched: 10 row(s)
```

Delimit column names using slashes for nested JSON attributes, such as "about/~id" in the previous example.  (Dots would
be preferable but it conflicts with Hive parsing.)

The following column names can be added to any table:

Column name     | Type      | Content
-----------     | ----      | -------
id              | STRING    | The EmoDB row ID ("~id")
table           | STRING    | The EmoDB table ("~table")
version         | INT       | The EmoDB version ("~version")
signature       | STRING    | The EmoDB signature ("~signature")
first_update_at | TIMESTAMP | The first time this row was updated in EmoDB ("~firstUpdateAt")
last_update_at  | TIMESTAMP | The most recent time this row was updated in EmoDB ("~lastUpdateAt")
json            | STRING    | The entire row as a JSON string
