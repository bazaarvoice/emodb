---
layout: post
title: "Guarantees"
date: 2016-05-23
excerpt: "Guarantees of EmoDB"
tags: [ACID, Guarantees, EmoDB]
type: [blog]
---

Consistency, ACID, Emo, and You
===============================

Any database should explicitly state the guarantees it gives you.

**Super-short version**:

* Emo offers full read-after-write consistency in the local data center by default. For full consistency across all data centers you can write with consistency level "GLOBAL". 
* Emo is ACID compliant
* The Databus gives you an alternative to "GLOBAL" writes for dealing with data changes consistently across regions.

Consistency
-----------

Let's talk about consistency first, because this is a source of confusion.

What is consistency? Consistency, with respect to distributed systems, typically
refers to the CAP theorem. If you'd like to learn more about this, you could obviously read the [Wikipedia Page](https://en.wikipedia.org/wiki/CAP_theorem), but I'd recommend [this more opinionated and clearer post](https://martin.kleppmann.com/2015/05/11/please-stop-calling-databases-cp-or-ap.html) before you risk exposing yourself to CAP snake oil.

Basically, the CAP theorem says that, in the presence of a network "partition" (some network failure between at least two nodes), a distributed system may provide _either_ "consistency" _or_ "availability" (or _neither_). Since you read that link, I don't have to tell you that neither of those words mean what you think they mean, or that in fact, no database you have heard of gives you "consistency" _or_ "availability" during a network "partition" (or even when the network is healthy).

Now that I have destroyed the entire foundation of this discussion, I am going to attempt to explain Emo's guarantees in terms that you care about without selling you a load of bs by oversimplifying things.

Rather than talking about the "consistency" in the CAP theorem (linearizability), I'm going to talk about the "consistency" you are thinking of, which is probably read-after-write or causal consistency (read-modify-write consistency).

Likewise, rather than talk about "availability", which includes both reads and writes, I'll focus on read-availability and write-availability, which again are most likely what you care about.

Finally, even though Emo is a _global_ multi-master database, I'm guessing that approximately 100% of the time, you care about single-data center semantics, so I'll start there, and come back to the global question at the end.

### Emo's consistency promises

It is easy to believe that Emo is "eventually consistent" and "highly available", following in the tradition of Cassandra's documentation (which is not even applicable to the CAP definition of consistency).

In fact, Emo allows you to choose your own tradeoffs among read-after-write consistency, read-availability, and write-availability. In any case, you also always have the option of enforcing read-modify-write consistency.

For any data item, Emo typically has three copies of it per data center (this is configurable by setting the replication factor of the backing Cassandra cluster). It is a dynamo-style database, so none of these are a primary or replica--they are all equal.

Emo gives you four write levels: NON_DURABLE, WEAK, STRONG, and GLOBAL. 
NON_DURABLE is not recommended, as it involves only one node, and it specifically does not require a durable write, so there is a decent chance of losing writes, no I'm not even going to talk about it. I'll get to GLOBAL later.
WEAK requires a durable write to at least two nodes (for redundancy).
STRONG requires a durable write to at least a quorum of nodes (In db-speak, this means `(n/2)+1`, where `n` is the number of nodes involved with storing your data).

For reads, Emo gives you two consistency levels: WEAK and STRONG. WEAK will read from any node, and STRONG reads from a quorum (same definition) of nodes and gives you the latest result.

With this scheme, you have one option for read-after-write consistency: `write(STRONG)+read(STRONG)`. There is a very simple proof that if you commit your write to over half the cluster and then read from over half the cluster, you are guaranteed to read from at least one node that saw your write, and you are therefore guaranteed to read your write.
Additionally, neither read-STRONG nor write-STRONG requires all the nodes to be up (just 2/3 or 3/5, etc.), so you get decent read and write availability in addition to your consistency.

If you decide you don't care that much about consistency, and you'd like to get more availability, you can back off to WEAK reads or writes. This might be a tradeoff you make if you want super-high throughput (or super-low latency), and you are fine with some stale reads.

The punch line is this: If you care about read-after-write consistency, **Emo is consistent**, and it can even be consistent in the face of nodes missing, if you're doing STRONG+STRONG. By the way, this is the default.

#### read-modify-write consistency

This is a stronger guarantee than read-after-write. It says that any data in the database is linearized with respect to the data it is based on. The obvious example is concurrently incrementing a value.

Say you have a data item `{"x":0}`, and two concurrent processes both read that record and increment `x`'s value before writing it back. What is the result? A crappy system will have both writes succeed, but could have `x`'s value as either 1 or 2. A causally consistent system will _either_ have `x` at 2 _or_ have one of the writes fail and have `x` at 1.

Most databases either provide row locks or Optimistic Concurrency Control (OCC), in which the document has a version, and you assert that the version hasn't changed since your read when you do a write. Either one allows causal consistency, but OCC gives better latency since you don't have to acquire locks. Retrofitting the example for OCC, the document looks like `{"x": 0, "version": 1}`. If they both read before either writes, they will both write asserting that the "current" version is 1, but only one such write can succeed, since the winner increments the version to 2. The loser recognizes that the write failed and retries, reading at version 2 and successfully writing version 3.

Emo actually does you one better by supporting **arbitrary conditions** on your writes. In our example, we could have a hundred other attributes besides `x` in the documents with a hundred other processes operating concurrently on different values. Under OCC, all writes would increment the version of the document, causing conflicts even though the operations are orthogonal. With Emo, both of our incrementers can write with the assertion that `x==0` before their write. One will obviously fail and retry, but they are unaffected by other writers to other attributes.

As a final note, Emo _also_ has a version on each document that auto-increments with every write, so you can just stick with OCC if that's your jam. Also, there is a signature on each document (a hash of all the fields), which allows you to simply assert that the document is entirely unchanged since your read (even if there were no-op updates that incremented the version).

Some OCC systems give you errors when your write is conflicting. Others require you to check after the write to see if the write actually applied. Emo is in the latter category, so this is one case in which you'll want to apply one of the read-after-write consistency techniques.

### Global consistency

As you hopefully know, Emo is designed to span multiple data centers, and the design permits you to easily expand to arbitrarily many others. Generally speaking, you think about write and read consistency in one data center only. It's not too common for someone to hit "submit" on the us-east-1 version of your app and then "reload" on the eu-west-1 version, expecting to immediately see the result. Emo will generally make such updates visible across regions within a few seconds.

However, if you do, for some reason, require true, global, read-after-write consistency, Emo supports a final write consistency level, GLOBAL. This will cause your write not to respond until all data centers have committed your write to a quorum of nodes. This will likely be slow, and may appear to fail fairly often (remember how insisting on consistency affects write-availability?), so I am mainly mentioning it for completeness.

ACID
----

Who doesn't love to talk about ACID compliance when they are talking about databases?

ACID stands for Atomicity, Consistency, Isolation, Durability. This is essentially the gold standard for what most people expect from a database. Emo is ACID compliant. Usually, it is assumed that {My,Postgre}SQL are always ACID compliant. I'm sure you will be shocked to hear me say that this is not true. Whether or not you get full ACID compliance with x-SQL databases depends entirely on the database and filesystem configurations.

All of these properties apply to "transactions". It's up to the database to define what is in a transaction. Some databases (MySQL and PostgreSQL among them) allow you to send single transactions that touch multiple records. Emo is not one of these. In Emo, you can only send transactions for single records, but that transaction can obviously apply to arbitrarily many properties of that record.

**Atomicity**: This property means that either the whole transaction applies or none of it does. In Emo, you create, update, and delete documents via [deltas](https://github.com/bazaarvoice/emodb/blob/master/docs/Deltas.md). There is an absolute guarantee that either the whole delta applied or none of it does.

**Consistency**: This "consistency" is totally different from the one mentioned in CAP, and also from the one I used in the previous section. This one just means that, if you use the database's API, you will not be able to corrupt it. So there can't be any sequence of legal operations that result in broken data or a broken database. This one is also pretty straightforward: since you can only write deltas to Emo, and since none of the deltas will corrupt Emo regardless of the starting state, Emo is "consistent" by this definition.

**Isolation**: This is not the right word for this property at all. I suspect Reuter and Härder used it just because they needed a vowel in the middle of the acronym. Really, the property is "Serializability", which simply means that concurrent operations have to have the same outcome as some sequential ordering of those same operations. This hearkens back to the concurrent update example. Since Emo supports concurrency control, it gives you the ability to arrive at concurrent operations that are serializable.

**Durability**: Any database that writes its transactions to disk before the write operation completes is durable, and Emo is one of these databases. Note that durability is a concept that applies to single-process databases (like traditional RDBMS) as well as distributed databases. This means that when we talk about whether a write is durable or not, what we mean is that, if the process crashes and then re-starts, the write is either still there or not. This is a different failure scenario from one in which you are running a distributed database on virtual hardware in which your process, as well as the disk it wrote to, may vanish in an instant. In fact, that deployment model is one of the factors that makes distributed databases more practical. By requiring writes to be redundant, Emo provides some protection from machine loss, which is a much stronger standard of durability than the one in ACID.

A small but important point is that to be sure any write to a file is actually on the disk, you have to do an `fsync`. There is a pretty big performance penalty for this, so any practical database will have some small delay in which it does a bunch of writes but doesn't actually fsync, so if it crashes in the midst of this, it will lose those writes. PostgreSQL, for example, has a default delay of 200ms. Emo's delay is greater than this at 10s, but Emo also has the advantage of being a distributed database, so if you write with consistency STRONG, over half the nodes in the database would have to crash in the same 10s window to actually lose any data. Nevertheless, this is a consideration if you were planning to write with consistency WEAK.

Other Guarantees
----------------

Emo provides quite a bit more than just a key/value store, so I expect to expand this section in the future. For now, I will just briefly touch on the basic guarantee you get out of the Databus.

### Databus

Like a couple of other databases, Emo allows you to subscribe to a stream of notifications about changes in the database. When you update a document, Emo publishes that event on the Databus, and when you poll your subscription, you will receive the updated document.
This is an interesting feature that allows you to mirror the state of the data, especially for building secondary indices.

There is another kind of consistency guarantee here. If you first create a subscription to your table and then scan the table, and subsequently continue polling your subscription, you are guaranteed not to miss any writes or deletes. This enables you to create a perfect replica of the data from that table and maintain it indefinitely.

#### Databus and global consistency

The fact that Emo replicates changes globally, combined with the fact that the databus notifies you only when changes become visible in your local data center gives you another way to achieve a kind of global consistency without requiring a GLOBAL-level write.

Imagine some process writes a document in the EU and wishes to notify a process in the US to ingest it. Without the databus, the EU process would have to use a GLOBAL write and then send a "handle this" queue message that the US process can poll. Without GLOBAL, though, there is no guarantee that the US process would get a consistent view of the document when it polls the queue message.

With the databus, the EU process can write STRONG only, but _tag_ the write as "handle this". The US process can listen on the databus only for "handle this" messages. The databus in the US will only deliver that event when a STRONG read is guaranteed to be consistent with the write. Basically, the databus replaces the queue from the previous case, but it is a special queue that only delivers its messages with fully consistent views of the data.
