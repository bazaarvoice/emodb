package com.bazaarvoice.emodb.sor.api;

public enum WriteConsistency {

    /**
     * Write to some server, but if that server fails there's no guarantee that the value
     * will ever be read.  This level of consistency is only appropriate when there are no
     * permanent consequences when a write is lost, such as a cache that will be rebuilt
     * as needed.
     */
    NON_DURABLE,

    /**
     * Write to at least two servers for durability, but do not guarantee when subsequent
     * reads at any consistency level will see the effect of the write.
     */
    WEAK,

    /**
     * Write such that subsequent strong consistency reads in the same data center are guaranteed
     * to see the effect of the write.
     */
    STRONG,

    /**
     * Write such that subsequent strong consistency reads in all data centers are guaranteed to
     * see the effect of the write.  If the write fails, it could be because the write did not
     * occur.  Or, it could be because the write succeeded in one or more data centers, but not
     * all data centers.  In that situation, the write will most likely eventually propagate to
     * all data centers, at some point in the future.
     */
    GLOBAL,
}
