package com.bazaarvoice.emodb.cachemgr.api;

public enum InvalidationScope {
    /** Same JVM. */
    LOCAL,

    /** Same data center. */
    DATA_CENTER,

    /** Everywhere. */
    GLOBAL,
}
