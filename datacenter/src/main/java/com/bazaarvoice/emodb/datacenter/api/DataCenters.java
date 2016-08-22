package com.bazaarvoice.emodb.datacenter.api;

import java.util.Collection;

public interface DataCenters {
    /** Reload the set of data centers now.  This is rarely necessary since it's refreshed automatically every few minutes. */
    void refresh();

    /** Returns all known data centers. */
    Collection<DataCenter> getAll();

    /** Returns the current data center. */
    DataCenter getSelf();

    /** Returns the data center where single-data center system operations (eg. CREATE/DROP TABLE) must occur. */
    DataCenter getSystem();

    Collection<DataCenter> getForKeyspace(String keyspace);
}
