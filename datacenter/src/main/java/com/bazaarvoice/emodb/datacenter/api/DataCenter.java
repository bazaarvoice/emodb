package com.bazaarvoice.emodb.datacenter.api;

import java.net.URI;
import java.util.Collection;

public interface DataCenter extends Comparable<DataCenter> {
    /** Returns the name of the data center, eg. "us-west-2". */
    String getName();

    /** Returns the load-balanced highly available base URL for the EmoDB service (eg. http://localhost:8080). */
    URI getServiceUri();

    /** Returns the load-balanced highly available base URL for the EmoDB administration tasks (eg. http://localhost:8081). */
    URI getAdminUri();

    /** Is this the data center where single-data center system operations (eg. CREATE/DROP TABLE) must occur? */
    boolean isSystem();

    /** Returns the name of this DataCenter that matches Cassandra's NetworkTopologyStrategy configuration. */
    String getCassandraName();

    /** Returns the EmoDB names of the keyspaces that are replicated to this data center. */
    Collection<String> getCassandraKeyspaces();
}
