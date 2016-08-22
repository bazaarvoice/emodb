package com.bazaarvoice.emodb.common.cassandra;

/**
 * Generic interface for connecting to a Cassandra cluster that abstracts away the actual implementation, such
 * as whether it uses Astyanax or the Datastax CQL driver.
 */
public interface CassandraCluster<C> {

    /**
     * Returns a connection object for the provided keyspace.  It is up to the implementation whether the returned
     * connection is unique per call and/or keyspace.
     */
    C connect(String keyspaceName);

    /**
     * Returns the name of the datacenter for this cluster.
     */
    String getDataCenter();

    /**
     * Returns the name of the cluster.
     */
    String getClusterName();
}
