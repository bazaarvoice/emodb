package com.bazaarvoice.emodb.common.cassandra.test;

import com.bazaarvoice.emodb.common.cassandra.CassandraConfiguration;
import com.bazaarvoice.emodb.common.cassandra.CassandraHealthCheckConfiguration;
import com.bazaarvoice.emodb.common.cassandra.KeyspaceConfiguration;
import com.google.common.collect.ImmutableMap;

/**
 * An instance of {@link com.bazaarvoice.emodb.common.cassandra.CassandraConfiguration} preconfigured to use from integration tests.
 */
public class TestCassandraConfiguration extends CassandraConfiguration {

    public TestCassandraConfiguration(String keyspace, String healthCheckColumnFamily) {
        setCluster(keyspace);
        setSeeds("localhost");
        setThriftPort(9160);
        setCqlPort(9164);
        setPartitioner("bop");
        setHealthCheck(new CassandraHealthCheckConfiguration().setHealthCheckCql("select * from " + healthCheckColumnFamily + " limit 1").setName(keyspace + "-cassandra"));
        setKeyspaces(ImmutableMap.of(
                keyspace, new KeyspaceConfiguration()));
    }
}
