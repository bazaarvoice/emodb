package com.bazaarvoice.emodb.common.cassandra;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.constraints.NotNull;

public class CassandraHealthCheckConfiguration {

    private final static String DEFAULT_HEALTH_CHECK_CQL = "SELECT peer FROM system.peers LIMIT 1";

    /**
     * Name for the health check as registered in DropWizard.  The name should uniquely identify this Cassandra
     * cluster and must not collide with any other health check names.
     */
    @NotNull
    @JsonProperty("name")
    private String _name;

    /**
     * Query to run for the health check.  This query is not guaranteed to run in any keyspace so the table name
     * must be fully qualified.
     *
     * Default health check CQL query is:
     *
     * <code>SELECT peer FROM system.peers LIMIT 1</code>
     */
    @NotNull
    @JsonProperty("healthCheckCql")
    private String _healthCheckCql = DEFAULT_HEALTH_CHECK_CQL;

    public String getName() {
        return _name;
    }

    public CassandraHealthCheckConfiguration setName(String name) {
        _name = name;
        return this;
    }

    public String getHealthCheckCql() {
        return _healthCheckCql;
    }

    public CassandraHealthCheckConfiguration setHealthCheckCql(String healthCheckCql) {
        _healthCheckCql = healthCheckCql;
        return this;
    }
}
