package com.bazaarvoice.emodb.queue;

import com.bazaarvoice.emodb.common.cassandra.CassandraConfiguration;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

public class QueueConfiguration {

    /**
     * Which Cassandra keyspace should the QueueService use to store messages?
     */
    @Valid
    @NotNull
    @JsonProperty("cassandra")
    private CassandraConfiguration _cassandraConfiguration;

    public CassandraConfiguration getCassandraConfiguration() {
        return _cassandraConfiguration;
    }

    public QueueConfiguration setCassandraConfiguration(CassandraConfiguration cassandraConfiguration) {
        _cassandraConfiguration = cassandraConfiguration;
        return this;
    }
}
