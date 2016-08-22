package com.bazaarvoice.emodb.web.ddl;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Maps;
import io.dropwizard.Configuration;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.util.Map;

/**
 * This should contain keyspace definitions for all keyspaces/column families with
 * data <em>in the current data center</em>.
 */
public final class DdlConfiguration extends Configuration {

    @Valid
    @NotNull
    @JsonProperty("systemOfRecord")
    private Keyspaces _systemOfRecord;

    @Valid
    @NotNull
    @JsonProperty("databus")
    private Keyspaces _databus;

    @Valid
    @NotNull
    @JsonProperty("queue")
    private Keyspaces _queue;

    @Valid
    @NotNull
    @JsonProperty("blobStore")
    private Keyspaces _blobStore;

    public Keyspaces getSystemOfRecord() {
        return _systemOfRecord;
    }

    public Keyspaces getDatabus() {
        return _databus;
    }

    public Keyspaces getQueue() {
        return _queue;
    }

    public Keyspaces getBlobStore() {
        return _blobStore;
    }

    /** Subconfiguration class. */
    public static final class Keyspaces {

        @Valid
        @NotNull
        @JsonProperty("keyspaces")
        private Map<String, Keyspace> _keyspaces = Maps.newHashMap();

        public Map<String, Keyspace> getKeyspaces() {
            return _keyspaces;
        }
    }

    /** Subconfiguration class. */
    public static final class Keyspace {

        @Valid
        @NotNull
        @JsonProperty("replicationFactor")
        private Integer _replicationFactor;

        @Valid
        @NotNull
        @JsonProperty("tables")
        private Map<String, Map<String, String>> _tables = Maps.newHashMap();

        public int getReplicationFactor() {
            return _replicationFactor;
        }

        public Map<String, Map<String, String>> getTables() {
            return _tables;
        }
    }
}
