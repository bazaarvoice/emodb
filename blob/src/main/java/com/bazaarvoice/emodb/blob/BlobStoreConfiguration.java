package com.bazaarvoice.emodb.blob;

import com.bazaarvoice.emodb.common.cassandra.CassandraConfiguration;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;
import com.netflix.astyanax.model.ConsistencyLevel;

import javax.annotation.Nullable;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.util.Map;
import java.util.Set;

public class BlobStoreConfiguration {

    @Valid
    @NotNull
    private Set<String> _validTablePlacements = ImmutableSet.of();

    /**
     * The minimum number of splits per table.  This configuration setting affects only newly created tables.
     * Once created, tables ignore changes to this setting.
     */
    private int _minimumSplitsPerTable = 8;

    @Valid
    @NotNull
    @JsonProperty("cassandraClusters")
    private Map<String, CassandraConfiguration> _cassandraClusters;

    @Valid
    @Nullable
    @JsonProperty("placementsUnderMove")
    private Map<String, String> _placementsUnderMove;

    @Valid
    @NotNull
    @JsonProperty("approvedContentTypes")
    private Set<String> _approvedContentTypes = ImmutableSet.of();

    @Valid
    @NotNull
    @JsonProperty("readConsistency")
    private ConsistencyLevel _readConsistency = ConsistencyLevel.CL_LOCAL_QUORUM;

    public Set<String> getValidTablePlacements() {
        return _validTablePlacements;
    }

    public BlobStoreConfiguration setValidTablePlacements(Set<String> validTablePlacements) {
        _validTablePlacements = validTablePlacements;
        return this;
    }

    public int getMinimumSplitsPerTable() {
        return _minimumSplitsPerTable;
    }

    public void setMinimumSplitsPerTable(int minimumSplitsPerTable) {
        _minimumSplitsPerTable = minimumSplitsPerTable;
    }

    public Map<String, CassandraConfiguration> getCassandraClusters() {
        return _cassandraClusters;
    }

    public BlobStoreConfiguration setCassandraClusters(Map<String, CassandraConfiguration> cassandraClusters) {
        _cassandraClusters = cassandraClusters;
        return this;
    }

    public Map<String, String> getPlacementsUnderMove() {
        return _placementsUnderMove;
    }

    public BlobStoreConfiguration setPlacementsUnderMove(Map<String, String> placementsUnderMove) {
        _placementsUnderMove = placementsUnderMove;
        return this;
    }

    public Set<String> getApprovedContentTypes() {
        return _approvedContentTypes;
    }

    public BlobStoreConfiguration setApprovedContentTypes(Set<String> approvedContentTypes) {
        _approvedContentTypes = approvedContentTypes;
        return this;
    }

    public ConsistencyLevel getReadConsistency() {
        return _readConsistency;
    }

    public BlobStoreConfiguration setReadConsistency(ConsistencyLevel readConsistency) {
        _readConsistency = readConsistency;
        return this;
    }
}
