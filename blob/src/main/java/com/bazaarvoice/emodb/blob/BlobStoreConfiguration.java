package com.bazaarvoice.emodb.blob;

import com.bazaarvoice.emodb.common.cassandra.CassandraConfiguration;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nullable;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.util.Map;
import java.util.Set;

public class BlobStoreConfiguration {
    /**
     * Where does the SoR store system information such as table definitions?
     */
    @Valid
    @NotNull
    private String _systemTablePlacement;

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

    public String getSystemTablePlacement() {
        return _systemTablePlacement;
    }

    public BlobStoreConfiguration setSystemTablePlacement(String systemTablePlacement) {
        _systemTablePlacement = systemTablePlacement;
        return this;
    }

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
}
