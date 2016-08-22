package com.bazaarvoice.emodb.sor;

import com.bazaarvoice.emodb.common.cassandra.CassandraConfiguration;
import com.bazaarvoice.emodb.sor.log.SlowQueryLogConfiguration;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import io.dropwizard.validation.ValidationMethod;
import org.joda.time.Period;

import javax.annotation.Nullable;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.util.Map;
import java.util.Set;

public class DataStoreConfiguration {
    /**
     * Where does the SoR store system information such as table definitions?
     */
    @Valid
    @NotNull
    private String _systemTablePlacement;

    @Valid
    @NotNull
    @JsonProperty("historyTtl")
    private Period _historyTtl;

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
    @JsonProperty("slowQueryLog")
    private SlowQueryLogConfiguration _slowQueryLogConfiguration = new SlowQueryLogConfiguration();

    @Valid
    @NotNull
    @JsonProperty("stashRoot")
    private Optional<String> _stashRoot = Optional.absent();

    public String getSystemTablePlacement() {
        return _systemTablePlacement;
    }

    public DataStoreConfiguration setSystemTablePlacement(String systemTablePlacement) {
        _systemTablePlacement = systemTablePlacement;
        return this;
    }

    public Set<String> getValidTablePlacements() {
        return _validTablePlacements;
    }

    public DataStoreConfiguration setValidTablePlacements(Set<String> validTablePlacements) {
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

    public Map<String, String> getPlacementsUnderMove() {
        return _placementsUnderMove;
    }

    public DataStoreConfiguration setCassandraClusters(Map<String, CassandraConfiguration> cassandraClusters) {
        _cassandraClusters = cassandraClusters;
        return this;
    }

    public DataStoreConfiguration setPlacementsUnderMove(Map<String, String> placementsUnderMove) {
        _placementsUnderMove = placementsUnderMove;
        return this;
    }

    @ValidationMethod(message = ".systemTablePlacement references a keyspace not defined in 'cassandraKeyspaces'")
    public boolean isValidSystemTablePlacement() {
        String systemKeyspace = _systemTablePlacement.substring(0, _systemTablePlacement.indexOf(':'));
        for (CassandraConfiguration cassandraConfig : _cassandraClusters.values()) {
            if (cassandraConfig.getKeyspaces().containsKey(systemKeyspace)) {
                return true;
            }
        }
        return false;
    }

    public SlowQueryLogConfiguration getSlowQueryLogConfiguration() {
        return _slowQueryLogConfiguration;
    }

    public DataStoreConfiguration setSlowQueryLogConfiguration(SlowQueryLogConfiguration slowQueryLogConfiguration) {
        _slowQueryLogConfiguration = slowQueryLogConfiguration;
        return this;
    }

    /**
     * How long should we retain historical deltas?
     */
    public Period getHistoryTtl() {
        return _historyTtl;
    }

    public DataStoreConfiguration setHistoryTtl(Period historyTtl) {
        _historyTtl = historyTtl;
        return this;
    }

    public Optional<String> getStashRoot() {
        return _stashRoot;
    }

    public DataStoreConfiguration setStashRoot(Optional<String> stashRoot) {
        _stashRoot = stashRoot;
        return this;
    }
}
