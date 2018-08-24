package com.bazaarvoice.emodb.sor;

import com.bazaarvoice.emodb.common.cassandra.CassandraConfiguration;
import com.bazaarvoice.emodb.sor.audit.AuditWriterConfiguration;
import com.bazaarvoice.emodb.sor.log.SlowQueryLogConfiguration;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nullable;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.time.Duration;
import java.util.Map;
import java.util.Set;

public class DataStoreConfiguration {

    @Valid
    @NotNull
    @JsonProperty("historyTtl")
    private Duration _historyTtl;

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
    @JsonProperty("deltaEncodingVersion")
    private int _deltaEncodingVersion = 3;

    @Valid
    @NotNull
    @JsonProperty("stashRoot")
    private Optional<String> _stashRoot = Optional.absent();

    @Valid
    @NotNull
    @JsonProperty("migrationPhase")
    private DeltaMigrationPhase _migrationPhase = DeltaMigrationPhase.PRE_MIGRATION;


    /*
    Only temporarily configurable during the migration period
    */
    @Valid
    @NotNull
    @JsonProperty("deltaBlockSizeInKb")
    private int _deltaBlockSizeInKb = 64;

    @Valid
    @NotNull
    @JsonProperty("stashBlackListTableCondition")
    private Optional<String> _stashBlackListTableCondition = Optional.absent();

    @Valid
    @NotNull
    @JsonProperty("auditWriter")
    private AuditWriterConfiguration _auditWriterConfiguration;

    public Optional<String> getStashBlackListTableCondition() {
        return _stashBlackListTableCondition;
    }

    public void setStashBlackListTableCondition(Optional<String> stashBlackListTableCondition) {
        _stashBlackListTableCondition = stashBlackListTableCondition;
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
    public Duration getHistoryTtl() {
        return _historyTtl;
    }

    public DataStoreConfiguration setHistoryTtl(Duration historyTtl) {
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

    public int getDeltaEncodingVersion() {
        return _deltaEncodingVersion;
    }

    public DataStoreConfiguration setDeltaEncodingVersion(int deltaEncodingVersion) {
        _deltaEncodingVersion = deltaEncodingVersion;
        return this;
    }

    public DeltaMigrationPhase getMigrationPhase() {
        return _migrationPhase;
    }

    public int getDeltaBlockSizeInKb() {
        return _deltaBlockSizeInKb;
    }

    public AuditWriterConfiguration getAuditWriterConfiguration() {
        return _auditWriterConfiguration;
    }
}
