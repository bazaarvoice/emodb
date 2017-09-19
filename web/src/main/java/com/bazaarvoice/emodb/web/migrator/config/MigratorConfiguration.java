package com.bazaarvoice.emodb.web.migrator.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

/**
 * Migrator-specific service configurations.
 */
public class MigratorConfiguration {

    private static final int DEFAULT_READ_THREAD_COUNT = 8;
    private static final String DEFAULT_MIGRATE_STATUS_TABLE = "__system_migrate";
    private static final String DEFAULT_MIGRATE_STATUS_TABLE_PLACEMENT = "app_global:migration";
    private static final int DEFAULT_MAX_CONCURRENT_WRITES = 500;

    // If using EmoDB queues, the API key to use
    @Valid
    @NotNull
    @JsonProperty ("migrateApiKey")
    private Optional<String> _migrateApiKey = Optional.absent();

    // Maximum number of read threads that can run concurrently on a single server.  Default is 8.
    @Valid
    @NotNull
    @JsonProperty ("readThreadCount")
    private int _readThreadCount = DEFAULT_READ_THREAD_COUNT;

    // Name of the table which holds migrator status entries.
    @Valid
    @NotNull
    @JsonProperty ("migrateStatusTable")
    private String _migrateStatusTable = DEFAULT_MIGRATE_STATUS_TABLE;

    // Name of the placement which holds migrator status table.
    @Valid
    @NotNull
    @JsonProperty ("migrateStatusTablePlacement")
    private String _migrateStatusTablePlacement = DEFAULT_MIGRATE_STATUS_TABLE_PLACEMENT;

    @Valid
    @NotNull
    @JsonProperty ("maxConcurrentWrites")
    private int _maxConcurrentWrites = DEFAULT_MAX_CONCURRENT_WRITES;

    @Valid
    @NotNull
    @JsonProperty ("pendingMigrationRangeQueueName")
    private Optional<String> _pendingMigrationRangeQueueName = Optional.absent();

    @Valid
    @NotNull
    @JsonProperty ("completeMigrationRangeQueueName")
    private Optional<String> _completeMigrationRangeQueueName = Optional.absent();

    public int getReadThreadCount() {
        return _readThreadCount;
    }

    public Optional<String> getMigrateApiKey() {
        return _migrateApiKey;
    }

    public void setMigrateApiKey(Optional<String> migrateApiKey) {
        _migrateApiKey = migrateApiKey;
    }

    public MigratorConfiguration setReadThreadCount(int readThreadCount) {
        _readThreadCount = readThreadCount;
        return this;
    }

    public String getMigrateStatusTable() {
        return _migrateStatusTable;
    }

    public MigratorConfiguration setMigrateStatusTable(String migrateStatusTable) {
        _migrateStatusTable = migrateStatusTable;
        return this;
    }

    public String getMigrateStatusTablePlacement() {
        return _migrateStatusTablePlacement;
    }

    public void setMigrateStatusTablePlacement(String migrateStatusTablePlacement) {
        _migrateStatusTablePlacement = migrateStatusTablePlacement;
    }

    public Optional<String> getPendingMigrationRangeQueueName() {
        return _pendingMigrationRangeQueueName;
    }

    public MigratorConfiguration setPendingMigrationRangeQueueName(Optional<String> pendingMigrationRangeQueueName) {
        _pendingMigrationRangeQueueName = pendingMigrationRangeQueueName;
        return this;
    }

    public Optional<String> getCompleteMigrationRangeQueueName() {
        return _completeMigrationRangeQueueName;
    }

    public MigratorConfiguration setCompleteMigrationRangeQueueName(Optional<String> completeMigrationRangeQueueName) {
        _completeMigrationRangeQueueName = completeMigrationRangeQueueName;
        return this;
    }

    public int getMaxConcurrentWrites() {
        return _maxConcurrentWrites;
    }
}