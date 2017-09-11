package com.bazaarvoice.emodb.sor;

public enum DeltaMigrationPhase {
    PRE_MIGRATION(true, true, false),
    DOUBLE_WRITE_LEGACY_READ(true, true, true),
    DOUBLE_WRITE_BLOCKED_READ(false, true, true),
    FULLY_MIGRATED(false, false, true);

    private final boolean _readFromLegacyDeltaTables;
    private final boolean _writeToLegacyDeltaTables;
    private final boolean _writeToBlockedDeltaTables;

    DeltaMigrationPhase(boolean readFromLegacyDeltaTables, boolean writeToLegacyDeltaTables, boolean writeToBlockedDeltaTables) {
        _readFromLegacyDeltaTables = readFromLegacyDeltaTables;
        _writeToLegacyDeltaTables = writeToLegacyDeltaTables;
        _writeToBlockedDeltaTables = writeToBlockedDeltaTables;
    }

    public boolean isReadFromLegacyDeltaTables() {
        return _readFromLegacyDeltaTables;
    }

    public boolean isWriteToLegacyDeltaTables() {
        return _writeToLegacyDeltaTables;
    }

    public boolean isWriteToBlockedDeltaTables() {
        return _writeToBlockedDeltaTables;
    }
}
