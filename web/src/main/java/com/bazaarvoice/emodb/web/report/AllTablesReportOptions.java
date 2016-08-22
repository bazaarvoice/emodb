package com.bazaarvoice.emodb.web.report;

import com.google.common.base.Optional;

import java.util.Set;

public class AllTablesReportOptions {
    private Optional<Set<String>> _placements = Optional.absent();
    private Optional<Integer> _fromShardId = Optional.absent();
    private Optional<Long> _fromTableUuid = Optional.absent();
    private boolean _readOnly = false;

    public Optional<Set<String>> getPlacements() {
        return _placements;
    }

    public void setPlacements(Set<String> placements) {
        _placements = Optional.fromNullable(placements);
    }

    public Optional<Integer> getFromShardId() {
        return _fromShardId;
    }

    public void setFromShardId(int fromShardId) {
        _fromShardId = Optional.of(fromShardId);
    }

    public Optional<Long> getFromTableUuid() {
        return _fromTableUuid;
    }

    public void setFromTableUuid(long fromTableUuid) {
        _fromTableUuid = Optional.of(fromTableUuid);
    }

    public boolean isReadOnly() {
        return _readOnly;
    }

    public void setReadOnly(boolean readOnly) {
        _readOnly = readOnly;
    }
}
