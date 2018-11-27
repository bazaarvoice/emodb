package com.bazaarvoice.emodb.sor.api.report;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class TableReportEntryTable {
    private final String _id;
    private final String _placement;
    private final List<Integer> _shardIds;
    private final boolean _dropped;
    private final boolean _facade;
    private final long _rowCount;
    private final LongStatistics _columnStatistics;
    private final LongStatistics _sizeStatistics;
    private final DateStatistics _updateTimeStatistics;

    @JsonCreator
    public TableReportEntryTable(
            @JsonProperty ("id") String id, @JsonProperty ("placement") String placement,
            @JsonProperty ("shardIds") List<Integer> shardIds, @JsonProperty ("dropped") boolean dropped,
            @JsonProperty ("facade") boolean facade, @JsonProperty ("recordCount") long rowCount,
            @JsonProperty ("columnStatistics") LongStatistics columnStatistics,
            @JsonProperty ("sizeStatistics") LongStatistics sizeStatistics,
            @JsonProperty ("updateTimeStatistics") DateStatistics updateTimeStatistics) {

        _id = requireNonNull(id, "id");
        _placement = requireNonNull(placement, "placement");
        _shardIds = Collections.unmodifiableList(requireNonNull(shardIds, "shardIds"));
        _dropped = dropped;
        _facade = facade;
        _rowCount = rowCount;
        _columnStatistics = columnStatistics != null ? columnStatistics : new LongStatistics();
        _sizeStatistics = sizeStatistics != null ? sizeStatistics : new LongStatistics();
        _updateTimeStatistics = updateTimeStatistics != null ? updateTimeStatistics : new DateStatistics();
    }

    public String getId() {
        return _id;
    }

    public String getPlacement() {
        return _placement;
    }

    public List<Integer> getShardIds() {
        return _shardIds;
    }

    public boolean isDropped() {
        return _dropped;
    }

    public boolean isFacade() {
        return _facade;
    }

    public long getRowCount() {
        return _rowCount;
    }

    public LongStatistics getColumnStatistics() {
        return _columnStatistics;
    }

    public LongStatistics getSizeStatistics() {
        return _sizeStatistics;
    }

    public DateStatistics getUpdateTimeStatistics() {
        return _updateTimeStatistics;
    }
}
