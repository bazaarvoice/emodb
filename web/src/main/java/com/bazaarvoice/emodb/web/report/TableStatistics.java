package com.bazaarvoice.emodb.web.report;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import static com.google.common.base.Preconditions.checkNotNull;

public class TableStatistics {

    private final int _recordCount;
    private final LongReportHistogram _columnStatistics;
    private final LongReportHistogram _sizeStatistics;
    private final DateReportHistogram _updateTimeStatistics;

    @JsonCreator
    public TableStatistics(@JsonProperty ("recordCount") int recordCount,
                           @JsonProperty ("columnStatistics") LongReportHistogram columnStatistics,
                           @JsonProperty ("sizeStatistics") LongReportHistogram sizeStatistics,
                           @JsonProperty ("updateTimeStatistics") DateReportHistogram updateTimeStatistics) {
        _recordCount = recordCount;
        _columnStatistics = columnStatistics;
        _sizeStatistics = sizeStatistics;
        _updateTimeStatistics = updateTimeStatistics;
    }

    public int getRecordCount() {
        return _recordCount;
    }

    public LongReportHistogram getColumnStatistics() {
        return _columnStatistics;
    }

    public LongReportHistogram getSizeStatistics() {
        return _sizeStatistics;
    }

    public DateReportHistogram getUpdateTimeStatistics() {
        return _updateTimeStatistics;
    }

    public static Aggregator newAggregator() {
        return new Aggregator();
    }

    public static TableStatistics aggregate(Iterable<TableStatistics> stats) {
        checkNotNull(stats, "stats");

        Aggregator aggregator = newAggregator();
        for (TableStatistics stat : stats) {
            aggregator.add(stat);
        }
        return aggregator.aggregate();
    }

    public static class Aggregator {
        private int recordCount = 0;
        private LongReportHistogram columnStats = new LongReportHistogram();
        private LongReportHistogram sizeStats = new LongReportHistogram();
        private DateReportHistogram updateTimeStats = new DateReportHistogram();

        private Aggregator() {
            // empty
        }

        public Aggregator add(TableStatistics stats) {
            recordCount += stats.getRecordCount();
            columnStats = columnStats.combinedWith(stats.getColumnStatistics());
            sizeStats = sizeStats.combinedWith(stats.getSizeStatistics());
            updateTimeStats = updateTimeStats.combinedWith(stats.getUpdateTimeStatistics());
            return this;
        }

        public TableStatistics aggregate() {
            return new TableStatistics(recordCount, columnStats, sizeStats, updateTimeStats);
        }
    }
}
