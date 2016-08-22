package com.bazaarvoice.emodb.web.report.db;

import com.bazaarvoice.emodb.web.report.TableStatistics;
import com.google.common.base.Optional;

import java.util.Date;

import static com.google.common.base.Preconditions.checkNotNull;

public class AllTablesReportDelta {

    private final String _reportId;
    private final String _description;

    private Optional<Date> _startTime = Optional.absent();
    private Optional<String> _placement = Optional.absent();
    private Optional<TableDelta> _table = Optional.absent();
    private Optional<Date> _completeTime = Optional.absent();
    private Optional<Boolean> _success = Optional.absent();

    public AllTablesReportDelta(String reportId, String description) {
        _reportId = checkNotNull(reportId, "reportId");
        _description = checkNotNull(description, "description");
    }

    public String getReportId() {
        return _reportId;
    }

    public String getDescription() {
        return _description;
    }

    public Optional<Date> getStartTime() {
        return _startTime;
    }

    public AllTablesReportDelta withStartTime(Date startTime) {
        _startTime = Optional.of(checkNotNull(startTime, "startTime"));
        return this;
    }

    public Optional<String> getPlacements() {
        return _placement;
    }

    public AllTablesReportDelta withPlacement(String placement) {
        _placement = Optional.of(checkNotNull(placement, "placement"));
        return this;
    }

    public Optional<TableDelta> getTable() {
        return _table;
    }

    public AllTablesReportDelta withTable(TableDelta table) {
        _table = Optional.of(checkNotNull(table, "table"));
        return this;
    }

    public Optional<Date> getCompleteTime() {
        return _completeTime;
    }

    public AllTablesReportDelta withCompleteTime(Date completeTime) {
        _completeTime = Optional.of(checkNotNull(completeTime, "completeTime"));
        return this;
    }

    public Optional<Boolean> getSuccess() {
        return _success;
    }

    public AllTablesReportDelta withSuccess(boolean success) {
        _success = Optional.of(success);
        return this;
    }

    public final static class TableDelta {
        final private String _tableId;
        final private String _tableName;
        final private String _placement;
        final private boolean _dropped;
        final private boolean _facade;
        final private int _shardId;
        final private TableStatistics _tableStatistics;

        public TableDelta(String tableId, String tableName, String placement, boolean dropped, boolean facade,
                          int shardId, TableStatistics tableStatistics) {
            _tableId = checkNotNull(tableId, "tableId");
            _tableName = checkNotNull(tableName, "tableName");
            _placement = checkNotNull(placement, "placement");
            _dropped = dropped;
            _facade = facade;
            _shardId = shardId;
            _tableStatistics = checkNotNull(tableStatistics, "tableStatistics");
        }

        public String getTableId() {
            return _tableId;
        }

        public String getTableName() {
            return _tableName;
        }

        public String getPlacement() {
            return _placement;
        }

        public boolean isDropped() {
            return _dropped;
        }

        public boolean isFacade() {
            return _facade;
        }

        public int getShardId() {
            return _shardId;
        }

        public TableStatistics getTableStatistics() {
            return _tableStatistics;
        }
    }
}
