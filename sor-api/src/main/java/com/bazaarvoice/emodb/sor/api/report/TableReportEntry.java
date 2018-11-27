package com.bazaarvoice.emodb.sor.api.report;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class TableReportEntry {
    private final String _tableName;
    private final List<TableReportEntryTable> _tables;

    @JsonCreator
    public TableReportEntry(
            @JsonProperty ("tableName") String tableName, @JsonProperty ("tables") List<TableReportEntryTable> tables) {
        _tableName = requireNonNull(tableName, "tableName");
        _tables = requireNonNull(tables, "tables");
    }

    public String getTableName() {
        return _tableName;
    }

    public List<TableReportEntryTable> getTables() {
        return _tables;
    }
}
