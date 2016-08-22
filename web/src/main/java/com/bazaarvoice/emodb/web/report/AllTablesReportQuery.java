package com.bazaarvoice.emodb.web.report;

import java.util.Collection;
import java.util.Collections;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class AllTablesReportQuery {

    private Collection<String> _tableNames = Collections.emptyList();
    private Collection<String> _placements = Collections.emptyList();
    private boolean _includeDropped = true;
    private boolean _includeFacades = true;
    private String _fromTable = null;
    private int _limit = 100;

    public Collection<String> getTableNames() {
        return _tableNames;
    }

    public void setTableNames(Collection<String> tableNames) {
        _tableNames = checkNotNull(tableNames, "tableNames");
    }

    public Collection<String> getPlacements() {
        return _placements;
    }

    public void setPlacements(Collection<String> placements) {
        _placements = checkNotNull(placements, "placements");
    }

    public boolean isIncludeDropped() {
        return _includeDropped;
    }

    public void setIncludeDropped(boolean includeDropped) {
        _includeDropped = includeDropped;
    }

    public boolean isIncludeFacades() {
        return _includeFacades;
    }

    public void setIncludeFacades(boolean includeFacades) {
        _includeFacades = includeFacades;
    }

    public String getFromTable() {
        return _fromTable;
    }

    public void setFromTable(String fromTable) {
        _fromTable = fromTable;
    }

    public int getLimit() {
        return _limit;
    }

    public void setLimit(int limit) {
        checkArgument(limit > 0, "Limit must be positive");
        _limit = limit;
    }
}
