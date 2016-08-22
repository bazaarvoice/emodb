package com.bazaarvoice.emodb.sor.db;

import static com.google.common.base.Preconditions.checkNotNull;

public class MultiTableScanOptions {
    private String _placement;
    private ScanRange _range = null;
    private boolean _includeDeletedTables = false;
    private boolean _includeMirrorTables = false;

    public String getPlacement() {
        return _placement;
    }

    public MultiTableScanOptions setPlacement(String placement) {
        _placement = checkNotNull(placement, "placement");
        return this;
    }

    public ScanRange getScanRange() {
        return _range;
    }

    public MultiTableScanOptions setScanRange(ScanRange range) {
        _range = range;
        return this;
    }

    public boolean isIncludeDeletedTables() {
        return _includeDeletedTables;
    }

    public MultiTableScanOptions setIncludeDeletedTables(boolean includeDeletedTables) {
        _includeDeletedTables = includeDeletedTables;
        return this;
    }

    public boolean isIncludeMirrorTables() {
        return _includeMirrorTables;
    }

    public MultiTableScanOptions setIncludeMirrorTables(boolean includeMirrorTables) {
        _includeMirrorTables = includeMirrorTables;
        return this;
    }
}
