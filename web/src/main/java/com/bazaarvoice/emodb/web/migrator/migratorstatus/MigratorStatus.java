package com.bazaarvoice.emodb.web.migrator.migratorstatus;

import com.bazaarvoice.emodb.web.scanner.ScanOptions;
import com.bazaarvoice.emodb.web.scanner.scanstatus.ScanRangeStatus;
import com.bazaarvoice.emodb.web.scanner.scanstatus.ScanStatus;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nullable;
import java.util.Date;
import java.util.List;

public class MigratorStatus extends ScanStatus {

    private int _maxConcurrentWrites;

    public MigratorStatus(ScanStatus scanStatus, int maxConcurrentWrites) {
        this(scanStatus.getScanId(), scanStatus.getOptions(), scanStatus.isCanceled(),
                scanStatus.getStartTime(), scanStatus.getPendingScanRanges(),
                scanStatus.getActiveScanRanges(), scanStatus.getCompleteScanRanges(),
                scanStatus.getCompleteTime(), maxConcurrentWrites);
    }

    @JsonCreator
    public MigratorStatus(@JsonProperty ("scanId") String scanId,
                      @JsonProperty ("options") ScanOptions options,
                      @JsonProperty ("canceled") boolean canceled,
                      @JsonProperty ("startTime") Date startTime,
                      @JsonProperty ("pendingScanRanges") List<ScanRangeStatus> pendingScanRanges,
                      @JsonProperty ("activeScanRanges") List<ScanRangeStatus> activeScanRanges,
                      @JsonProperty ("completeScanRanges") List<ScanRangeStatus> completeScanRanges,
                      @JsonProperty ("completeTime") @Nullable Date completeTime,
                      @JsonProperty ("maxConcurrentWrites") int maxConcurrentWrites) {
        super(scanId, options, canceled, startTime, pendingScanRanges, activeScanRanges, completeScanRanges);
        _maxConcurrentWrites = maxConcurrentWrites;
    }

    public int getMaxConcurrentWrites() {
        return _maxConcurrentWrites;
    }

    public void setMaxConcurrentWrites(int maxConcurrentWrites) {
        _maxConcurrentWrites = maxConcurrentWrites;
    }
}
