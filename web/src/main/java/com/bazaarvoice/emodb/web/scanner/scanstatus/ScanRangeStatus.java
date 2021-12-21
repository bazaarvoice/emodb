package com.bazaarvoice.emodb.web.scanner.scanstatus;

import com.bazaarvoice.emodb.sor.db.ScanRange;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;
import java.util.Date;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * POJO which maintains metadata about the scan ranges being processed as part of a scan and upload operation.
 */
public class ScanRangeStatus {

    private final int _taskId;
    private final String _placement;
    private final ScanRange _scanRange;
    private final int _batchId;
    private final Optional<Integer> _blockedByBatchId;
    private final Optional<Integer> _concurrencyId;
    private Date _scanQueuedTime;
    private Date _scanStartTime;
    private Date _scanCompleteTime;
    private Optional<ScanRange> _resplitRange = Optional.empty();

    @JsonCreator
    public ScanRangeStatus(@JsonProperty ("taskId") int taskId,
                           @JsonProperty ("placement") String placement,
                           @JsonProperty ("scanRange") ScanRange scanRange,
                           @JsonProperty ("batchId") int batchId,
                           @Nullable @JsonProperty ("blockedByBatchId") Integer blockedByBatchId,
                           @Nullable @JsonProperty ("concurrencyId") Integer concurrencyId) {
        this(taskId, placement, scanRange, batchId, Optional.ofNullable(blockedByBatchId), Optional.ofNullable(concurrencyId));
    }

    public ScanRangeStatus(int taskId, String placement, ScanRange scanRange, int batchId,
                           Optional<Integer> blockedByBatchId, Optional<Integer> concurrencyId) {
        _taskId = taskId;
        _placement = requireNonNull(placement, "placement");
        _scanRange = requireNonNull(scanRange, "scanRange");
        _batchId = batchId;
        _blockedByBatchId = requireNonNull(blockedByBatchId, "blockedByBatchId");
        _concurrencyId = requireNonNull(concurrencyId, "concurrencyId");
    }

    public int getTaskId() {
        return _taskId;
    }

    public String getPlacement() {
        return _placement;
    }

    public ScanRange getScanRange() {
        return _scanRange;
    }

    public int getBatchId() {
        return _batchId;
    }

    @JsonIgnore
    public Optional<Integer> getBlockedByBatchId() {
        return _blockedByBatchId;
    }

    @JsonProperty ("blockedByBatchId")
    Integer getBlockedByBatchIdOrNull() {
        return _blockedByBatchId.orElse(null);
    }

    @JsonIgnore
    public Optional<Integer> getConcurrencyId() {
        return _concurrencyId;
    }

    @JsonProperty ("concurrencyId")
    Integer getConcurrencyIdOrNull() {
        return _concurrencyId.orElse(null);
    }

    public Date getScanQueuedTime() {
        return _scanQueuedTime;
    }

    public void setScanQueuedTime(Date scanQueuedTime) {
        _scanQueuedTime = scanQueuedTime;
    }

    public Date getScanStartTime() {
        return _scanStartTime;
    }

    public void setScanStartTime(Date scanStartTime) {
        _scanStartTime = scanStartTime;
    }

    public Date getScanCompleteTime() {
        return _scanCompleteTime;
    }

    public void setScanCompleteTime(Date scanCompleteTime) {
        _scanCompleteTime = scanCompleteTime;
    }

    @JsonProperty ("resplitRange")
    @JsonInclude (JsonInclude.Include.NON_NULL)
    public ScanRange getResplitRangeOrNull() {
        return _resplitRange.orElse(null);
    }

    @JsonIgnore
    public Optional<ScanRange> getResplitRange() {
        return _resplitRange;
    }

    @JsonProperty ("resplitRange")
    public void setResplitRange(@Nullable ScanRange resplitRange) {
        _resplitRange = Optional.ofNullable(resplitRange);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ScanRangeStatus)) {
            return false;
        }

        ScanRangeStatus that = (ScanRangeStatus) o;

        return _batchId == that._batchId &&
                _taskId == that._taskId &&
                _blockedByBatchId.equals(that._blockedByBatchId) &&
                _concurrencyId.equals(that._concurrencyId) &&
                _placement.equals(that._placement) &&
                Objects.equals(_scanStartTime, that._scanStartTime) &&
                Objects.equals(_scanQueuedTime, that._scanQueuedTime) &&
                Objects.equals(_scanCompleteTime, that._scanCompleteTime) &&
                _scanRange.equals(that._scanRange) &&
                Objects.equals(_resplitRange, that._resplitRange);
    }

    @Override
    public int hashCode() {
        return _taskId;
    }
}
