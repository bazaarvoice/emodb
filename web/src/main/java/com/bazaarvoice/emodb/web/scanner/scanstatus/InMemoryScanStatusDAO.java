package com.bazaarvoice.emodb.web.scanner.scanstatus;

import com.bazaarvoice.emodb.common.api.impl.LimitCounter;
import com.bazaarvoice.emodb.sor.db.ScanRange;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import javax.annotation.Nullable;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class InMemoryScanStatusDAO implements ScanStatusDAO {

    private Map<String, ScanStatus> _scanStatuses = Maps.newConcurrentMap();

    @Override
    public Iterator<ScanStatus> list(@Nullable String fromIdExclusive, long limit) {
        Iterator<ScanStatus> results = ImmutableList.copyOf(_scanStatuses.values()).iterator();
        if (fromIdExclusive != null) {
            boolean found = false;
            while (results.hasNext() && !found) {
                ScanStatus status = results.next();
                if (status.getScanId().equals(fromIdExclusive)) {
                    found = true;
                }
            }
        }
        return new LimitCounter(limit).limit(results);
    }

    @Override
    public void updateScanStatus(ScanStatus status) {
        // Store a copy so subsequent changes to the passed in instance don't affect the persisted value
        ScanStatus mutableCopy = new ScanStatus(
                status.getScanId(), status.getOptions(), status.isCanceled(), status.getStartTime(),
                Lists.newArrayList(status.getPendingScanRanges()),
                Lists.newArrayList(status.getActiveScanRanges()),
                Lists.newArrayList(status.getCompleteScanRanges()),
                status.getCompleteTime());

        _scanStatuses.put(status.getScanId(), mutableCopy);
    }

    @Override
    public ScanStatus getScanStatus(String scanId) {
        ScanStatus status = _scanStatuses.get(scanId);
        if (status == null) {
            return null;
        }

        // Return an immutable copy
        return new ScanStatus(
                status.getScanId(), status.getOptions(), status.isCanceled(), status.getStartTime(),
                ImmutableList.copyOf(status.getPendingScanRanges()),
                ImmutableList.copyOf(status.getActiveScanRanges()),
                ImmutableList.copyOf(status.getCompleteScanRanges()),
                status.getCompleteTime());
    }

    @Override
    public void setScanRangeTaskQueued(String scanId, int taskId, Date queuedTime) {
        ScanStatus status = _scanStatuses.get(scanId);

        for (ScanRangeStatus rangeStatus : status.getPendingScanRanges()) {
            if (rangeStatus.getTaskId() == taskId) {
                rangeStatus.setScanQueuedTime(queuedTime);
                return;
            }
        }
    }

    @Override
    public void setScanRangeTaskActive(String scanId, int taskId, Date startTime) {
        ScanStatus status = _scanStatuses.get(scanId);

        for (ScanRangeStatus rangeStatus : status.getPendingScanRanges()) {
            if (rangeStatus.getTaskId() == taskId) {
                rangeStatus.setScanStartTime(startTime);
                status.getPendingScanRanges().remove(rangeStatus);
                status.getActiveScanRanges().add(rangeStatus);
                return;
            }
        }
    }

    @Override
    public void setScanRangeTaskInactive(String scanId, int taskId) {
        ScanStatus status = _scanStatuses.get(scanId);

        for (ScanRangeStatus rangeStatus : status.getActiveScanRanges()) {
            if (rangeStatus.getTaskId() == taskId) {
                rangeStatus.setScanQueuedTime(null);
                rangeStatus.setScanStartTime(null);
                rangeStatus.setScanCompleteTime(null);
                status.getActiveScanRanges().remove(rangeStatus);
                status.getPendingScanRanges().add(rangeStatus);
                return;
            }
        }
    }

    @Override
    public void setScanRangeTaskComplete(String scanId, int taskId, Date completeTime) {
        ScanStatus status = _scanStatuses.get(scanId);

        for (ScanRangeStatus rangeStatus : status.getActiveScanRanges()) {
            if (rangeStatus.getTaskId() == taskId) {
                rangeStatus.setScanCompleteTime(completeTime);
                status.getActiveScanRanges().remove(rangeStatus);
                status.getCompleteScanRanges().add(rangeStatus);
                return;
            }
        }
    }

    @Override
    public void setScanRangeTaskPartiallyComplete(String scanId, int taskId, ScanRange completeRange, ScanRange resplitRange, Date completeTime) {
        ScanStatus status = _scanStatuses.get(scanId);

        for (ScanRangeStatus rangeStatus : status.getActiveScanRanges()) {
            if (rangeStatus.getTaskId() == taskId) {
                ScanRangeStatus updatedRangeStatus = new ScanRangeStatus(rangeStatus.getTaskId(), rangeStatus.getPlacement(),
                        completeRange, rangeStatus.getBatchId(), rangeStatus.getBlockedByBatchId(),
                        rangeStatus.getConcurrencyId());
                updatedRangeStatus.setScanQueuedTime(rangeStatus.getScanQueuedTime());
                updatedRangeStatus.setScanStartTime(rangeStatus.getScanStartTime());
                updatedRangeStatus.setScanCompleteTime(completeTime);
                updatedRangeStatus.setResplitRange(resplitRange);

                status.getActiveScanRanges().remove(rangeStatus);
                status.getCompleteScanRanges().add(updatedRangeStatus);
                return;
            }
        }
    }

    @Override
    public void resplitScanRangeTask(String scanId, int taskId, List<ScanRangeStatus> splitScanStatuses) {
        ScanStatus status = _scanStatuses.get(scanId);

        for (ScanRangeStatus rangeStatus : status.getCompleteScanRanges()) {
            if (rangeStatus.getTaskId() == taskId) {
                rangeStatus.setResplitRange(null);
                break;
            }
        }

        status.getPendingScanRanges().addAll(splitScanStatuses);
    }

    @Override
    public void setCompleteTime(String scanId, Date completeTime) {
        ScanStatus status = _scanStatuses.get(scanId);
        _scanStatuses.put(scanId, new ScanStatus(scanId, status.getOptions(), status.isCanceled(), status.getStartTime(),
                status.getPendingScanRanges(), status.getActiveScanRanges(), status.getCompleteScanRanges(), completeTime));
    }

    @Override
    public void setCanceled(String scanId) {
        ScanStatus status = _scanStatuses.get(scanId);
        _scanStatuses.put(scanId, new ScanStatus(scanId, status.getOptions(), true, status.getStartTime(),
                status.getPendingScanRanges(), status.getActiveScanRanges(), status.getCompleteScanRanges(),
                status.getCompleteTime()));
    }
}
