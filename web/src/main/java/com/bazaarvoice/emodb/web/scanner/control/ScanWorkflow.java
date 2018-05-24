package com.bazaarvoice.emodb.web.scanner.control;

import com.bazaarvoice.emodb.sor.db.ScanRange;
import java.time.Duration;

import java.util.Collection;
import java.util.List;

/**
 * Defines the workflow for scanning and uploading all of the ranges for a scan upload operation.
 */
public interface ScanWorkflow {

    /**
     * Signals that a scan's status has been updated, usually because it is a new scan that has just been created
     * or because one of the scan range tasks has completed.
     */
    void scanStatusUpdated(String scanId);

    /**
     * Marks a scan range and available for uploading as part of a scan.
     */
    ScanRangeTask addScanRangeTask(String scanId, int taskId, String placement, ScanRange range);

    /**
     * Claims up to max scan ranges for processing locally.  The caller must periodically call
     * {@link #renewScanRangeTasks(Collection, Duration)} for the tasks returned otherwise they be may made available
     * to future calls to this method.  Once the task is completed the caller must call
     * {@link #releaseScanRangeTask(ScanRangeTask)} to signal that it is complete and should not be made available
     * again.
     */
    List<ScanRangeTask> claimScanRangeTasks(int max, Duration ttl);

    /**
     * Marks that the current process is still actively working on the provided scan ranges, preventing the system
     * from making the available for claiming again for an interval.
     */
    void renewScanRangeTasks(Collection<ScanRangeTask> tasks, Duration ttl);

    /**
     * Releases the scan range, effectively signaling that no more processing is required.  As a consequence of this call
     * the scan range is immediately made available to a future call of {@link #claimCompleteScanRanges(Duration)}.
     */
    void releaseScanRangeTask(ScanRangeTask task);

    /**
     * Claims all scan ranges that have been completed by prior calls to {@link #releaseScanRangeTask(ScanRangeTask)}.
     * The caller must call {@link #releaseCompleteScanRanges(Collection)} for all returned ranges, otherwise
     * they may be made available to future calls of this method.
     */
    List<ScanRangeComplete> claimCompleteScanRanges(Duration ttl);

    /**
     * Releases all completions claimed by a prior call to {@link #claimCompleteScanRanges(Duration)}.
     */
    void releaseCompleteScanRanges(Collection<ScanRangeComplete> completions);
}
