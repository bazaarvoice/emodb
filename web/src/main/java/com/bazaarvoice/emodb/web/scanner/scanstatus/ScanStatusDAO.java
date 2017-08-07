package com.bazaarvoice.emodb.web.scanner.scanstatus;

import com.bazaarvoice.emodb.sor.db.ScanRange;

import javax.annotation.Nullable;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

/**
 * Provides access methods for creating, reading, and updating scan upload operations.  Note that implementations of
 * this interface only store scan states; it is up to the caller to actually move the scan through the workflow to
 * match the state being stored.
 */
public interface ScanStatusDAO {
    /**
     * Lists all scans.
     */
    Iterator<ScanStatus> list(@Nullable String fromIdExclusive, long limit);

    /**
     * Stores the given scan status as-is.
     */
    void updateScanStatus(ScanStatus scanStatus);

    /**
     * Returns the status for a previously created scan.
     */
    ScanStatus getScanStatus(String scanId);

    /**
     * Updates the scan to mark the given scan range as queued.
     */
    void setScanRangeTaskQueued(String scanId, int taskId, Date queuedTime);

    /**
     * Updates the scan to mark the given scan range as active.
     */
    void setScanRangeTaskActive(String scanId, int taskId, Date startTime);

    /**
     * Updates the scan to mark the given scan range as inactive.  This can happen if the range fails
     * and will be retried.
     */
    void setScanRangeTaskInactive(String scanId, int taskId);

    /**
     * Updates the scan to mark the given scan range as complete
     */
    void setScanRangeTaskComplete(String scanId, int taskId, Date completeTime);

    /**
     * Updates the scan to mark the given scan range as partially complete.  This can happen if the range was determined
     * to be too large and a portion of the range needs to be resplit into separate tasks.
     */
    void setScanRangeTaskPartiallyComplete(String scanId, int taskId, ScanRange completeRange, ScanRange resplitRange, Date completeTime);

    /**
     * Unmarks a scan range that had been marked for resplitting and adds the new scan ranges that were
     * calculated as a result of the resplit.
     */
    void resplitScanRangeTask(String scanId, int taskId, List<ScanRangeStatus> splitScanStatuses);

    /**
     * Updates the scan to mark it as canceled.  This can potentially cause one or more scan ranges to
     * not be uploaded.
     */
    void setCanceled(String scanId);

    /**
     * Sets the time this scan completed.
     */
    void setCompleteTime(String scanId, Date completeTime);

    /**
     * Updates the scan to mark that the table snapshot have been created.
     */
    void setTableSnapshotCreated(String scanId);
}
