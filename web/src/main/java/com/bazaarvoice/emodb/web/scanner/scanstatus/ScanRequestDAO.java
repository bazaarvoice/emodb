package com.bazaarvoice.emodb.web.scanner.scanstatus;

import com.bazaarvoice.emodb.web.scanner.scheduling.ScheduledDailyScanUpload;

import java.util.Set;

/**
 * Data layer for maintaining scan requests.
 */
public interface ScanRequestDAO {

    /**
     * Adds a request for the scan with the corresponding ID.  Note that this is the ID of an individual instance
     * of a scan, not the ID of the series returned by {@link ScheduledDailyScanUpload#getId()}.  If multiple requests
     * are made for the same scan by the same {@link ScanRequest#getRequestedBy()} then only the last request made
     * is maintained.
     */
    void requestScan(String scanId, ScanRequest request);

    /**
     * Removes a previously made request to {@link #requestScan(String, ScanRequest)} for the scan with the corresponding
     * ID and whose {@link ScanRequest#getRequestedBy()} matches the original request.  If multiple such requests
     * were made then this call cancels all of them.  If no matching request was made then this is a no-op.
     */
    void undoRequestScan(String scanId, ScanRequest request);

    /**
     * Gets all requests made for the same ID to {@link #requestScan(String, ScanRequest)}.  If multiple requests
     * were made by the same {@link ScanRequest#getRequestedBy()} then only the most recent is returned.  Requests
     * canceled using {@link #undoRequestScan(String, ScanRequest)} are not returned.
     */
    Set<ScanRequest> getRequestsForScan(String scanId);
}
