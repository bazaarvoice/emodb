package com.bazaarvoice.emodb.web.scanner.scanstatus;

import com.bazaarvoice.emodb.web.scanner.scheduling.ScheduledDailyScanUpload;

import java.util.Set;

/**
 * Data layer for maintaining stash requests.
 */
public interface StashRequestDAO {

    /**
     * Adds a request for the stash with the corresponding ID.  Note that this is the ID of an individual instance
     * of a scan, not the ID of the series returned by {@link ScheduledDailyScanUpload#getId()}.  If multiple requests
     * are made for the same scan by the same {@link StashRequest#getRequestedBy()} then only the last request made
     * is maintained.
     */
    void requestStash(String scanId, StashRequest request);

    /**
     * Removes a previously made request to {@link #requestStash(String, StashRequest)} for the scan with the corresponding
     * ID and whose {@link StashRequest#getRequestedBy()} matches the original request.  If multiple such requests
     * were made then this call cancels all of them.  If no matching request was made then this is a no-op.
     */
    void undoRequestStash(String scanId, StashRequest request);

    /**
     * Gets all requests made for the same ID to {@link #requestStash(String, StashRequest)}.  If multiple requests
     * were made by the same {@link StashRequest#getRequestedBy()} then only the most recent is returned.  Requests
     * canceled using {@link #undoRequestStash(String, StashRequest)} are not returned.
     */
    Set<StashRequest> getRequestsForStash(String scanId);
}
