package com.bazaarvoice.emodb.web.throttling;

/**
 * Interface to enable the DataStore API to enforce all active update throttles.
 */
public interface DataStoreUpdateThrottler {
    /**
     * Should be called before each update applied at the API level.  If there is an instance-wide or API-key level rate
     * limit active on this instance this method will block if necessary to satisfy the rate limit.  In a more
     * sophisticated implementation this could reject an update if the wait is too long, allowing the API to return
     * a meaningful "rate limit exceeded" response.  However, until the EmoDB clients are updated to handle this
     * condition the safest approach is to wait instead of fail.
     *
     * @param id The ID of the API key making the update request.
     */
    void beforeUpdate(String id);
}
