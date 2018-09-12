package com.bazaarvoice.emodb.web.throttling;

/**
 * Implementation of {@link DataStoreUpdateThrottler} which never throttles.  Useful for testing.
 */
public class UnlimitedDataStoreUpdateThrottler implements DataStoreUpdateThrottler {

    @Override
    public void beforeUpdate(String id) {
        // no-op
    }
}
