package com.bazaarvoice.emodb.cachemgr.invalidate;

import com.bazaarvoice.emodb.cachemgr.api.InvalidationEvent;

/**
 * Invalidates cache entries on remote server.
 */
public interface RemoteInvalidationProvider {

    /**
     * Invalidate the specified cache keys in all servers within the same data center.
     */
    void invalidateOtherServersInSameDataCenter(InvalidationEvent event);

    /**
     * Invalidate the specified cache keys in all servers in other data centers.
     */
    void invalidateOtherDataCenters(InvalidationEvent event);
}
