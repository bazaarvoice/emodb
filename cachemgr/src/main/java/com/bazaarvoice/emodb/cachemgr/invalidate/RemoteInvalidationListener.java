package com.bazaarvoice.emodb.cachemgr.invalidate;

import com.bazaarvoice.emodb.cachemgr.api.CacheRegistry;
import com.bazaarvoice.emodb.cachemgr.api.InvalidationEvent;
import com.bazaarvoice.emodb.cachemgr.api.InvalidationListener;
import com.bazaarvoice.emodb.cachemgr.api.InvalidationScope;
import com.google.inject.Inject;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Listens for {@link InvalidationEvent} and forwards those events to other servers and other
 * data centers based on {@link InvalidationEvent#getScope()}.
 */
public class RemoteInvalidationListener implements InvalidationListener {
    private final RemoteInvalidationProvider _invalidator;

    @Inject
    public RemoteInvalidationListener(CacheRegistry cacheRegistry, RemoteInvalidationProvider invalidator) {
        _invalidator = checkNotNull(invalidator, "invalidator");
        cacheRegistry.addListener(this);
    }

    @Override
    public void handleInvalidation(InvalidationEvent event) {
        InvalidationScope scope = event.getScope();
        // First, flush every individual server in our local data center.
        if (scope == InvalidationScope.DATA_CENTER || scope == InvalidationScope.GLOBAL) {
            _invalidator.invalidateOtherServersInSameDataCenter(event);
        }
        // Next, flush remote data centers.
        if (scope == InvalidationScope.GLOBAL) {
            _invalidator.invalidateOtherDataCenters(event);
        }
    }
}
