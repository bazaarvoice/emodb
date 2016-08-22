package com.bazaarvoice.emodb.cachemgr.api;

import java.util.Collection;

/**
 * Handle to a cache registered with a {@link CacheRegistry}.
 */
public interface CacheHandle {

    boolean matches(InvalidationEvent event);

    void invalidateAll(InvalidationScope scope);

    void invalidateAll(InvalidationScope scope, Collection<String> keys);

    void invalidate(InvalidationScope scope, String key);
}
