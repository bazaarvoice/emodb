package com.bazaarvoice.emodb.auth.shiro;

import org.apache.shiro.cache.CacheManager;

/**
 * Extension of shiro CacheManager that permits invalidating its caches by an explicit external call.
 */
public interface InvalidatableCacheManager extends CacheManager {

    void invalidateAll();
}
