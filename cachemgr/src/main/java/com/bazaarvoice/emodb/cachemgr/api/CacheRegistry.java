package com.bazaarvoice.emodb.cachemgr.api;

import com.google.common.cache.Cache;

public interface CacheRegistry {

    CacheRegistry withNamespace(String prefix);

    CacheHandle register(String name, Cache<String, ?> cache, boolean instrumented);

    CacheHandle register(String name, Cache<String, ?> cache, boolean instrumented, boolean loadingCache);

    CacheHandle lookup(String name, boolean create);

    void addListener(InvalidationListener listener);
}
