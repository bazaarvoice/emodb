package com.bazaarvoice.emodb.cachemgr.core;

import com.bazaarvoice.emodb.cachemgr.api.CacheHandle;
import com.bazaarvoice.emodb.cachemgr.api.CacheRegistry;
import com.bazaarvoice.emodb.cachemgr.api.InvalidationListener;
import com.google.common.base.Strings;
import com.google.common.cache.Cache;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

class NamespacedCacheRegistry implements CacheRegistry {
    private final CacheRegistry _root;
    private final String _prefix;

    NamespacedCacheRegistry(CacheRegistry root, String namespace) {
        checkArgument(!Strings.isNullOrEmpty(namespace), "Namespace is null or empty");
        checkArgument(!namespace.startsWith("."), "Namespace starts with \".\"");
        checkArgument(!namespace.endsWith("."), "Namespace ends with \".\"");
        _root = requireNonNull(root, "root");
        _prefix = namespace + ".";
    }

    @Override
    public CacheRegistry withNamespace(String namespace) {
        return new NamespacedCacheRegistry(_root, _prefix + namespace);
    }

    @Override
    public CacheHandle register(String name, Cache<String, ?> cache, boolean instrumented) {
        return _root.register(_prefix + name, cache, instrumented);
    }

    @Override
    public CacheHandle register(String name, Cache<String, ?> cache, boolean instrumented, boolean loadingCache) {
        return _root.register(_prefix + name, cache, instrumented, loadingCache);
    }

    @Override
    public CacheHandle lookup(String name, boolean create) {
        return _root.lookup(_prefix + name, create);
    }

    @Override
    public void addListener(InvalidationListener listener) {
        _root.addListener(listener);
    }
}
