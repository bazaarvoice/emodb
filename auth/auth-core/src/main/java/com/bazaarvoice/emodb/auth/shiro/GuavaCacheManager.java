package com.bazaarvoice.emodb.auth.shiro;

import com.bazaarvoice.emodb.cachemgr.api.CacheHandle;
import com.bazaarvoice.emodb.cachemgr.api.CacheRegistry;
import com.bazaarvoice.emodb.cachemgr.api.InvalidationScope;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheBuilderSpec;
import org.apache.shiro.cache.AbstractCacheManager;
import org.apache.shiro.cache.CacheException;
import org.apache.shiro.subject.SimplePrincipalCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static java.util.Objects.requireNonNull;

/**
 * Implementation of {@link org.apache.shiro.cache.CacheManager} which uses a Guava Cache for storage.
 */
public class GuavaCacheManager extends AbstractCacheManager implements InvalidatableCacheManager {
    private static Logger _log = LoggerFactory.getLogger(GuavaCacheManager.class);

    private final static String DEFAULT_CACHE_BUILDER_SPEC = "maximumSize=500";

    private CacheBuilderSpec _spec;

    private final ConcurrentMap<String, GuavaCache> _allAvailableCaches = new ConcurrentHashMap<>();
    private final CacheRegistry _cacheRegistry; // already backed by ConcurrentMap

    public GuavaCacheManager(CacheRegistry cacheRegistry, String cacheBuilderSpec) {
        _spec = CacheBuilderSpec.parse(requireNonNull(cacheBuilderSpec));
        _cacheRegistry = cacheRegistry;
    }

    public GuavaCacheManager(CacheRegistry cacheRegistry) {
        this(cacheRegistry, DEFAULT_CACHE_BUILDER_SPEC);
    }

    @Override
    protected org.apache.shiro.cache.Cache createCache(String name) throws CacheException {
        // name will be e.g.: "EmoDB.authorizationCache", "EmoDB.authenticationCache"
        Cache<String, Object> cache = CacheBuilder.from(_spec).<String, Object>build();

        GuavaCache managedCache = _allAvailableCaches.get(name);
        if (managedCache != null) {
            _log.debug("Returned existing {} named {}", cache.getClass().getName(), name);
        } else {
            GuavaCache newCache = new GuavaCache(name, cache);
            managedCache = _allAvailableCaches.putIfAbsent(name, newCache);

            if (managedCache == null) {
                // First time registering this cache
                _log.debug("Created {} named {}", cache.getClass().getName(), name);
                if (_cacheRegistry != null) {
                    _cacheRegistry.register(name, cache, true);
                }
                managedCache = newCache;
            } else {
               _log.debug("Returned existing {} named {}", cache.getClass().getName(), name);
            }
        }

        return managedCache;
    }

    @Override
    public void invalidateAll() {
        if(_cacheRegistry != null) {
            for(String name : _allAvailableCaches.keySet()) {
                // There is a potential race condition here since the cache is added to _allAvailableCaches
                // then subsequently added to _cacheRegistry in createCache(), making it possible for a value to
                // be cached but not invalidated on this call.  However, to avoid this issue all caches used by
                // this manager are pre-created in ApiKeyRealm.onInit(), so in practice this can never happen.
                CacheHandle cacheHandle = _cacheRegistry.lookup(name, false);
                cacheHandle.invalidateAll(InvalidationScope.GLOBAL);
            }
        } else {
            for(GuavaCache cache : _allAvailableCaches.values()) {
                cache.clear();
            }
        }
    }

    private class GuavaCache implements org.apache.shiro.cache.Cache {
        private final String _name;
        private final Cache<String, Object> _cache;

        private GuavaCache(String name, Cache<String, Object> cache) {
            _name = name;
            _cache = cache;
        }

        @Override
        public Object get(Object key)
                throws CacheException {
            if(key == null) return null;
            String stringKey = extractStringKey(key);
            Object hit = _cache.getIfPresent(stringKey);
            _log.debug("{} cache hit for key {} in {}", hit == null ? "No " : "Got ", key, _name);
            return hit;
        }

        private String extractStringKey(Object key) {
            if(key instanceof SimplePrincipalCollection) {
                SimplePrincipalCollection coll = (SimplePrincipalCollection) key;
                return "SimplePrincipalCollection:" + coll.getPrimaryPrincipal();
            } else {
                return key.toString();
            }
        }

        @Override
        public Object put(Object key, Object value)
                throws CacheException {
            String stringKey = extractStringKey(key);
            Object oldValue = _cache.getIfPresent(stringKey);
            _cache.put(stringKey, value);
            return oldValue;
        }

        @Override
        public Object remove(Object key)
                throws CacheException {
            String stringKey = extractStringKey(key);
            Object oldValue = _cache.getIfPresent(stringKey);
            _cache.invalidate(key);
            return oldValue;
        }

        @Override
        public void clear()
                throws CacheException {
            _cache.invalidateAll();
        }

        @Override
        public int size() {
            return (int) _cache.size();
        }

        @Override
        public Set keys() {
            return _cache.asMap().keySet();
        }

        @Override
        public Collection values() {
            return _cache.asMap().values();
        }
    }
}

