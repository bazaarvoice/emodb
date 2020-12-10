package com.bazaarvoice.emodb.cachemgr.core;

import com.bazaarvoice.emodb.cachemgr.api.CacheHandle;
import com.bazaarvoice.emodb.cachemgr.api.CacheRegistry;
import com.bazaarvoice.emodb.cachemgr.api.InvalidationEvent;
import com.bazaarvoice.emodb.cachemgr.api.InvalidationListener;
import com.bazaarvoice.emodb.cachemgr.api.InvalidationScope;
import com.bazaarvoice.emodb.common.dropwizard.lifecycle.LifeCycleRegistry;
import com.bazaarvoice.emodb.common.dropwizard.metrics.InstrumentedCache;
import com.bazaarvoice.emodb.common.dropwizard.metrics.MetricsSet;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Throwables;
import com.google.common.cache.Cache;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

public class DefaultCacheRegistry implements CacheRegistry, Closeable {
    private static final Logger _log = LoggerFactory.getLogger(DefaultCacheRegistry.class);

    private final ConcurrentMap<String, HandleImpl> _handles = new ConcurrentHashMap<>();
    private final List<InvalidationListener> _listeners = Lists.newCopyOnWriteArrayList();
    private final List<MetricsSet> _metrics = new ArrayList<>();
    private final MetricRegistry _metricRegistry;

    @Inject
    public DefaultCacheRegistry(LifeCycleRegistry lifeCycle, MetricRegistry metricRegistry) {
        lifeCycle.manage(this);
        _metricRegistry = metricRegistry;
    }

    @Override
    public CacheRegistry withNamespace(String namespace) {
        return new NamespacedCacheRegistry(this, namespace);
    }

    @Override
    public CacheHandle register(String name, Cache<String, ?> cache, boolean instrumented) {
        return register(name, cache, instrumented, cache instanceof LoadingCache);
    }

    @Override
    public CacheHandle register(String name, Cache<String, ?> cache, boolean instrumented, boolean loadingCache) {
        HandleImpl handle = getOrPut(name);
        handle.setCache(cache);
        if (instrumented) {  // eg. CacheBuilder.recordStats() was used when constructing the cache
            _metrics.add(InstrumentedCache.instrument(cache, _metricRegistry, "bv.emodb.cachemgr", name, loadingCache));
        }
        return handle;
    }

    @Override
    public CacheHandle lookup(String name, boolean create) {
        return create ? getOrPut(name) : _handles.get(name);
    }

    private HandleImpl getOrPut(String name) {
        HandleImpl handle = new HandleImpl(name);
        return Optional.ofNullable(_handles.putIfAbsent(name, handle)).orElse(handle);
    }

    @Override
    public void addListener(InvalidationListener listener) {
        _listeners.add(listener);
    }

    private void notifyListeners(InvalidationEvent event) {
        // Call all the listeners.  If any fail, propagate an exception so the caller discovers that some
        // listener has not been able to observe the new value.
        Throwable first = null;
        for (InvalidationListener listener : _listeners) {
            try {
                listener.handleInvalidation(event);
            } catch (Throwable t) {
                // Propagate the first exception, log the rest.
                if (first == null) {
                    first = t;
                } else {
                    _log.error("Exception handling cache invalidation event: {}", event.getCache(), t);
                }
            }
        }
        if (first != null) {
            throw Throwables.propagate(first);
        }
    }

    @Override
    public void close() {
        for (MetricsSet metrics : _metrics) {
            metrics.close();
        }
    }

    private class HandleImpl implements CacheHandle {
        private final String _name;
        private final AtomicReference<Cache<String, ?>> _cache = new AtomicReference<>();

        private HandleImpl(String name) {
            _name = Objects.requireNonNull(name, "name");
        }

        private void setCache(Cache<String, ?> cache) {
            if (!_cache.compareAndSet(null, cache)) {
                throw new IllegalStateException("Cache has been registered already: " + _name);
            }
        }

        @Override
        public boolean matches(InvalidationEvent event) {
            return _name.equals(event.getCache());
        }

        @Override
        public void invalidateAll(InvalidationScope scope) {
            invalidate(new InvalidationEvent(this, _name, scope));
        }

        @Override
        public void invalidateAll(InvalidationScope scope, Collection<String> keys) {
            invalidate(new InvalidationEvent(this, _name, scope, keys));
        }

        @Override
        public void invalidate(InvalidationScope scope, String key) {
            invalidate(new InvalidationEvent(this, _name, scope, ImmutableList.of(key)));
        }

        private void invalidate(InvalidationEvent event) {
            // Invalidate the actual cache.
            Cache<String, ?> cache = _cache.get();
            if (cache != null) {
                if (event.hasKeys()) {
                    cache.invalidateAll(event.getKeys());
                } else {
                    cache.invalidateAll();
                }
            }
            notifyListeners(event);
        }
    }
}
