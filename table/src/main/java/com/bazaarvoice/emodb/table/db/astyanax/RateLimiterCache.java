package com.bazaarvoice.emodb.table.db.astyanax;

import com.bazaarvoice.emodb.common.zookeeper.store.ChangeType;
import com.bazaarvoice.emodb.common.zookeeper.store.MapStore;
import com.bazaarvoice.emodb.common.zookeeper.store.MapStoreListener;
import com.google.common.base.Objects;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.RateLimiter;

/**
 * A set of named rate limiters where the configured rates can be set dynamically via ZooKeeper.
 */
public class RateLimiterCache {
    private final MapStore<Double> _rates;
    private final double _defaultRate;
    private final LoadingCache<String, RateLimiter> _cache;

    public RateLimiterCache(MapStore<Double> rates, double defaultRate) {
        _rates = rates;
        _defaultRate = defaultRate;
        _cache = CacheBuilder.newBuilder().build(new CacheLoader<String, RateLimiter>() {
            @Override
            public RateLimiter load(String key) throws Exception {
                return RateLimiter.create(getRate(key));
            }
        });
        _rates.addListener(new MapStoreListener() {
            @Override
            public void entryChanged(String key, ChangeType changeType)
                    throws Exception {
                updateRateLimiter(key);
            }

            private void updateRateLimiter(String key) {
                RateLimiter limiter = _cache.getIfPresent(key);
                if (limiter != null) {
                    limiter.setRate(getRate(key));
                }
            }
        });
    }

    private double getRate(String key) {
        Double rate = _rates.get(key);
        return Objects.firstNonNull(rate, _defaultRate);
    }

    public RateLimiter get(String key) {
        return _cache.getUnchecked(key);
    }
}
