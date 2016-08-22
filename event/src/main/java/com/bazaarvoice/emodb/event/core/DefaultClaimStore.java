package com.bazaarvoice.emodb.event.core;

import com.bazaarvoice.emodb.common.dropwizard.lifecycle.LifeCycleRegistry;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import io.dropwizard.lifecycle.ExecutorServiceManager;
import io.dropwizard.util.Duration;

import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * In-memory implementation of the {@link ClaimSet} interface.
 */
public class DefaultClaimStore implements ClaimStore {
    private final Map<String, Handle> _map = Maps.newHashMap();

    @Inject
    public DefaultClaimStore(LifeCycleRegistry lifeCycle, @MetricsGroupName String metricsGroup, MetricRegistry metricRegistry) {
        ScheduledExecutorService scheduledExecutor = defaultScheduledExecutor(lifeCycle, metricsGroup);

        // Periodically cleanup ClaimSets with no active claims.
        scheduledExecutor.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                removeEmptyClaimSets();
            }
        }, 15, 15, TimeUnit.SECONDS);

        // Expose gauges for the # of channels and # of claims.
        metricRegistry.register(MetricRegistry.name(metricsGroup, "DefaultClaimStore", "channels"), new Gauge<Integer>() {
            @Override
            public Integer getValue() {
                return getNumChannels();
            }
        });
        metricRegistry.register(MetricRegistry.name(metricsGroup, "DefaultClaimStore", "claims"), new Gauge<Integer>() {
            @Override
            public Integer getValue() {
                return getNumClaims();
            }
        });
    }

    private static ScheduledExecutorService defaultScheduledExecutor(LifeCycleRegistry lifeCycle, String metricsGroup) {
        String nameFormat = "Events Claim Cleanup-" + metricsGroup.substring(metricsGroup.lastIndexOf('.') + 1) + "-%d";
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(1,
                new ThreadFactoryBuilder().setNameFormat(nameFormat).setDaemon(true).build());
        lifeCycle.manage(new ExecutorServiceManager(executor, Duration.seconds(5), nameFormat));
        return executor;
    }

    private synchronized Integer getNumChannels() {
        return _map.size();
    }

    private synchronized Integer getNumClaims() {
        int size = 0;
        for (Handle handle : _map.values()) {
            size += handle.getClaimSet().size();
        }
        return size;
    }

    @Override
    public <T> T withClaimSet(String name, Function<ClaimSet, T> function) {
        Handle handle = acquire(name);
        try {
            return function.apply(handle.getClaimSet());
        } finally {
            release(handle);
        }
    }

    private synchronized Handle acquire(String name) {
        Handle handle = _map.get(name);
        if (handle == null) {
            handle = new Handle(new DefaultClaimSet());
            _map.put(name, handle);
        }
        handle.getRefCount().incrementAndGet();
        return handle;
    }

    private synchronized void release(Handle handle) {
        handle.getRefCount().decrementAndGet();
    }

    @Override
    public synchronized Map<String, Long> snapshotClaimCounts() {
        Map<String, Long> snapshot = Maps.newHashMap();
        for (Map.Entry<String, Handle> entry : _map.entrySet()) {
            snapshot.put(entry.getKey(), entry.getValue().getClaimSet().size());
        }
        return snapshot;
    }

    /**
     * Cleans up old claim sets for subscriptions that have become inactive.  Ensures the map of claim sets doesn't
     * grow forever.
     */
    private synchronized void removeEmptyClaimSets() {
        Iterables.removeIf(_map.values(), new Predicate<Handle>() {
            @Override
            public boolean apply(Handle handle) {
                handle.getClaimSet().pump();
                return handle.getRefCount().get() == 0 && handle.getClaimSet().size() == 0;
            }
        });
    }

    private static class Handle {
        private final ClaimSet _claimSet;
        private final AtomicInteger _refCount = new AtomicInteger();

        Handle(ClaimSet claimSet) {
            _claimSet = claimSet;
        }

        ClaimSet getClaimSet() {
            return _claimSet;
        }

        AtomicInteger getRefCount() {
            return _refCount;
        }
    }
}
