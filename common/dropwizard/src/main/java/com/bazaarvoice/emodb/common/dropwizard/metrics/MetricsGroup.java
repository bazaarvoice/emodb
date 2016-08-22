package com.bazaarvoice.emodb.common.dropwizard.metrics;

import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.io.Closeable;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkState;

/**
 * A group of metrics that are updated as a group.  Metrics are created on demand, and any that aren't
 * updated are assumed to be obsolete and are destroyed.
 * <p>
 * This class is useful when a single process subject to leader election updates metrics while other processes that
 * aren't leaders and aren't updating metrics should not report stale data.
 */
public class MetricsGroup implements Closeable {
    private final MetricRegistry _registry;
    private final Map<String, Metric> _metrics = Maps.newHashMap();
    private Set<String> _recentlyTouched;

    public MetricsGroup(MetricRegistry registry) {
        _registry = registry;
    }

    public synchronized void beginUpdates() {
        _recentlyTouched = Sets.newHashSet();
    }

    public GenericGauge gauge(String metric) {
        return getOrCreate(metric, new Factory<GenericGauge>() {
            @Override
            public GenericGauge create(MetricRegistry registry, String name) {
                return registry.register(name, new GenericGauge());
            }

            @Override
            public GenericGauge cast(Metric metric) {
                return (GenericGauge) metric;
            }
        });
    }

    public Timer timer(String metric, final TimeUnit durationUnit, final TimeUnit rateUnit) {
        return getOrCreate(metric, new Factory<Timer>() {
            @Override
            public Timer create(MetricRegistry registry, String name) {
                return registry.timer(name);
            }

            @Override
            public Timer cast(Metric metric) {
                return (Timer) metric;
            }
        });
    }

    private synchronized <T extends Metric> T getOrCreate(String name, Factory<T> factory) {
        checkState(_recentlyTouched != null, "The beginUpdates method has not been called.");
        Metric metric = _metrics.get(name);
        if (metric == null) {
            _registry.remove(name);
            metric = factory.create(_registry, name);
            _metrics.put(name, metric);
        }
        _recentlyTouched.add(name);
        return factory.cast(metric);
    }

    public synchronized void endUpdates() {
        // Remove active gauges that weren't updated since, if they weren't updated, they're no longer active.
        checkState(_recentlyTouched != null, "The beginUpdates method has not been called.");
        Iterator<String> iter = _metrics.keySet().iterator();
        while (iter.hasNext()) {
            String metric = iter.next();
            if (!_recentlyTouched.contains(metric)) {
                _registry.remove(metric);
                iter.remove();
            }
        }
        _recentlyTouched = null;
    }

    @Override
    public synchronized void close() {
        for (String metric : _metrics.keySet()) {
            _registry.remove(metric);
        }
        _metrics.clear();
    }

    private static interface Factory<T extends Metric> {
        T create(MetricRegistry registry, String name);

        T cast(Metric metric);
    }
}
