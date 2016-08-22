package com.bazaarvoice.emodb.common.dropwizard.metrics;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.Lists;

import java.io.Closeable;
import java.util.List;

/**
 * Wraps a set of metrics registrations with a {@link java.io.Closeable}.  This helps manage metrics that can come and
 * go as the server runs.
 */
public class MetricsSet implements Closeable {
    private final String _group;
    private final MetricRegistry _registry;
    private final List<String> _metrics = Lists.newArrayList();

    public MetricsSet(MetricRegistry registry, String group) {
        _registry = registry;
        _group = group;
    }

    public void newGauge(Class<?> type, String name, String scope, Gauge<?> gauge) {
        newGauge(MetricRegistry.name(_group, type.getSimpleName().replaceAll("\\$$", ""), name, scope), gauge);
    }

    public synchronized void newGauge(String metric, Gauge<?> gauge) {
        if (_registry.register(metric, gauge) == gauge) {
            _metrics.add(metric);
        }
    }

    @Override
    public synchronized void close() {
        for (String metricName : _metrics) {
            _registry.remove(metricName);
        }
        _metrics.clear();
    }
}
