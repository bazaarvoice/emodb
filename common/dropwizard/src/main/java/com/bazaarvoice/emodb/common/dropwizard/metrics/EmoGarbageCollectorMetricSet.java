package com.bazaarvoice.emodb.common.dropwizard.metrics;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricSet;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

// This acts as a variant of the com.codahale.metrics.jvm.GarbageCollectorMetricSet class. The key difference is that it reports metric names in a format more suitable to Datadog (with the collector
// name appended as a parameter, not included in the middle of the metric name)."
public class EmoGarbageCollectorMetricSet implements MetricSet {

    private List<GarbageCollectorMXBean> _garbageCollectorMXBeans;

    public EmoGarbageCollectorMetricSet() {
        _garbageCollectorMXBeans = ManagementFactory.getGarbageCollectorMXBeans();
    }

    @Override
    public Map<String, Metric> getMetrics() {
        final Map<String, Metric> metrics = new HashMap();
        for (final GarbageCollectorMXBean garbageCollectorMXBean : _garbageCollectorMXBeans) {
            final String collectorName = garbageCollectorMXBean.getName().replaceAll("[\\s]+", "_");
            String timeName = String.format("time[collector:%s]", collectorName);
            metrics.put(timeName, new Gauge<Long>() {
                @Override
                public Long getValue() {
                    return garbageCollectorMXBean.getCollectionTime();
                }
            });

            String runsName = String.format("runs[collector:%s]", collectorName);
            metrics.put(runsName, new Gauge<Long>() {
                @Override
                public Long getValue() {
                    return garbageCollectorMXBean.getCollectionCount();
                }
            });
        }
        return metrics;
    }
}
