package com.bazaarvoice.emodb.databus.core;

import com.bazaarvoice.emodb.common.dropwizard.metrics.MetricsGroup;
import com.bazaarvoice.emodb.datacenter.api.DataCenter;
import com.bazaarvoice.emodb.datacenter.api.DataCenters;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AbstractScheduledService;
import org.joda.time.Duration;

import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Service which monitors the lags for all fanouts currently being performed by this server and publishes the maximum
 * as a single metric.
 */
public class FanoutLagMonitor extends AbstractScheduledService {

    private static final Duration LAG_UPDATE_INTERVAL = Duration.standardSeconds(10);

    private final int _masterFanoutPartitions;
    private final int _dataCenterFanoutPartitions;
    private final DataCenters _dataCenters;
    private final MetricRegistry _metricRegistry;
    private final MetricsGroup _gauges;

    public FanoutLagMonitor(int masterFanoutPartitions, int dataCenterFanoutPartitions, DataCenters dataCenters,
                            MetricRegistry metricRegistry) {
        _masterFanoutPartitions = masterFanoutPartitions;
        _dataCenterFanoutPartitions = dataCenterFanoutPartitions;
        _dataCenters = checkNotNull(dataCenters, "dataCenters");
        _metricRegistry = checkNotNull(metricRegistry, "metricRegistry");
        _gauges = new MetricsGroup(metricRegistry);
    }

    @Override
    protected Scheduler scheduler() {
        return Scheduler.newFixedDelaySchedule(0, LAG_UPDATE_INTERVAL.getMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    protected void shutDown() throws Exception {
        _gauges.close();
    }

    @Override
    protected void runOneIteration() throws Exception {
        _gauges.beginUpdates();

        _gauges.gauge(lagMetric("master")).set(getMaximumLag("master", _masterFanoutPartitions));

        DataCenter self = _dataCenters.getSelf();
        for (DataCenter dataCenter : _dataCenters.getAll()) {
            if (!dataCenter.equals(self)) {
                String metricName = "in-" + dataCenter.getName();
                _gauges.gauge(lagMetric(metricName)).set(getMaximumLag(metricName, _dataCenterFanoutPartitions));
            }
        }

        _gauges.endUpdates();
    }

    private String lagMetric(String name) {
        return MetricRegistry.name("bv.emodb.databus", "DefaultFanout", "lagSeconds", name);
    }

    private String partitionLagMetric(String name, int partition) {
        return MetricRegistry.name("bv.emodb.databus", "DefaultFanout", "lagSeconds", name, "partition-" + partition);
    }

    private long getMaximumLag(String name, int partitions) {
        final Set<String> metricNames = Sets.newHashSet();
        for (int partition=0; partition < partitions; partition++) {
            metricNames.add(partitionLagMetric(name, partition));
        }
        MetricFilter metricFilter = (metricName, metric) -> metricNames.contains(metricName);

        return _metricRegistry.getGauges(metricFilter).values().stream()
                .map(gauge -> ((Number) gauge.getValue()).longValue())
                .max(Long::compareTo)
                .orElse(0L);
    }
}
