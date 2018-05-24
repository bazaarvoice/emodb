package com.bazaarvoice.emodb.databus.core;

import com.bazaarvoice.emodb.common.dropwizard.lifecycle.ServiceFailureListener;
import com.bazaarvoice.emodb.common.dropwizard.metrics.MetricsGroup;
import com.bazaarvoice.emodb.databus.ChannelNames;
import com.bazaarvoice.emodb.datacenter.api.DataCenter;
import com.bazaarvoice.emodb.datacenter.api.DataCenters;
import com.bazaarvoice.emodb.table.db.ClusterInfo;
import com.codahale.metrics.MetricRegistry;
import com.google.common.util.concurrent.AbstractScheduledService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A service that polls system queue sizes periodically and reports them to Datadog via Yammer Metrics, subject
 * to leader election.
 */
public class SystemQueueMonitor extends AbstractScheduledService {
    private static final Logger _log = LoggerFactory.getLogger(SystemQueueMonitor.class);

    private static final Duration POLL_INTERVAL = Duration.ofMinutes(1);

    private final DatabusEventStore _eventStore;
    private final DataCenters _dataCenters;
    private final Collection<ClusterInfo> _clusterInfo;
    private final int _masterFanoutPartitions;
    private final int _dataCenterFanoutPartitions;
    private final MetricsGroup _gauges;

    public SystemQueueMonitor(DatabusEventStore eventStore, DataCenters dataCenters,
                              Collection<ClusterInfo> clusterInfo, int masterFanoutPartitions,
                              int dataCenterFanoutPartitions, MetricRegistry metricRegistry) {
        _eventStore = checkNotNull(eventStore, "eventStore");
        _dataCenters = checkNotNull(dataCenters, "dataCenters");
        _clusterInfo = checkNotNull(clusterInfo, "clusterInfo");
        _masterFanoutPartitions = masterFanoutPartitions;
        _dataCenterFanoutPartitions = dataCenterFanoutPartitions;
        _gauges = new MetricsGroup(metricRegistry);
        ServiceFailureListener.listenTo(this, metricRegistry);
    }

    @Override
    protected Scheduler scheduler() {
        return Scheduler.newFixedDelaySchedule(0, POLL_INTERVAL.toMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    protected void shutDown() {
        _gauges.close();
    }

    @Override
    protected void runOneIteration() {
        try {
            pollQueueSizes();
        } catch (Throwable t) {
            _log.error("Unexpected exception.", t);
        }
    }

    private void pollQueueSizes() {
        _gauges.beginUpdates();

        long totalMasterQueueSize = 0;
        for (int partition = 0; partition < _masterFanoutPartitions; partition++) {
            totalMasterQueueSize += pollQueueSize("master-" + partition, ChannelNames.getMasterFanoutChannel(partition));
        }
        _gauges.gauge(newMetric("master")).set(totalMasterQueueSize);

        for (ClusterInfo cluster : _clusterInfo) {
            pollQueueSize("canary-" + cluster.getClusterMetric(), ChannelNames.getMasterCanarySubscription(cluster.getCluster()));
        }

        DataCenter self = _dataCenters.getSelf();
        for (DataCenter dataCenter : _dataCenters.getAll()) {
            if (!dataCenter.equals(self)) {
                long totalDataCenterQueueSize = 0;
                for (int partition = 0; partition < _dataCenterFanoutPartitions; partition++) {
                    totalDataCenterQueueSize += pollQueueSize("out-" + dataCenter.getName() + "-" + partition,
                            ChannelNames.getReplicationFanoutChannel(dataCenter, partition));
                }
                _gauges.gauge(newMetric("out-" + dataCenter.getName())).set(totalDataCenterQueueSize);
            }
        }

        _gauges.endUpdates();
    }

    private long pollQueueSize(String name, String channel) {
        try {
            // Exact count up to 500, estimate above that.
            long count = _eventStore.getSizeEstimate(channel, 500);
            _log.debug("System queue size {}: {} (channel={})", name, count, channel);
            _gauges.gauge(newMetric(name)).set(count);
            return count;
        } catch (Exception e) {
            _log.error("Unexpected exception polling channel size: {}", channel, e);
            return 0;
        }
    }

    private String newMetric(String name) {
        return MetricRegistry.name("bv.emodb.databus", "SystemQueue", name);
    }
}
