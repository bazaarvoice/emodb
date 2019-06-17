package com.bazaarvoice.megabus.refproducer;

import com.bazaarvoice.emodb.common.dropwizard.lifecycle.ServiceFailureListener;
import com.bazaarvoice.emodb.common.dropwizard.metrics.MetricsGroup;
import com.bazaarvoice.emodb.databus.ChannelNames;
import com.bazaarvoice.emodb.databus.core.DatabusEventStore;
import com.codahale.metrics.MetricRegistry;
import com.google.common.util.concurrent.AbstractScheduledService;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A service that polls ref subscription queue sizes periodically and reports them to Datadog via Dropwizard Metrics, subject
 * to leader election.
 */
public class MegabusRefSubscriptionMonitor extends AbstractScheduledService {
    private static final Logger _log = LoggerFactory.getLogger(com.bazaarvoice.emodb.databus.core.SystemQueueMonitor.class);

    private static final Duration POLL_INTERVAL = Duration.ofMinutes(1);

    private final DatabusEventStore _eventStore;
    private final MetricsGroup _gauges;
    private final String _applicationId;
    private final int _partitions;

    public MegabusRefSubscriptionMonitor(DatabusEventStore eventStore,
                                         MetricRegistry metricRegistry,
                                         String applicationId,
                                         int partitions) {
        _eventStore = checkNotNull(eventStore, "eventStore");
        _gauges = new MetricsGroup(metricRegistry);
        _applicationId = checkNotNull(applicationId, "applicationId");
        _partitions = partitions;
        checkArgument(_partitions > 0);
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

        long totalSubscriptionQueueSize = 0;
        for (int partition = 0; partition < _partitions; partition++) {
            totalSubscriptionQueueSize += pollQueueSize("megabus-ref-" + partition,
                    ChannelNames.getMegabusRefProducerChannel(_applicationId, partition));
        }
        _gauges.gauge(newMetric("megabus-ref")).set(totalSubscriptionQueueSize);

        _gauges.endUpdates();
    }

    private long pollQueueSize(String name, String channel) {
        try {
            // Exact count up to 500, estimate above that.
            long count = _eventStore.getSizeEstimate(channel, 500);
            _log.debug("Megabus subscription queue size {}: {} (channel={})", name, count, channel);
            _gauges.gauge(newMetric(name)).set(count);
            return count;
        } catch (Exception e) {
            _log.error("Unexpected exception polling channel size: {}", channel, e);
            return 0;
        }
    }

    private String newMetric(String name) {
        return MetricRegistry.name("bv.emodb.megabus", "MegabusRefSubscription", name);
    }
}