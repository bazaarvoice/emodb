package com.bazaarvoice.emodb.databus.core;

import com.bazaarvoice.emodb.common.dropwizard.lifecycle.LifeCycleRegistry;
import com.bazaarvoice.emodb.common.dropwizard.lifecycle.ManagedGuavaService;
import com.bazaarvoice.emodb.common.dropwizard.metrics.MetricsGroup;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import com.google.common.util.concurrent.AbstractScheduledService;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Service which monitors the lags for all fanouts currently being performed by this server and publishes the maximum
 * per fanout as a single metric.
 */
public class FanoutLagMonitor extends AbstractScheduledService {

    private static final Duration LAG_UPDATE_INTERVAL = Duration.ofSeconds(10);

    private final Logger _log = LoggerFactory.getLogger(FanoutLagMonitor.class);

    private final MetricRegistry _metricRegistry;
    private final MetricsGroup _gauges;
    private final Table<String, String, Lag> _lagTable;

    @Inject
    public FanoutLagMonitor(LifeCycleRegistry lifeCycle, MetricRegistry metricRegistry) {
        _metricRegistry = checkNotNull(metricRegistry, "metricRegistry");
        _gauges = new MetricsGroup(metricRegistry);
        _lagTable = HashBasedTable.create();

        lifeCycle.manage(new ManagedGuavaService(this));
    }

    @Override
    protected Scheduler scheduler() {
        return Scheduler.newFixedDelaySchedule(0, LAG_UPDATE_INTERVAL.toMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    protected void shutDown() throws Exception {
        _gauges.close();
    }

    @Override
    protected void runOneIteration() throws Exception {
        _gauges.beginUpdates();

        try {
            for (String fanout : _lagTable.rowKeySet()) {
                Integer maximumLagSeconds = null;
                for (Lag lag : _lagTable.row(fanout).values()) {
                    Integer lagSeconds = lag.getLagSeconds();
                    if (lagSeconds != null) {
                        maximumLagSeconds = maximumLagSeconds == null ? lagSeconds : Math.max(lagSeconds, maximumLagSeconds);
                    }
                }

                if (maximumLagSeconds != null) {
                    _gauges.gauge(lagMetric(fanout)).set(maximumLagSeconds);
                }
            }
        } catch (Exception e) {
            _log.warn("Failed to update fanout lag metrics", e);
        } finally {
            _gauges.endUpdates();
        }
    }

    public Lag createForFanout(String fanout, String partitionName) {
        Lag lag = new Lag(fanout, partitionName);
        synchronized (_lagTable) {
            if (_lagTable.contains(fanout, partitionName)) {
                throw new IllegalStateException(String.format("Fanout lag gauge already exists for [fanout=%s, partition=%s]", fanout, partitionName));
            }
            _lagTable.put(fanout, partitionName, lag);
        }
        return lag;
    }

    private String lagMetric(String name) {
        return MetricRegistry.name("bv.emodb.databus", "DefaultFanout", "lagSeconds", name);
    }

    private String partitionLagMetric(String name, String partitionName) {
        return MetricRegistry.name("bv.emodb.databus", "DefaultFanout", "lagSeconds", name, partitionName);
    }

    public class Lag {
        private final String _fanout;
        private final String _partitionName;
        private final MetricsGroup _lagGroup;
        private volatile Integer _lag;

        private Lag(String fanout, String partitionName) {
            _fanout = fanout;
            _partitionName = partitionName;
            _lagGroup = new MetricsGroup(_metricRegistry);
        }

        public void setLagSeconds(int lagSeconds) {
            _lagGroup.beginUpdates();
            _lagGroup.gauge(partitionLagMetric(_fanout, _partitionName)).set(lagSeconds);
            _lagGroup.endUpdates();
            _lag = lagSeconds;
        }

        public void close() {
            _lagGroup.close();
            synchronized (_lagTable) {
                _lagTable.remove(_fanout, _partitionName);
            }
        }

        private Integer getLagSeconds() {
            return _lag;
        }
    }
}
