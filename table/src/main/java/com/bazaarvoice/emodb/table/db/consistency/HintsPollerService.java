package com.bazaarvoice.emodb.table.db.consistency;

import com.bazaarvoice.emodb.common.dropwizard.lifecycle.ServiceFailureListener;
import com.bazaarvoice.emodb.common.zookeeper.store.ValueStore;
import com.codahale.metrics.MetricRegistry;
import com.datastax.driver.core.Session;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AbstractScheduledService;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Polls to check if there are any hinted handoffs in any of the nodes in the SoR cluster
 * <p/>
 * HintsPoller is a single process in the EmoDB cluster. Polls the cassandra cluster every 5 minutes.
 */
public class HintsPollerService extends AbstractScheduledService {
    private static final Logger _log = LoggerFactory.getLogger(HintsPollerService.class);

    private static final Duration POLL_INTERVAL = Duration.standardMinutes(5);

    @VisibleForTesting
    protected static final Duration CASSANDRA_RPC_TIMEOUT = Duration.standardSeconds(10);

    private final String _clusterName;
    private final ValueStore<Long> _timestamp;
    private final Session _cqlSession;
    @VisibleForTesting
    protected Set<InetAddress> _hosts = Sets.newHashSet();

    private ClusterHintsPoller _clusterHintsPoller;

    public HintsPollerService(String clusterName, ValueStore<Long> timestamp, Session cqlSession, ClusterHintsPoller clusterHintsPoller, MetricRegistry metricRegistry) {
        _clusterName = checkNotNull(clusterName, "cluster");
        _timestamp = checkNotNull(timestamp, "value");
        _cqlSession = checkNotNull(cqlSession, "cqlSession");
        _clusterHintsPoller = checkNotNull(clusterHintsPoller, "clusterHintsPoller");
        ServiceFailureListener.listenTo(this, metricRegistry);
    }

    public ValueStore<Long> getTimestamp() {
        return _timestamp;
    }

    @Override
    protected Scheduler scheduler() {
        return Scheduler.newFixedDelaySchedule(0, POLL_INTERVAL.getMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    protected void runOneIteration() {
        try {
            pollForHints();
        } catch (Throwable t) {
            _log.error("Unexpected HintsPoller exception for Cassandra cluster {}.", _clusterName, t);
            stop();  // Give up leadership temporarily.  Maybe another server will have more success.
        }
    }

    @VisibleForTesting
    protected void pollForHints()
            throws Exception {
        // Get the current time *before* we poll the ring.  The full consistency timestamp is calculated as follows:
        //   time_no_hints_reported - 2 * rpc_timeout_in_ms (property in cassandra for rpc timeouts)
        // Where rpc_timeout_in_ms is configured to 10 seconds.
        // Ideally, we would get this value by querying Cassandra instead of hard-coding it here.
        long timestamp = System.currentTimeMillis() - (CASSANDRA_RPC_TIMEOUT.getMillis() * 2);

        HintsPollerResult oldestHintsInfo = _clusterHintsPoller.getOldestHintsInfo(_cqlSession);

        // If we were not able to run the hints query on any node, then
        // we don't want to update the Full Consistency Timestamp (FCT).
        Joiner joiner = Joiner.on(",");
        if (!oldestHintsInfo.areAllHostsPolling()) {
            _log.warn("Host {} is failing the polling request.", oldestHintsInfo.getHostFailure().iterator().next());
            return;
        }

        // Cache the hosts and log when a change is observed
        Set<InetAddress> hostsPolled = oldestHintsInfo.getAllPolledHosts();
        if (!_hosts.equals(hostsPolled)) {
            _log.info("Ring for {} is updated. Total nodes: {}. Newly added nodes are: {}; Nodes that left are: {}",
                    _clusterName,
                    hostsPolled.size(), joiner.join(Sets.difference(hostsPolled, _hosts)),
                    joiner.join(Sets.difference(_hosts, hostsPolled)));
            _hosts = hostsPolled;
        }

        // Get oldest hint from the ring. Note an absent optional means there are no hints
        Optional<Long> oldestHintTimeInRing = oldestHintsInfo.getOldestHintTimestamp();

        // If there are any hints, take the oldest hint timestamp else go with the current timestamp
        if (oldestHintTimeInRing.isPresent()) {
            timestamp = oldestHintTimeInRing.get() - (CASSANDRA_RPC_TIMEOUT.getMillis() * 2);
        }

        // Update ZooKeeper with the poll time.
        _timestamp.set(timestamp);

        _log.debug("Full Consistency Timestamp for cluster '{}' updated to: {}", _clusterName, timestamp);
    }
}
