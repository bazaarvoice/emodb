package com.bazaarvoice.emodb.cachemgr.invalidate;

import com.bazaarvoice.emodb.common.dropwizard.guice.Global;
import com.bazaarvoice.emodb.common.dropwizard.lifecycle.LifeCycleRegistry;
import com.bazaarvoice.emodb.common.zookeeper.Sync;
import com.bazaarvoice.ostrich.HostDiscovery;
import com.bazaarvoice.ostrich.ServiceEndPoint;
import com.bazaarvoice.ostrich.discovery.zookeeper.ZooKeeperHostDiscovery;
import com.codahale.metrics.MetricRegistry;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import io.dropwizard.lifecycle.Managed;
import org.apache.curator.framework.CuratorFramework;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkState;

/**
 * Queries the local data center's ZooKeeper server for the hosts that implement the specified end point.
 */
public class LocalDataCenterEndPointProvider implements EndPointProvider, Managed {
    private static final Duration SYNC_TIMEOUT = Duration.standardSeconds(10);
    private static final long DELAY_TIMEOUT = Duration.standardSeconds(2).getMillis();

    private final Logger _log = LoggerFactory.getLogger(LocalDataCenterEndPointProvider.class);

    private final CuratorFramework _curator;
    private final InvalidationServiceEndPointAdapter _endPointAdapter;
    private final ServiceEndPoint _self;
    private final MetricRegistry _metricRegistry;

    private final ReentrantLock _lock = new ReentrantLock();
    private final Condition _endpointsAvailable = _lock.newCondition();
    private final DelayQueue<DelayedInvalidationCheck> _delayedInvalidationQueue;
    private ZooKeeperHostDiscovery _hostDiscovery;
    private ExecutorService _delayedInvalidationService;
    private boolean _shutdownDelayedInvalidationService;

    private volatile ImmutableList<EndPoint> _endPoints = ImmutableList.of();


    @Inject
    public LocalDataCenterEndPointProvider(@Global CuratorFramework curator,
                                           InvalidationServiceEndPointAdapter endPointAdapter,
                                           ServiceEndPoint self,
                                           MetricRegistry metricRegistry,
                                           LifeCycleRegistry lifeCycleRegistry) {
        this(curator, endPointAdapter, self, metricRegistry, lifeCycleRegistry, null);
    }

    @VisibleForTesting
    LocalDataCenterEndPointProvider(CuratorFramework curator,
                                    InvalidationServiceEndPointAdapter endPointAdapter,
                                    ServiceEndPoint self,
                                    MetricRegistry metricRegistry,
                                    LifeCycleRegistry lifeCycleRegistry,
                                    ExecutorService delayedInvalidationService) {
        _curator = curator;
        _endPointAdapter = endPointAdapter;
        _self = self;
        _metricRegistry = metricRegistry;
        _delayedInvalidationService = delayedInvalidationService;

        _delayedInvalidationQueue = new DelayQueue<>();
        
        lifeCycleRegistry.manage(this);
    }

    @Override
    public void start() throws Exception {
        _hostDiscovery = new ZooKeeperHostDiscovery(_curator, _endPointAdapter.getServiceName(), _metricRegistry);
        _hostDiscovery.addListener(new HostDiscovery.EndPointListener() {
            @Override
            public void onEndPointAdded(final ServiceEndPoint host) {
                rebuildEndpoints();
            }

            @Override
            public void onEndPointRemoved(ServiceEndPoint endPoint) {
                rebuildEndpoints();
            }
        });

        if (_delayedInvalidationService == null) {
            _delayedInvalidationService = Executors.newSingleThreadExecutor(
                    new ThreadFactoryBuilder().setNameFormat("local-dc-invalidation-%d").build());
            _shutdownDelayedInvalidationService = true;
        }

        _delayedInvalidationService.execute(this::pollForDelayedEndPoints);

        // Initialize endpoints by rebuilding
        rebuildEndpoints();
    }

    @Override
    public void stop() throws Exception {
        if (_shutdownDelayedInvalidationService) {
            _delayedInvalidationService.shutdownNow();
        }
        try {
            _hostDiscovery.close();
        } catch (IOException e) {
            // Ignore errors from stopping host discovery
        }
    }

    private void rebuildEndpoints() {
        // Null out the endpoints to invalidate them immediately
        _endPoints = null;

        _lock.lock();
        try {
            _endPoints = getEndPointsFromHostDiscovery();
            _endpointsAvailable.signalAll();

            if (_log.isDebugEnabled()) {
                _log.debug("Endpoints are now: {}", Joiner.on(',').join(_endPoints.stream().map(EndPoint::getAddress).iterator()));
            }
        } finally {
            _lock.unlock();
        }
    }

    /**
     * Returns the current list of endPoints.  If possible a cached version is used, so typically this is faster than
     * calling {@link #getEndPointsFromHostDiscovery()} and reuses an existing immutable list rather than rebuilding a
     * new one on each call.
     */
    private ImmutableList<EndPoint> getEndPoints() {
        ImmutableList<EndPoint> endPoints = _endPoints;
        if (endPoints == null) {
            boolean locked = false;
            try {
                long timeout = System.currentTimeMillis() + 10;

                locked = _lock.tryLock(10, TimeUnit.MILLISECONDS);
                if (locked) {
                    endPoints = _endPoints;
                    long waitTime = timeout - System.currentTimeMillis();
                    if (endPoints == null && waitTime > 0) {
                        _endpointsAvailable.await(waitTime, TimeUnit.MILLISECONDS);
                        endPoints = _endPoints;
                    }
                }
            } catch (InterruptedException e) {
                // ignore
            } finally {
                if (locked) {
                    _lock.unlock();
                }
            }

            if (endPoints == null) {
                // Took too long waiting for the background to re-populate endpoints, just fetch from the source
                endPoints = getEndPointsFromHostDiscovery();
            }
        }
        
        return endPoints;
    }

    /**
     * Returns the current list of endPoints.  Unlike {@link #getEndPoints()} the host discovery is queried and a new
     * list of endPoints is generated on every call.
     */
    private ImmutableList<EndPoint> getEndPointsFromHostDiscovery() {
        Iterable<ServiceEndPoint> hosts = _hostDiscovery.getHosts();

        ImmutableList.Builder<EndPoint> endPoints = ImmutableList.builder();
        for (final ServiceEndPoint host : hosts) {
            if (host.equals(_self)) {
                continue;
            }
            endPoints.add(new EndPoint() {
                @Override
                public String getAddress() {
                    return _endPointAdapter.toEndPointAddress(host);
                }

                @Override
                public boolean isValid() {
                    return Iterables.contains(_hostDiscovery.getHosts(), host);
                }
            });
        }

        return endPoints.build();
    }

    @Override
    public void withEndPoints(Function<Collection<EndPoint>, ?> function) {
        // Make sure all reads we perform against ZooKeeper see the most up-to-date information.  For most ZooKeeper
        // reads this isn't necessary, but in this case we want to be 100% sure we send a message to every server that
        // is up as of the time the sendToAll() method begins.
        checkState(Sync.synchronousSync(_curator, SYNC_TIMEOUT), "ZooKeeper sync failed.");

        List<EndPoint> endPoints = getEndPoints();

        function.apply(endPoints);

        // Although the previous sync ensures ZooKeeper has the most up-to-date information it may take a few
        // milliseconds for the host discovery to asynchronously be updated with any changes to the set of endpoints.
        // To account for this, asynchronously re-visit this function in 2 seconds so any new endpoints that come
        // online between now and then also are acted upon.
        _delayedInvalidationQueue.offer(new DelayedInvalidationCheck(endPoints, function));
    }

    private void pollForDelayedEndPoints() {
        while (!_delayedInvalidationService.isShutdown()) {
            try {
                // Don't block for more than 5 seconds so this loop can terminate if the provider is stopped
                DelayedInvalidationCheck delayedInvalidationCheck = _delayedInvalidationQueue.poll(5, TimeUnit.SECONDS);
                if (delayedInvalidationCheck != null) {
                    // The list of endpoints should be updated infrequently, only when instances come online or offline.
                    // Whenever the list is updated a new list is generated.  So start by performing a fast identity
                    // check to verify the list is unchanged.
                    List<EndPoint> sentEndPoints = delayedInvalidationCheck.getEndPoints();
                    List<EndPoint> currentEndPoints = getEndPoints();

                    if (sentEndPoints != currentEndPoints) {
                        // Not the same instance; perform a deeper check to verify any new instances are acted upon

                        Set<String> sentAddresses = sentEndPoints.stream().map(EndPoint::getAddress).collect(Collectors.toSet());
                        List<EndPoint> unsentEndPoints = currentEndPoints.stream()
                                .filter(endPoint -> !sentAddresses.contains(endPoint.getAddress()))
                                .collect(Collectors.toList());

                        if (!unsentEndPoints.isEmpty()) {
                            delayedInvalidationCheck.getFunction().apply(unsentEndPoints);
                        }
                    }
                }
            } catch (Exception e) {
                // Catch all exceptions so the loop doesn't terminate while the service is still running
                if (!_delayedInvalidationService.isShutdown()) {
                    _log.error("Exception caught polling for delayed endpoints", e);
                }
            }
        }
    }

    final static class DelayedInvalidationCheck implements Delayed {
        private final List<EndPoint> _endPoints;
        private final Function<Collection<EndPoint>, ?> _function;
        private final long _executeAtMs;

        public DelayedInvalidationCheck(List<EndPoint> endPoints, Function<Collection<EndPoint>, ?> function) {
            _endPoints = endPoints;
            _function = function;
            _executeAtMs = System.currentTimeMillis() + DELAY_TIMEOUT;
        }

        public List<EndPoint> getEndPoints() {
            return _endPoints;
        }

        public Function<Collection<EndPoint>, ?> getFunction() {
            return _function;
        }

        @Override
        public long getDelay(TimeUnit unit) {
            return unit.convert(_executeAtMs - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
        }

        @Override
        public int compareTo(Delayed o) {
            if (o == this || ((DelayedInvalidationCheck) o)._executeAtMs == _executeAtMs) {
                return 0;
            }
            return _executeAtMs < ((DelayedInvalidationCheck) o)._executeAtMs ? -1 : 1;
        }
    }
}
