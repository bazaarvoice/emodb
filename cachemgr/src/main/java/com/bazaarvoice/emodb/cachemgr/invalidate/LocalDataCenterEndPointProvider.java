package com.bazaarvoice.emodb.cachemgr.invalidate;

import com.bazaarvoice.emodb.common.dropwizard.guice.Global;
import com.bazaarvoice.emodb.common.zookeeper.Sync;
import com.bazaarvoice.ostrich.HostDiscovery;
import com.bazaarvoice.ostrich.ServiceEndPoint;
import com.bazaarvoice.ostrich.discovery.zookeeper.ZooKeeperHostDiscovery;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import org.apache.curator.framework.CuratorFramework;
import org.joda.time.Duration;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;

/**
 * Queries the local data center's ZooKeeper server for the hosts that implement the specified end point.
 */
public class LocalDataCenterEndPointProvider implements EndPointProvider {
    private static final Duration SYNC_TIMEOUT = Duration.standardSeconds(10);

    private final CuratorFramework _curator;
    private final InvalidationServiceEndPointAdapter _endPointAdapter;
    private final ServiceEndPoint _self;
    private final MetricRegistry _metricRegistry;

    @Inject
    public LocalDataCenterEndPointProvider(@Global CuratorFramework curator,
                                           InvalidationServiceEndPointAdapter endPointAdapter,
                                           ServiceEndPoint self,
                                           MetricRegistry metricRegistry) {
        _curator = curator;
        _endPointAdapter = endPointAdapter;
        _self = self;
        _metricRegistry = metricRegistry;
    }

    @Override
    public void withEndPoints(Function<Collection<EndPoint>, ?> function) {
        // Make sure all reads we perform against ZooKeeper see the most up-to-date information.  For most ZooKeeper
        // reads this isn't necessary, but in this case we want to be 100% sure we send a message to every server that
        // is up as of the time the sendToAll() method begins.
        checkState(Sync.synchronousSync(_curator, SYNC_TIMEOUT), "ZooKeeper sync failed.");

        try (final HostDiscovery hostDiscovery = new ZooKeeperHostDiscovery(_curator, _endPointAdapter.getServiceName(), _metricRegistry)) {
            Iterable<ServiceEndPoint> hosts = hostDiscovery.getHosts();

            List<EndPoint> endPoints = Lists.newArrayList();
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
                        return Iterables.contains(hostDiscovery.getHosts(), host);
                    }
                });
            }

            function.apply(endPoints);
        } catch (IOException ex) {
            // suppress any IOExceptions that might result from closing our ZooKeeperHostDiscovery
        }
    }
}
