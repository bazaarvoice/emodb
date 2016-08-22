package com.bazaarvoice.emodb.common.cassandra.discovery;

import com.bazaarvoice.ostrich.HostDiscovery;
import com.bazaarvoice.ostrich.ServiceEndPoint;
import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import com.google.inject.Inject;

import java.util.Collection;
import java.util.List;

/**
 * Find Cassandra seeds and filter Cassandra host lists via a BV SOA (Ostrich) host discovery, usually in ZooKeeper.
 */
public class HostDiscoverySupplier implements Supplier<Collection<String>> {
    private final HostDiscovery _hostDiscovery;

    @Inject
    public HostDiscoverySupplier(HostDiscovery hostDiscovery) {
        _hostDiscovery = hostDiscovery;
    }

    @Override
    public Collection<String> get() {
        List<String> hosts = Lists.newArrayList();
        for (ServiceEndPoint endPoint : _hostDiscovery.getHosts()) {
            hosts.add(endPoint.getId());
        }
        return hosts;
    }
}
