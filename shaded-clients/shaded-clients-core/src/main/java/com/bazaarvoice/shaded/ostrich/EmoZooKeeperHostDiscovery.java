package com.bazaarvoice.shaded.ostrich;

import com.codahale.metrics.MetricRegistry;
import org.apache.curator.framework.CuratorFramework;

public class EmoZooKeeperHostDiscovery extends com.bazaarvoice.ostrich.discovery.zookeeper.ZooKeeperHostDiscovery {

    public EmoZooKeeperHostDiscovery(CuratorFramework curator, String serviceName) {
        super(curator, serviceName, new MetricRegistry());
    }
}
