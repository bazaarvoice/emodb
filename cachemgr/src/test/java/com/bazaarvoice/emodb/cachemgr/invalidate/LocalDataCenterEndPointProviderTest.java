package com.bazaarvoice.emodb.cachemgr.invalidate;

import com.bazaarvoice.emodb.common.json.JsonHelper;
import com.bazaarvoice.ostrich.ServiceEndPoint;
import com.bazaarvoice.ostrich.ServiceEndPointBuilder;
import com.bazaarvoice.ostrich.registry.zookeeper.ZooKeeperServiceRegistry;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.net.HostAndPort;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.BoundedExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

public class LocalDataCenterEndPointProviderTest {
    private TestingServer _zooKeeperServer;
    private CuratorFramework _curator;

    @BeforeMethod
    private void setup() throws Exception {
        _zooKeeperServer = new TestingServer();
        _curator = CuratorFrameworkFactory.newClient(_zooKeeperServer.getConnectString(),
                new BoundedExponentialBackoffRetry(100, 1000, 5));
        _curator.start();
    }

    @AfterMethod
    private void teardown() throws IOException {
        _curator.close();
        _zooKeeperServer.stop();
    }

    @Test
    public void testEndPointProvider() throws Exception {
        InvalidationServiceEndPointAdapter adapter = new InvalidationServiceEndPointAdapter("emodb-cachemgr",
            HostAndPort.fromString("localhost:8080"), HostAndPort.fromString("localhost:8081"));
        ServiceEndPoint self = adapter.toSelfEndPoint();

        MetricRegistry metricRegistry = new MetricRegistry();

        ZooKeeperServiceRegistry serviceRegistry = new ZooKeeperServiceRegistry(_curator, metricRegistry);
        ServiceEndPoint host1 = getRemoteHost("169.254.1.1");
        ServiceEndPoint host2 = getRemoteHost("169.254.1.2");
        serviceRegistry.register(host1);
        serviceRegistry.register(host2);

        LocalDataCenterEndPointProvider provider = new LocalDataCenterEndPointProvider(_curator, adapter, self, metricRegistry);

        final boolean[] called = {false};
        provider.withEndPoints(new Function<Collection<EndPoint>, Void>() {
            @Override
            public Void apply(Collection<EndPoint> endPoints) {
                called[0] = true;
                assertEndPointsMatch(endPoints, ImmutableSet.of(
                        "http://169.254.1.1:8081/tasks/invalidate",
                        "http://169.254.1.2:8081/tasks/invalidate"));
                return null;
            }
        });
        Assert.assertTrue(called[0]);

        // Unregister a host and verify that withEndPoints excludes it immediately (ie. performs zk sync).
        serviceRegistry.unregister(host2);
        called[0] = false;
        provider.withEndPoints(new Function<Collection<EndPoint>, Void>() {
            @Override
            public Void apply(Collection<EndPoint> endPoints) {
                called[0] = true;
                assertEndPointsMatch(endPoints, ImmutableSet.of("http://169.254.1.1:8081/tasks/invalidate"));
                return null;
            }
        });
        Assert.assertTrue(called[0]);

        // Cleanup
        serviceRegistry.unregister(host1);
        serviceRegistry.close();
    }

    private void assertEndPointsMatch(Collection<EndPoint> endPoints, Collection<String> expected) {
        Set<String> expectedSet = Sets.newHashSet(expected);
        for (EndPoint endPoint : endPoints) {
            Assert.assertTrue(endPoint.isValid());
            Assert.assertTrue(expectedSet.remove(endPoint.getAddress()));
        }
        Assert.assertTrue(expectedSet.isEmpty());
    }

    private ServiceEndPoint getRemoteHost(String host) throws IOException {
        Map<String, String> payload = ImmutableMap.of("invalidateUrl", "http://" + host + ":8081/tasks/invalidate");
        return new ServiceEndPointBuilder()
                .withServiceName("emodb-cachemgr")
                .withId(host + ':' + 80)
                .withPayload(JsonHelper.asJson(payload))
                .build();
    }
}
