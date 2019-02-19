package com.bazaarvoice.emodb.cachemgr.invalidate;

import com.bazaarvoice.emodb.common.dropwizard.lifecycle.SimpleLifeCycleRegistry;
import com.bazaarvoice.emodb.common.json.JsonHelper;
import com.bazaarvoice.ostrich.ServiceEndPoint;
import com.bazaarvoice.ostrich.ServiceEndPointBuilder;
import com.bazaarvoice.ostrich.registry.zookeeper.ZooKeeperServiceRegistry;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Function;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.net.HostAndPort;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.BoundedExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.mockito.ArgumentCaptor;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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

        SimpleLifeCycleRegistry lifeCycleRegistry = new SimpleLifeCycleRegistry();

        LocalDataCenterEndPointProvider provider = new LocalDataCenterEndPointProvider(
                _curator, adapter, self, metricRegistry, lifeCycleRegistry);
        lifeCycleRegistry.start();

        assertWithin(provider, Duration.ZERO, endPoints -> {
            assertEndPointsMatch(endPoints, ImmutableSet.of(
                    "http://169.254.1.1:8081/tasks/invalidate",
                    "http://169.254.1.2:8081/tasks/invalidate"));
            return null;
        });

        // Unregister a host and verify that withEndPoints excludes it immediately, with a brief consideration
        // for the PathChildrenCache to be updated.
        serviceRegistry.unregister(host2);

        assertWithin(provider, Duration.ofMillis(200), endPoints -> {
            assertEndPointsMatch(endPoints, ImmutableSet.of("http://169.254.1.1:8081/tasks/invalidate"));
            return null;
        });

        // Cleanup
        serviceRegistry.unregister(host1);
        serviceRegistry.close();
        lifeCycleRegistry.stop();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testEndPointProviderWithDelay() throws Exception {
        InvalidationServiceEndPointAdapter adapter = new InvalidationServiceEndPointAdapter("emodb-cachemgr",
                HostAndPort.fromString("localhost:8080"), HostAndPort.fromString("localhost:8081"));
        ServiceEndPoint self = adapter.toSelfEndPoint();

        MetricRegistry metricRegistry = new MetricRegistry();

        ZooKeeperServiceRegistry serviceRegistry = new ZooKeeperServiceRegistry(_curator, metricRegistry);
        ServiceEndPoint host1 = getRemoteHost("169.254.1.1");
        ServiceEndPoint host2 = getRemoteHost("169.254.1.2");
        serviceRegistry.register(host1);

        SimpleLifeCycleRegistry lifeCycleRegistry = new SimpleLifeCycleRegistry();

        // To control for the unreliability of thread timing provide our own implementation for the invalidation service
        ExecutorService service = mock(ExecutorService.class);

        // Simulate the service being shutdown after exactly one iteration
        when(service.isShutdown())
                .thenReturn(false)
                .thenReturn(true);

        LocalDataCenterEndPointProvider provider = new LocalDataCenterEndPointProvider(
                _curator, adapter, self, metricRegistry, lifeCycleRegistry, service);
        lifeCycleRegistry.start();

        final Set<String> endPointsInvalidated = Sets.newHashSet();

        assertWithin(provider, Duration.ZERO, endPoints -> {
            endPoints.forEach(endPoint -> endPointsInvalidated.add(endPoint.getAddress()));
            return null;
        });

        Assert.assertEquals(endPointsInvalidated, ImmutableSet.of("http://169.254.1.1:8081/tasks/invalidate"));
        endPointsInvalidated.clear();

        // Register the second host
        serviceRegistry.register(host2);

        // Now that the second host is definitively registered execute the invalidation runnable synchronously
        ArgumentCaptor<Runnable> runnableCaptor = ArgumentCaptor.forClass(Runnable.class);
        verify(service).execute(runnableCaptor.capture());
        Runnable runnable = runnableCaptor.getValue();
        runnable.run();
        
        Assert.assertEquals(endPointsInvalidated, ImmutableSet.of("http://169.254.1.2:8081/tasks/invalidate"));

        serviceRegistry.unregister(host1);
        serviceRegistry.close();
        lifeCycleRegistry.stop();
    }

    private void assertWithin(LocalDataCenterEndPointProvider provider, Duration timeLimit, Function<Collection<EndPoint>, Void> function) {
        AtomicBoolean called = new AtomicBoolean(false);
        Stopwatch stopwatch = Stopwatch.createStarted();
        do {
            provider.withEndPoints(endPoints -> {
                try {
                    function.apply(endPoints);
                    called.set(true);
                } catch (AssertionError e) {
                    // Yield and try again until time is expired
                    Thread.yield();
                }
                return null;
            });
        } while (!called.get() && stopwatch.elapsed(TimeUnit.MILLISECONDS) < timeLimit.toMillis());
        Assert.assertTrue(called.get());
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
