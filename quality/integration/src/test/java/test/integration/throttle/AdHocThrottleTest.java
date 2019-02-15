package test.integration.throttle;

import com.bazaarvoice.emodb.auth.apikey.ApiKey;
import com.bazaarvoice.emodb.auth.apikey.ApiKeyModification;
import com.bazaarvoice.emodb.auth.identity.InMemoryAuthIdentityManager;
import com.bazaarvoice.emodb.auth.permissions.InMemoryPermissionManager;
import com.bazaarvoice.emodb.auth.role.InMemoryRoleManager;
import com.bazaarvoice.emodb.blob.api.BlobStore;
import com.bazaarvoice.emodb.client.EmoClientException;
import com.bazaarvoice.emodb.common.jersey.dropwizard.JerseyEmoClient;
import com.bazaarvoice.emodb.common.zookeeper.store.ZkMapStore;
import com.bazaarvoice.emodb.job.api.JobHandlerRegistry;
import com.bazaarvoice.emodb.job.api.JobService;
import com.bazaarvoice.emodb.sor.api.DataStore;
import com.bazaarvoice.emodb.sor.client.DataStoreAuthenticator;
import com.bazaarvoice.emodb.sor.client.DataStoreClient;
import com.bazaarvoice.emodb.sor.compactioncontrol.InMemoryCompactionControlSource;
import com.bazaarvoice.emodb.sor.core.DefaultDataStoreAsync;
import com.bazaarvoice.emodb.test.ResourceTest;
import com.bazaarvoice.emodb.web.auth.EmoPermissionResolver;
import com.bazaarvoice.emodb.web.resources.sor.DataStoreResource1;
import com.bazaarvoice.emodb.web.throttling.AdHocConcurrentRequestRegulatorSupplier;
import com.bazaarvoice.emodb.web.throttling.AdHocThrottle;
import com.bazaarvoice.emodb.web.throttling.AdHocThrottleEndpoint;
import com.bazaarvoice.emodb.web.throttling.AdHocThrottleManager;
import com.bazaarvoice.emodb.web.throttling.ConcurrentRequestRegulator;
import com.bazaarvoice.emodb.web.throttling.ConcurrentRequestRegulatorSupplier;
import com.bazaarvoice.emodb.web.throttling.ConcurrentRequestsThrottlingFilter;
import com.bazaarvoice.emodb.web.throttling.UnlimitedDataStoreUpdateThrottler;
import com.bazaarvoice.emodb.web.throttling.ZkAdHocThrottleSerializer;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Queues;
import com.google.common.reflect.AbstractInvocationHandler;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.dropwizard.testing.junit.ResourceTestRule;
import javax.ws.rs.container.ContainerRequestContext;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.test.TestingServer;
import org.apache.curator.utils.ZKPaths;
import org.eclipse.jetty.http.HttpStatus;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.internal.util.Primitives;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class AdHocThrottleTest extends ResourceTest {

    private final static String API_KEY = "apikey";

    private static TestingServer _testingServer;
    private static CuratorFramework _rootCurator;

    private final DataStore _dataStore = mock(DataStore.class);
    private ConcurrentRequestRegulatorSupplier _deferringRegulatorSupplier = mock(ConcurrentRequestRegulatorSupplier.class);
    private CuratorFramework _curator;
    private ZkMapStore<AdHocThrottle> _mapStore;
    private AdHocThrottleManager _adHocThrottleManager;
    private ListeningExecutorService _service;
    private String _zkNamespace;
    private int _nextBaseIndex = 0;

    @Rule
    public ResourceTestRule _resourceTestRule = setupDataStoreResourceTestRule();

    private ResourceTestRule setupDataStoreResourceTestRule() {
        InMemoryAuthIdentityManager<ApiKey> authIdentityManager = new InMemoryAuthIdentityManager<>();
        authIdentityManager.createIdentity(API_KEY, new ApiKeyModification().addRoles("all-sor-role"));
        EmoPermissionResolver permissionResolver = new EmoPermissionResolver(_dataStore, mock(BlobStore.class));
        InMemoryPermissionManager permissionManager = new InMemoryPermissionManager(permissionResolver);
        InMemoryRoleManager roleManager = new InMemoryRoleManager(permissionManager);

        createRole(roleManager, null, "all-sor-role", ImmutableSet.of("sor|*|*"));

        return setupResourceTestRule(
                Collections.<Object>singletonList(new DataStoreResource1(
                        _dataStore, new DefaultDataStoreAsync(_dataStore, mock(JobService.class), mock(JobHandlerRegistry.class)),
                        new InMemoryCompactionControlSource(), new UnlimitedDataStoreUpdateThrottler())),
                Collections.<Object>singletonList(new ConcurrentRequestsThrottlingFilter(_deferringRegulatorSupplier)),
                authIdentityManager, permissionManager);
    }

    @BeforeClass
    public static void startZookeeper() throws Exception {
        System.setProperty("zookeeper.admin.enableServer", "false");
        _testingServer = new TestingServer();
        _rootCurator = CuratorFrameworkFactory.builder()
                .connectString(_testingServer.getConnectString())
                .retryPolicy(new RetryNTimes(3, 100))
                .threadFactory(new ThreadFactoryBuilder().setNameFormat("test-%d").setDaemon(true).build())
                .build();
        _rootCurator.start();
    }

    @AfterClass
    public static void stopZookeeper() throws Exception {
        _rootCurator.close();
        _testingServer.stop();
    }

    @Before
    public void setUp() throws Exception {
        // Create a unique base path for each test so each tests ZooKeeper data is independent.
        _zkNamespace = "emodb/test" + (_nextBaseIndex++);
        _curator = _rootCurator.usingNamespace(_zkNamespace);

        _mapStore = new ZkMapStore<>(_curator, "/adhoc-throttle", new ZkAdHocThrottleSerializer());
        _mapStore.start();

        _adHocThrottleManager = new AdHocThrottleManager(_mapStore);

        // Set up the regulator supplier provided to Jersey to defer an instance created specifically for this test.
        final AdHocConcurrentRequestRegulatorSupplier regulatorSupplier =
                new AdHocConcurrentRequestRegulatorSupplier(_adHocThrottleManager, new MetricRegistry());
        when(_deferringRegulatorSupplier.forRequest(any(ContainerRequestContext.class))).thenAnswer(
                new Answer<ConcurrentRequestRegulator>() {
                    @Override
                    public ConcurrentRequestRegulator answer(InvocationOnMock invocation) throws Throwable {
                        ContainerRequestContext request = (ContainerRequestContext) invocation.getArguments()[0];
                        return regulatorSupplier.forRequest(request);
                    }
                });
    }

    @After
    public void tearDown() throws Exception {
        _mapStore.stop();
        if (_service != null) {
            _service.shutdownNow();
        }
        reset(_dataStore, _deferringRegulatorSupplier);
    }

    @Test
    public void testIndependentThrottling() throws Exception {
        final DataStore client = createClient();

        // Set up the size operation to perform a controlled block
        final CountDownLatch sizeInProgress = new CountDownLatch(1);
        final CountDownLatch finishSize = new CountDownLatch(1);

        when(_dataStore.getTableApproximateSize("table1")).thenAnswer(
                new Answer<Long>() {
                    @Override
                    public Long answer(InvocationOnMock invocation) throws Throwable {
                        // Signal that a caller has entered the size request for table1
                        sizeInProgress.countDown();
                        // Wait for the main thread to signal our return
                        finishSize.await();
                        return 1L;
                    }
                });
        when(_dataStore.getTableApproximateSize("table2")).thenReturn(2L);

        // Throttle requests to table1's size to 1 concurrent request, but leave table2 unlimited.
        _adHocThrottleManager.addThrottle(
                new AdHocThrottleEndpoint("GET", "/sor/1/_table/table1/size"),
                AdHocThrottle.create(1, Instant.now().plus(Duration.ofDays(1))));

        // Wait until we've verified that the map store has been updated
        for (int i=0; i < 100; i++) {
            if (_mapStore.get("GET_sor~1~_table~table1~size") != null) {
                break;
            }
            Thread.sleep(100);
        }
        assertNotNull(_mapStore.get("GET_sor~1~_table~table1~size"));

        // Create a thread to lock and hold a size call on table one
        _service = MoreExecutors.listeningDecorator(Executors.newSingleThreadExecutor());
        Future<Long> future1 = _service.submit(new Callable<Long>() {
            @Override
            public Long call() throws Exception {
                return client.getTableApproximateSize("table1");
            }
        });

        // Block until the thread has entered
        assertTrue(sizeInProgress.await(10, TimeUnit.SECONDS), "Size operation on table1 failed to start after 10 seconds");

        // Verify table1 is blocked
        verifyThrottled(client).getTableApproximateSize("table1");

        // Get the size for table2, which is unblocked
        assertEquals(client.getTableApproximateSize("table2"), 2L);

        finishSize.countDown();
        assertEquals(future1.get(10, TimeUnit.SECONDS).longValue(), 1L);
    }

    @Test
    public void testAdHocThrottling() throws Exception {
        final DataStore client = createClient();

        // For the first four calls to get the table's size perform a controlled block so all 4 run concurrently
        final CountDownLatch unthrottledSizeInProgress = new CountDownLatch(4);
        final CountDownLatch unthrottledFinishSize = new CountDownLatch(1);
        Answer<Long> unthrottledAnswer = new Answer<Long>() {
            @Override
            public Long answer(InvocationOnMock invocation) throws Throwable {
                // Signal that a caller has entered the size request for table-name
                unthrottledSizeInProgress.countDown();
                // Wait for the main thread to signal our return
                unthrottledFinishSize.await();
                return 1L;
            }
        };

        // The next three calls should each block until they are independently released
        final CountDownLatch throttledSizeInProgress = new CountDownLatch(3);
        final List<CountDownLatch> throttledFinishSizes = Lists.newArrayListWithCapacity(3);
        List<Answer<Long>> throttledAnswers = Lists.newArrayListWithCapacity(3);

        for (int i=0; i < 3; i++) {
            final CountDownLatch throttledFinishSize = new CountDownLatch(1);

            Answer<Long> throttledAnswer = new Answer<Long>() {
                @Override
                public Long answer(InvocationOnMock invocation) throws Throwable {
                    // Signal that a caller has entered the size request for table-name
                    throttledSizeInProgress.countDown();
                    // Wait for the main thread to signal our return
                    throttledFinishSize.await();
                    return 1L;
                }
            };

            throttledFinishSizes.add(throttledFinishSize);
            throttledAnswers.add(throttledAnswer);
        }

        // Set up the mock so that the first 4 calls are unthrottled and block as a group,
        // the next three calls are throttled and block as a group, and all subsequent calls
        // return immediately.

        when(_dataStore.getTableApproximateSize("table-name"))
                .thenAnswer(unthrottledAnswer)
                .thenAnswer(unthrottledAnswer)
                .thenAnswer(unthrottledAnswer)
                .thenAnswer(unthrottledAnswer)
                .thenAnswer(throttledAnswers.get(0))
                .thenAnswer(throttledAnswers.get(1))
                .thenAnswer(throttledAnswers.get(2))
                .thenReturn(1L);

        // Verify that, while unblocked, the first four size calls can run concurrently.
        _service = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(5));
        List<ListenableFuture<Long>> futures = Lists.newArrayListWithCapacity(4);

        for (int i=0; i < 4; i++) {
            futures.add(_service.submit(new Callable<Long>() {
                @Override
                public Long call() throws Exception {
                    return client.getTableApproximateSize("table-name");
                }
            }));
        }

        // Block until all four threads have entered
        assertTrue(unthrottledSizeInProgress.await(10, TimeUnit.SECONDS), "Size operation on table-name failed to start after 10 seconds");

        // Unblock all four calls and verify the results
        unthrottledFinishSize.countDown();

        for (Future<Long> future : futures) {
            assertEquals(future.get(10, TimeUnit.SECONDS).longValue(), 1L);
        }

        // Now set the throttle to only 3 concurrent requests
        _adHocThrottleManager.addThrottle(
                new AdHocThrottleEndpoint("GET", "/sor/1/_table/table-name/size"),
                AdHocThrottle.create(3, Instant.now().plus(Duration.ofDays(1))));

        // Wait until we've verified that the map store has been updated
        for (int i=0; i < 100; i++) {
            if (_mapStore.get("GET_sor~1~_table~table-name~size") != null) {
                break;
            }
            Thread.sleep(100);
        }
        assertNotNull(_mapStore.get("GET_sor~1~_table~table-name~size"));

        // Like before, create and block three concurrent requests
        futures.clear();
        final BlockingQueue<Future<Long>> completeThrottledFutures = Queues.newArrayBlockingQueue(3);

        for (int i=0;i < 3; i++) {
            final ListenableFuture<Long> future = _service.submit(new Callable<Long>() {
                @Override
                public Long call() throws Exception {
                    return client.getTableApproximateSize("table-name");
                }
            });
            // After the call completes let the main test thread know that the Future is complete.  This is simpler
            // than polling each future independently later on.
            future.addListener(
                    new Runnable() {
                        @Override
                        public void run() {
                            completeThrottledFutures.add(future);
                        }
                    },
                    MoreExecutors.sameThreadExecutor());
        }

        // Wait until all three threads are blocked getting the table size
        assertTrue(throttledSizeInProgress.await(10, TimeUnit.SECONDS), "Size operation on table-name failed to start after 10 seconds");

        // Unlike last time, the fourth call to get the tables size should be throttled
        verifyThrottled(client).getTableApproximateSize("table-name");

        // Release one of the blocked requests
        throttledFinishSizes.get(0).countDown();

        // Wait until the corresponding future is verified finished
        assertEquals(completeThrottledFutures.poll(10, TimeUnit.SECONDS).get().longValue(), 1L);

        // Perform another size query.  This one should be unthrottled and return immediately
        assertEquals(client.getTableApproximateSize("table-name"), 1L);

        // Release all threads
        for (CountDownLatch latch : throttledFinishSizes) {
            latch.countDown();
        }
        for (Future<Long> future : futures) {
            assertEquals(future.get(10, TimeUnit.SECONDS).longValue(), 1L);
        }

        // Verify that the data store actually received all 8 of the size calls but not the throttled one
        verify(_dataStore, times(8)).getTableApproximateSize("table-name");
    }

    @Test
    public void testExpiration() throws Exception {
        // Artificially inject a throttle that is expired
        _curator.create()
                .creatingParentsIfNeeded()
                .forPath(
                        ZKPaths.makePath("adhoc-throttle", "GET_sor~1~_table~expired-test~size"),
                        "0,1955-11-05T22:04:00.000-07:00".getBytes(Charsets.UTF_8));

        // Wait until we've verified that the map store has been updated
        for (int i=0; i < 100; i++) {
            if (_mapStore.get("GET_sor~1~_table~expired-test~size") != null) {
                break;
            }
            Thread.sleep(100);
        }
        assertNotNull(_mapStore.get("GET_sor~1~_table~expired-test~size"));

        // Perform a size request on the table and verify it is not throttled
        when(_dataStore.getTableApproximateSize("expired-test")).thenReturn(5L);

        assertEquals(createClient().getTableApproximateSize("expired-test"), 5L);
    }

    @Test
    public void testThrottleWithSpecialCharacters() throws Exception {
        // Throttle requests a path with numerous characters which could cause problems for ZkMapStore.
        String path = "/sor/1/test:table/chars \t!%$@\u0080.\u0099";

        // Throttle the endpoint to accept no connections
        _adHocThrottleManager.addThrottle(
                new AdHocThrottleEndpoint("GET", path),
                AdHocThrottle.create(0, Instant.now().plus(Duration.ofDays(1))));


        // Wait until we've verified that the map store has been updated
        for (int i=0; i < 100; i++) {
            if (_mapStore.get("GET_sor~1~test:table~chars \t!%$@\u0080.\u0099") != null) {
                break;
            }
            Thread.sleep(100);
        }
        assertNotNull(_mapStore.get("GET_sor~1~test:table~chars \t!%$@\u0080.\u0099"));

        verifyThrottled(createClient()).get("test:table", "chars \t!%$@\u0080.\u0099");
    }

    private DataStore createClient() {
        return DataStoreAuthenticator.proxied(new DataStoreClient(URI.create("/sor/1"), new JerseyEmoClient(_resourceTestRule.client())))
                .usingCredentials(API_KEY);
    }

    private DataStore verifyThrottled(final DataStore dataStore) {
        return (DataStore) Proxy.newProxyInstance(getClass().getClassLoader(), new Class[] { DataStore.class },
                new AbstractInvocationHandler() {
                    @Override
                    protected Object handleInvocation(Object proxy, Method method, Object[] args) throws Throwable {
                        try {
                            method.invoke(dataStore, args);
                            fail(String.format("Throttled request did not generate a 503 error: %s(%s)",
                                    method.getName(), Joiner.on(",").join(args)));
                        } catch (InvocationTargetException e) {
                            assertTrue(e.getCause() instanceof EmoClientException);
                            EmoClientException ce = (EmoClientException) e.getCause();
                            assertEquals(ce.getResponse().getStatus(), HttpStatus.SERVICE_UNAVAILABLE_503);
                        }
                        // This should be unreachable; the caller doesn't care what the result is
                        if (method.getReturnType().isPrimitive()) {
                            return Primitives.defaultValueForPrimitiveOrWrapper(method.getReturnType());
                        }
                        return null;
                    }
                });
    }
}
