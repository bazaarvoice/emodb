package test.integration.throttle;

import com.bazaarvoice.emodb.auth.apikey.ApiKey;
import com.bazaarvoice.emodb.auth.apikey.ApiKeyModification;
import com.bazaarvoice.emodb.auth.identity.InMemoryAuthIdentityManager;
import com.bazaarvoice.emodb.auth.permissions.InMemoryPermissionManager;
import com.bazaarvoice.emodb.auth.role.InMemoryRoleManager;
import com.bazaarvoice.emodb.blob.api.BlobStore;
import com.bazaarvoice.emodb.common.jersey.dropwizard.JerseyEmoClient;
import com.bazaarvoice.emodb.common.uuid.TimeUUIDs;
import com.bazaarvoice.emodb.common.zookeeper.store.ZkMapStore;
import com.bazaarvoice.emodb.job.api.JobHandlerRegistry;
import com.bazaarvoice.emodb.job.api.JobService;
import com.bazaarvoice.emodb.sor.api.AuditBuilder;
import com.bazaarvoice.emodb.sor.api.DataStore;
import com.bazaarvoice.emodb.sor.api.Update;
import com.bazaarvoice.emodb.sor.client.DataStoreAuthenticator;
import com.bazaarvoice.emodb.sor.client.DataStoreClient;
import com.bazaarvoice.emodb.sor.compactioncontrol.InMemoryCompactionControlSource;
import com.bazaarvoice.emodb.sor.core.DefaultDataStoreAsync;
import com.bazaarvoice.emodb.sor.delta.Deltas;
import com.bazaarvoice.emodb.test.ResourceTest;
import com.bazaarvoice.emodb.web.auth.EmoPermissionResolver;
import com.bazaarvoice.emodb.web.resources.sor.DataStoreResource1;
import com.bazaarvoice.emodb.web.throttling.DataStoreUpdateThrottle;
import com.bazaarvoice.emodb.web.throttling.DataStoreUpdateThrottleManager;
import com.bazaarvoice.emodb.web.throttling.DataStoreUpdateThrottler;
import com.bazaarvoice.emodb.web.throttling.ZkDataStoreUpdateThrottleSerializer;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.RateLimiter;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.dropwizard.testing.junit.ResourceTestRule;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import java.net.URI;
import java.time.Clock;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyDouble;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class DataStoreUpdateThrottleTest extends ResourceTest {

    private final static String API_KEY_LIMITED = "apikeylimited";
    private final static String API_KEY_UNLIMITED = "apikeyunlimited";

    private static TestingServer _testingServer;
    private static CuratorFramework _rootCurator;

    private final DataStore _dataStore = mock(DataStore.class);
    private String _limitedId;
    private String _unlimitedId;
    private CuratorFramework _curator;
    private ZkMapStore<DataStoreUpdateThrottle> _mapStore;
    private DataStoreUpdateThrottleManager _throttleManager;
    private String _zkNamespace;
    private Instant _now;
    private volatile RateLimiterFactory _rateLimiterFactory;

    @Rule
    public ResourceTestRule _resourceTestRule = setupDataStoreResourceTestRule();

    private ResourceTestRule setupDataStoreResourceTestRule() {
        InMemoryAuthIdentityManager<ApiKey> authIdentityManager = new InMemoryAuthIdentityManager<>();
        _limitedId = authIdentityManager.createIdentity(API_KEY_LIMITED, new ApiKeyModification().addRoles("all-sor-role"));
        _unlimitedId = authIdentityManager.createIdentity(API_KEY_UNLIMITED, new ApiKeyModification().addRoles("all-sor-role"));
        EmoPermissionResolver permissionResolver = new EmoPermissionResolver(_dataStore, mock(BlobStore.class));
        InMemoryPermissionManager permissionManager = new InMemoryPermissionManager(permissionResolver);
        InMemoryRoleManager roleManager = new InMemoryRoleManager(permissionManager);

        createRole(roleManager, null, "all-sor-role", ImmutableSet.of("sor|*|*"));

        // Since a unique throttler will be created for each test construct the resource with a throttle which delegates
        // the the test-specific instance.

        DataStoreUpdateThrottler throttler = id -> _throttleManager.beforeUpdate(id);

        return setupResourceTestRule(
                Collections.<Object>singletonList(new DataStoreResource1(
                        _dataStore, new DefaultDataStoreAsync(_dataStore, mock(JobService.class), mock(JobHandlerRegistry.class)),
                        new InMemoryCompactionControlSource(), throttler)),
                ImmutableList.of(),
                authIdentityManager, permissionManager);
    }

    @BeforeClass
    public static void startZookeeper() throws Exception {
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
        _zkNamespace = "emodb/test" + UUID.randomUUID();
        _curator = _rootCurator.usingNamespace(_zkNamespace);

        _mapStore = new ZkMapStore<>(_curator, "sor-update-throttle", new ZkDataStoreUpdateThrottleSerializer());
        _mapStore.start();

        Clock clock = mock(Clock.class);
        when(clock.instant()).then(ignore -> _now);
        when(clock.millis()).then(ignore-> _now.toEpochMilli());
        _now = Instant.ofEpochMilli(1514764800000L);

        _rateLimiterFactory = mock(RateLimiterFactory.class);

        _throttleManager = new DataStoreUpdateThrottleManager(_mapStore, clock, new MetricRegistry()) {
            // Defer rate limit generation to our controlled mock
            @Override
            protected RateLimiter createRateLimiter(double rate) {
                return _rateLimiterFactory.create(rate);
            }
        };

        // Whenever an update to the data store occurs it is passed in a list of updates.  Rate limiting is applied
        // as the list is iterated, so we have to configure the mock data store accordingly.
        doAnswer(invocation -> {
            //noinspection unchecked
            Iterable<Update> updates = (Iterable<Update>) invocation.getArguments()[0];
            updates.forEach(ignore -> {});
            return null;
        }).when(_dataStore).updateAll(any(), any());
    }

    @After
    public void tearDown() throws Exception {
        _mapStore.stop();
        reset(_dataStore);
        verifyNoMoreInteractions(_rateLimiterFactory);
    }

    @Test
    public void testNoLimits() throws Exception {
        // Simply update a document with no rate limits applied
        createClient(API_KEY_LIMITED).update("table", "key", TimeUUIDs.newUUID(), Deltas.delete(), new AuditBuilder().build());
    }

    @Test
    public void testApiKeyLimited() throws Exception {
        final RateLimiter rateLimiter = mock(RateLimiter.class);
        when(_rateLimiterFactory.create(10)).thenReturn(rateLimiter);

        updateAPIKeyRateLimit(_limitedId, new DataStoreUpdateThrottle(10, _now.plusSeconds(1)));

        createClient(API_KEY_LIMITED).update("table", "key", TimeUUIDs.newUUID(), Deltas.delete(), new AuditBuilder().build());

        verify(_rateLimiterFactory).create(10);
        verify(rateLimiter).acquire();
    }

    @Test
    public void testNotThisApiKeyLimited() throws Exception {
        final RateLimiter rateLimiter = mock(RateLimiter.class);
        when(rateLimiter.acquire()).thenThrow(new AssertionError("Unlimited key should not have been rate limited"));
        when(_rateLimiterFactory.create(anyDouble())).thenReturn(rateLimiter);

        updateAPIKeyRateLimit(_limitedId, new DataStoreUpdateThrottle(10, _now.plusSeconds(1)));

        createClient(API_KEY_UNLIMITED).update("table", "key", TimeUUIDs.newUUID(), Deltas.delete(), new AuditBuilder().build());

        verify(_rateLimiterFactory).create(10);
    }

    @Test
    public void testInstanceRateLimited() throws Exception {
        final RateLimiter keyRateLimiter = mock(RateLimiter.class);
        final RateLimiter instanceRateLimiter = mock(RateLimiter.class);
        when(_rateLimiterFactory.create(10)).thenReturn(keyRateLimiter);
        when(_rateLimiterFactory.create(20)).thenReturn(instanceRateLimiter);

        updateAPIKeyRateLimit(_limitedId, new DataStoreUpdateThrottle(10, _now.plusSeconds(1)));
        updateAPIKeyRateLimit("*", new DataStoreUpdateThrottle(20, _now.plusSeconds(1)));

        createClient(API_KEY_LIMITED).update("table", "key", TimeUUIDs.newUUID(), Deltas.delete(), new AuditBuilder().build());
        createClient(API_KEY_UNLIMITED).update("table", "key", TimeUUIDs.newUUID(), Deltas.delete(), new AuditBuilder().build());

        verify(_rateLimiterFactory).create(10);
        verify(_rateLimiterFactory).create(20);

        verify(keyRateLimiter).acquire();
        verify(instanceRateLimiter, times(2)).acquire();
    }

    @Test
    public void testStreamRateLimited() throws Exception {
        final RateLimiter rateLimiter = mock(RateLimiter.class);
        when(_rateLimiterFactory.create(10)).thenReturn(rateLimiter);

        updateAPIKeyRateLimit(_limitedId, new DataStoreUpdateThrottle(10, _now.plusSeconds(1)));

        List<Update> updates = Lists.newArrayListWithCapacity(20);
        for (int i=0; i < 20; i++) {
            updates.add(new Update("table", "key" + i, TimeUUIDs.newUUID(), Deltas.delete(), new AuditBuilder().build()));
        }
        createClient(API_KEY_LIMITED).updateAll(updates);

        verify(_rateLimiterFactory).create(10);
        verify(rateLimiter, times(20)).acquire();
    }

    @Test
    public void testRemoveRateLimit() throws Exception {
        final RateLimiter rateLimiter = mock(RateLimiter.class);
        when(_rateLimiterFactory.create(10)).thenReturn(rateLimiter);
        when(rateLimiter.acquire()).thenReturn(0.0).thenThrow(new AssertionError("Rate limit should only have been applied once"));

        updateAPIKeyRateLimit(_limitedId, new DataStoreUpdateThrottle(10, _now.plusSeconds(1)));
        createClient(API_KEY_LIMITED).update("table", "key", TimeUUIDs.newUUID(), Deltas.delete(), new AuditBuilder().build());

        clearAPIKeyRateLimit(_limitedId);
        createClient(API_KEY_LIMITED).update("table", "key", TimeUUIDs.newUUID(), Deltas.delete(), new AuditBuilder().build());

        verify(_rateLimiterFactory).create(10);
        verify(rateLimiter).acquire();
    }

    @Test
    public void testRateLimitExpiration() throws Exception {
        final RateLimiter rateLimiter = mock(RateLimiter.class);
        when(_rateLimiterFactory.create(10)).thenReturn(rateLimiter);
        when(rateLimiter.acquire()).thenReturn(0.0).thenThrow(new AssertionError("Rate limit should only have been applied once"));

        updateAPIKeyRateLimit(_limitedId, new DataStoreUpdateThrottle(10, _now.plusSeconds(1)));
        createClient(API_KEY_LIMITED).update("table", "key", TimeUUIDs.newUUID(), Deltas.delete(), new AuditBuilder().build());

        // Advance time to just after the rate limit's expiration
        _now = _now.plusSeconds(1).plusMillis(1);

        createClient(API_KEY_LIMITED).update("table", "key", TimeUUIDs.newUUID(), Deltas.delete(), new AuditBuilder().build());
        verify(_rateLimiterFactory).create(10);
        verify(rateLimiter).acquire();
    }

    /**
     * Behind the scenes when the rate limit for an API key is changed the manager creates a new {@link RateLimiter}.
     * This test depends on that behavior.  If it is changed such that the same RateLimiter instance is updated
     * using {@link RateLimiter#setRate(double)} then this test will fail unless it is similarly updated.
     */
    @Test
    public void testRateLimitChange() throws Exception {
        final RateLimiter rateLimiter10 = mock(RateLimiter.class);
        when(_rateLimiterFactory.create(10)).thenReturn(rateLimiter10);
        final RateLimiter rateLimiter20 = mock(RateLimiter.class);
        when(_rateLimiterFactory.create(20)).thenReturn(rateLimiter20);

        updateAPIKeyRateLimit(_limitedId, new DataStoreUpdateThrottle(10, _now.plusSeconds(1)));
        DataStore client = createClient(API_KEY_LIMITED);
        for (int i=0; i < 3; i++) {
            client.update("table", "key", TimeUUIDs.newUUID(), Deltas.delete(), new AuditBuilder().build());
        }

        updateAPIKeyRateLimit(_limitedId, new DataStoreUpdateThrottle(20, _now.plusSeconds(1)));
        client.update("table", "key", TimeUUIDs.newUUID(), Deltas.delete(), new AuditBuilder().build());

        verify(_rateLimiterFactory).create(10);
        verify(_rateLimiterFactory).create(20);

        verify(rateLimiter10, times(3)).acquire();
        verify(rateLimiter20).acquire();
    }

    private void updateAPIKeyRateLimit(String id, DataStoreUpdateThrottle throttle) throws Exception {
        // Make the underlying call
        _throttleManager.updateAPIKeyRateLimit(id, throttle);
        // Wait up to 10 seconds for the MapStore on the throttle manager to be updated.
        Stopwatch stopwatch = Stopwatch.createStarted();
        while (stopwatch.elapsed(TimeUnit.SECONDS) < 10) {
            Thread.sleep(10);
            DataStoreUpdateThrottle actualThrottle = _throttleManager.getThrottle(id);
            if (actualThrottle != null && actualThrottle.equals(throttle)) {
                return;
            }
        }
        fail("Updated rate limit was not applied after 10 seconds");
    }

    private void clearAPIKeyRateLimit(String id) throws Exception {
        // Make the underlying call
        _throttleManager.clearAPIKeyRateLimit(id);
        // Wait up to 10 seconds for the MapStore on the throttle manager to be updated.
        Stopwatch stopwatch = Stopwatch.createStarted();
        while (stopwatch.elapsed(TimeUnit.SECONDS) < 10) {
            Thread.sleep(10);
            if (_throttleManager.getThrottle(id) == null) {
                return;
            }
        }
        fail("Updated rate limit was not applied after 10 seconds");
    }

    private DataStore createClient(String apiKey) {
        return DataStoreAuthenticator.proxied(new DataStoreClient(URI.create("/sor/1"), new JerseyEmoClient(_resourceTestRule.client())))
                .usingCredentials(apiKey);
    }

    private interface RateLimiterFactory {
        RateLimiter create(double rate);
    }
}
