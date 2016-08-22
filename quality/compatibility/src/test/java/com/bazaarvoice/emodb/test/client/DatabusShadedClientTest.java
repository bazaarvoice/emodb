package com.bazaarvoice.emodb.test.client;

import com.bazaarvoice.emodb.databus.api.AuthDatabus;
import com.bazaarvoice.emodb.databus.api.Databus;
import com.bazaarvoice.emodb.databus.api.Event;
import com.bazaarvoice.emodb.databus.api.Subscription;
import com.bazaarvoice.emodb.databus.client.DatabusAuthenticator;
import com.bazaarvoice.emodb.databus.client.DatabusClient;
import com.bazaarvoice.emodb.sor.condition.Conditions;
import com.bazaarvoice.shaded.emodb.databus.client.DatabusClientFactory;
import com.bazaarvoice.shaded.emodb.dropwizard6.DropWizard6EmoClient;
import com.bazaarvoice.shaded.ostrich.EmoServicePoolBuilder;
import com.bazaarvoice.shaded.ostrich.EmoZooKeeperHostDiscovery;
import com.bazaarvoice.shaded.ostrich.pool.ServiceCachingPolicyBuilder;
import com.bazaarvoice.shaded.ostrich.retry.ExponentialBackoffRetry;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.yammer.dropwizard.testing.ResourceTest;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.test.TestingServer;
import org.joda.time.Duration;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import java.net.URI;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class DatabusShadedClientTest extends ResourceTest {

    private final static String API_KEY = "test-api-key";

    @Override
    protected void setUpResources() throws Exception {
        addResource(new DatabusClientTestResource());
    }

    @BeforeTest
    public void setUp() throws Exception {
        super.setUpJersey();
    }

    @AfterTest
    public void tearDown() throws Exception {
        super.tearDownJersey();
    }

    @Test
    public void testShading() {
        // Choose a few representative classes that should not be present if the jar was properly shaded.
        List<String> classNames = ImmutableList.of(
                "io.dropwizard.server.ServerFactory",
                "io.dropwizard.metrics.MetricsFactory");

        for (String className : classNames) {
            try {
                Class.forName(className);
                fail("Class should not be present: " + className);
            } catch (ClassNotFoundException e) {
                // Ok
            }
        }
    }

    @Test
    public void testHostDiscovery() throws Exception {
        TestingServer server = new TestingServer();

        try (CuratorFramework curator = CuratorFrameworkFactory.newClient(
                server.getConnectString(), new RetryNTimes(3, 100))) {
            curator.start();

            ObjectMapper objectMapper = new ObjectMapper();
            curator.newNamespaceAwareEnsurePath("/ostrich/emodb-bus").ensure(curator.getZookeeperClient());
            curator.create().forPath("/ostrich/emodb-bus/local_default", objectMapper.writeValueAsString(
                    ImmutableMap.builder()
                            .put("name", "test-name")
                            .put("id", "test-id")
                            .put("payload", objectMapper.writeValueAsString(ImmutableMap.of(
                                    "serviceUrl", "/bus/1",
                                    "adminUrl", "/")))
                            .build())
                    .getBytes(Charsets.UTF_8));

            AuthDatabus client = EmoServicePoolBuilder.create(AuthDatabus.class)
                    .withHostDiscovery(new EmoZooKeeperHostDiscovery(curator, "emodb-bus"))
                    .withServiceFactory(DatabusClientFactory.forClusterAndHttpClient("local_default", client()))
                    .withCachingPolicy(ServiceCachingPolicyBuilder.getMultiThreadedClientPolicy())
                    .buildProxy(new ExponentialBackoffRetry(5, 50, 1000, TimeUnit.MILLISECONDS));

            long size = client.getEventCount(API_KEY, "test-subscription");
            assertEquals(size, 10);
        }
    }

    @Test
    public void testPoll() {
        Databus client = DatabusAuthenticator.proxied(new DatabusClient(URI.create("/bus/1"), new DropWizard6EmoClient(client())))
                .usingCredentials(API_KEY);

        List<Event> events = client.poll("test-subscription", Duration.standardMinutes(5), 10);

        assertEquals(events.size(), 10);
        for (int i=0; i < 10; i++) {
            assertEquals(events.get(i).getEventKey(), "id" + i);
            assertEquals(events.get(i).getContent(), ImmutableMap.of("key", "value" + i));
        }
    }

    @Test
    public void testListSubscriptions() {
        Databus client = DatabusAuthenticator.proxied(new DatabusClient(URI.create("/bus/1"), new DropWizard6EmoClient(client())))
                .usingCredentials(API_KEY);

        Iterator<Subscription> subscriptions = client.listSubscriptions(null, 1);

        assertTrue(subscriptions.hasNext());
        Subscription subscription = subscriptions.next();
        assertEquals(subscription.getName(), "test-subscription");
        assertEquals(subscription.getTableFilter(), Conditions.mapBuilder().contains("type", "review").build());
        assertEquals(subscription.getEventTtl(), Duration.millis(259200000));
        assertEquals(subscription.getExpiresAt(), new Date(1445270400000L));
        assertFalse(subscriptions.hasNext());
    }

    /**
     * Test resource that simulates responses from EmoDB.  It isn't fully fleshed out, since this test only exercises
     * a few representative API calls.
     */
    @Path("/bus/1")
    @Produces(MediaType.APPLICATION_JSON)
    public final static class DatabusClientTestResource {

        @GET
        @Path("test-subscription/size")
        public long getSize(@HeaderParam("X-BV-API-Key") String apiKey) {
            assertEquals(apiKey, API_KEY);
            return 10;
        }

        @GET
        @Path("test-subscription/poll")
        public List<Map<String, Object>> listRecords(@HeaderParam("X-BV-API-Key") String apiKey,
                                                     @QueryParam("ttl") Integer claimTtl,
                                                     @QueryParam("limit") Integer limit) {
            assertEquals(apiKey, API_KEY);
            assertEquals(claimTtl, new Integer(300));
            assertEquals(limit, new Integer(10));

            List<Map<String, Object>> events = Lists.newArrayListWithCapacity(10);
            for (int i=0; i < 10; i++) {
                events.add(ImmutableMap.<String, Object>of(
                        "eventKey", "id" + i,
                        "content", ImmutableMap.<String, Object>of("key", "value" + i)));
            }
            return events;
        }

        @GET
        public List<Map<String, Object>> listSubscriptions(@HeaderParam("X-BV-API-Key") String apiKey,
                                                           @QueryParam("limit") Integer limit) {
            assertEquals(apiKey, API_KEY);
            assertEquals(limit, new Integer(1));

            return ImmutableList.<Map<String, Object>>of(
                    ImmutableMap.<String, Object>builder()
                            .put("name", "test-subscription")
                            .put("tableFilter", "{..,\"type\":\"review\"}")
                            .put("expiresAt", "2015-10-19T16:00:00.000")
                            .put("eventTtl", 259200000)
                            .build());
        }
    }
}
