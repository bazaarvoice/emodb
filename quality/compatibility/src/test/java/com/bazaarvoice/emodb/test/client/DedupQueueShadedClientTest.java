package com.bazaarvoice.emodb.test.client;

import com.bazaarvoice.emodb.queue.api.AuthDedupQueueService;
import com.bazaarvoice.emodb.queue.api.DedupQueueService;
import com.bazaarvoice.emodb.queue.api.Message;
import com.bazaarvoice.emodb.queue.client.DedupQueueClient;
import com.bazaarvoice.emodb.queue.client.DedupQueueServiceAuthenticator;
import com.bazaarvoice.shaded.emodb.dropwizard6.DropWizard6EmoClient;
import com.bazaarvoice.shaded.emodb.queue.client.DedupQueueClientFactory;
import com.bazaarvoice.shaded.ostrich.EmoServicePoolBuilder;
import com.bazaarvoice.shaded.ostrich.EmoZooKeeperHostDiscovery;
import com.bazaarvoice.shaded.ostrich.pool.ServiceCachingPolicyBuilder;
import com.bazaarvoice.shaded.ostrich.retry.ExponentialBackoffRetry;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.yammer.dropwizard.testing.ResourceTest;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.test.TestingServer;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.net.URI;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class DedupQueueShadedClientTest extends ResourceTest {

    private final static String API_KEY = "test-api-key";

    private final DedupQueueClientTestResource _resource = new DedupQueueClientTestResource();

    @Override
    protected void setUpResources() throws Exception {
        addResource(_resource);
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
        System.setProperty("zookeeper.admin.enableServer", "false");
        TestingServer server = new TestingServer();

        try (CuratorFramework curator = CuratorFrameworkFactory.newClient(
                server.getConnectString(), new RetryNTimes(3, 100))) {
            curator.start();

            ObjectMapper objectMapper = new ObjectMapper();
            curator.newNamespaceAwareEnsurePath("/ostrich/emodb-dedupq").ensure(curator.getZookeeperClient());
            curator.create().forPath("/ostrich/emodb-dedupq/local_default", objectMapper.writeValueAsString(
                    ImmutableMap.builder()
                            .put("name", "test-name")
                            .put("id", "test-id")
                            .put("payload", objectMapper.writeValueAsString(ImmutableMap.of(
                                    "serviceUrl", "/dedupq/1",
                                    "adminUrl", "/")))
                            .build())
                    .getBytes(Charsets.UTF_8));

            AuthDedupQueueService client = EmoServicePoolBuilder.create(AuthDedupQueueService.class)
                    .withHostDiscovery(new EmoZooKeeperHostDiscovery(curator, "emodb-dedupq"))
                    .withServiceFactory(DedupQueueClientFactory.forClusterAndHttpClient("local_default", client()))
                    .withCachingPolicy(ServiceCachingPolicyBuilder.getMultiThreadedClientPolicy())
                    .buildProxy(new ExponentialBackoffRetry(5, 50, 1000, TimeUnit.MILLISECONDS));

            long size = client.getMessageCount(API_KEY, "test-queue");
            assertEquals(size, 10);
        }
    }

    @Test
    public void testSend() throws Exception {
        DedupQueueService client = DedupQueueServiceAuthenticator.proxied(new DedupQueueClient(URI.create("/dedupq/1"), new DropWizard6EmoClient(client())))
                .usingCredentials(API_KEY);

        client.send("test-queue1", "hello");
        client.send("test-queue1", null);
        client.send("test-queue1", ImmutableMap.of("price", 100));

        assertEquals(_resource.getSentMessagesForQueue("test-queue1"),
                Lists.newArrayList("hello", null, ImmutableMap.of("price", 100)));
    }

    @Test
    public void testSendAllSingleQueue() throws Exception {
        DedupQueueService client = DedupQueueServiceAuthenticator.proxied(new DedupQueueClient(URI.create("/dedupq/1"), new DropWizard6EmoClient(client())))
                .usingCredentials(API_KEY);

        client.sendAll("test-queue2", Lists.newArrayList("bonk", 42, null, ImmutableList.of("do", "re", "mi")));

        assertEquals(_resource.getSentMessagesForQueue("test-queue2"),
                Lists.newArrayList("bonk", 42, null, ImmutableList.of("do", "re", "mi")));
    }

    @Test
    public void testSendAllMultipleQueues() throws Exception {
        DedupQueueService client = DedupQueueServiceAuthenticator.proxied(new DedupQueueClient(URI.create("/dedupq/1"), new DropWizard6EmoClient(client())))
                .usingCredentials(API_KEY);

        client.sendAll(ImmutableMap.<String, Collection<Object>>of(
                "test-queue3", Lists.<Object>newArrayList("hot", "cross", "buns"),
                "test-queue4", Lists.<Object>newArrayList(null, ImmutableMap.of("a", 1), ImmutableMap.of("b", 2))));

        assertEquals(_resource.getSentMessagesForQueue("test-queue3"),
                Lists.<Object>newArrayList("hot", "cross", "buns"));
        assertEquals(_resource.getSentMessagesForQueue("test-queue4"),
                Lists.<Object>newArrayList(null, ImmutableMap.of("a", 1), ImmutableMap.of("b", 2)));
    }

    @Test
    public void testPoll() throws Exception {
        DedupQueueService client = DedupQueueServiceAuthenticator.proxied(new DedupQueueClient(URI.create("/dedupq/1"), new DropWizard6EmoClient(client())))
                .usingCredentials(API_KEY);

        List<Message> messages = client.poll("test-queue", Duration.ofMinutes(5), 10);

        assertEquals(messages.size(), 5);
        assertEquals(messages.get(0).getId(), "0");
        assertEquals(messages.get(0).getPayload(), null);
        assertEquals(messages.get(1).getId(), "1");
        assertEquals(messages.get(1).getPayload(), "dink");
        assertEquals(messages.get(2).getId(), "2");
        assertEquals(messages.get(2).getPayload(), 123);
        assertEquals(messages.get(3).getId(), "3");
        assertEquals(messages.get(3).getPayload(), ImmutableMap.of("ub", 40));
        assertEquals(messages.get(4).getId(), "4");
        assertEquals(messages.get(4).getPayload(), ImmutableList.of(99, "red balloons"));
    }

    @Test
    public void testMoveAsync() throws Exception {
        DedupQueueService client = DedupQueueServiceAuthenticator.proxied(new DedupQueueClient(URI.create("/dedupq/1"), new DropWizard6EmoClient(client())))
                .usingCredentials(API_KEY);

        String result = client.moveAsync("test-queue-old", "test-queue-new");
        assertEquals(result, "test-move");
    }

    /**
     * Test resource that simulates responses from EmoDB.  It isn't fully fleshed out, since this test only exercises
     * a few representative API calls.
     */
    @Path("/dedupq/1")
    @Produces(MediaType.APPLICATION_JSON)
    public final static class DedupQueueClientTestResource {

        private final Multimap<String, Object> _sentMessages = ArrayListMultimap.create();

        @GET
        @Path("test-queue/size")
        public long getSize(@HeaderParam("X-BV-API-Key") String apiKey) {
            assertEquals(apiKey, API_KEY);
            return 10;
        }

        @POST
        @Path("test-queue1/send")
        @Consumes(MediaType.APPLICATION_JSON)
        public Response send(@HeaderParam("X-BV-API-Key") String apiKey, Object message) {
            assertEquals(apiKey, API_KEY);
            _sentMessages.put("test-queue1", message);
            return Response.ok().build();
        }

        @POST
        @Path("test-queue2/sendbatch")
        @Consumes(MediaType.APPLICATION_JSON)
        public Response sendBatch(@HeaderParam("X-BV-API-Key") String apiKey, Collection<Object> messages) {
            assertEquals(apiKey, API_KEY);
            _sentMessages.putAll("test-queue2", messages);
            return Response.ok().build();
        }

        @POST
        @Path("_sendbatch")
        @Consumes(MediaType.APPLICATION_JSON)
        public Response sendBatches(@HeaderParam("X-BV-API-Key") String apiKey, Map<String, Collection<Object>> messagesByQueue) {
            assertEquals(apiKey, API_KEY);
            for (Map.Entry<String, Collection<Object>> entry : messagesByQueue.entrySet()) {
                _sentMessages.putAll(entry.getKey(), entry.getValue());
            }
            return Response.ok().build();
        }

        @GET
        @Path("test-queue/poll")
        public List<Map<String, Object>> poll(@HeaderParam("X-BV-API-Key") String apiKey,
                                              @QueryParam("ttl") Integer claimTtl,
                                              @QueryParam("limit") Integer limit) {
            assertEquals(apiKey, API_KEY);
            assertEquals(claimTtl, new Integer(300));
            assertEquals(limit, new Integer(10));

            // ImmutableMap doesn't do null values, so this message is special
            Map<String, Object> nullPayload = Maps.newHashMap();
            nullPayload.put("id", "0");
            nullPayload.put("payload", null);

            //noinspection unchecked
            return Lists.newArrayList(
                    nullPayload,
                    ImmutableMap.<String, Object>of("id", "1", "payload", "dink"),
                    ImmutableMap.<String, Object>of("id", "2", "payload", 123),
                    ImmutableMap.<String, Object>of("id", "3", "payload", ImmutableMap.of("ub", 40)),
                    ImmutableMap.<String, Object>of("id", "4", "payload", ImmutableList.of(99, "red balloons")));
        }

        @POST
        @Path("_move")
        public Map<String, Object> moveAsync(@HeaderParam("X-BV-API-Key") String apiKey,
                                             @QueryParam("from") String from, @QueryParam("to") String to) {
            assertEquals(apiKey, API_KEY);
            assertEquals(from, "test-queue-old");
            assertEquals(to, "test-queue-new");
            return ImmutableMap.<String, Object>of("id", "test-move");
        }

        public Collection<Object> getSentMessagesForQueue(String queue) {
            return _sentMessages.get(queue);
        }
    }
}