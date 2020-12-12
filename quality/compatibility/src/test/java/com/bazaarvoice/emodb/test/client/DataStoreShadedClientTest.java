package com.bazaarvoice.emodb.test.client;

import com.bazaarvoice.emodb.common.json.JsonHelper;
import com.bazaarvoice.emodb.common.uuid.TimeUUIDs;
import com.bazaarvoice.emodb.sor.api.AuditBuilder;
import com.bazaarvoice.emodb.sor.api.AuthDataStore;
import com.bazaarvoice.emodb.sor.api.DataStore;
import com.bazaarvoice.emodb.sor.api.Intrinsic;
import com.bazaarvoice.emodb.sor.api.ReadConsistency;
import com.bazaarvoice.emodb.sor.api.Table;
import com.bazaarvoice.emodb.sor.api.Update;
import com.bazaarvoice.emodb.sor.client.DataStoreAuthenticator;
import com.bazaarvoice.emodb.sor.client.DataStoreClient;
import com.bazaarvoice.emodb.sor.delta.Deltas;
import com.bazaarvoice.shaded.emodb.dropwizard6.DropWizard6EmoClient;
import com.bazaarvoice.shaded.emodb.sor.client.DataStoreClientFactory;
import com.bazaarvoice.shaded.ostrich.EmoServicePoolBuilder;
import com.bazaarvoice.shaded.ostrich.EmoZooKeeperHostDiscovery;
import com.bazaarvoice.shaded.ostrich.pool.ServiceCachingPolicyBuilder;
import com.bazaarvoice.shaded.ostrich.retry.ExponentialBackoffRetry;
import com.google.common.base.Charsets;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.yammer.dropwizard.testing.ResourceTest;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.test.TestingServer;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;
import shaded.emodb.com.fasterxml.jackson.core.type.TypeReference;
import shaded.emodb.com.fasterxml.jackson.databind.ObjectMapper;

import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import shaded.emodb.com.fasterxml.jackson.core.type.TypeReference;
import shaded.emodb.com.fasterxml.jackson.databind.ObjectMapper;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class DataStoreShadedClientTest extends ResourceTest {

    private final static String API_KEY = "test-api-key";

    private DataStoreClientTestResource _resource = new DataStoreClientTestResource();

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
        TestingServer server = new TestingServer();

        try (CuratorFramework curator = CuratorFrameworkFactory.newClient(
                server.getConnectString(), new RetryNTimes(3, 100))) {
            curator.start();

            ObjectMapper objectMapper = new ObjectMapper();
            curator.newNamespaceAwareEnsurePath("/ostrich/emodb-sor").ensure(curator.getZookeeperClient());
            curator.create().forPath("/ostrich/emodb-sor/local_default", objectMapper.writeValueAsString(
                    ImmutableMap.builder()
                            .put("name", "test-name")
                            .put("id", "test-id")
                            .put("payload", objectMapper.writeValueAsString(ImmutableMap.of(
                                    "serviceUrl", "/sor/1",
                                    "adminUrl", "/")))
                            .build())
                    .getBytes(Charsets.UTF_8));

            AuthDataStore client = EmoServicePoolBuilder.create(AuthDataStore.class)
                    .withHostDiscovery(new EmoZooKeeperHostDiscovery(curator, "emodb-sor"))
                    .withServiceFactory(DataStoreClientFactory.forClusterAndHttpClient("local_default", client()))
                    .withCachingPolicy(ServiceCachingPolicyBuilder.getMultiThreadedClientPolicy())
                    .buildProxy(new ExponentialBackoffRetry(5, 50, 1000, TimeUnit.MILLISECONDS));

            long size = client.getTableApproximateSize(API_KEY, "test:table");
            assertEquals(size, 10);
        }
    }

    @Test
    public void testGetRecord() {
        DataStore client = DataStoreAuthenticator.proxied(new DataStoreClient(URI.create("/sor/1"), new DropWizard6EmoClient(client())))
                .usingCredentials(API_KEY);

        Map<String, Object> record = client.get("test:table", "record1");
        assertEquals(Intrinsic.getId(record), "record1");
        assertEquals(Intrinsic.getTable(record), "test:table");
        assertEquals(Intrinsic.getVersion(record), new Long(1));
        assertEquals(Intrinsic.getFirstUpdateAt(record), new Date(1444910400000L));
        assertEquals(record.get("value"), "test");
    }

    @Test
    public void testListTable() {
        DataStore client = DataStoreAuthenticator.proxied(new DataStoreClient(URI.create("/sor/1"), new DropWizard6EmoClient(client())))
                .usingCredentials(API_KEY);

        Iterator<Map<String, Object>> records = client.scan("test:table", null, 10, ReadConsistency.STRONG);

        for (int i = 1; i <= 10; i++) {
            assertTrue(records.hasNext());
            Map<String, Object> record = records.next();
            assertEquals(Intrinsic.getId(record), "record" + i);
            assertEquals(Intrinsic.getTable(record), "test:table");
            assertEquals(record.get("value"), "test" + i);
        }

        assertFalse(records.hasNext());
    }

    @Test
    public void testGetStashRoot() {
        DataStore client = DataStoreAuthenticator.proxied(new DataStoreClient(URI.create("/sor/1"), new DropWizard6EmoClient(client())))
                .usingCredentials(API_KEY);
        URI expectedStashURI = URI.create("s3://emodb-us-east-1/stash/ci");
        URI actualStashURI = client.getStashRoot();
        Assert.assertEquals(actualStashURI, expectedStashURI);
    }

    @Test
    public void testListTables() {
        DataStore client = DataStoreAuthenticator.proxied(new DataStoreClient(URI.create("/sor/1"), new DropWizard6EmoClient(client())))
                .usingCredentials(API_KEY);

        Iterator<Table> tables = client.listTables(null, 1);

        assertTrue(tables.hasNext());
        Table table = tables.next();
        assertEquals(table.getName(), "test:table");
        assertEquals(table.getOptions().getPlacement(), "ugc_us:ugc");
        assertEquals(table.getTemplate(), ImmutableMap.of("type", "test"));
        assertEquals(table.getAvailability().getPlacement(), "ugc_us:ugc");
        assertFalse(tables.hasNext());
    }

    @Test
    public void testStreamingUpdates() {
        DataStore client = DataStoreAuthenticator.proxied(new DataStoreClient(URI.create("/sor/1"), new DropWizard6EmoClient(client())))
                .usingCredentials(API_KEY);

        Map<String, List<Update>> updatesByTable = Maps.newHashMap();
        List<Update> allUpdates = Lists.newArrayList();

        for (int table = 0; table < 3; table++) {
            String tableName = "table" + table;
            List<Update> tableUpdates = Lists.newArrayListWithCapacity(2);
            for (int value = 0; value < 2; value++) {
                Update update = new Update(tableName, "key" + value,
                        TimeUUIDs.newUUID(),
                        Deltas.mapBuilder().put("value", value).build(),
                        new AuditBuilder().setComment("test-comment").build());
                tableUpdates.add(update);
                allUpdates.add(update);
            }
            updatesByTable.put(tableName, tableUpdates);
        }

        client.updateAll(allUpdates);

        assertEquals(_resource.getUpdatesForTable("table0"), updatesByTable.get("table0"));
        assertEquals(_resource.getUpdatesForTable("table1"), updatesByTable.get("table1"));
        assertEquals(_resource.getUpdatesForTable("table2"), updatesByTable.get("table2"));
    }

    /**
     * Test resource that simulates responses from EmoDB.  It isn't fully fleshed out, since this test only exercises
     * a few representative API calls.
     */
    @Path("/sor/1")
    @Produces(MediaType.APPLICATION_JSON)
    public final static class DataStoreClientTestResource {

        private final ListMultimap<String, Update> _updates = ArrayListMultimap.create();

        @GET
        @Path("_table/test:table/size")
        public long getTableSize(@HeaderParam("X-BV-API-Key") String apiKey) {
            assertEquals(apiKey, API_KEY);
            return 10;
        }

        @GET
        @Path("test:table/record1")
        public Map<String, Object> getRecord(@HeaderParam("X-BV-API-Key") String apiKey) {
            assertEquals(apiKey, API_KEY);

            return ImmutableMap.<String, Object>builder()
                    .put("value", "test")
                    .put("~id", "record1")
                    .put("~table", "test:table")
                    .put("~version", 1)
                    .put("~signature", "b6ccdb9c654009d2ba5cfc9599f68881")
                    .put("~deleted", false)
                    .put("~firstUpdateAt", "2015-10-15T12:00:00.000Z")
                    .put("~lastUpdateAt", "2015-10-15T12:00:00.000Z")
                    .build();
        }

        @GET
        @Path("_stashroot")
        @Produces(MediaType.TEXT_PLAIN)
        public String stashRoot() {
            return "s3://emodb-us-east-1/stash/ci";
        }

        @GET
        @Path("test:table")
        public List<Map<String, Object>> listRecords(@HeaderParam("X-BV-API-Key") String apiKey,
                                                     @QueryParam("limit") Integer limit,
                                                     @QueryParam("consistency") String consistency) {
            assertEquals(apiKey, API_KEY);
            assertEquals(limit, new Integer(10));
            assertEquals(consistency, "STRONG");

            List<Map<String, Object>> results = Lists.newArrayListWithCapacity(10);
            for (int i = 1; i <= 10; i++) {
                results.add(ImmutableMap.<String, Object>builder()
                        .put("value", "test" + i)
                        .put("~id", "record" + i)
                        .put("~table", "test:table")
                        .put("~version", 1)
                        .put("~signature", "b6ccdb9c654009d2ba5cfc9599f68881")
                        .put("~deleted", false)
                        .put("~firstUpdateAt", "2015-10-15T12:00:00.000Z")
                        .put("~lastUpdateAt", "2015-10-15T12:00:00.000Z")
                        .build());
            }
            return results;
        }

        @GET
        @Path("_table")
        public List<Map<String, Object>> listTables(@HeaderParam("X-BV-API-Key") String apiKey,
                                                    @QueryParam("limit") Integer limit) {
            assertEquals(apiKey, API_KEY);
            assertEquals(limit, new Integer(1));

            return ImmutableList.<Map<String, Object>>of(
                    ImmutableMap.<String, Object>builder()
                            .put("name", "test:table")
                            .put("options", ImmutableMap.<String, Object>of(
                                    "placement", "ugc_us:ugc",
                                    "facades", ImmutableList.of()))
                            .put("template", ImmutableMap.<String, Object>of(
                                    "type", "test"))
                            .put("availability", ImmutableMap.<String, Object>of(
                                    "placement", "ugc_us:ugc",
                                    "facade", false))
                            .build());
        }

        @POST
        @Path("_stream")
        public Response streamingUpdate(@HeaderParam("X-BV-API-Key") String apiKey, InputStream updateStream)
                throws IOException {
            assertEquals(apiKey, API_KEY);

            // Because the Jackson and its annotation were shaded we need use the shaded JsonHelper to read it back
            List<Update> updates = JsonHelper.readJson(updateStream, new TypeReference<List<Update>>() {});
            for (Update update : updates) {
                _updates.put(update.getTable(), update);
            }

            return Response.ok().build();
        }

        public List<Update> getUpdatesForTable(String table) {
            return _updates.get(table);
        }
    }
}
