package com.bazaarvoice.emodb.test.client;

import com.bazaarvoice.emodb.blob.api.AuthBlobStore;
import com.bazaarvoice.emodb.blob.api.Blob;
import com.bazaarvoice.emodb.blob.api.BlobMetadata;
import com.bazaarvoice.emodb.blob.api.BlobStore;
import com.bazaarvoice.emodb.blob.client.BlobStoreAuthenticator;
import com.bazaarvoice.emodb.blob.client.BlobStoreClient;
import com.bazaarvoice.shaded.emodb.blob.client.BlobStoreClientFactory;
import com.bazaarvoice.shaded.emodb.dropwizard6.DropWizard6EmoClient;
import com.bazaarvoice.shaded.ostrich.EmoServicePoolBuilder;
import com.bazaarvoice.shaded.ostrich.EmoZooKeeperHostDiscovery;
import com.bazaarvoice.shaded.ostrich.pool.ServiceCachingPolicyBuilder;
import com.bazaarvoice.shaded.ostrich.retry.ExponentialBackoffRetry;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.yammer.dropwizard.testing.ResourceTest;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.test.TestingServer;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import javax.ws.rs.GET;
import javax.ws.rs.HEAD;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.Path;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class BlobStoreShadedClientTest extends ResourceTest {

    private final static String API_KEY = "test-api-key";

    private ScheduledExecutorService _connectionManagementService = mock(ScheduledExecutorService.class);

    @Override
    protected void setUpResources() throws Exception {
        addResource(new BlobStoreClientTestResource());
    }

    @BeforeTest
    public void setUp() throws Exception {
        super.setUpJersey();
    }

    @AfterTest
    public void tearDown() throws Exception {
        super.tearDownJersey();
        reset(_connectionManagementService);
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
            curator.newNamespaceAwareEnsurePath("/ostrich/emodb-blob").ensure(curator.getZookeeperClient());
            curator.create().forPath("/ostrich/emodb-blob/local_default", objectMapper.writeValueAsString(
                    ImmutableMap.builder()
                            .put("name", "test-name")
                            .put("id", "test-id")
                            .put("payload", objectMapper.writeValueAsString(ImmutableMap.of(
                                    "serviceUrl", "/blob/1",
                                    "adminUrl", "/")))
                            .build())
                    .getBytes(Charsets.UTF_8));

            AuthBlobStore client = EmoServicePoolBuilder.create(AuthBlobStore.class)
                    .withHostDiscovery(new EmoZooKeeperHostDiscovery(curator, "emodb-blob"))
                    .withServiceFactory(BlobStoreClientFactory.forClusterAndHttpClient("local_default", client()))
                    .withCachingPolicy(ServiceCachingPolicyBuilder.getMultiThreadedClientPolicy())
                    .buildProxy(new ExponentialBackoffRetry(5, 50, 1000, TimeUnit.MILLISECONDS));

            long size = client.getTableApproximateSize(API_KEY, "test:table");
            assertEquals(size, 10);
        }
    }

    @Test
    public void testGetBlob() throws Exception {
        BlobStore client = BlobStoreAuthenticator.proxied(new BlobStoreClient(URI.create("/blob/1"),
                new DropWizard6EmoClient(client()), _connectionManagementService))
                .usingCredentials(API_KEY);

        Blob blob = client.get("test:table", "blob1");

        assertEquals(blob.getId(), "blob1");
        assertEquals(blob.getLength(), 12);
        assertEquals(blob.getAttributes().get("size"), "medium");
        assertEquals(blob.getTimestamp(), new Date(1444937753000L));

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        blob.writeTo(out);
        assertEquals(new String(out.toByteArray(), Charsets.UTF_8), "blob-content");

        verify(_connectionManagementService).schedule(any(Runnable.class), eq(2000L), eq(TimeUnit.MILLISECONDS));
    }

    @Test
    public void testGetMetadata() throws Exception {
        BlobStore client = BlobStoreAuthenticator.proxied(new BlobStoreClient(URI.create("/blob/1"),
                new DropWizard6EmoClient(client()), _connectionManagementService))
                .usingCredentials(API_KEY);

        BlobMetadata metadata = client.getMetadata("test:table", "blob1");

        assertEquals(metadata.getId(), "blob1");
        assertEquals(metadata.getLength(), 12);
        assertEquals(metadata.getAttributes().get("size"), "medium");
        assertEquals(metadata.getTimestamp(), new Date(1444937753000L));
    }

    /**
     * Test resource that simulates responses from EmoDB.  It isn't fully fleshed out, since this test only exercises
     * a few representative API calls.
     */
    @Path("/blob/1")
    public final static class BlobStoreClientTestResource {

        @GET
        @Path("_table/test:table/size")
        public long getTableSize(@HeaderParam("X-BV-API-Key") String apiKey) {
            assertEquals(apiKey, API_KEY);
            return 10;
        }

        @GET
        @Path("test:table/blob1")
        public Response getBlob(@HeaderParam("X-BV-API-Key") String apiKey) {
            assertEquals(apiKey, API_KEY);

            byte[] content = "blob-content".getBytes(Charsets.UTF_8);
            return Response.ok(content)
                    .type(MediaType.APPLICATION_OCTET_STREAM)
                    .header(HttpHeaders.CONTENT_LENGTH, content.length)
                    .header(HttpHeaders.ETAG, "etag")
                    .header("X-BV-Length", content.length)
                    .header("X-BVA-size", "medium")
                    .lastModified(new Date(1444937753000L))
                    .build();
        }

        @HEAD
        @Path("test:table/blob1")
        public Response getBlobMetadata(@HeaderParam("X-BV-API-Key") String apiKey) {
            assertEquals(apiKey, API_KEY);

            return Response.ok()
                    .type(MediaType.APPLICATION_OCTET_STREAM)
                    .header(HttpHeaders.CONTENT_LENGTH, 12)
                    .header(HttpHeaders.ETAG, "etag")
                    .header("X-BV-Length", 12)
                    .header("X-BVA-size", "medium")
                    .lastModified(new Date(1444937753000L))
                    .build();
        }

    }
}
