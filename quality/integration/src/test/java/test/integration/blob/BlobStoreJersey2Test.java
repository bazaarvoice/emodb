package test.integration.blob;

import com.bazaarvoice.emodb.auth.apikey.ApiKey;
import com.bazaarvoice.emodb.auth.apikey.ApiKeyModification;
import com.bazaarvoice.emodb.auth.identity.AuthIdentityManager;
import com.bazaarvoice.emodb.auth.identity.InMemoryAuthIdentityManager;
import com.bazaarvoice.emodb.auth.permissions.InMemoryPermissionManager;
import com.bazaarvoice.emodb.auth.permissions.PermissionManager;
import com.bazaarvoice.emodb.auth.permissions.PermissionUpdateRequest;
import com.bazaarvoice.emodb.auth.role.InMemoryRoleManager;
import com.bazaarvoice.emodb.auth.role.RoleIdentifier;
import com.bazaarvoice.emodb.auth.role.RoleManager;
import com.bazaarvoice.emodb.auth.role.RoleModification;
import com.bazaarvoice.emodb.blob.api.AuthBlobStore;
import com.bazaarvoice.emodb.blob.api.BlobStore;
import com.bazaarvoice.emodb.blob.api.DefaultTable;
import com.bazaarvoice.emodb.blob.api.Table;
import com.bazaarvoice.emodb.blob.client.BlobStoreAuthenticator;
import com.bazaarvoice.emodb.blob.client.BlobStoreClient;
import com.bazaarvoice.emodb.blob.client.BlobStoreStreaming;
import com.bazaarvoice.emodb.blob.jersey2.client.BlobStoreAuthenticator2;
import com.bazaarvoice.emodb.blob.jersey2.client.BlobStoreJersey2Client;
import com.bazaarvoice.emodb.blob.jersey2.client.BlobStoreJersey2ClientFactory;
import com.bazaarvoice.emodb.blob.jersey2.client.BlobStoreStreaming2;
import com.bazaarvoice.emodb.common.jersey2.Jersey2EmoClient;
import com.bazaarvoice.emodb.sor.api.DataStore;
import com.bazaarvoice.emodb.sor.api.TableAvailability;
import com.bazaarvoice.emodb.sor.api.TableOptions;
import com.bazaarvoice.emodb.sor.api.TableOptionsBuilder;
import com.bazaarvoice.emodb.test.ResourceTest;
import com.bazaarvoice.emodb.web.auth.EmoPermissionResolver;
import com.bazaarvoice.emodb.web.resources.blob.BlobStoreResource1;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.junit.Rule;
import org.junit.Test;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Named;
import javax.ws.rs.client.ClientBuilder;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;

import static org.mockito.Mockito.*;
import static org.testng.Assert.assertEquals;


public class BlobStoreJersey2Test extends ResourceTest {
    @Inject
    @Named("Jersey2BlobStore")
    BlobStore blobStore;

    private final BlobStore _server = mock(BlobStore.class);
    private final AuthBlobStore _authBlobStore = mock(AuthBlobStore.class);

    private static final String APIKEY_BLOB = "blob-key";
    private static final String APIKEY_UNAUTHORIZED = "unauthorized-key";
    private static final String APIKEY_BLOB_A = "a-blob-key";
    private static final String APIKEY_BLOB_B = "b-blob-key";
    private final ScheduledExecutorService _connectionManagementService = mock(ScheduledExecutorService.class);

    @Test
    public void testListTables() {
        TableOptions options = new TableOptionsBuilder().setPlacement("my:placement").build();
        TableAvailability availability = new TableAvailability("my:placement", false);
        List<Table> expected = ImmutableList.of(
                new DefaultTable("table-1", options, ImmutableMap.of("key", "value1"), availability),
                new DefaultTable("table-2", options, ImmutableMap.of("key", "value2"), availability));
        when(_server.listTables(null, Long.MAX_VALUE)).thenReturn(expected.iterator());

        BlobStore blobStore2 = blobClient(APIKEY_BLOB);
        System.out.println("******* BLOBSTORE FROM INJECT**** "+blobStore2.toString());
        List<Table> actual = Lists.newArrayList(
                BlobStoreStreaming2.listTables(blobStore2));

        assertEquals(expected, actual);
        verify(_server).listTables(null, Long.MAX_VALUE);
        verifyNoMoreInteractions(_server);
    }

    private BlobStore blobClient(String apiKey) {
       /* return BlobStoreJersey2ClientFactory
                .forClusterAndHttpClient("local_default",  ClientBuilder.newClient(), URI.create("http://localhost:8480/blob/1"))
                .usingCredentials(apiKey);*/
        return BlobStoreAuthenticator2.proxied(new BlobStoreJersey2Client(URI.create("http://localhost:8481/healthcheck"),
                        new Jersey2EmoClient(ClientBuilder.newClient()), _connectionManagementService))
                .usingCredentials(apiKey);
    }

}
