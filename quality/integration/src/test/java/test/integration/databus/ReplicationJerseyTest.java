package test.integration.databus;

import com.bazaarvoice.emodb.auth.apikey.ApiKey;
import com.bazaarvoice.emodb.auth.identity.InMemoryAuthIdentityManager;
import com.bazaarvoice.emodb.auth.permissions.InMemoryPermissionManager;
import com.bazaarvoice.emodb.auth.permissions.PermissionUpdateRequest;
import com.bazaarvoice.emodb.blob.api.BlobStore;
import com.bazaarvoice.emodb.databus.repl.ReplicationClient;
import com.bazaarvoice.emodb.databus.repl.ReplicationSource;
import com.bazaarvoice.emodb.sor.api.DataStore;
import com.bazaarvoice.emodb.test.ResourceTest;
import com.bazaarvoice.emodb.web.auth.DefaultRoles;
import com.bazaarvoice.emodb.web.auth.EmoPermissionResolver;
import com.bazaarvoice.emodb.web.resources.databus.ReplicationResource1;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.UniformInterfaceException;
import io.dropwizard.testing.junit.ResourceTestRule;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;

import java.net.URI;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.testng.Assert.fail;

public class ReplicationJerseyTest extends ResourceTest {
    private static final String APIKEY_REPLICATION = "replication-key";
    private static final String APIKEY_UNAUTHORIZED = "unauthorized-key";

    private final ReplicationSource _server = mock(ReplicationSource.class);

    @Rule
    public ResourceTestRule _resourceTestRule = setupReplicationResourceTestRule(ImmutableList.<Object>of(new ReplicationResource1(_server)),
            new ApiKey(APIKEY_REPLICATION, ImmutableSet.of("replication-role")),
            new ApiKey(APIKEY_UNAUTHORIZED, ImmutableSet.of("unauthorized-role")));

    protected static ResourceTestRule setupReplicationResourceTestRule(List<Object> resourceList, ApiKey apiKey, ApiKey unauthorizedKey) {
        InMemoryAuthIdentityManager<ApiKey> authIdentityManager = new InMemoryAuthIdentityManager<>();
        authIdentityManager.updateIdentity(apiKey);
        authIdentityManager.updateIdentity(unauthorizedKey);

        EmoPermissionResolver permissionResolver = new EmoPermissionResolver(mock(DataStore.class), mock(BlobStore.class));
        InMemoryPermissionManager permissionManager = new InMemoryPermissionManager(permissionResolver);
        permissionManager.updateForRole(
               "replication-role", new PermissionUpdateRequest().permit(DefaultRoles.replication.getPermissions()));

        return setupResourceTestRule(resourceList, authIdentityManager, permissionManager);
    }

    @After
    public void tearDownMocksAndClearState() {
        verifyNoMoreInteractions(_server);
        reset(_server);
    }

    private ReplicationSource replicationClient() {
        return replicationClient(APIKEY_REPLICATION);
    }

    private ReplicationSource replicationClient(String apiKey) {
        return new ReplicationClient(URI.create("/busrepl/1"), _resourceTestRule.client(), apiKey);
    }

    @Test
    public void testGet() {
        replicationClient().get("channel", 123);

        verify(_server).get("channel", 123);
        verifyNoMoreInteractions(_server);
    }

    @Test
    public void testDelete() {
        List<String> ids = ImmutableList.of("first", "second");

        replicationClient().delete("channel", ids);

        verify(_server).delete("channel", ids);
        verifyNoMoreInteractions(_server);
    }

    /** Test delete w/an invalid API key. */
    @Test
    public void testDeleteUnauthenticated() {
        List<String> ids = ImmutableList.of("first", "second");

        try {
            replicationClient(APIKEY_UNAUTHORIZED).delete("channel", ids);
            fail();
        } catch (UniformInterfaceException e) {
            if (e.getResponse().getClientResponseStatus() != ClientResponse.Status.FORBIDDEN) {
                throw e;
            }
        }

        verifyNoMoreInteractions(_server);
    }

    /** Test delete w/a valid API key but not one that has permission to delete. */
    @Test
    public void testDeleteForbidden() {
        List<String> ids = ImmutableList.of("first", "second");

        try {
            replicationClient("completely-unknown-key").delete("channel", ids);
            fail();
        } catch (UniformInterfaceException e) {
            if (e.getResponse().getClientResponseStatus() != ClientResponse.Status.FORBIDDEN) {
                throw e;
            }
        }

        verifyNoMoreInteractions(_server);
    }
}
