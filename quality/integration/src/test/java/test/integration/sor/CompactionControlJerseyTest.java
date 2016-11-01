package test.integration.sor;

import com.bazaarvoice.emodb.auth.apikey.ApiKey;
import com.bazaarvoice.emodb.auth.identity.InMemoryAuthIdentityManager;
import com.bazaarvoice.emodb.auth.permissions.InMemoryPermissionManager;
import com.bazaarvoice.emodb.auth.permissions.PermissionUpdateRequest;
import com.bazaarvoice.emodb.blob.api.BlobStore;
import com.bazaarvoice.emodb.client.EmoClientException;
import com.bazaarvoice.emodb.common.jersey.dropwizard.JerseyEmoClient;
import com.bazaarvoice.emodb.sor.api.CompactionControlSource;
import com.bazaarvoice.emodb.sor.api.DataStore;
import com.bazaarvoice.emodb.sor.client.CompactionControlClient;
import com.bazaarvoice.emodb.sor.core.DataStoreAsync;
import com.bazaarvoice.emodb.test.ResourceTest;
import com.bazaarvoice.emodb.web.auth.DefaultRoles;
import com.bazaarvoice.emodb.web.auth.EmoPermissionResolver;
import com.bazaarvoice.emodb.web.resources.sor.DataStoreResource1;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.sun.jersey.api.client.ClientResponse;
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

public class CompactionControlJerseyTest extends ResourceTest {
    private static final String APIKEY_COMPACTION_CONTROL = "compaction-control-key";
    private static final String APIKEY_UNAUTHORIZED = "unauthorized-key";

    private DataStore _dataStoreServer = mock(DataStore.class);
    private CompactionControlSource _compactionControlSourceServer = mock(CompactionControlSource.class);

    @Rule
    public ResourceTestRule _resourceTestRule = setupReplicationResourceTestRule(ImmutableList.<Object>of(new DataStoreResource1(_dataStoreServer, mock(DataStoreAsync.class), _compactionControlSourceServer)),
            new ApiKey(APIKEY_COMPACTION_CONTROL, "compaction-control", ImmutableSet.of("compaction-control-role")),
            new ApiKey(APIKEY_UNAUTHORIZED, "unauth", ImmutableSet.of("unauthorized-role")));

    protected static ResourceTestRule setupReplicationResourceTestRule(List<Object> resourceList, ApiKey apiKey, ApiKey unauthorizedKey) {
        InMemoryAuthIdentityManager<ApiKey> authIdentityManager = new InMemoryAuthIdentityManager<>();
        authIdentityManager.updateIdentity(apiKey);
        authIdentityManager.updateIdentity(unauthorizedKey);

        EmoPermissionResolver permissionResolver = new EmoPermissionResolver(mock(DataStore.class), mock(BlobStore.class));
        InMemoryPermissionManager permissionManager = new InMemoryPermissionManager(permissionResolver);
        permissionManager.updateForRole(
                "compaction-control-role", new PermissionUpdateRequest().permit(DefaultRoles.compaction_control.getPermissions()));

        return setupResourceTestRule(resourceList, authIdentityManager, permissionManager);
    }

    @After
    public void tearDownMocksAndClearState() {
        verifyNoMoreInteractions(_dataStoreServer);
        reset(_dataStoreServer);
        verifyNoMoreInteractions(_compactionControlSourceServer);
        reset(_compactionControlSourceServer);
    }

    private CompactionControlSource compactionControlClient() {
        return compactionControlClient(APIKEY_COMPACTION_CONTROL);
    }

    private CompactionControlSource compactionControlClient(String apiKey) {
        return new CompactionControlClient(URI.create("/sor/1"), new JerseyEmoClient(_resourceTestRule.client()), apiKey);
    }

    @Test
    public void testUpdateStashTime() {
        compactionControlClient().updateStashTime("1", 123L, ImmutableList.of("placement-name"), 123L, "datacenter");

        verify(_compactionControlSourceServer).updateStashTime("1", 123L, ImmutableList.of("placement-name"), 123L, "datacenter");
        verifyNoMoreInteractions(_compactionControlSourceServer);
    }

    @Test
    public void testdeleteStashTime() {
        compactionControlClient().deleteStashTime("1", "datacenter");

        verify(_compactionControlSourceServer).deleteStashTime("1", "datacenter");
        verifyNoMoreInteractions(_compactionControlSourceServer);
    }

    @Test
    public void testGetStashTime() {
        compactionControlClient().getStashTime("1", "datacenter");

        verify(_compactionControlSourceServer).getStashTime("1", "datacenter");
        verifyNoMoreInteractions(_compactionControlSourceServer);
    }

    @Test
    public void testAllStashTimes() {
        compactionControlClient().getAllStashTimes();

        verify(_compactionControlSourceServer).getAllStashTimes();
        verifyNoMoreInteractions(_compactionControlSourceServer);
    }

    @Test
    public void testAllStashTimesForPlacement() {
        compactionControlClient().getStashTimesForPlacement("placement");

        verify(_compactionControlSourceServer).getStashTimesForPlacement("placement");
        verifyNoMoreInteractions(_compactionControlSourceServer);
    }


    /**
     * Test delete w/an invalid API key.
     */
    @Test
    public void testdeleteStashTimeUnauthenticated() {
        try {
            compactionControlClient(APIKEY_UNAUTHORIZED).deleteStashTime("1", "datacenter");
            fail();
        } catch (EmoClientException e) {
            if (e.getResponse().getStatus() != ClientResponse.Status.FORBIDDEN.getStatusCode()) {
                throw e;
            }
        }
        verifyNoMoreInteractions(_compactionControlSourceServer);
    }

    /**
     * Test delete w/a valid API key but not one that has permission to delete.
     */
    @Test
    public void testDeleteForbidden() {
        try {
            compactionControlClient("completely-unknown-key").deleteStashTime("1", "datacenter");
            fail();
        } catch (EmoClientException e) {
            if (e.getResponse().getStatus() != ClientResponse.Status.FORBIDDEN.getStatusCode()) {
                throw e;
            }
        }
        verifyNoMoreInteractions(_compactionControlSourceServer);
    }
}
