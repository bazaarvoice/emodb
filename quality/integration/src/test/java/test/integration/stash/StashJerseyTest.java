package test.integration.stash;

import com.bazaarvoice.emodb.auth.apikey.ApiKey;
import com.bazaarvoice.emodb.auth.apikey.ApiKeyModification;
import com.bazaarvoice.emodb.auth.apikey.ApiKeyRequest;
import com.bazaarvoice.emodb.auth.identity.InMemoryAuthIdentityManager;
import com.bazaarvoice.emodb.auth.permissions.InMemoryPermissionManager;
import com.bazaarvoice.emodb.auth.permissions.PermissionUpdateRequest;
import com.bazaarvoice.emodb.auth.role.InMemoryRoleManager;
import com.bazaarvoice.emodb.auth.role.RoleIdentifier;
import com.bazaarvoice.emodb.auth.role.RoleManager;
import com.bazaarvoice.emodb.auth.role.RoleModification;
import com.bazaarvoice.emodb.blob.api.BlobStore;
import com.bazaarvoice.emodb.sor.api.DataStore;
import com.bazaarvoice.emodb.test.ResourceTest;
import com.bazaarvoice.emodb.web.auth.EmoPermissionResolver;
import com.bazaarvoice.emodb.web.scanner.ScanUploader;
import com.bazaarvoice.emodb.web.scanner.resource.StashResource1;
import com.bazaarvoice.emodb.web.scanner.scanstatus.StashRequest;
import com.bazaarvoice.emodb.web.scanner.scheduling.StashRequestManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.dropwizard.testing.junit.ResourceTestRule;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.client.Entity;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;

import javax.ws.rs.core.Response;
import java.util.Date;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

/**
 * Note:  This class uses the "JerseyTest" naming convention even though there is currently no Java Jersey client
 *        implementation for accessing the Stash API.
 */
public class StashJerseyTest extends ResourceTest {

    private final static String STASH_API_KEY = "stash-api-key";
    private final static String UNAUTH_API_KEY = "unauth-api-key";

    private String _stashKeyId;
    private String _unauthKeyId;
    private ScanUploader _scanUploader;
    private StashRequestManager _stashRequestManager;

    @Rule
    public ResourceTestRule _resourceTestRule = setupResourceTestRule();

    private ResourceTestRule setupResourceTestRule() {
        final InMemoryAuthIdentityManager<ApiKey> authIdentityManager = new InMemoryAuthIdentityManager<>();
        _stashKeyId = authIdentityManager.createIdentity(STASH_API_KEY, new ApiKeyModification().addRoles("stash"));
        _unauthKeyId = authIdentityManager.createIdentity(UNAUTH_API_KEY, new ApiKeyModification());

        final EmoPermissionResolver permissionResolver = new EmoPermissionResolver(mock(DataStore.class), mock(BlobStore.class));
        final InMemoryPermissionManager permissionManager = new InMemoryPermissionManager(permissionResolver);
        final RoleManager roleManager = new InMemoryRoleManager(permissionManager);

        roleManager.createRole(RoleIdentifier.fromString("stash"),
                new RoleModification()
                        .withName("stash")
                        .withPermissionUpdate(new PermissionUpdateRequest().permit("stash|*")));

        _scanUploader = mock(ScanUploader.class);
        _stashRequestManager = mock(StashRequestManager.class);

        return setupResourceTestRule(
                ImmutableList.of(new StashResource1(_scanUploader, _stashRequestManager)),
                authIdentityManager,
                permissionManager);
    }

    @After
    public void resetMocks() {
        verifyNoMoreInteractions(_scanUploader, _stashRequestManager);
        reset(_scanUploader, _stashRequestManager);
    }

    @Test
    public void testRequestStash() {
        _resourceTestRule.client().target("/stash/1/request/id0")
                .request()
                .header(ApiKeyRequest.AUTHENTICATION_HEADER, STASH_API_KEY)
                .put(Entity.json(null));

        verify(_stashRequestManager).requestStashOnOrAfter("id0", null, _stashKeyId);
    }

    @Test
    public void testRequestStashNoPermission() {
        try {
            _resourceTestRule.client().target("/stash/1/request/id0")
                    .request()
                    .header(ApiKeyRequest.AUTHENTICATION_HEADER, UNAUTH_API_KEY)
                    .put(Entity.json(null));
        } catch (WebApplicationException e) {
            assertEquals(Response.Status.FORBIDDEN.getStatusCode(), e.getResponse().getStatus());
        }
    }

    @Test
    public void testViewRequestStash() {
        when(_stashRequestManager.getRequestsForStash("id0", null)).thenReturn(ImmutableSet.of(
                new StashRequest("__otherKey0", new Date(1511820710000L)),
                new StashRequest(_stashKeyId, new Date(1511820711000L)),
                new StashRequest("__otherKey1", new Date(1511820712000L))));

        String response = _resourceTestRule.client().target("/stash/1/request/id0")
                .request()
                .header(ApiKeyRequest.AUTHENTICATION_HEADER, STASH_API_KEY)
                .get(String.class);

        assertEquals("\"2017-11-27T22:11:51.000Z\"", response);

        verify(_stashRequestManager).getRequestsForStash("id0", null);
    }

    @Test
    public void testViewUnrequestedStash() {
        when(_stashRequestManager.getRequestsForStash("id0", null)).thenReturn(ImmutableSet.of());

        try {
            _resourceTestRule.client().target("/stash/1/request/id0")
                    .request()
                    .header(ApiKeyRequest.AUTHENTICATION_HEADER, STASH_API_KEY)
                    .get(String.class);
        } catch (WebApplicationException e) {
            assertEquals(Response.Status.NOT_FOUND.getStatusCode(), e.getResponse().getStatus());
        }

        verify(_stashRequestManager).getRequestsForStash("id0", null);
    }

    @Test
    public void testViewRequestStashNoPermission() {
        try {
            _resourceTestRule.client().target("/stash/1/request/id0")
                    .request()
                    .header(ApiKeyRequest.AUTHENTICATION_HEADER, UNAUTH_API_KEY)
                    .get(String.class);
        } catch (WebApplicationException e) {
            assertEquals(Response.Status.FORBIDDEN.getStatusCode(), e.getResponse().getStatus());
        }
    }

    @Test
    public void testUndoRequestStash() {
        _resourceTestRule.client().target("/stash/1/request/id0")
                .request()
                .header(ApiKeyRequest.AUTHENTICATION_HEADER, STASH_API_KEY)
                .delete();

        verify(_stashRequestManager).undoRequestForStashOnOrAfter("id0", null, _stashKeyId);
    }

    @Test
    public void testUndoRequestStashNoPermission() {
        try {
            _resourceTestRule.client().target("/stash/1/request/id0")
                    .request()
                    .header(ApiKeyRequest.AUTHENTICATION_HEADER, UNAUTH_API_KEY)
                    .delete();
        } catch (WebApplicationException e) {
            assertEquals(Response.Status.FORBIDDEN.getStatusCode(), e.getResponse().getStatus());
        }
    }
}
