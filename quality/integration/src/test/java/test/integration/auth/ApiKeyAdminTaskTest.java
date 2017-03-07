package test.integration.auth;

import com.bazaarvoice.emodb.auth.apikey.ApiKey;
import com.bazaarvoice.emodb.auth.apikey.ApiKeyRealm;
import com.bazaarvoice.emodb.auth.apikey.ApiKeyRequest;
import com.bazaarvoice.emodb.auth.apikey.ApiKeySecurityManager;
import com.bazaarvoice.emodb.auth.identity.IdentityState;
import com.bazaarvoice.emodb.auth.identity.InMemoryAuthIdentityManager;
import com.bazaarvoice.emodb.auth.permissions.InMemoryPermissionManager;
import com.bazaarvoice.emodb.auth.permissions.PermissionUpdateRequest;
import com.bazaarvoice.emodb.blob.api.BlobStore;
import com.bazaarvoice.emodb.common.dropwizard.task.TaskRegistry;
import com.bazaarvoice.emodb.sor.api.DataStore;
import com.bazaarvoice.emodb.web.auth.ApiKeyAdminTask;
import com.bazaarvoice.emodb.web.auth.DefaultRoles;
import com.bazaarvoice.emodb.web.auth.EmoPermissionResolver;
import com.bazaarvoice.emodb.web.auth.Permissions;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.net.HostAndPort;
import org.apache.shiro.cache.MemoryConstrainedCacheManager;
import org.apache.shiro.util.ThreadContext;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.PrintWriter;
import java.io.StringWriter;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class ApiKeyAdminTaskTest {

    private ApiKeyAdminTask _task;
    private InMemoryAuthIdentityManager<ApiKey> _authIdentityManager;
    private InMemoryPermissionManager _permissionManager;

    @BeforeMethod
    public void setUp() {
        _authIdentityManager = new InMemoryAuthIdentityManager<>(ApiKey.class);
        EmoPermissionResolver permissionResolver = new EmoPermissionResolver(mock(DataStore.class), mock(BlobStore.class));
        _permissionManager = new InMemoryPermissionManager(permissionResolver);

        _permissionManager.updateForRole(DefaultRoles.admin.toString(), new PermissionUpdateRequest().permit(Permissions.manageApiKeys()));
        ApiKeySecurityManager securityManager = new ApiKeySecurityManager(
                new ApiKeyRealm("test", new MemoryConstrainedCacheManager(), _authIdentityManager, _permissionManager,
                        null));

        _task = new ApiKeyAdminTask(securityManager, mock(TaskRegistry.class), _authIdentityManager,
                HostAndPort.fromParts("0.0.0.0", 8080), ImmutableSet.of("reservedrole"));
        _authIdentityManager.updateIdentity(new ApiKey("test-admin", "id_admin", IdentityState.ACTIVE, ImmutableSet.of(DefaultRoles.admin.toString())));
    }

    @AfterMethod
    public void tearDown() {
        ThreadContext.remove();
    }

    @Test
    public void testCreateNewApiKey() throws Exception {
        StringWriter output = new StringWriter();
        PrintWriter pw = new PrintWriter(output);
        _task.execute(ImmutableMultimap.<String, String>builder()
                .put(ApiKeyRequest.AUTHENTICATION_PARAM, "test-admin")
                .put("action", "create")
                .putAll("role", "role1", "role2")
                .put("owner", "joe")
                .put("description", "desc")
                .build(), pw);
        String key = output.toString().split("\\n")[0].split(":")[1].trim();

        ApiKey apiKey = _authIdentityManager.getIdentity(key);
        assertNotNull(apiKey);
        assertEquals(apiKey.getId(), key);
        assertEquals(apiKey.getRoles(), ImmutableSet.of("role1", "role2"));
        assertEquals(apiKey.getOwner(), "joe");
        assertEquals(apiKey.getDescription(), "desc");
    }

    @Test
    public void testUpdateApiKey() throws Exception {
        String key = "updateapikeytestkey";

        _authIdentityManager.updateIdentity(new ApiKey(key, "id_update", IdentityState.ACTIVE, ImmutableSet.of("role1", "role2", "role3")));

        _task.execute(ImmutableMultimap.<String, String>builder()
                .put(ApiKeyRequest.AUTHENTICATION_PARAM, "test-admin")
                .put("action", "update")
                .put("key", key)
                .putAll("addRole", "role4", "role5")
                .put("removeRole", "role3")
                .build(), mock(PrintWriter.class));

        ApiKey apiKey = _authIdentityManager.getIdentity(key);
        assertNotNull(apiKey);
        assertEquals(apiKey.getRoles(), ImmutableSet.of("role1", "role2", "role4", "role5"));
    }

    @Test
    public void testMigrateApiKey() throws Exception {
        String key = "migrateapikeytestkey";

        _authIdentityManager.updateIdentity(new ApiKey(key, "id_migrate", IdentityState.ACTIVE, ImmutableSet.of("role1", "role2")));
        assertNotNull(_authIdentityManager.getIdentity(key));

        StringWriter output = new StringWriter();
        PrintWriter pw = new PrintWriter(output);
        _task.execute(ImmutableMultimap.of(
                ApiKeyRequest.AUTHENTICATION_PARAM, "test-admin",
                "action", "migrate", "key", key), pw);
        String newKey = output.toString().split("\\n")[0].split(":")[1].trim();

        ApiKey apiKey = _authIdentityManager.getIdentity(newKey);
        assertNotNull(apiKey);
        assertEquals(apiKey.getState(), IdentityState.ACTIVE);
        assertEquals(apiKey.getRoles(), ImmutableSet.of("role1", "role2"));
        assertEquals(apiKey.getInternalId(), "id_migrate");
        assertEquals(_authIdentityManager.getIdentity(key).getState(), IdentityState.MIGRATED);
    }

    @Test
    public void testInactivateApiKey() throws Exception {
        String key = "inactivateapikeytestkey";

        _authIdentityManager.updateIdentity(new ApiKey(key, "id_inactive", IdentityState.ACTIVE, ImmutableSet.of("role1", "role2")));
        assertEquals(_authIdentityManager.getIdentity(key).getState(), IdentityState.ACTIVE);

        _task.execute(ImmutableMultimap.of(
                ApiKeyRequest.AUTHENTICATION_PARAM, "test-admin",
                "action", "inactivate", "key", key), mock(PrintWriter.class));
        assertEquals(_authIdentityManager.getIdentity(key).getState(), IdentityState.INACTIVE);
    }

    @Test
    public void testDeleteApiKey() throws Exception {
        String key = "deleteapikeytestkey";

        _authIdentityManager.updateIdentity(new ApiKey(key, "id_delete", IdentityState.ACTIVE, ImmutableSet.of("role1", "role2")));
        assertEquals(_authIdentityManager.getIdentity(key).getState(), IdentityState.ACTIVE);

        // Attempt to delete the key without the confirmation parameter
        _task.execute(ImmutableMultimap.of(
                ApiKeyRequest.AUTHENTICATION_PARAM, "test-admin",
                "action", "delete", "key", key), mock(PrintWriter.class));
        // The key should be unchanged
        assertEquals(_authIdentityManager.getIdentity(key).getState(), IdentityState.ACTIVE);

        // Delete the key again, this time with the necessary confirmation parameter
        _task.execute(ImmutableMultimap.of(
                ApiKeyRequest.AUTHENTICATION_PARAM, "test-admin",
                "action", "delete", "key", key, "confirm", "true"), mock(PrintWriter.class));
        assertNull(_authIdentityManager.getIdentity(key));
        assertNull(_authIdentityManager.getInternalIdentity("id_delete"));
    }

    @Test
    public void testBadAdminCredentials() throws Exception {
        StringWriter output = new StringWriter();
        PrintWriter pw = new PrintWriter(output);

        _task.execute(ImmutableMultimap.of(
                ApiKeyRequest.AUTHENTICATION_PARAM, "invalid-api-key",
                "action", "delete", "key", "somekey"), pw);

        assertEquals(output.toString(), "Not authorized\n");
    }

    @Test (expectedExceptions = IllegalArgumentException.class)
    public void testCreateApiKeyWithReservedRole() throws Exception {
        StringWriter output = new StringWriter();
        PrintWriter pw = new PrintWriter(output);
        _task.execute(ImmutableMultimap.<String, String>builder()
                .put(ApiKeyRequest.AUTHENTICATION_PARAM, "test-admin")
                .put("action", "create")
                .putAll("role", "role1", "reservedrole")
                .put("owner", "joe")
                .put("description", "desc")
                .build(), pw);
    }
}
