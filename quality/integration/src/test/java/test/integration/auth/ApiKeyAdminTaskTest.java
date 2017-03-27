package test.integration.auth;

import com.bazaarvoice.emodb.auth.apikey.ApiKey;
import com.bazaarvoice.emodb.auth.apikey.ApiKeyModification;
import com.bazaarvoice.emodb.auth.apikey.ApiKeyRealm;
import com.bazaarvoice.emodb.auth.apikey.ApiKeyRequest;
import com.bazaarvoice.emodb.auth.apikey.ApiKeySecurityManager;
import com.bazaarvoice.emodb.auth.identity.InMemoryAuthIdentityManager;
import com.bazaarvoice.emodb.auth.permissions.InMemoryPermissionManager;
import com.bazaarvoice.emodb.auth.permissions.PermissionUpdateRequest;
import com.bazaarvoice.emodb.auth.role.InMemoryRoleManager;
import com.bazaarvoice.emodb.auth.role.RoleIdentifier;
import com.bazaarvoice.emodb.auth.role.RoleModification;
import com.bazaarvoice.emodb.blob.api.BlobStore;
import com.bazaarvoice.emodb.common.dropwizard.task.TaskRegistry;
import com.bazaarvoice.emodb.sor.api.DataStore;
import com.bazaarvoice.emodb.web.auth.ApiKeyAdminTask;
import com.bazaarvoice.emodb.web.auth.EmoPermissionResolver;
import com.bazaarvoice.emodb.web.auth.Permissions;
import com.bazaarvoice.emodb.web.auth.resource.NamedResource;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.net.HostAndPort;
import org.apache.shiro.cache.MemoryConstrainedCacheManager;
import org.apache.shiro.util.ThreadContext;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

public class ApiKeyAdminTaskTest {

    private ApiKeyAdminTask _task;
    private InMemoryAuthIdentityManager<ApiKey> _authIdentityManager;
    private InMemoryRoleManager _roleManager;
    private Map<String, String> _apiKeyToIdMap;

    @BeforeMethod
    public void setUp() {
        _authIdentityManager = new InMemoryAuthIdentityManager<>();
        EmoPermissionResolver permissionResolver = new EmoPermissionResolver(mock(DataStore.class), mock(BlobStore.class));
        InMemoryPermissionManager permissionManager = new InMemoryPermissionManager(permissionResolver);
        _roleManager = new InMemoryRoleManager(permissionManager);

        ApiKeySecurityManager securityManager = new ApiKeySecurityManager(
                new ApiKeyRealm("test", new MemoryConstrainedCacheManager(), _authIdentityManager, permissionManager,
                        null));

        _task = new ApiKeyAdminTask(securityManager, mock(TaskRegistry.class), _authIdentityManager,
                HostAndPort.fromParts("0.0.0.0", 8080), ImmutableSet.of("reservedrole"));

        _apiKeyToIdMap = Maps.newHashMap();

        createApiKeyWithPermissions("test-admin", Permissions.unlimitedApiKey(), Permissions.unlimitedRole());
    }

    private String createApiKeyWithPermissions(String key, String... permissions) {
        String role = key + "-role";

        _roleManager.createRole(new RoleIdentifier(null, role),
                new RoleModification().withPermissionUpdate(new PermissionUpdateRequest().permit(
                        ImmutableSet.copyOf(permissions))));

        String id = _authIdentityManager.createIdentity(key, new ApiKeyModification().addRoles(role));
        _apiKeyToIdMap.put(key, id);

        return id;
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
        String[] lines = output.toString().split("\\n");
        String key = lines[0].split(":")[1].trim();
        String id = lines[1].split(":")[1].trim();

        ApiKey apiKey = _authIdentityManager.getIdentityByAuthenticationId(key);
        assertNotNull(apiKey);
        assertEquals(apiKey.getRoles(), ImmutableSet.of("role1", "role2"));
        assertEquals(apiKey.getOwner(), "joe");
        assertEquals(apiKey.getDescription(), "desc");

        assertEquals(_authIdentityManager.getIdentity(id), apiKey);
    }

    @Test
    public void testCreateNewApiKeyNoCreatePermission() throws Exception {
        createApiKeyWithPermissions("no-create-perm");
        Set<String> expectedIds = ImmutableSet.copyOf(_apiKeyToIdMap.values());

        StringWriter output = new StringWriter();
        PrintWriter pw = new PrintWriter(output);
        _task.execute(ImmutableMultimap.<String, String>builder()
                .put(ApiKeyRequest.AUTHENTICATION_PARAM, "no-create-perm")
                .put("action", "create")
                .put("owner", "joe")
                .put("description", "desc")
                .build(), pw);

        assertEquals(output.toString(), "Not authorized\n");
        // No new identities except for the two generated by the unit test itself should exist.
        assertEquals(
                _authIdentityManager.getAllIdentities().stream().map(ApiKey::getId).collect(Collectors.toSet()),
                expectedIds);
    }

    @Test
    public void testCreateNewApiKeyInsufficientGrantPermission() throws Exception {
        createApiKeyWithPermissions("create-no-grant-perm",
                Permissions.createApiKey(),
                Permissions.grantRole(new NamedResource("group1")));

        Set<String> expectedIds = ImmutableSet.copyOf(_apiKeyToIdMap.values());

        StringWriter output = new StringWriter();
        PrintWriter pw = new PrintWriter(output);
        _task.execute(ImmutableMultimap.<String, String>builder()
                .put(ApiKeyRequest.AUTHENTICATION_PARAM, "create-no-grant-perm")
                .put("action", "create")
                .putAll("role", "group1/allow", "deny")
                .put("owner", "joe")
                .put("description", "desc")
                .build(), pw);

        assertEquals(output.toString(), "Not authorized\n");
        // No new identities except for the two generated by the unit test itself should exist.
        assertEquals(
                _authIdentityManager.getAllIdentities().stream().map(ApiKey::getId).collect(Collectors.toSet()),
                expectedIds);
    }

    @Test
    public void testUpdateApiKey() throws Exception {
        String key = "updateapikeytestkey";
        String id = _authIdentityManager.createIdentity(key, new ApiKeyModification().addRoles("role1", "role2", "role3"));

        _task.execute(ImmutableMultimap.<String, String>builder()
                .put(ApiKeyRequest.AUTHENTICATION_PARAM, "test-admin")
                .put("action", "update")
                .put("id", id)
                .putAll("addRole", "role4", "role5")
                .put("removeRole", "role3")
                .build(), mock(PrintWriter.class));

        ApiKey apiKey = _authIdentityManager.getIdentity(id);
        assertNotNull(apiKey);
        assertEquals(apiKey.getRoles(), ImmutableSet.of("role1", "role2", "role4", "role5"));
    }

    @Test
    public void testUpdateNewApiKeyInsufficientGrantPermissionOnAddedRole() throws Exception {
        createApiKeyWithPermissions("update-no-add-grant-perm",
                Permissions.grantRole(new NamedResource("group1")));

        String key = "updateapikeytestkey";
        String id = _authIdentityManager.createIdentity(key, new ApiKeyModification().addRoles("role1", "role2", "role3"));

        StringWriter output = new StringWriter();
        PrintWriter pw = new PrintWriter(output);
        _task.execute(ImmutableMultimap.<String, String>builder()
                .put(ApiKeyRequest.AUTHENTICATION_PARAM, "update-no-add-grant-perm")
                .put("action", "update")
                .put("id", id)
                .putAll("addRole", "group2/role4")
                .build(), pw);

        assertEquals(output.toString(), "Not authorized\n");
        // Roles should be unchanged
        assertEquals(_authIdentityManager.getIdentity(id).getRoles(), ImmutableSet.of("role1", "role2", "role3"));
    }

    @Test
    public void testUpdateNewApiKeyInsufficientGrantPermissionOnRemovedRole() throws Exception {
        createApiKeyWithPermissions("update-no-remove-grant-perm",
                Permissions.grantRole(new NamedResource("group1")));

        String key = "updateapikeytestkey";
        String id = _authIdentityManager.createIdentity(key, new ApiKeyModification().addRoles("role1", "role2", "role3"));

        StringWriter output = new StringWriter();
        PrintWriter pw = new PrintWriter(output);
        _task.execute(ImmutableMultimap.<String, String>builder()
                .put(ApiKeyRequest.AUTHENTICATION_PARAM, "update-no-remove-grant-perm")
                .put("action", "update")
                .put("id", id)
                .putAll("removeRole", "role1")
                .build(), pw);

        assertEquals(output.toString(), "Not authorized\n");
        // Roles should be unchanged
        assertEquals(_authIdentityManager.getIdentity(id).getRoles(), ImmutableSet.of("role1", "role2", "role3"));
    }

    @Test
    public void testMigrateApiKey() throws Exception {
        String key = "migrateapikeytestkey";

        String id = _authIdentityManager.createIdentity(key, new ApiKeyModification().addRoles("role1", "role2"));
        assertNotNull(_authIdentityManager.getIdentity(id));

        StringWriter output = new StringWriter();
        PrintWriter pw = new PrintWriter(output);
        _task.execute(ImmutableMultimap.of(
                ApiKeyRequest.AUTHENTICATION_PARAM, "test-admin",
                "action", "migrate", "id", id), pw);
        String newKey = output.toString().split("\\n")[0].split(":")[1].trim();

        ApiKey apiKey = _authIdentityManager.getIdentityByAuthenticationId(newKey);
        assertNotNull(apiKey);
        assertEquals(apiKey.getRoles(), ImmutableSet.of("role1", "role2"));
        assertEquals(apiKey.getId(), id);
        assertEquals(_authIdentityManager.getIdentity(id), apiKey);
        assertNull(_authIdentityManager.getIdentityByAuthenticationId(key));
    }

    @Test
    public void testMigrateNewApiKeyNoUpdatePermission() throws Exception {
        createApiKeyWithPermissions("no-update-perm");

        String key = "migrateapikeytestkey";
        String id = _authIdentityManager.createIdentity(key, new ApiKeyModification().addRoles("role1", "role2"));
        assertNotNull(_authIdentityManager.getIdentityByAuthenticationId(key));

        StringWriter output = new StringWriter();
        PrintWriter pw = new PrintWriter(output);
        _task.execute(ImmutableMultimap.of(
                ApiKeyRequest.AUTHENTICATION_PARAM, "no-update-perm",
                "action", "migrate", "id", id), pw);

        assertEquals(output.toString(), "Not authorized\n");
        // API key should be unchanged
        assertNotNull(_authIdentityManager.getIdentity(id));
    }

    @Test
    public void testDeleteApiKey() throws Exception {
        String key = "deleteapikeytestkey";

        String id = _authIdentityManager.createIdentity(key, new ApiKeyModification().addRoles("role1", "role2"));
        assertNotNull(_authIdentityManager.getIdentity(id));

        _task.execute(ImmutableMultimap.of(
                ApiKeyRequest.AUTHENTICATION_PARAM, "test-admin",
                "action", "delete", "id", id), mock(PrintWriter.class));
        assertNull(_authIdentityManager.getIdentity(id));
    }

    public void testDeleteApiKeyNoDeletePermission() throws Exception {
        createApiKeyWithPermissions("no-delete-perm");

        String key = "deleteapikeytestkey";
        String id = _authIdentityManager.createIdentity(key, new ApiKeyModification().addRoles("role1", "role2"));
        assertNotNull(_authIdentityManager.getIdentity(id));

        StringWriter output = new StringWriter();
        PrintWriter pw = new PrintWriter(output);
        _task.execute(ImmutableMultimap.of(
                ApiKeyRequest.AUTHENTICATION_PARAM, "no-delete-perm",
                "action", "delete", "id", id), pw);

        assertEquals(output.toString(), "Not authorized\n");
        assertNotNull(_authIdentityManager.getIdentity(id));
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
