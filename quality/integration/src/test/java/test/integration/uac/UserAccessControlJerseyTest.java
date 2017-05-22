package test.integration.uac;

import com.bazaarvoice.emodb.auth.apikey.ApiKey;
import com.bazaarvoice.emodb.auth.apikey.ApiKeyModification;
import com.bazaarvoice.emodb.auth.identity.AuthIdentityManager;
import com.bazaarvoice.emodb.auth.identity.InMemoryAuthIdentityManager;
import com.bazaarvoice.emodb.auth.permissions.InMemoryPermissionManager;
import com.bazaarvoice.emodb.auth.permissions.PermissionUpdateRequest;
import com.bazaarvoice.emodb.auth.role.InMemoryRoleManager;
import com.bazaarvoice.emodb.auth.role.Role;
import com.bazaarvoice.emodb.auth.role.RoleIdentifier;
import com.bazaarvoice.emodb.auth.role.RoleManager;
import com.bazaarvoice.emodb.auth.role.RoleModification;
import com.bazaarvoice.emodb.blob.api.BlobStore;
import com.bazaarvoice.emodb.common.api.UnauthorizedException;
import com.bazaarvoice.emodb.common.jersey.dropwizard.JerseyEmoClient;
import com.bazaarvoice.emodb.sor.api.DataStore;
import com.bazaarvoice.emodb.test.ResourceTest;
import com.bazaarvoice.emodb.uac.api.CreateEmoApiKeyRequest;
import com.bazaarvoice.emodb.uac.api.CreateEmoApiKeyResponse;
import com.bazaarvoice.emodb.uac.api.CreateEmoRoleRequest;
import com.bazaarvoice.emodb.uac.api.EmoApiKey;
import com.bazaarvoice.emodb.uac.api.EmoApiKeyNotFoundException;
import com.bazaarvoice.emodb.uac.api.EmoRole;
import com.bazaarvoice.emodb.uac.api.EmoRoleExistsException;
import com.bazaarvoice.emodb.uac.api.EmoRoleKey;
import com.bazaarvoice.emodb.uac.api.EmoRoleNotFoundException;
import com.bazaarvoice.emodb.uac.api.InsufficientRolePermissionException;
import com.bazaarvoice.emodb.uac.api.InvalidEmoPermissionException;
import com.bazaarvoice.emodb.uac.api.MigrateEmoApiKeyRequest;
import com.bazaarvoice.emodb.uac.api.UpdateEmoApiKeyRequest;
import com.bazaarvoice.emodb.uac.api.UpdateEmoRoleRequest;
import com.bazaarvoice.emodb.uac.api.UserAccessControl;
import com.bazaarvoice.emodb.uac.client.UserAccessControlAuthenticator;
import com.bazaarvoice.emodb.uac.client.UserAccessControlClient;
import com.bazaarvoice.emodb.web.auth.EmoPermissionResolver;
import com.bazaarvoice.emodb.web.auth.Permissions;
import com.bazaarvoice.emodb.web.auth.resource.CreateTableResource;
import com.bazaarvoice.emodb.web.resources.uac.ApiKeyResource1;
import com.bazaarvoice.emodb.web.resources.uac.RoleResource1;
import com.bazaarvoice.emodb.web.resources.uac.UserAccessControlRequestMessageBodyReader;
import com.bazaarvoice.emodb.web.resources.uac.UserAccessControlResource1;
import com.bazaarvoice.emodb.web.uac.LocalSubjectUserAccessControl;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.net.HostAndPort;
import io.dropwizard.testing.junit.ResourceTestRule;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;

import java.net.URI;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class UserAccessControlJerseyTest extends ResourceTest {

    private final static String UAC_ALL_API_KEY = "uac-all-api-key";
    private final static String UAC_NONE_API_KEY = "uac-none-api-key";

    private AuthIdentityManager<ApiKey> _authIdentityManager;
    private RoleManager _roleManager;

    private List<String> _createdApiKeyInternalIds = Lists.newArrayList();
    private List<RoleIdentifier> _createdRoleIds = Lists.newArrayList();

    @Rule
    public ResourceTestRule _resourceTestRule = setupResourceTestRule();

    private ResourceTestRule setupResourceTestRule() {
        _authIdentityManager = new InMemoryAuthIdentityManager<>();
        _authIdentityManager.createIdentity(UAC_ALL_API_KEY, new ApiKeyModification().addRoles("uac-all"));

        final EmoPermissionResolver permissionResolver = new EmoPermissionResolver(mock(DataStore.class), mock(BlobStore.class));
        final InMemoryPermissionManager permissionManager = new InMemoryPermissionManager(permissionResolver);
        _roleManager = new InMemoryRoleManager(permissionManager);

        LocalSubjectUserAccessControl uac = new LocalSubjectUserAccessControl(_roleManager, permissionResolver,
                _authIdentityManager, HostAndPort.fromParts("localhost", 8080));
        
        createRole(_roleManager, null, "uac-all", ImmutableSet.of("role|*", "apikey|*"));
        createRole(_roleManager, null, "uac-none", ImmutableSet.of());

        return setupResourceTestRule(
                ImmutableList.of(
                        new UserAccessControlResource1(
                                new RoleResource1(uac),
                                new ApiKeyResource1(uac)),
                        new UserAccessControlRequestMessageBodyReader()),
                _authIdentityManager,
                permissionManager);
    }

    @After
    public void deleteTestAssets() {
        for (String internalId : _createdApiKeyInternalIds) {
            _authIdentityManager.deleteIdentity(internalId);
        }
        for (RoleIdentifier roleId : _createdRoleIds) {
            _roleManager.deleteRole(roleId);
        }
        _createdApiKeyInternalIds.clear();
        _createdRoleIds.clear();
    }

    private UserAccessControl uacClient(String apiKey) {
        return UserAccessControlAuthenticator.proxied(new UserAccessControlClient(
                URI.create("/uac/1"), new JerseyEmoClient(_resourceTestRule.client())))
                .usingCredentials(apiKey);
    }

    @Test
    public void testListRoles() {
        String key = "group-x-roles-only";
        createRole(null, "group-x-roles-only", null, "role|*|group*|*");
        createApiKey(key, null, null, "group-x-roles-only");

        createRole("group1", "id1", "name1", "perm1", "perm2");
        createRole("group2", "id2", "name2", "perm1", "perm2");
        createRole("group3", "id3", "name3", "perm1", "perm2");
        Set<String> expectedRoles = ImmutableSet.of("group1/id1", "group2/id2", "group3/id3");

        Set<String> actualRoles = ImmutableList.copyOf(uacClient(key).getAllRoles())
                .stream().map(EmoRole::getId).map(Object::toString).collect(Collectors.toSet());
        
        assertEquals(actualRoles, expectedRoles);
    }

    @Test
    public void testListRolesInGroup() {
        String key = "group1-roles-only";
        createRole(null, "group1-roles-only", null, "role|*|group1|*");
        createApiKey(key, null, null, "group1-roles-only");

        createRole("group1", "id1", "name1");
        createRole("group2", "id2", "name2");
        createRole("group1", "id3", "name2");
        createRole("group3", "id4", "name2");
        Set<String> expectedRoles = ImmutableSet.of("group1/id1", "group1/id3");

        Set<String> actualRoles = ImmutableList.copyOf(uacClient(key).getAllRoles())
                .stream().map(EmoRole::getId).map(Object::toString).collect(Collectors.toSet());

        assertEquals(actualRoles, expectedRoles);
    }

    @Test
    public void testGetRole() {
        createRole("group1", "id1", "name1", "perm1");

        EmoRole role = uacClient(UAC_ALL_API_KEY).getRole(new EmoRoleKey("group1", "id1"));
        assertEquals(role.getId(), new EmoRoleKey("group1", "id1"));
        assertEquals(role.getName(), "name1");
        assertEquals(role.getPermissions(), ImmutableSet.of("perm1"));
    }

    @Test
    public void testGetRoleWithNoGroup() {
        createRole(null, "id1", "name1", "perm1");

        EmoRole role = uacClient(UAC_ALL_API_KEY).getRole(new EmoRoleKey(EmoRoleKey.NO_GROUP, "id1"));
        assertEquals(role.getId(), new EmoRoleKey(EmoRoleKey.NO_GROUP, "id1"));
        assertEquals(role.getName(), "name1");
        assertEquals(role.getPermissions(), ImmutableSet.of("perm1"));
    }

    @Test(expected = UnauthorizedException.class)
    public void testGetRoleNoPermission() {
        createRole("group1", "id1", "name1", "perm1");
        uacClient(UAC_NONE_API_KEY).getRole(new EmoRoleKey("group1", "id1"));
    }

    @Test
    public void testGetAssignedRoleNoPermission() {
        String key = "group1-id1-role-only";
        createRole("group1", "id1", "name1", "blob|*"); // No permission to read roles
        createApiKey(key, null, null, "group1/id1");

        // Even though key does not have permission to read roles it can view a role which is assigned to itself
        EmoRole actualRole = uacClient(key).getRole(new EmoRoleKey("group1", "id1"));
        assertEquals(actualRole.getName(), "name1");
        assertEquals(actualRole.getPermissions(), ImmutableSet.of("blob|*"));
    }

    @Test
    public void testGetRoleNotFound() {
        EmoRole role = uacClient(UAC_ALL_API_KEY).getRole(new EmoRoleKey("no_such_group", "no_such_id"));
        assertNull(role);
    }

    @Test
    public void testCreateRole() {
        String key = "create-role-key";
        createRole(null, "id1", "name1", "role|*", "sor|*");
        createApiKey(key, null, null, "id1");

        uacClient(key).createRole(
                new CreateEmoRoleRequest(new EmoRoleKey("create1", "id1"))
                        .setName("create_name")
                        .setDescription("create_description")
                        .setPermissions(ImmutableSet.of("sor|read|table1", "sor|update|table2")));

        Role role = _roleManager.getRole(new RoleIdentifier("create1", "id1"));
        assertEquals(role.getName(), "create_name");
        assertEquals(role.getDescription(), "create_description");

        Set<String> permissions = _roleManager.getPermissionsForRole(new RoleIdentifier("create1", "id1"));
        assertEquals(permissions, ImmutableSet.of("sor|read|table1", "sor|update|table2"));
    }

    @Test(expected = UnauthorizedException.class)
    public void testCreateRoleNoPermission() {
        uacClient(UAC_NONE_API_KEY).createRole(
                new CreateEmoRoleRequest(new EmoRoleKey("create2", "id2"))
                        .setName("create_name")
                        .setDescription("create_description")
                        .setPermissions(ImmutableSet.of("perm1", "perm2")));
    }

    @Test(expected = EmoRoleExistsException.class)
    public void testCreateRoleNotUnique() {
        createRole("create3", "id3", "name3");
        uacClient(UAC_ALL_API_KEY).createRole(
                new CreateEmoRoleRequest(new EmoRoleKey("create3", "id3"))
                        .setName("create_name")
                        .setDescription("create_description")
                        .setPermissions(ImmutableSet.of("role|*")));
    }

    @Test(expected = InvalidEmoPermissionException.class)
    public void testCreateRoleUnassignablePermission() {
        // When creating tables a special permission is used internally by Emo and is not assignable.
        String unassignablePermission = Permissions.createSorTable(new CreateTableResource("name", "ugc_global:ugc", ImmutableMap.of()));

        uacClient(UAC_ALL_API_KEY).createRole(
                new CreateEmoRoleRequest(new EmoRoleKey("create4", "id4"))
                        .setName("create_name")
                        .setDescription("create_description")
                        .setPermissions(ImmutableSet.of(unassignablePermission)));
    }

    @Test(expected = InvalidEmoPermissionException.class)
    public void testCreateRoleBadPermission() {
        uacClient(UAC_ALL_API_KEY).createRole(
                new CreateEmoRoleRequest(new EmoRoleKey("create5", "id5"))
                        .setName("create_name")
                        .setDescription("create_description")
                        .setPermissions(ImmutableSet.of("sor|*|if({)")));
    }

    @Test(expected = InsufficientRolePermissionException.class)
    public void testCreateInsufficientRolePermission() {
        uacClient(UAC_ALL_API_KEY).createRole(
                new CreateEmoRoleRequest(new EmoRoleKey("create6", "id6"))
                        .setName("create_name")
                        .setDescription("create_description")
                        .setPermissions(ImmutableSet.of("sor|read|*")));
    }

    @Test
    public void testUpdateRole() {
        String key = "update-role-key";
        createRole(null, "id1", "name1", "role|*", "sor|read|*");
        createApiKey(key, null, null, "id1");

        createRole("update1", "id1", "name1", "sor|read|table1", "sor|read|table2");

        uacClient(key).updateRole(
                new UpdateEmoRoleRequest(new EmoRoleKey("update1", "id1"))
                        .setName("update_name")
                        .setDescription("update_description")
                        .revokePermissions(ImmutableSet.of("sor|read|table1"))
                        .grantPermissions(ImmutableSet.of("sor|read|table3")));

        Role role = _roleManager.getRole(new RoleIdentifier("update1", "id1"));
        assertEquals(role.getName(), "update_name");
        assertEquals(role.getDescription(), "update_description");

        Set<String> permissions = _roleManager.getPermissionsForRole(new RoleIdentifier("update1", "id1"));
        assertEquals(permissions, ImmutableSet.of("sor|read|table2", "sor|read|table3"));
    }

    @Test(expected = UnauthorizedException.class)
    public void testUpdateRoleNoPermission() {
        createRole("update2", "id2", "name3");
        uacClient(UAC_NONE_API_KEY).updateRole(
                new UpdateEmoRoleRequest(new EmoRoleKey("update2", "id2"))
                        .setName("update_name")
                        .setDescription("update_description"));
    }

    @Test(expected = EmoRoleNotFoundException.class)
    public void testUpdateRoleNotFound() {
        uacClient(UAC_ALL_API_KEY).updateRole(
                new UpdateEmoRoleRequest(new EmoRoleKey("update3", "id3"))
                        .setName("update_name")
                        .setDescription("update_description"));
    }

    @Test(expected = InvalidEmoPermissionException.class)
    public void testUpdateRoleUnassignablePermission() {
        // When creating tables a special permission is used internally by Emo and is not assignable.
        String unassignablePermission = Permissions.createSorTable(new CreateTableResource("name", "ugc_global:ugc", ImmutableMap.of()));

        createRole("update5", "id5", "name5");
        uacClient(UAC_ALL_API_KEY).updateRole(
                new UpdateEmoRoleRequest(new EmoRoleKey("create5", "id5"))
                        .grantPermissions(ImmutableSet.of(unassignablePermission)));
    }

    @Test(expected = InvalidEmoPermissionException.class)
    public void testUpdateRoleBadPermission() {
        createRole("update6", "id6", "name6");
        uacClient(UAC_ALL_API_KEY).updateRole(
                new UpdateEmoRoleRequest(new EmoRoleKey("create6", "id6"))
                        .grantPermissions(ImmutableSet.of("sor|*|if({)")));
    }

    @Test(expected = InsufficientRolePermissionException.class)
    public void testUpdateInsufficientRolePermission() {
        createRole("update6", "id6", "name6");
        uacClient(UAC_ALL_API_KEY).updateRole(
                new UpdateEmoRoleRequest(new EmoRoleKey("create7", "id7"))
                        .grantPermissions(ImmutableSet.of("sor|read|*")));
    }

    @Test
    public void testDeleteRole() {
        String key = "delete-role-key";
        createRole(null, "id1", "name1", "role|*", "sor|*");
        createApiKey(key, null, null, "id1");

        createRole("delete1", "id1", "name1", "sor|read|table1", "sor|update|table1");

        uacClient(key).deleteRole(new EmoRoleKey("delete1", "id1"));
        Role role = _roleManager.getRole(new RoleIdentifier("delete1", "id1"));
        assertNull(role);
    }

    @Test(expected = UnauthorizedException.class)
    public void testDeleteRoleNoPermission() {
        createRole("delete2", "id2", "name3");
        uacClient(UAC_NONE_API_KEY).deleteRole(new EmoRoleKey("delete2", "id2"));
    }

    @Test(expected = EmoRoleNotFoundException.class)
    public void testDeleteRoleNotFound() {
        uacClient(UAC_ALL_API_KEY).deleteRole(new EmoRoleKey("delete3", "id3"));
    }

    @Test(expected = InsufficientRolePermissionException.class)
    public void testDeleteInsufficientRolePermission() {
        createRole("delete4", "id4", "name4", "sor|read|*");
        uacClient(UAC_ALL_API_KEY).deleteRole(new EmoRoleKey("delete4", "id4"));
    }

    @Test
    public void testRoleHasPermissionCheck() {
        createRole("perm1", "id1", "name1", "sor|read|table1", "sor|read|table2");
        EmoRoleKey roleKey = new EmoRoleKey("perm1", "id1");
        UserAccessControl uac = uacClient(UAC_ALL_API_KEY);
        assertTrue(uac.checkRoleHasPermission(roleKey, "sor|read|table1"));
        assertTrue(uac.checkRoleHasPermission(roleKey, "sor|read|table2"));
        assertFalse(uac.checkRoleHasPermission(roleKey, "sor|read|table3"));
    }

    @Test(expected = EmoRoleNotFoundException.class)
    public void testRoleHasPermissionNoRole() {
        uacClient(UAC_ALL_API_KEY).checkRoleHasPermission(new EmoRoleKey("perm2", "id2"), "sor|read|*");
    }

    @Test(expected = UnauthorizedException.class)
    public void testRoleHasPermissionNoPermission() {
        uacClient(UAC_NONE_API_KEY).checkRoleHasPermission(new EmoRoleKey("perm3", "id3"), "sor|read|*");
    }

    @Test(expected = InvalidEmoPermissionException.class)
    public void testRoleHasPermissionBadPermission() {
        uacClient(UAC_ALL_API_KEY).checkRoleHasPermission(new EmoRoleKey("perm4", "id4"), "sor|read|if({)");
    }

    @Test
    public void testGetApiKey() {
        String id = createApiKey("key1", "owner1", "description1", "group1/role1", "role2");
        EmoApiKey apiKey = uacClient(UAC_ALL_API_KEY).getApiKey(id);
        assertEquals(apiKey.getId(), id);
        assertEquals(apiKey.getOwner(), "owner1");
        assertEquals(apiKey.getDescription(), "description1");
        assertEquals(apiKey.getRoles(), ImmutableSet.of(
                new EmoRoleKey("group1", "role1"), new EmoRoleKey(EmoRoleKey.NO_GROUP, "role2")));
    }

    @Test
    public void testGetSelfApiKey() {
        // Create an API key with no permissions
        String id = createApiKey("key2", "owner2", "description2");
        // Even though the key has no permissions it should be allowed to view itself
        EmoApiKey apiKey = uacClient("key2").getApiKey(id);
        assertEquals(apiKey.getId(), id);
        assertEquals(apiKey.getOwner(), "owner2");
        assertEquals(apiKey.getDescription(), "description2");
        assertEquals(apiKey.getRoles(), ImmutableSet.of());
    }

    @Test(expected = UnauthorizedException.class)
    public void testGetApiKeyNoPermission() {
        String id = createApiKey("key3", "owner3", "description3");
        uacClient(UAC_NONE_API_KEY).getApiKey(id);
    }

    @Test
    public void testGetApiKeyByKey() {
        String id = createApiKey("key4", "owner4", "description4");
        EmoApiKey apiKey = uacClient(UAC_ALL_API_KEY).getApiKeyByKey("key4");
        assertEquals(apiKey.getId(), id);
    }

    @Test
    public void testGetApiKeyByKeyNoPermission() {
        // Create an API key which has permission to view API keys but not by key
        String id = createApiKey("key5", "owner5", "description5", Permissions.readApiKey());
        // Verify the key can view itself
        EmoApiKey apiKey = uacClient("key5").getApiKey(id);
        assertEquals(apiKey.getId(), id);
        // Verify the key can't view itself by key
        try {
            uacClient("key5").getApiKeyByKey("key5");
            fail("UnauthorizedException not thrown");
        } catch (UnauthorizedException e) {
            // Ok
        }
    }

    @Test
    public void testGetApiKeyNotFound() {
        EmoApiKey key = uacClient(UAC_ALL_API_KEY).getApiKey("no_such_id");
        assertNull(key);
    }

    @Test
    public void testCreateApiKey() {
        CreateEmoApiKeyResponse response = uacClient(UAC_ALL_API_KEY).createApiKey(
                new CreateEmoApiKeyRequest()
                        .setOwner("owner1")
                        .setDescription("description1")
                        .setRoles(ImmutableSet.of(
                                new EmoRoleKey("group1", "id1"),
                                new EmoRoleKey(EmoRoleKey.NO_GROUP, "id2"))));

        ApiKey key = _authIdentityManager.getIdentity(response.getId());
        assertEquals(key, _authIdentityManager.getIdentityByAuthenticationId(response.getKey()));
        assertEquals(key.getOwner(), "owner1");
        assertEquals(key.getDescription(), "description1");
        assertEquals(key.getRoles(), ImmutableSet.of("group1/id1", "id2"));
    }

    @Test
    public void testCreateExactApiKey() {
        String forcedKey = "forcedkey000000000000000000000000000000000000000";

        CreateEmoApiKeyRequest request = new CreateEmoApiKeyRequest()
                .setOwner("owner1")
                .setDescription("description1");
        request.setCustomRequestParameter("key", forcedKey);

        CreateEmoApiKeyResponse response = uacClient(UAC_ALL_API_KEY).createApiKey(request);


        assertEquals(response.getKey(), forcedKey);
        ApiKey key = _authIdentityManager.getIdentity(response.getId());
        assertEquals(key, _authIdentityManager.getIdentityByAuthenticationId(response.getKey()));
        assertEquals(key.getOwner(), "owner1");
        assertEquals(key.getDescription(), "description1");
        assertEquals(key.getRoles(), ImmutableSet.of());
    }

    @Test(expected = UnauthorizedException.class)
    public void testCreateApiKeyNoPermission() {
        uacClient(UAC_NONE_API_KEY).createApiKey(
                new CreateEmoApiKeyRequest()
                        .setOwner("owner1")
                        .setDescription("description1"));
    }

    @Test
    public void testCreateApiKeyLimitedGrantRolePermission() {
        // Create an API key which has permission to create an API key but only to assign it one role
        String key = "create-limit-grant-role-perms";
        createApiKey(key, null, null, "create-key-limited-roles");
        createRole(null, "create-key-limited-roles", null,
                Permissions.createApiKey(), Permissions.grantRole(new RoleIdentifier("group1", "id1")));
        
        try {
            uacClient(key).createApiKey(
                    new CreateEmoApiKeyRequest()
                            .setOwner("owner1")
                            .setDescription("description1")
                            .setRoles(ImmutableSet.of(
                                    new EmoRoleKey("group1", "id1"),
                                    new EmoRoleKey("group2", "id2"))));
            fail("UnauthorizedException not thrown");
        } catch (UnauthorizedException e) {
            // Make sure it's unauthorized for the right reason
            assertEquals(e.getMessage(), "Not authorized for roles: group2/id2");
        }
    }

    @Test
    public void testUpdateApiKey() {
        String id = createApiKey("update1", "owner1", "description1", "role1", "role2");

        uacClient(UAC_ALL_API_KEY).updateApiKey(
                new UpdateEmoApiKeyRequest(id)
                        .setOwner("update_owner")
                        .setDescription("update_description")
                        .assignRoles(ImmutableSet.of(new EmoRoleKey(EmoRoleKey.NO_GROUP, "role3")))
                        .unassignRoles(ImmutableSet.of(new EmoRoleKey(EmoRoleKey.NO_GROUP, "role1"))));

        ApiKey key = _authIdentityManager.getIdentity(id);
        assertEquals(key.getOwner(), "update_owner");
        assertEquals(key.getDescription(), "update_description");
        assertEquals(key.getRoles(), ImmutableSet.of("role2", "role3"));
    }

    @Test(expected = UnauthorizedException.class)
    public void testUpdateApiKeyNoPermission() {
        String id = createApiKey("update2", "owner2", "description2", "role1", "role2");

        uacClient(UAC_NONE_API_KEY).updateApiKey(
                new UpdateEmoApiKeyRequest(id)
                        .setOwner("update_owner")
                        .setDescription("update_description"));
    }

    @Test
    public void testUpdateApiKeyLimitedGrantRolePermission() {
        // Create an API key which has permission to create an API key but only to assign it a single role
        String key = "update-limited-grant-role-perms";
        createApiKey(key, null, null, "create-key-limited-role-grant");
        createRole(null, "create-key-limited-role-grant", null,
                Permissions.createApiKey(), Permissions.grantRole(new RoleIdentifier(null, "role1")));

        String id = createApiKey("update3", "owner3", "description3", "role1", "role2");

        // Run the test twice, once to add a role and once to remove a role
        try {
            uacClient(key).updateApiKey(
                    new UpdateEmoApiKeyRequest(id)
                            .assignRoles(ImmutableSet.of(
                                    new EmoRoleKey(EmoRoleKey.NO_GROUP, "role1"),
                                    new EmoRoleKey(EmoRoleKey.NO_GROUP, "role3"))));
            fail("UnauthorizedException not thrown");
        } catch (UnauthorizedException e) {
            // Make sure it's unauthorized for the right reason
            assertEquals(e.getMessage(), "Not authorized for roles: role3");
        }

        try {
            uacClient(key).updateApiKey(
                    new UpdateEmoApiKeyRequest(id)
                            .unassignRoles(ImmutableSet.of(
                                    new EmoRoleKey(EmoRoleKey.NO_GROUP, "role1"),
                                    new EmoRoleKey(EmoRoleKey.NO_GROUP, "role3"))));
            fail("UnauthorizedException not thrown");
        } catch (UnauthorizedException e) {
            // Make sure it's unauthorized for the right reason
            assertEquals(e.getMessage(), "Not authorized for roles: role3");
        }
    }

    @Test
    public void testUpdateApiKeyUnassignOtherRoles() {
        String id = createApiKey("update4", "owner4", "description4", "role1", "role2", "role3");

        uacClient(UAC_ALL_API_KEY).updateApiKey(
                new UpdateEmoApiKeyRequest(id)
                        .assignRoles(ImmutableSet.of(
                                new EmoRoleKey(EmoRoleKey.NO_GROUP, "role1"),
                                new EmoRoleKey(EmoRoleKey.NO_GROUP, "role4")))
                        .setUnassignOtherRoles(true));

        ApiKey key = _authIdentityManager.getIdentity(id);
        assertEquals(key.getRoles(), ImmutableSet.of("role1", "role4"));
    }

    @Test
    public void testUpdateApiKeyUnassignOtherRolesNoPermission() {
        // Create an API key which has permission to create an API key but only to assign it a single role
        String key = "update-limited-grant-role-perms";
        createApiKey(key, null, null, "create-key-limited-role-grant");
        createRole(null, "create-key-limited-role-grant", null,
                Permissions.createApiKey(), Permissions.grantRole(new RoleIdentifier(null, "role1")));

        String id = createApiKey("update5", "owner5", "description5", "role1", "role2");

        try {
            uacClient(key).updateApiKey(
                    new UpdateEmoApiKeyRequest(id)
                            .setUnassignOtherRoles(true));
            fail("UnauthorizedException not thrown");
        } catch (UnauthorizedException e) {
            // Make sure it's unauthorized for the right reason
            assertEquals(e.getMessage(), "Not authorized for roles: role2");
        }
    }

    @Test(expected = EmoApiKeyNotFoundException.class)
    public void testUpdateApiKeyNotFound() {
        uacClient(UAC_ALL_API_KEY).updateApiKey(
                new UpdateEmoApiKeyRequest("no_such_id")
                        .setOwner("update_owner")
                        .setDescription("update_description"));
    }

    @Test
    public void testMigrateApiKey() {
        String id = createApiKey("migrate1", "owner1", "description1", "role1", "role2");

        String newKey = uacClient(UAC_ALL_API_KEY).migrateApiKey(id);

        ApiKey key = _authIdentityManager.getIdentity(id);
        assertEquals(_authIdentityManager.getIdentityByAuthenticationId(newKey), key);
        assertNull(_authIdentityManager.getIdentityByAuthenticationId("migrate1"));
        assertEquals(key.getOwner(), "owner1");
        assertEquals(key.getDescription(), "description1");
        assertEquals(key.getRoles(), ImmutableSet.of("role1", "role2"));
    }

    @Test
    public void testMigrateToExactApiKey() {
        String forcedKey = "forcedkey000000000000000000000000000000000000001";
        String id = createApiKey("migrate1", "owner1", "description1");

        MigrateEmoApiKeyRequest request = new MigrateEmoApiKeyRequest(id);
        request.setCustomRequestParameter("key", forcedKey);

        String newKey = uacClient(UAC_ALL_API_KEY).migrateApiKey(request);

        assertEquals(newKey, forcedKey);
        ApiKey key = _authIdentityManager.getIdentity(id);
        assertEquals(_authIdentityManager.getIdentityByAuthenticationId(newKey), key);
        assertNull(_authIdentityManager.getIdentityByAuthenticationId("migrate1"));
        assertEquals(key.getOwner(), "owner1");
        assertEquals(key.getDescription(), "description1");
        assertEquals(key.getRoles(), ImmutableSet.of());
    }

    @Test(expected = UnauthorizedException.class)
    public void testMigrateApiKeyNoPermission() {
        String id = createApiKey("migrate3", "owner3", "description3", "role1", "role2");
        uacClient(UAC_NONE_API_KEY).migrateApiKey(id);
    }

    @Test(expected = EmoApiKeyNotFoundException.class)
    public void testMigrateApiKeyNotFound() {
        uacClient(UAC_ALL_API_KEY).migrateApiKey("no_such_id");
    }

    @Test
    public void testDeleteApiKey() {
        String id = createApiKey("delete1", "owner1", "description1", "role1", "role2");

        uacClient(UAC_ALL_API_KEY).deleteApiKey(id);

        assertNull(_authIdentityManager.getIdentity(id));
        assertNull(_authIdentityManager.getIdentityByAuthenticationId("delete1"));
    }

    @Test(expected = UnauthorizedException.class)
    public void testDeleteApiKeyNoPermission() {
        String id = createApiKey("delete2", "owner2", "description2", "role1", "role2");
        uacClient(UAC_NONE_API_KEY).deleteApiKey(id);
    }

    @Test
    public void testDeleteApiKeyLimitedGrantRolePermission() {
        // Create an API key which has permission to delete an API key but only to unassign a single role
        String key = "delete-limited-grant-role-perms";
        createApiKey(key, null, null, "delete-key-limited-role");
        createRole(null, "delete-key-limited-role", null,
                Permissions.deleteApiKey(), Permissions.grantRole(new RoleIdentifier(null, "role1")));

        String id = createApiKey("delete3", "owner3", "description3", "role1", "role2");

        try {
            uacClient(key).deleteApiKey(id);
            fail("UnauthorizedException not thrown");
        } catch (UnauthorizedException e) {
            // Make sure it's unauthorized for the right reason
            assertEquals(e.getMessage(), "Not authorized for roles: role2");
        }
    }

    @Test(expected = EmoApiKeyNotFoundException.class)
    public void testDeleteApiKeyNotFound() {
        uacClient(UAC_ALL_API_KEY).deleteApiKey("no_such_id");
    }

    @Test
    public void testApiKeyHasPermissionCheck() {
        createRole("perm1", "id1", "name1", "sor|read|table1");
        createRole("perm2", "id2", "name2", "sor|read|table2");
        String id = createApiKey("perm-key1", null, null, "perm1/id1", "perm2/id2", "no_such_role");

        UserAccessControl uac = uacClient(UAC_ALL_API_KEY);
        assertTrue(uac.checkApiKeyHasPermission(id, "sor|read|table1"));
        assertTrue(uac.checkApiKeyHasPermission(id, "sor|read|table2"));
        assertFalse(uac.checkApiKeyHasPermission(id, "sor|read|table3"));
    }

    @Test(expected = EmoApiKeyNotFoundException.class)
    public void testApiKeyHasPermissionNoApiKey() {
        uacClient(UAC_ALL_API_KEY).checkApiKeyHasPermission("no_such_id", "sor|read|*");
    }

    @Test(expected = UnauthorizedException.class)
    public void testApiKeyHasPermissionNoPermission() {
        String id = createApiKey("perm-key2", "owner2", "description2");
        uacClient(UAC_NONE_API_KEY).checkApiKeyHasPermission(id, "sor|read|*");
    }

    @Test(expected = InvalidEmoPermissionException.class)
    public void testApiKeyHasPermissionBadPermission() {
        String id = createApiKey("perm-key3", "owner3", "description3");
        uacClient(UAC_ALL_API_KEY).checkApiKeyHasPermission(id, "sor|read|if({)");
    }

    //******************************************************************************************************************

    private void createRole(String group, String id, String name, String... permissions) {
        RoleIdentifier roleId = new RoleIdentifier(group, id);
        _roleManager.createRole(new RoleIdentifier(group, id),
                new RoleModification()
                        .withName(name)
                        .withPermissionUpdate(new PermissionUpdateRequest()
                                .permit(permissions)));

        _createdRoleIds.add(roleId);
    }

    private String createApiKey(String key, String owner, String description, String... roles) {
        String internalId = _authIdentityManager.createIdentity(key,
                new ApiKeyModification()
                        .withOwner(owner)
                        .withDescription(description)
                        .addRoles(ImmutableSet.copyOf(roles)));

        _createdApiKeyInternalIds.add(internalId);
        
        return internalId;
    }
}
