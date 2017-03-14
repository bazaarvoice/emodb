package test.integration.auth;

import com.bazaarvoice.emodb.auth.apikey.ApiKey;
import com.bazaarvoice.emodb.auth.apikey.ApiKeyRealm;
import com.bazaarvoice.emodb.auth.apikey.ApiKeyRequest;
import com.bazaarvoice.emodb.auth.apikey.ApiKeySecurityManager;
import com.bazaarvoice.emodb.auth.identity.InMemoryAuthIdentityManager;
import com.bazaarvoice.emodb.auth.permissions.InMemoryPermissionManager;
import com.bazaarvoice.emodb.auth.permissions.PermissionUpdateRequest;
import com.bazaarvoice.emodb.auth.role.InMemoryRoleManager;
import com.bazaarvoice.emodb.auth.role.RoleIdentifier;
import com.bazaarvoice.emodb.auth.role.RoleUpdateRequest;
import com.bazaarvoice.emodb.blob.api.BlobStore;
import com.bazaarvoice.emodb.common.dropwizard.task.TaskRegistry;
import com.bazaarvoice.emodb.sor.api.DataStore;
import com.bazaarvoice.emodb.sor.condition.Conditions;
import com.bazaarvoice.emodb.web.auth.DefaultRoles;
import com.bazaarvoice.emodb.web.auth.EmoPermissionResolver;
import com.bazaarvoice.emodb.web.auth.Permissions;
import com.bazaarvoice.emodb.web.auth.RoleAdminTask;
import com.bazaarvoice.emodb.web.auth.resource.ConditionResource;
import com.bazaarvoice.emodb.web.auth.resource.NamedResource;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.ByteStreams;
import org.apache.shiro.cache.MemoryConstrainedCacheManager;
import org.apache.shiro.util.ThreadContext;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.fail;

public class RoleAdminTaskTest {
    private RoleAdminTask _task;
    private InMemoryAuthIdentityManager<ApiKey> _authIdentityManager;
    private InMemoryRoleManager _roleManager;

    @BeforeMethod
    public void setUp() {
        _authIdentityManager = new InMemoryAuthIdentityManager<>();
        EmoPermissionResolver permissionResolver = new EmoPermissionResolver(mock(DataStore.class), mock(BlobStore.class));
        InMemoryPermissionManager permissionManager = new InMemoryPermissionManager(permissionResolver);
        _roleManager = new InMemoryRoleManager(permissionManager);

        RoleIdentifier adminId = new RoleIdentifier(null, DefaultRoles.admin.toString());
        _roleManager.createRole(adminId, new RoleUpdateRequest()
                .withPermissionUpdate(new PermissionUpdateRequest().permit(ImmutableSet.of(Permissions.unlimitedRole()))));
        ApiKeySecurityManager securityManager = new ApiKeySecurityManager(
                new ApiKeyRealm("test", new MemoryConstrainedCacheManager(), _authIdentityManager, permissionManager,
                        null));

        _task = new RoleAdminTask(securityManager, _roleManager, permissionManager.getPermissionResolver(), mock(TaskRegistry.class));
        _authIdentityManager.updateIdentity(new ApiKey("test-admin", "id_admin", ImmutableSet.of(DefaultRoles.admin.toString())));
    }

    @AfterMethod
    public void tearDown() {
        ThreadContext.remove();
    }

    @Test
    public void testInvalidApiKey()
            throws Exception {
        StringWriter out = new StringWriter();
        _task.execute(ImmutableMultimap.of(
                        ApiKeyRequest.AUTHENTICATION_PARAM, "invalid-key",
                        "action", "view",
                        "role", "any-role"),
                new PrintWriter(out));

        assertEquals(out.toString(), "Not authorized\n");
    }

    @Test
    public void testViewRole()
            throws Exception {
        _roleManager.createRole(new RoleIdentifier(null, "view-role"), new RoleUpdateRequest()
                .withPermissionUpdate(new PermissionUpdateRequest().permit(
                        ImmutableSet.of("queue|post|foo", "queue|poll|foo", "sor|update|test:*", "blob|update|test:*"))));

        StringWriter out = new StringWriter();

        _task.execute(ImmutableMultimap.of(
                        ApiKeyRequest.AUTHENTICATION_PARAM, "test-admin",
                        "action", "view",
                        "role", "view-role"),
                new PrintWriter(out));

        String[] lines = out.toString().split("\\n");
        assertEquals(lines.length, 5);
        // First line is a header.  Skip this and verify the remaining lines are the permissions alphabetically sorted
        assertEquals(lines[1], "- blob|update|test:*");
        assertEquals(lines[2], "- queue|poll|foo");
        assertEquals(lines[3], "- queue|post|foo");
        assertEquals(lines[4], "- sor|update|test:*");
    }

    @Test
    public void testCreateRole()
            throws Exception {
        _task.execute(ImmutableMultimap.of(
                ApiKeyRequest.AUTHENTICATION_PARAM, "test-admin",
                "action", "update",
                "role", "new-role",
                "permit", "sor|update|if({..,\"foo\":\"bar\"})",
                "permit", "queue|post|*"),
                new PrintWriter(ByteStreams.nullOutputStream()));

        Set<String> permissions = _roleManager.getPermissionsForRole(new RoleIdentifier(null, "new-role"));
        Set<String> expected = ImmutableSet.of(
                Permissions.updateSorTable(
                        new ConditionResource(Conditions.mapBuilder()
                                .contains("foo", "bar")
                                .build())),
                Permissions.postQueue(Permissions.ALL));
        assertEquals(permissions, expected);
    }

    @Test
    public void testUpdateRole()
            throws Exception {
        _roleManager.createRole(new RoleIdentifier(null, "existing-role"), new RoleUpdateRequest()
                .withPermissionUpdate(new PermissionUpdateRequest().permit(
                        ImmutableSet.of("queue|post|foo", "queue|post|bar"))));

        _task.execute(ImmutableMultimap.of(
                        ApiKeyRequest.AUTHENTICATION_PARAM, "test-admin",
                        "action", "update",
                        "role", "existing-role",
                        "permit", "queue|post|baz",
                        "revoke", "queue|post|bar"),
                new PrintWriter(ByteStreams.nullOutputStream()));

        Set<String> permissions = _roleManager.getPermissionsForRole(new RoleIdentifier(null, "existing-role"));
        Set<String> expected = ImmutableSet.of(
                Permissions.postQueue(new NamedResource("foo")),
                Permissions.postQueue(new NamedResource("baz")));
        assertEquals(permissions, expected);
    }

    @Test
    public void testDeleteRole()
            throws Exception {
        _roleManager.createRole(new RoleIdentifier(null, "delete-role"), new RoleUpdateRequest()
                .withPermissionUpdate(new PermissionUpdateRequest().permit(
                        ImmutableSet.of("queue|post|foo", "queue|post|bar"))));

        _task.execute(ImmutableMultimap.of(
                        ApiKeyRequest.AUTHENTICATION_PARAM, "test-admin",
                        "action", "delete",
                        "role", "delete-role"),
                new PrintWriter(ByteStreams.nullOutputStream()));

        assertNull(_roleManager.getRole(new RoleIdentifier(null, "delete-role")));
    }

    @Test
    public void testCannotModifyDefaultRole()
            throws Exception {

        for (DefaultRoles defaultRole : DefaultRoles.values()) {
            try {
                _task.execute(ImmutableMultimap.of(
                                ApiKeyRequest.AUTHENTICATION_PARAM, "test-admin",
                                "action", "update",
                                "role", defaultRole.toString(),
                                "permit", "sor|update|test:*"),
                        new PrintWriter(ByteStreams.nullOutputStream()));
                 fail("Update role did not fail " + defaultRole);
            } catch (IllegalArgumentException e) {
                assertEquals(e.getMessage(), "Cannot update default role: " + defaultRole);
            }

            try {
                _task.execute(ImmutableMultimap.of(
                                ApiKeyRequest.AUTHENTICATION_PARAM, "test-admin",
                                "action", "delete",
                                "role", defaultRole.toString()),
                        new PrintWriter(ByteStreams.nullOutputStream()));
                fail("Delete role did not fail " + defaultRole);
            } catch (IllegalArgumentException e) {
                assertEquals(e.getMessage(), "Cannot delete default role: " + defaultRole);
            }
        }
    }

    @Test
    public void testCheckRole()
            throws Exception {
        _roleManager.createRole(new RoleIdentifier("check-group", "check-role"), new RoleUpdateRequest()
                .withPermissionUpdate(new PermissionUpdateRequest().permit(
                        ImmutableSet.of("sor|update|c*", "sor|update|ch*"))));

        Map<String, List<String>> expected = ImmutableMap.<String, List<String>> of(
                "sor|update|check", ImmutableList.of("- sor|update|c*", "- sor|update|ch*"),
                "sor|update|create", ImmutableList.of("- sor|update|c*"),
                "sor|update|notme", ImmutableList.<String>of());

        for (Map.Entry<String, List<String>> entry : expected.entrySet()) {
            StringWriter out = new StringWriter();

            _task.execute(ImmutableMultimap.of(
                            ApiKeyRequest.AUTHENTICATION_PARAM, "test-admin",
                            "action", "check",
                            "role", "check-role",
                            "group", "check-group",
                            "permission", entry.getKey()),
                    new PrintWriter(out));

            List<String> actual = ImmutableList.copyOf(out.toString().split("\n"));

            // The first line is a header; skip it and check the remaining lines
            assertEquals(actual.subList(1, actual.size()), entry.getValue());
        }
    }
}
