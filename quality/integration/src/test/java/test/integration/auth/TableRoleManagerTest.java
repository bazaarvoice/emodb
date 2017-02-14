package test.integration.auth;

import com.bazaarvoice.emodb.auth.permissions.InMemoryPermissionManager;
import com.bazaarvoice.emodb.auth.permissions.PermissionManager;
import com.bazaarvoice.emodb.auth.permissions.PermissionUpdateRequest;
import com.bazaarvoice.emodb.auth.role.Role;
import com.bazaarvoice.emodb.auth.role.RoleExistsException;
import com.bazaarvoice.emodb.auth.role.RoleIdentifier;
import com.bazaarvoice.emodb.auth.role.RoleNotFoundException;
import com.bazaarvoice.emodb.auth.role.RoleUpdateRequest;
import com.bazaarvoice.emodb.auth.role.TableRoleManager;
import com.bazaarvoice.emodb.sor.api.Audit;
import com.bazaarvoice.emodb.sor.api.DataStore;
import com.bazaarvoice.emodb.sor.api.Intrinsic;
import com.bazaarvoice.emodb.sor.api.ReadConsistency;
import com.bazaarvoice.emodb.sor.api.TableOptionsBuilder;
import com.bazaarvoice.emodb.sor.api.WriteConsistency;
import com.bazaarvoice.emodb.sor.core.test.InMemoryDataStore;
import com.bazaarvoice.emodb.sor.delta.Deltas;
import com.bazaarvoice.emodb.web.auth.EmoPermissionResolver;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.shiro.authz.permission.PermissionResolver;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TableRoleManagerTest {

    private final static String ROLE_TABLE = "roles";
    private final static String GROUP_TABLE = "groups";
    private final static String PLACEMENT = "system";

    private DataStore _backendDataStore;
    private DataStore _dataStore;
    private PermissionResolver _permissionResolver;
    private PermissionManager _backendPermissionManager;
    private PermissionManager _permissionManager;
    private TableRoleManager _roleManager;

    @BeforeMethod
    public void setUp() {
        // DataStore and PermissionManager are fairly heavy to fully mock.  Use spies on in-memory implementations instead
        _backendDataStore = new InMemoryDataStore(new MetricRegistry());
        _dataStore = spy(_backendDataStore);
        _permissionResolver = new EmoPermissionResolver(null, null);
        _backendPermissionManager = new InMemoryPermissionManager(_permissionResolver);
        _permissionManager = spy(_backendPermissionManager);
        _roleManager = new TableRoleManager(_dataStore, ROLE_TABLE, GROUP_TABLE, PLACEMENT, _permissionManager);
    }
    
    @Test
    public void testCreatesTables() throws Exception {
        // For this test make a call to read a single role.  The actual role returned is irrelevant, this test verifies
        // that on the first call only the role and group tables are checked and created when necessary.

        RoleIdentifier id = new RoleIdentifier("g1", "r1");

        for (int i=0; i < 3; i++) {
            _roleManager.getRole(id);
        }

        verify(_dataStore, times(3)).get(ROLE_TABLE, "g1/r1", ReadConsistency.STRONG);
        
        // Each table is checked and created once
        verify(_dataStore).getTableExists(ROLE_TABLE);
        verify(_dataStore).createTable(
                eq(ROLE_TABLE), eq(new TableOptionsBuilder().setPlacement(PLACEMENT).build()), eq(ImmutableMap.of()), any(Audit.class));
        verify(_dataStore).getTableExists(GROUP_TABLE);
        verify(_dataStore).createTable(
                eq(GROUP_TABLE), eq(new TableOptionsBuilder().setPlacement(PLACEMENT).build()), eq(ImmutableMap.of()), any(Audit.class));
    }

    @Test
    public void testCreateRole() throws Exception {
        RoleIdentifier id = new RoleIdentifier("g1", "r1");
        _roleManager.createRole(id, "d1", ImmutableSet.of("p1", "p2"));

        Map<String, Object> roleMap = _backendDataStore.get(ROLE_TABLE, "g1/r1", ReadConsistency.STRONG);
        assertEquals(Intrinsic.getId(roleMap), "g1/r1");
        assertEquals(roleMap.get("description"), "d1");

        Map<String, Object> groupMap = _backendDataStore.get(GROUP_TABLE, "g1", ReadConsistency.STRONG);
        assertEquals(Intrinsic.getId(groupMap), "g1");
        assertEquals(groupMap.get("names"), ImmutableList.of("r1"));

        assertEquals(_backendPermissionManager.getPermissions("role:g1/r1"),
                ImmutableSet.of(_permissionResolver.resolvePermission("p1"), _permissionResolver.resolvePermission("p2")));
    }

    @Test
    public void testCreateRoleWitNoGroup() throws Exception {
        RoleIdentifier id = new RoleIdentifier(null, "r1");
        _roleManager.createRole(id, "d1", ImmutableSet.of("p1", "p2"));

        Map<String, Object> roleMap = _backendDataStore.get(ROLE_TABLE, "r1", ReadConsistency.STRONG);
        assertEquals(Intrinsic.getId(roleMap), "r1");
        assertEquals(roleMap.get("description"), "d1");

        Map<String, Object> groupMap = _backendDataStore.get(GROUP_TABLE, "_", ReadConsistency.STRONG);
        assertEquals(Intrinsic.getId(groupMap), "_");
        assertEquals(groupMap.get("names"), ImmutableList.of("r1"));

        assertEquals(_backendPermissionManager.getPermissions("role:r1"),
                ImmutableSet.of(_permissionResolver.resolvePermission("p1"), _permissionResolver.resolvePermission("p2")));
    }

    @Test
    public void testAddRoleToExistingGroup() throws Exception {
        RoleIdentifier id1 = new RoleIdentifier("g1", "r1");
        _roleManager.createRole(id1, null, null);
        RoleIdentifier id2 = new RoleIdentifier("g1", "r2");
        _roleManager.createRole(id2, null, null);

        Map<String, Object> groupMap = _backendDataStore.get(GROUP_TABLE, "g1", ReadConsistency.STRONG);
        assertEquals(Intrinsic.getId(groupMap), "g1");
        assertEquals(groupMap.get("names"), ImmutableList.of("r1", "r2"));
    }

    @Test
    public void testCreateExistingRoleFails() throws Exception {
        // Create a role once
        RoleIdentifier id = new RoleIdentifier("g1", "r1");
        _roleManager.createRole(id, null, null);
        // Creating the same role again should fail
        try {
            _roleManager.createRole(id, null, null);
            fail("RoleExistsException not thrown");
        } catch (RoleExistsException e) {
            // expected
        }
    }

    @Test
    public void testCreateInvalidRoleName() throws Exception {
        RoleIdentifier id = new RoleIdentifier("g1", "bad/role");
        try {
            _roleManager.createRole(id, null, null);
            fail("IllegalArgumentException not thrown");
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    public void testCreateInvalidGroupName() throws Exception {
        RoleIdentifier id = new RoleIdentifier("bad/group", "r1");
        try {
            _roleManager.createRole(id, null, null);
            fail("IllegalArgumentException not thrown");
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    public void testUpdateRole() throws Exception {
        RoleIdentifier id = new RoleIdentifier("g1", "r1");
        _roleManager.createRole(id, "d1", ImmutableSet.of("p1", "p2"));

        // Modify the role
        _roleManager.updateRole(id, new RoleUpdateRequest()
                .withDescription("new description")
                .withPermissionUpdate(new PermissionUpdateRequest().revoke("p2").permit("p3")));

        Map<String, Object> roleMap = _backendDataStore.get(ROLE_TABLE, "g1/r1", ReadConsistency.STRONG);
        assertEquals(Intrinsic.getId(roleMap), "g1/r1");
        assertEquals(roleMap.get("description"), "new description");

        assertEquals(_backendPermissionManager.getPermissions("role:g1/r1"),
                ImmutableSet.of(_permissionResolver.resolvePermission("p1"), _permissionResolver.resolvePermission("p3")));
    }

    @Test
    public void testUpdateNonExistentRole() throws Exception {
        try {
            _roleManager.updateRole(new RoleIdentifier("g1", "r1"), new RoleUpdateRequest()
                    .withDescription("new description"));
            fail("RoleNotFoundException not thrown");
        } catch (RoleNotFoundException e) {
            // expected
        }
    }

    @Test
    public void testGetPermissionsForRole() throws Exception {
        RoleIdentifier id = new RoleIdentifier("g1", "r1");
        _roleManager.createRole(id, "d1", ImmutableSet.of("p1", "p2"));

        Set<String> permissions = _roleManager.getPermissionsForRole(id);
        assertEquals(permissions, ImmutableSet.of("p1", "p2"));
    }

    @Test
    public void testGetPermissionsForNonExistentRole() throws Exception {
        RoleIdentifier id = new RoleIdentifier("g1", "no_such_role");
        Set<String> permissions = _roleManager.getPermissionsForRole(id);
        // Expected behavior is to return an empty set of permissions, not to raise an exception or return null
        assertEquals(permissions, ImmutableSet.of());
    }

    @Test
    public void testGetRolesByGroup() throws Exception {
        _roleManager.createRole(new RoleIdentifier("g1", "r1"), null, null);
        _roleManager.createRole(new RoleIdentifier("g1", "r2"), null, null);
        _roleManager.createRole(new RoleIdentifier("g2", "r3"), null, null);
        _roleManager.createRole(new RoleIdentifier(null, "r4"), null, null);

        assertEquals(_roleManager.getRolesByGroup("g1").stream().map(Role::getName).collect(Collectors.toSet()),
                ImmutableSet.of("r1", "r2"));
        assertEquals(_roleManager.getRolesByGroup("g2").stream().map(Role::getName).collect(Collectors.toSet()),
                ImmutableSet.of("r3"));
        assertEquals(_roleManager.getRolesByGroup(null).stream().map(Role::getName).collect(Collectors.toSet()),
                ImmutableSet.of("r4"));
        assertEquals(_roleManager.getRolesByGroup("no_such_group"), ImmutableList.of());
    }

    @Test
    public void testGetAll() throws Exception {
        Set<RoleIdentifier> roleIds = ImmutableSet.of(
                new RoleIdentifier("g1", "r1"),
                new RoleIdentifier("g1", "r2"),
                new RoleIdentifier("g2", "r3"),
                new RoleIdentifier(null, "r4"));

        for (RoleIdentifier id : roleIds) {
            _roleManager.createRole(id, null, null);
        }
        List<Role> actual = ImmutableList.copyOf(_roleManager.getAll());

        assertEquals(actual.stream().map(Role::getId).collect(Collectors.toSet()), roleIds);
    }

    @Test
    public void testDeleteRole() throws Exception {
        RoleIdentifier id = new RoleIdentifier("g1", "r1");
        _roleManager.createRole(id, "d1", ImmutableSet.of("p1", "p2"));

        // Verify role exists
        assertNotNull(_roleManager.getRole(id));
        assertFalse(_roleManager.getPermissionsForRole(id).isEmpty());

        _roleManager.deleteRole(id);

        // Test the interfaces
        assertNull(_roleManager.getRole(id));
        assertTrue(_roleManager.getPermissionsForRole(id).isEmpty());

        // Verify the expected API calls to the delegates were made
        verify(_dataStore).update(eq(ROLE_TABLE), eq("g1/r1"), any(UUID.class), eq(Deltas.delete()),
                any(Audit.class), eq(WriteConsistency.GLOBAL));
        verify(_dataStore).update(eq(GROUP_TABLE), eq("g1"), any(UUID.class),
                eq(Deltas.mapBuilder()
                        .update("names", Deltas.setBuilder()
                                .remove("r1")
                                .deleteIfEmpty()
                                .build())
                        .deleteIfEmpty()
                        .build()),
                any(Audit.class), eq(WriteConsistency.GLOBAL));
        verify(_permissionManager).revokePermissions("role:g1/r1");
    }
}
