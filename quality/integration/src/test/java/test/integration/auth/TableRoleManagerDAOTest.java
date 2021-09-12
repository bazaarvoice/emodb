package test.integration.auth;

import com.bazaarvoice.emodb.auth.permissions.InMemoryPermissionManager;
import com.bazaarvoice.emodb.auth.permissions.PermissionManager;
import com.bazaarvoice.emodb.auth.permissions.PermissionUpdateRequest;
import com.bazaarvoice.emodb.auth.role.Role;
import com.bazaarvoice.emodb.auth.role.RoleExistsException;
import com.bazaarvoice.emodb.auth.role.RoleIdentifier;
import com.bazaarvoice.emodb.auth.role.RoleModification;
import com.bazaarvoice.emodb.auth.role.RoleNotFoundException;
import com.bazaarvoice.emodb.auth.role.TableRoleManagerDAO;
import com.bazaarvoice.emodb.sor.api.Audit;
import com.bazaarvoice.emodb.sor.api.DataStore;
import com.bazaarvoice.emodb.sor.api.Intrinsic;
import com.bazaarvoice.emodb.sor.api.ReadConsistency;
import com.bazaarvoice.emodb.sor.api.TableOptionsBuilder;
import com.bazaarvoice.emodb.sor.api.Update;
import com.bazaarvoice.emodb.sor.api.WriteConsistency;
import com.bazaarvoice.emodb.sor.core.test.InMemoryDataStore;
import com.bazaarvoice.emodb.sor.delta.Deltas;
import com.bazaarvoice.emodb.web.auth.EmoPermissionResolver;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import org.apache.shiro.authz.permission.PermissionResolver;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TableRoleManagerDAOTest {

    private final static String ROLE_TABLE = "roles";
    private final static String GROUP_TABLE = "groups";
    private final static String PLACEMENT = "system";

    private DataStore _backendDataStore;
    private DataStore _dataStore;
    private PermissionResolver _permissionResolver;
    private PermissionManager _backendPermissionManager;
    private PermissionManager _permissionManager;
    private TableRoleManagerDAO _roleManager;

    @BeforeMethod
    public void setUp() {
        // DataStore and PermissionManager are fairly heavy to fully mock.  Use spies on in-memory implementations instead
        _backendDataStore = new InMemoryDataStore(new MetricRegistry());
        _dataStore = spy(_backendDataStore);
        _permissionResolver = new EmoPermissionResolver(null, null);
        _backendPermissionManager = new InMemoryPermissionManager(_permissionResolver);
        _permissionManager = spy(_backendPermissionManager);
        _roleManager = new TableRoleManagerDAO(_dataStore, ROLE_TABLE, GROUP_TABLE, PLACEMENT, _permissionManager);
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
        createRole(id, "n1", "d1", ImmutableSet.of("p1", "p2"));

        Map<String, Object> roleMap = _backendDataStore.get(ROLE_TABLE, "g1/r1", ReadConsistency.STRONG);
        assertEquals(Intrinsic.getId(roleMap), "g1/r1");
        assertEquals(roleMap.get("name"), "n1");
        assertEquals(roleMap.get("description"), "d1");

        Map<String, Object> groupMap = _backendDataStore.get(GROUP_TABLE, "g1", ReadConsistency.STRONG);
        assertEquals(Intrinsic.getId(groupMap), "g1");
        assertEquals(groupMap.get("ids"), ImmutableList.of("r1"));

        assertEquals(_backendPermissionManager.getPermissions("role:g1/r1"),
                ImmutableSet.of(_permissionResolver.resolvePermission("p1"), _permissionResolver.resolvePermission("p2")));
    }

    @Test
    public void testCreateRoleWitNoGroup() throws Exception {
        RoleIdentifier id = new RoleIdentifier(null, "r1");
        createRole(id, "n1", "d1", ImmutableSet.of("p1", "p2"));

        Map<String, Object> roleMap = _backendDataStore.get(ROLE_TABLE, "r1", ReadConsistency.STRONG);
        assertEquals(Intrinsic.getId(roleMap), "r1");
        assertEquals(roleMap.get("name"), "n1");
        assertEquals(roleMap.get("description"), "d1");

        Map<String, Object> groupMap = _backendDataStore.get(GROUP_TABLE, "_", ReadConsistency.STRONG);
        assertEquals(Intrinsic.getId(groupMap), "_");
        assertEquals(groupMap.get("ids"), ImmutableList.of("r1"));

        assertEquals(_backendPermissionManager.getPermissions("role:r1"),
                ImmutableSet.of(_permissionResolver.resolvePermission("p1"), _permissionResolver.resolvePermission("p2")));
    }

    @Test
    public void testAddRoleToExistingGroup() throws Exception {
        RoleIdentifier id1 = new RoleIdentifier("g1", "r1");
        createRole(id1, null,null, null);
        RoleIdentifier id2 = new RoleIdentifier("g1", "r2");
        createRole(id2, null, null, null);

        Map<String, Object> groupMap = _backendDataStore.get(GROUP_TABLE, "g1", ReadConsistency.STRONG);
        assertEquals(Intrinsic.getId(groupMap), "g1");
        assertEquals(groupMap.get("ids"), ImmutableList.of("r1", "r2"));
    }

    @Test
    public void testCreateExistingRoleFails() throws Exception {
        // Create a role once
        RoleIdentifier id = new RoleIdentifier("g1", "r1");
        createRole(id, null, null, null);
        // Creating the same role again should fail
        try {
            createRole(id, null, null, null);
            fail("RoleExistsException not thrown");
        } catch (RoleExistsException e) {
            // expected
        }
    }

    @Test
    public void testCreateInvalidId() throws Exception {
        RoleIdentifier id = new RoleIdentifier("g1", "bad/role");
        try {
            createRole(id, null, null, null);
            fail("IllegalArgumentException not thrown");
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    public void testCreateInvalidGroupName() throws Exception {
        RoleIdentifier id = new RoleIdentifier("bad/group", "r1");
        try {
            createRole(id, null,null, null);
            fail("IllegalArgumentException not thrown");
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    public void testUpdateRole() throws Exception {
        RoleIdentifier id = new RoleIdentifier("g1", "r1");
        createRole(id, "n1", "d1", ImmutableSet.of("p1", "p2"));

        // Modify the role
        _roleManager.updateRole(id, new RoleModification()
                .withName("new name")
                .withDescription("new description")
                .withPermissionUpdate(new PermissionUpdateRequest().revoke("p2").permit("p3")));

        Map<String, Object> roleMap = _backendDataStore.get(ROLE_TABLE, "g1/r1", ReadConsistency.STRONG);
        assertEquals(Intrinsic.getId(roleMap), "g1/r1");
        assertEquals(roleMap.get("name"), "new name");
        assertEquals(roleMap.get("description"), "new description");

        assertEquals(_backendPermissionManager.getPermissions("role:g1/r1"),
                ImmutableSet.of(_permissionResolver.resolvePermission("p1"), _permissionResolver.resolvePermission("p3")));
    }

    @Test
    public void testUpdateNonExistentRole() throws Exception {
        try {
            _roleManager.updateRole(new RoleIdentifier("g1", "r1"), new RoleModification()
                    .withDescription("new description"));
            fail("RoleNotFoundException not thrown");
        } catch (RoleNotFoundException e) {
            // expected
        }
    }

    @Test
    public void testGetPermissionsForRole() throws Exception {
        RoleIdentifier id = new RoleIdentifier("g1", "r1");
        createRole(id, "n1","d1", ImmutableSet.of("p1", "p2"));

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
        createRole(new RoleIdentifier("g1", "r1"), "n1", null, null);
        createRole(new RoleIdentifier("g1", "r2"), "n2", null, null);
        createRole(new RoleIdentifier("g2", "r3"), "n3", null, null);
        createRole(new RoleIdentifier(null, "r4"), "n4", null, null);

        assertEquals(_roleManager.getRolesByGroup("g1").stream().map(Role::getId).collect(Collectors.toSet()),
                ImmutableSet.of("r1", "r2"));
        assertEquals(_roleManager.getRolesByGroup("g2").stream().map(Role::getId).collect(Collectors.toSet()),
                ImmutableSet.of("r3"));
        assertEquals(_roleManager.getRolesByGroup(null).stream().map(Role::getId).collect(Collectors.toSet()),
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
            createRole(id, null,null, null);
        }
        List<Role> actual = ImmutableList.copyOf(_roleManager.getAll());

        assertEquals(actual.stream().map(Role::getRoleIdentifier).collect(Collectors.toSet()), roleIds);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testDeleteRole() throws Exception {
        RoleIdentifier id = new RoleIdentifier("g1", "r1");
        createRole(id, "n1", "d1", ImmutableSet.of("p1", "p2"));

        // Verify role exists
        assertNotNull(_roleManager.getRole(id));
        assertFalse(_roleManager.getPermissionsForRole(id).isEmpty());

        // Clear method captures made while creating the role
        reset(_permissionManager, _dataStore);

        _roleManager.deleteRole(id);

        // Test the interfaces
        assertNull(_roleManager.getRole(id));
        assertTrue(_roleManager.getPermissionsForRole(id).isEmpty());

        // Verify the expected API calls to the delegates were made
        ArgumentCaptor<Iterable> updateCaptor = ArgumentCaptor.forClass(Iterable.class);

        verify(_dataStore).updateAll(updateCaptor.capture());
        List<Update> updates = ImmutableList.copyOf((Iterable<Update>) updateCaptor.getValue());
        assertEquals(updates.size(), 2);
        Map<String, Update> updateByTable = Maps.uniqueIndex(updates, Update::getTable);

        Update update = updateByTable.get(ROLE_TABLE);
        assertNotNull(update);
        assertEquals(update.getKey(), "g1/r1");
        assertEquals(update.getDelta(), Deltas.delete());
        assertEquals(update.getConsistency(), WriteConsistency.GLOBAL);

        update = updateByTable.get(GROUP_TABLE);
        assertNotNull(update);
        assertEquals(update.getKey(), "g1");
        assertEquals(update.getDelta(), Deltas.mapBuilder()
                .update("ids", Deltas.setBuilder()
                        .remove("r1")
                        .deleteIfEmpty()
                        .build())
                .deleteIfEmpty()
                .build());
        assertEquals(update.getConsistency(), WriteConsistency.GLOBAL);

        verify(_permissionManager).revokePermissions("role:g1/r1");
    }

    private void createRole(RoleIdentifier id, String name, String description, Set<String> permissions) {
        RoleModification modification = new RoleModification()
                .withName(name)
                .withDescription(description);

        if (permissions != null) {
            modification = modification.withPermissionUpdate(new PermissionUpdateRequest().permit(permissions));
        }

        _roleManager.createRole(id, modification);
    }
}
