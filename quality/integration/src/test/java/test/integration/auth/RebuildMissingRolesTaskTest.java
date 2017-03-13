package test.integration.auth;

import com.bazaarvoice.emodb.auth.permissions.InMemoryPermissionManager;
import com.bazaarvoice.emodb.auth.permissions.PermissionManager;
import com.bazaarvoice.emodb.auth.permissions.PermissionUpdateRequest;
import com.bazaarvoice.emodb.auth.role.InMemoryRoleManager;
import com.bazaarvoice.emodb.auth.role.RoleIdentifier;
import com.bazaarvoice.emodb.auth.role.RoleManager;
import com.bazaarvoice.emodb.auth.role.RoleModification;
import com.bazaarvoice.emodb.blob.api.BlobStore;
import com.bazaarvoice.emodb.common.dropwizard.task.TaskRegistry;
import com.bazaarvoice.emodb.sor.api.DataStore;
import com.bazaarvoice.emodb.web.auth.EmoPermissionResolver;
import com.bazaarvoice.emodb.web.auth.RebuildMissingRolesTask;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import org.apache.shiro.authz.permission.PermissionResolver;
import org.testng.annotations.Test;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;

public class RebuildMissingRolesTaskTest {

    @Test
    public void testTask() throws Exception {
        PermissionResolver permissionResolver = new EmoPermissionResolver(mock(DataStore.class), mock(BlobStore.class));
        PermissionManager permissionManager = new InMemoryPermissionManager(permissionResolver);
        RoleManager roleManager = new InMemoryRoleManager(permissionManager);

        RebuildMissingRolesTask task = new RebuildMissingRolesTask(permissionManager, roleManager, mock(TaskRegistry.class));

        // Create pre-existing permissions for two roles, one with a group and one without
        permissionManager.updatePermissions("role:role1", new PermissionUpdateRequest().permit("role1|*"));
        permissionManager.updatePermissions("role:group2/role2", new PermissionUpdateRequest().permit("role2|*"));

        // Create a role complete with permissions which should be untouched by the task
        roleManager.createRole(new RoleIdentifier(null, "role3"),
                new RoleModification()
                        .withName("role3")
                        .withPermissionUpdate(new PermissionUpdateRequest().permit(ImmutableSet.of("role3|*"))));

        // Run the task
        StringWriter out = new StringWriter();
        task.execute(ImmutableMultimap.of(), new PrintWriter(out));

        // Verify all three roles exist with the correct permissions
        assertEquals(roleManager.getRole(new RoleIdentifier(null, "role1")).getName(), "role1");
        assertEquals(roleManager.getPermissionsForRole(new RoleIdentifier(null, "role1")), ImmutableSet.of("role1|*"));
        assertEquals(roleManager.getRole(new RoleIdentifier("group2", "role2")).getName(), "role2");
        assertEquals(roleManager.getPermissionsForRole(new RoleIdentifier("group2", "role2")), ImmutableSet.of("role2|*"));
        assertEquals(roleManager.getRole(new RoleIdentifier(null, "role3")).getName(), "role3");
        assertEquals(roleManager.getPermissionsForRole(new RoleIdentifier(null, "role3")), ImmutableSet.of("role3|*"));

        Set<String> lines = ImmutableSet.copyOf(out.toString().split("\n"));
        assertEquals(lines, ImmutableSet.of("Created missing role: role1", "Created missing role: group2/role2"));
    }
}
