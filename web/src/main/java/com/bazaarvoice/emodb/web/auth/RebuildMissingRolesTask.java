package com.bazaarvoice.emodb.web.auth;

import com.bazaarvoice.emodb.auth.permissions.PermissionManager;
import com.bazaarvoice.emodb.auth.permissions.PermissionUpdateRequest;
import com.bazaarvoice.emodb.auth.role.Role;
import com.bazaarvoice.emodb.auth.role.RoleIdentifier;
import com.bazaarvoice.emodb.auth.role.RoleManager;
import com.bazaarvoice.emodb.auth.role.RoleModification;
import com.bazaarvoice.emodb.common.dropwizard.task.TaskRegistry;
import com.google.common.collect.ImmutableMultimap;
import com.google.inject.Inject;
import io.dropwizard.servlets.tasks.Task;
import org.apache.shiro.authz.Permission;

import java.io.PrintWriter;
import java.util.Map;
import java.util.Set;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Task for populating role tables with missing roles.  This is accomplished by scanning for all permissions associated
 * with roles and then creating the role if it does not exist.
 *
 * Use for this task should be infrequent.  It was introduced to handle the upgrade case prior to the introduction of
 * roles as first-order objects.  Prior to that roles only existed as the keys for identifying permissions.  In this
 * case this task would be run once after the upgrade to ensure that all pre-existing roles have records in the role
 * related tables.
 *
 * Example:
 *
 * <code>
 *     $ curl -XPOST "localhost:8081/tasks/rebuild-missing-roles"
 * </code>
 */
public class RebuildMissingRolesTask extends Task {

    private final PermissionManager _permissionManager;
    private final RoleManager _roleManager;

    @Inject
    public RebuildMissingRolesTask(PermissionManager permissionManager, RoleManager roleManager, TaskRegistry taskRegistry) {
        super("rebuild-missing-roles");
        _permissionManager = permissionManager;
        _roleManager = roleManager;

        taskRegistry.addTask(this);
    }

    @Override
    public void execute(ImmutableMultimap<String, String> params, PrintWriter out) throws Exception {
        Set<RoleIdentifier> existingRoles = StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(_roleManager.getAll(), 0), false)
                .map(Role::getRoleIdentifier)
                .collect(Collectors.toSet());

        for (Map.Entry<String, Set<Permission>> entry : _permissionManager.getAll()) {
            String key = entry.getKey();
            if (key.startsWith("role:")) {
                RoleIdentifier id = RoleIdentifier.fromString(key.substring(5));
                if (!existingRoles.contains(id)) {
                    // Permission exists for a role which does not exist.  Create the role now with reasonable defaults.
                    Set<String> initialPermissions = entry.getValue().stream().map(Object::toString).collect(Collectors.toSet());
                    _roleManager.createRole(id, new RoleModification()
                            .withName(id.getId())
                            .withPermissionUpdate(new PermissionUpdateRequest()
                                    .permit(initialPermissions)));
                    out.println("Created missing role: " + id);
                }
            }
        }
    }
}
