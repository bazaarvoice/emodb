package com.bazaarvoice.emodb.web.auth;

import com.bazaarvoice.emodb.auth.apikey.ApiKeyAuthenticationToken;
import com.bazaarvoice.emodb.auth.apikey.ApiKeyRequest;
import com.bazaarvoice.emodb.auth.permissions.PermissionManager;
import com.bazaarvoice.emodb.auth.permissions.PermissionUpdateRequest;
import com.bazaarvoice.emodb.common.dropwizard.task.TaskRegistry;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import io.dropwizard.servlets.tasks.Task;
import org.apache.shiro.authc.AuthenticationException;
import org.apache.shiro.authz.AuthorizationException;
import org.apache.shiro.authz.Permission;
import org.apache.shiro.mgt.SecurityManager;
import org.apache.shiro.subject.Subject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.PrintWriter;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.String.format;

/**
 * Task for managing roles and the permissions associated with them.
 *
 * The following examples demonstrate the various ways to use this task.  In order to actually run this task you must
 * provide an API key which has permission to manage roles (see {@link Permissions#manageRoles()}).  For
 * the purposes of this example the API key "admin-key" is a valid key with this permission.
 *
 * Create or update role
 * =====================
 *
 * EmoDB does not distinguish between creating a new role and updating an existing role.  An API key can be associated with
 * any role name, even one that has not been created.  However, until an administrator explicitly assigns permissions
 * to the role it will only be able to perform actions with implicit permissions, such as reading from a SoR table.
 *
 * The following example adds databus poll permissions for subscriptions starting with "foo_" and removes poll permissions
 * for subscriptions starting with "bar_" for role "sample-role":
 *
 * <code>
 *     $ curl -XPOST "localhost:8081/tasks/role?action=update&APIKey=admin-key&role=sample-role\
 *     &permit=databus|poll|foo_*\
 *     &revoke=databus|poll|bar_*"
 * </code>
 *
 * View role
 * =========
 *
 * The following example displays all permissions granted to "sample-role":
 *
 * <code>
 *     $ $ curl -XPOST "localhost:8081/tasks/role?action=view&APIKey=admin-key&role=sample-role"
 * </code>
 *
 * Check role
 * ==========
 *
 * The following example checks whether "sample-role" has permission to poll databus subscription "subscription1":
 *
 * <code>
 *     $ curl -XPOST "localhost:8081/tasks/role?action=check&APIKey=admin-key&role=sample-role&permission=databus|poll|subscription1"
 * </code>
 *
 * Delete role
 * ===========
 *
 * The following example deletes (or, more accurately, removes all permissions from) "sample-role":
 *
 * <code>
 *     $ curl -XPOST "localhost:8081/tasks/role?action=delete&APIKey=admin-key&role=sample-role"
 * </code>
 *
 * Find deprecated permissions
 * ===========================
 *
 * The following example finds all conditions in all roles that are deprecated.  This should be used to replace
 * all deprecated permissions with equivalent current versions.  Once this returns no values support for deprecated
 * permissions can be safely removed from code.
 *
 * <code>
 *     $ curl -XPOST "localhost:8081/tasks/role?action=find-deprecated-permissions&APIKey=admin-key"
 * </code>
 */
public class RoleAdminTask extends Task {
    private final Logger _log = LoggerFactory.getLogger(RoleAdminTask.class);

    private enum Action {
        VIEW,
        UPDATE,
        DELETE,
        CHECK,
        FIND_DEPRECATED_PERMISSIONS
    }

    private final SecurityManager _securityManager;
    private final PermissionManager _permissionManager;

    @Inject
    public RoleAdminTask(SecurityManager securityManager, PermissionManager permissionManager, TaskRegistry taskRegistry) {
        super("role");
        _securityManager = checkNotNull(securityManager, "securityManager");
        _permissionManager = checkNotNull(permissionManager, "permissionManager");

        taskRegistry.addTask(this);
    }

    @Override
    public void execute(ImmutableMultimap<String, String> parameters, PrintWriter output)
            throws Exception {
        Subject subject = new Subject.Builder(_securityManager).buildSubject();
        try {
            // Make sure the API key is valid
            String apiKey = getValueFromParams(ApiKeyRequest.AUTHENTICATION_PARAM, parameters);
            subject.login(new ApiKeyAuthenticationToken(apiKey));

            // Make sure the API key is permitted to manage roles
            subject.checkPermission(Permissions.manageRoles());

            String activityStr = getValueFromParams("action", parameters);
            Action action = Action.valueOf(activityStr.toUpperCase().replace('-', '_'));

            switch (action) {
                case VIEW:
                    viewRole(getRole(parameters), output);
                    break;
                case UPDATE:
                    updateRole(getRole(parameters), parameters, output);
                    break;
                case DELETE:
                    deleteRole(getRole(parameters), output);
                    break;
                case CHECK:
                    checkPermission(getRole(parameters), parameters, output);
                    break;
                case FIND_DEPRECATED_PERMISSIONS:
                    findDeprecatedPermissions(output);
            }
        } catch (AuthenticationException | AuthorizationException e) {
            _log.warn("Unauthorized attempt to access role management task");
            output.println("Not authorized");
        } finally {
            subject.logout();
        }
    }

    private String getRole(ImmutableMultimap<String, String> parameters) {
        return getValueFromParams("role", parameters);
    }

    private void viewRole(String role, PrintWriter output) {
        List<String> permissions = FluentIterable.from(_permissionManager.getAllForRole(role))
                .transform(Functions.toStringFunction())
                .toSortedList(Ordering.natural());

        output.println(String.format("%s has %d permissions", role, permissions.size()));
        for (String permission : permissions) {
            output.println("- " + permission);
        }
    }

    private void updateRole(String role, ImmutableMultimap<String, String> parameters, PrintWriter output) {
        checkArgument(!DefaultRoles.isDefaultRole(role), "Cannot update default role: %s", role);

        Set<String> permitSet = ImmutableSet.copyOf(parameters.get("permit"));
        Set<String> revokeSet = ImmutableSet.copyOf(parameters.get("revoke"));

        checkArgument(Sets.intersection(permitSet, revokeSet).isEmpty(),
                "Cannot permit and revoke the same permission in a single request");

        // Verify that all permissions being permitted can be assigned to a role.

        boolean allAssignable = true;
        for (String permit : permitSet) {
            // All permissions returned are EmoPermission instances.
            EmoPermission resolved = (EmoPermission) _permissionManager.getPermissionResolver().resolvePermission(permit);
            if (!resolved.isAssignable()) {
                if (allAssignable) {
                    output.println("The following permission(s) cannot be assigned to a role:");
                    allAssignable = false;
                }
                output.println("- " + permit);
            }
        }

        if (!allAssignable) {
            output.println("Please rewrite the above permission(s) using constants, wildcard strings, or \"if()\" expressions");
            return;
        }

        _permissionManager.updateForRole(role,
                new PermissionUpdateRequest()
                        .permit(permitSet)
                        .revoke(revokeSet));

        output.println("Role updated.");
        viewRole(role, output);
    }

    private void deleteRole(String role, PrintWriter output) {
        // A role technically cannot be deleted.  What this method does is revoke all permissions associated with
        // the role.

        checkArgument(!DefaultRoles.isDefaultRole(role), "Cannot delete default role: %s", role);

        Set<Permission> permissions = _permissionManager.getAllForRole(role);

        // Bound the number of times we'll try to revoke all permissions.
        for (int attempt = 0; !permissions.isEmpty() && attempt < 10; attempt++) {
            _permissionManager.updateForRole(role,
                    new PermissionUpdateRequest()
                            .revoke(FluentIterable.from(permissions)
                                    .transform(Functions.toStringFunction())));

            permissions = _permissionManager.getAllForRole(role);
        }

        if (permissions.isEmpty()) {
            output.println("Role deleted");
        } else {
            output.println(String.format("WARNING:  Role still had %d permissions after 10 delete attempts", permissions.size()));
        }
    }

    private void checkPermission(String role, ImmutableMultimap<String, String> parameters, PrintWriter output) {
        String permissionStr = getValueFromParams("permission", parameters);
        final Permission permission = _permissionManager.getPermissionResolver().resolvePermission(permissionStr);

        List<String> matchingPermissions = FluentIterable.from(_permissionManager.getAllForRole(role))
                .filter(
                        new Predicate<Permission>() {
                                @Override
                                public boolean apply(Permission grantedPermission) {
                                    return grantedPermission.implies(permission);
                                }
                        })
                .transform(Functions.toStringFunction())
                .toSortedList(Ordering.natural());

        if (!matchingPermissions.isEmpty()) {
            output.println(String.format("%s is permitted %s by the following:", role, permissionStr));
            for (String matchingPermission : matchingPermissions) {
                output.println("- " + matchingPermission);
            }
        } else {
            output.println(String.format("%s is not permitted %s", role, permissionStr));
        }
    }

    private void findDeprecatedPermissions(PrintWriter output) {
        Iterator<Map.Entry<String, Set<Permission>>> deprecatedPermissionsByRole =
                FluentIterable.from(_permissionManager.getAll())
                        .transform(new Function<Map.Entry<String, Set<Permission>>, Map.Entry<String, Set<Permission>>>() {
                            @Nullable
                            @Override
                            public Map.Entry<String, Set<Permission>> apply(Map.Entry<String, Set<Permission>> rolePermissions) {
                                Set<Permission> deprecatedPermissions = Sets.newLinkedHashSet();
                                for (Permission permission : rolePermissions.getValue()) {
                                    if (!((EmoPermission) permission).isAssignable()) {
                                        deprecatedPermissions.add(permission);
                                    }
                                }

                                if (deprecatedPermissions.isEmpty()) {
                                    return null;
                                }

                                return Maps.immutableEntry(rolePermissions.getKey(), deprecatedPermissions);
                            }
                        })
                        .filter(Predicates.notNull())
                        .iterator();

        if (!deprecatedPermissionsByRole.hasNext()) {
            output.println("There are no roles with deprecated permissions.");
        } else {
            output.println("The following roles have deprecated permissions:\n");
            while (deprecatedPermissionsByRole.hasNext()) {
                Map.Entry<String, Set<Permission>> entry = deprecatedPermissionsByRole.next();
                output.println(entry.getKey());
                for (Permission permission : entry.getValue()) {
                    output.println("- " + permission);
                }
            }
        }
    }

    private String getValueFromParams(String value, ImmutableMultimap<String, String> parameters) {
        try {
            return Iterables.getOnlyElement(parameters.get(value));
        } catch (Exception e) {
            throw new IllegalArgumentException(format("A single '%s' parameter value is required", value));
        }
    }
}
