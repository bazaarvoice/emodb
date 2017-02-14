package com.bazaarvoice.emodb.web.auth;

import com.bazaarvoice.emodb.auth.apikey.ApiKeyAuthenticationToken;
import com.bazaarvoice.emodb.auth.apikey.ApiKeyRequest;
import com.bazaarvoice.emodb.auth.permissions.PermissionUpdateRequest;
import com.bazaarvoice.emodb.auth.role.RoleExistsException;
import com.bazaarvoice.emodb.auth.role.RoleIdentifier;
import com.bazaarvoice.emodb.auth.role.RoleManager;
import com.bazaarvoice.emodb.auth.role.RoleNotFoundException;
import com.bazaarvoice.emodb.auth.role.RoleUpdateRequest;
import com.bazaarvoice.emodb.common.dropwizard.task.TaskRegistry;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import io.dropwizard.servlets.tasks.Task;
import org.apache.shiro.authc.AuthenticationException;
import org.apache.shiro.authz.AuthorizationException;
import org.apache.shiro.authz.Permission;
import org.apache.shiro.authz.permission.PermissionResolver;
import org.apache.shiro.mgt.SecurityManager;
import org.apache.shiro.subject.Subject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

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
        CREATE,
        UPDATE,
        DELETE,
        CHECK,
        FIND_DEPRECATED_PERMISSIONS
    }

    private final SecurityManager _securityManager;
    private final RoleManager _roleManager;
    private final PermissionResolver _permissionResolver;

    @Inject
    public RoleAdminTask(SecurityManager securityManager, RoleManager roleManager, PermissionResolver permissionResolver,
                         TaskRegistry taskRegistry) {
        super("role");
        _securityManager = checkNotNull(securityManager, "securityManager");
        _roleManager = checkNotNull(roleManager, "roleManager");
        _permissionResolver = checkNotNull(permissionResolver, "permissionResolver");

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
                case CREATE:
                    createOrUpdateRole(getRole(parameters), parameters, output, true);
                    break;
                case UPDATE:
                    createOrUpdateRole(getRole(parameters), parameters, output, false);
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

    private RoleIdentifier getRole(ImmutableMultimap<String, String> parameters) {
        String name = getValueFromParams("role", parameters);
        String group = parameters.get("group").stream().findFirst().orElse(null);
        return new RoleIdentifier(group, name);
    }

    private void viewRole(RoleIdentifier id, PrintWriter output) {
        Set<String> permissions = _roleManager.getPermissionsForRole(id);
        output.println(String.format("%s has %d permissions", id, permissions.size()));
        for (String permission : permissions) {
            output.println("- " + permission);
        }
    }

    private void createOrUpdateRole(RoleIdentifier id, ImmutableMultimap<String, String> parameters, PrintWriter output,
                                    boolean isCreate) {
        checkArgument(!DefaultRoles.isDefaultRole(id), "Cannot update default role: %s", id);

        Set<String> permitSet = ImmutableSet.copyOf(parameters.get("permit"));
        Set<String> revokeSet = ImmutableSet.copyOf(parameters.get("revoke"));

        checkArgument(Sets.intersection(permitSet, revokeSet).isEmpty(),
                "Cannot permit and revoke the same permission in a single request");

        // Verify that all permissions being permitted can be assigned to a role.

        boolean allAssignable = true;
        for (String permit : permitSet) {
            // All permissions returned are EmoPermission instances.
            EmoPermission resolved = (EmoPermission) _permissionResolver.resolvePermission(permit);
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

        if (isCreate) {
            try {
                _roleManager.createRole(id, null, permitSet);
                output.println("Role created.");
                viewRole(id, output);
            } catch (RoleExistsException e) {
                output.write("Role already exists");
            }
        } else {
            try {
                _roleManager.updateRole(id, new RoleUpdateRequest()
                        .withPermissionUpdate(new PermissionUpdateRequest()
                                .permit(permitSet)
                                .revoke(revokeSet)));

                output.println("Role updated.");
                viewRole(id, output);
            } catch (RoleNotFoundException e) {
                output.write("Role not found");
            }
        }
    }

    private void deleteRole(RoleIdentifier id, PrintWriter output) {
        checkArgument(!DefaultRoles.isDefaultRole(id), "Cannot delete default role: %s", id);
        _roleManager.deleteRole(id);
        output.println("Role deleted");
    }

    private void checkPermission(RoleIdentifier id, ImmutableMultimap<String, String> parameters, PrintWriter output) {
        String permissionStr = getValueFromParams("permission", parameters);
        final Permission permission = _permissionResolver.resolvePermission(permissionStr);

        List<String> matchingPermissions = _roleManager.getPermissionsForRole(id)
                .stream()
                .filter(p -> _permissionResolver.resolvePermission(p).implies(permission))
                .sorted()
                .collect(Collectors.toList());

        if (!matchingPermissions.isEmpty()) {
            output.println(String.format("%s is permitted %s by the following:", id, permissionStr));
            for (String matchingPermission : matchingPermissions) {
                output.println("- " + matchingPermission);
            }
        } else {
            output.println(String.format("%s is not permitted %s", id, permissionStr));
        }
    }

    private void findDeprecatedPermissions(PrintWriter output) {
        final AtomicBoolean anyDeprecated = new AtomicBoolean(false);
        
        _roleManager.getAll().forEachRemaining(role -> {
            List<Permission> deprecatedPermissions = _roleManager.getPermissionsForRole(role.getId())
                    .stream()
                    .map(p -> (EmoPermission) _permissionResolver.resolvePermission(p))
                    .filter(p -> !p.isAssignable())
                    .collect(Collectors.toList());

            if (!deprecatedPermissions.isEmpty()) {
                if (anyDeprecated.compareAndSet(false, true)) {
                    output.println("The following roles have deprecated permissions:\n");
                }
                output.println(role.getId());
                deprecatedPermissions.forEach(p -> output.println("- " + p));
            }
        });

        if (!anyDeprecated.get()) {
            output.println("There are no roles with deprecated permissions.");
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
