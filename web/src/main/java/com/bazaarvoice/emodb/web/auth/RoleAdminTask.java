package com.bazaarvoice.emodb.web.auth;

import com.bazaarvoice.emodb.auth.apikey.ApiKeyAuthenticationToken;
import com.bazaarvoice.emodb.auth.apikey.ApiKeyRequest;
import com.bazaarvoice.emodb.auth.permissions.PermissionUpdateRequest;
import com.bazaarvoice.emodb.auth.role.RoleIdentifier;
import com.bazaarvoice.emodb.auth.role.RoleManager;
import com.bazaarvoice.emodb.auth.role.RoleModification;
import com.bazaarvoice.emodb.common.dropwizard.task.TaskRegistry;
import com.bazaarvoice.emodb.web.auth.resource.VerifiableResource;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
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
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.String.format;

/**
 * Task for managing roles and the permissions associated with them.
 *
 * The following examples demonstrate the various ways to use this task.  In order to actually run this task you must
 * provide an API key which has permission to perform the action requested, such as
 * {@link Permissions#updateRole(VerifiableResource, VerifiableResource)}.  For the purposes of this example the API key
 * "admin-key" is a valid key with all required permissions.
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

            String activityStr = getValueFromParams("action", parameters);
            Action action = Action.valueOf(activityStr.toUpperCase().replace('-', '_'));
            RoleIdentifier roleId = getRole(parameters);

            switch (action) {
                case VIEW:
                    viewRole(subject, roleId, output);
                    break;
                case UPDATE:
                    createOrUpdateRole(subject, roleId, parameters, output);
                    break;
                case DELETE:
                    deleteRole(subject, roleId, output);
                    break;
                case CHECK:
                    checkPermission(subject, roleId, parameters, output);
                    break;
                case FIND_DEPRECATED_PERMISSIONS:
                    findDeprecatedPermissions(subject, output);
            }
        } catch (AuthenticationException | AuthorizationException e) {
            _log.warn("Unauthorized attempt to access role management task");
            output.println("Not authorized");
        } catch (Exception e) {
            if (Throwables.getRootCause(e) instanceof TimeoutException) {
                output.println("Timed out, try again later");
            } else {
                throw Throwables.propagate(e);
            }
        } finally {
            subject.logout();
        }
    }

    private RoleIdentifier getRole(ImmutableMultimap<String, String> parameters) {
        // For legacy purposes the ID field can be named either the current "id" or the legacy "role".  Whichever is
        // used the caller must provide exactly one value.
        List<String> ids = ImmutableList.<String>builder()
                .addAll(parameters.get("id"))
                .addAll(parameters.get("role"))
                .build();

        checkArgument(ids.size() == 1, "A single 'id' parameter value is required");
        String group = parameters.get("group").stream().findFirst().orElse(null);
        return new RoleIdentifier(group, ids.get(0));
    }

    private void viewRole(Subject subject, RoleIdentifier id, PrintWriter output) {
        subject.checkPermission(Permissions.readRole(id));
        Set<String> permissions = _roleManager.getPermissionsForRole(id);
        output.println(String.format("%s has %d permissions", id, permissions.size()));
        for (String permission : permissions) {
            output.println("- " + permission);
        }
    }

    private void createOrUpdateRole(Subject subject, RoleIdentifier id, ImmutableMultimap<String, String> parameters,
                                    PrintWriter output) {
        checkArgument(!DefaultRoles.isDefaultRole(id), "Cannot update default role: %s", id);

        String name = parameters.get("name").stream().findFirst().orElse(null);
        String description = parameters.get("description").stream().findFirst().orElse(null);
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

        RoleModification modification = new RoleModification();

        if (name != null) {
            modification = modification.withName(Strings.emptyToNull(name));
        }
        if (description != null) {
            modification = modification.withDescription(Strings.emptyToNull(description));
        }
        if (!permitSet.isEmpty() || !revokeSet.isEmpty()) {
            modification = modification.withPermissionUpdate(new PermissionUpdateRequest()
                    .permit(permitSet)
                    .revoke(revokeSet));
        }

        // Although the backend distinguishes between creating and updating roles this task does not.  The long-term
        // goal is to migrate away from using tasks to a proper REST API, at which time there would be distinct
        // endpoints for creating versus updating a role.

        if (_roleManager.getRole(id) == null) {  // Creating role
            subject.checkPermission(Permissions.createRole(id));
            _roleManager.createRole(id, modification);
        } else {
            subject.checkPermission(Permissions.updateRole(id));
            _roleManager.updateRole(id, modification);
        }
        output.println("Role updated.");
        viewRole(subject, id, output);
    }

    private void deleteRole(Subject subject, RoleIdentifier id, PrintWriter output) {
        subject.checkPermission(Permissions.deleteRole(id));
        checkArgument(!DefaultRoles.isDefaultRole(id), "Cannot delete default role: %s", id);
        _roleManager.deleteRole(id);
        output.println("Role deleted");
    }

    private void checkPermission(Subject subject, RoleIdentifier id, ImmutableMultimap<String, String> parameters, PrintWriter output) {
        subject.checkPermission(Permissions.readRole(id));

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

    private void findDeprecatedPermissions(Subject subject, PrintWriter output) {
        // Since finding deprecated permissions could potentially return information from any role the caller must
        // have read permission for all roles.
        subject.checkPermission(Permissions.readRole(Permissions.ALL, Permissions.ALL));

        final AtomicBoolean anyDeprecated = new AtomicBoolean(false);
        
        _roleManager.getAll().forEachRemaining(role -> {
            List<Permission> deprecatedPermissions = _roleManager.getPermissionsForRole(role.getRoleIdentifier())
                    .stream()
                    .map(p -> (EmoPermission) _permissionResolver.resolvePermission(p))
                    .filter(p -> !p.isAssignable())
                    .collect(Collectors.toList());

            if (!deprecatedPermissions.isEmpty()) {
                if (anyDeprecated.compareAndSet(false, true)) {
                    output.println("The following roles have deprecated permissions:\n");
                }
                output.println(role.getRoleIdentifier());
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
