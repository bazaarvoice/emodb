package com.bazaarvoice.emodb.auth.permissions;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import org.apache.shiro.authz.Permission;
import org.apache.shiro.authz.permission.PermissionResolver;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * {@link PermissionManager} which can explicitly provide some permissions and defer all others to another instance.
 */
public class DeferringPermissionManager implements PermissionManager {

    private final PermissionManager _manager;
    private final Map<String, Set<Permission>> _rolePermissions;

    public DeferringPermissionManager(PermissionManager manager, @Nullable Map<String, Set<Permission>> rolePermissions) {
        _manager = checkNotNull(manager, "manager");
        _rolePermissions = rolePermissions != null ? ImmutableMap.copyOf(rolePermissions) : ImmutableMap.<String, Set<Permission>>of();
    }

    @Override
    public Set<Permission> getAllForRole(String role) {
        checkNotNull(role, "role");
        Set<Permission> permissions = _rolePermissions.get(role);
        if (permissions == null) {
            permissions = _manager.getAllForRole(role);
        }
        return permissions;
    }

    @Override
    public void updateForRole(String role, PermissionUpdateRequest updates) {
        checkArgument(!_rolePermissions.containsKey(role), "Cannot update permissions for static role: %s", role);
        _manager.updateForRole(role, updates);
    }

    @Override
    public void revokeAllForRole(String role) {
        checkArgument(!_rolePermissions.containsKey(role), "Cannot revoke all permissions for static role: %s", role);
        _manager.revokeAllForRole(role);
    }

    @Override
    public Iterable<Map.Entry<String, Set<Permission>>> getAll() {
        return Iterables.concat(
                _rolePermissions.entrySet(),
                _manager.getAll());
    }

    @Override
    public PermissionResolver getPermissionResolver() {
        return _manager.getPermissionResolver();
    }
}
