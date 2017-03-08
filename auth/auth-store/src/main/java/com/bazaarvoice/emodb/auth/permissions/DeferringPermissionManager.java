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
    private final Map<String, Set<Permission>> _permissions;

    public DeferringPermissionManager(PermissionManager manager, @Nullable Map<String, Set<Permission>> permissions) {
        _manager = checkNotNull(manager, "manager");
        _permissions = permissions != null ? ImmutableMap.copyOf(permissions) : ImmutableMap.<String, Set<Permission>>of();
    }

    @Override
    public Set<Permission> getPermissions(String id) {
        checkNotNull(id, "id");
        Set<Permission> permissions = _permissions.get(id);
        if (permissions == null) {
            permissions = _manager.getPermissions(id);
        }
        return permissions;
    }

    @Override
    public void updatePermissions(String id, PermissionUpdateRequest updates) {
        checkArgument(!_permissions.containsKey(id), "Cannot update permissions for static id: %s", id);
        _manager.updatePermissions(id, updates);
    }

    @Override
    public void revokePermissions(String id) {
        checkArgument(!_permissions.containsKey(id), "Cannot revoke permissions for static id: %s", id);
        _manager.revokePermissions(id);
    }

    @Override
    public Iterable<Map.Entry<String, Set<Permission>>> getAll() {
        return Iterables.concat(
                _permissions.entrySet(),
                _manager.getAll());
    }

    @Override
    public PermissionResolver getPermissionResolver() {
        return _manager.getPermissionResolver();
    }
}
