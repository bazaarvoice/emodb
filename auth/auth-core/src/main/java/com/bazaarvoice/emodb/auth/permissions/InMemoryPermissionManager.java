package com.bazaarvoice.emodb.auth.permissions;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;
import org.apache.shiro.authz.Permission;
import org.apache.shiro.authz.permission.PermissionResolver;

import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Simple in-memory implementation of a {@link PermissionManager}.
 */
public class InMemoryPermissionManager implements PermissionManager {
    private final PermissionResolver _permissionResolver;
    private final SetMultimap<String, Permission> _permissionMap = HashMultimap.create();

    public InMemoryPermissionManager(PermissionResolver permissionResolver) {
        _permissionResolver = permissionResolver;
    }

    @Override
    public Set<Permission> getPermissions(String id) {
        checkNotNull(id, "id");
        return _permissionMap.get(id);
    }

    @Override
    public void updatePermissions(String id, PermissionUpdateRequest updates) {
        if (updates.isRevokeRest()) {
            revokePermissions(id);
        }
        for (String permissionString : updates.getPermitted()) {
            Permission permission = _permissionResolver.resolvePermission(permissionString);
            _permissionMap.put(id, permission);
        }
        if (!updates.isRevokeRest()) {
            for (String permissionString : updates.getRevoked()) {
                Permission permission = _permissionResolver.resolvePermission(permissionString);
                _permissionMap.remove(id, permission);
            }
        }
    }

    @Override
    public void revokePermissions(String id) {
        checkNotNull(id, "id");
        _permissionMap.removeAll(id);
    }

    @Override
    public Iterable<Map.Entry<String, Set<Permission>>> getAll() {
        return Multimaps.asMap(_permissionMap).entrySet();
    }

    @Override
    public PermissionResolver getPermissionResolver() {
        return _permissionResolver;
    }

    public void reset() {
        _permissionMap.clear();
    }
}
