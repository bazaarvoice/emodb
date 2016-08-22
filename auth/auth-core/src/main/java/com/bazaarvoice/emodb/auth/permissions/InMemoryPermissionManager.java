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
    private final SetMultimap<String, Permission> _principalPermissionMap = HashMultimap.create();
    private final SetMultimap<String, Permission> _rolePermissionMap = HashMultimap.create();

    public InMemoryPermissionManager(PermissionResolver permissionResolver) {
        _permissionResolver = permissionResolver;
    }

    @Override
    public Set<Permission> getAllForRole(String role) {
        return getAll(_rolePermissionMap, role);
    }

    @Override
    public void updateForRole(String role, PermissionUpdateRequest updates) {
        update(_rolePermissionMap, role, updates);
    }

    @Override
    public void revokeAllForRole(String role) {
        revokeAll(_rolePermissionMap, role);
    }

    @Override
    public Iterable<Map.Entry<String, Set<Permission>>> getAll() {
        return Multimaps.asMap(_rolePermissionMap).entrySet();
    }

    private Set<Permission> getAll(SetMultimap<String, Permission> permissionMap, String key) {
        checkNotNull(key, "key");
        return permissionMap.get(key);
    }

    private void update(SetMultimap<String, Permission> permissionMap, String key, PermissionUpdateRequest request) {
        for (String permissionString : request.getPermitted()) {
            Permission permission = _permissionResolver.resolvePermission(permissionString);
            permissionMap.put(key, permission);
        }
        for (String permissionString : request.getRevoked()) {
            Permission permission = _permissionResolver.resolvePermission(permissionString);
            permissionMap.remove(key, permission);
        }
    }

    private void revokeAll(SetMultimap<String, Permission> permissionMap, String key) {
        checkNotNull(key, "key");
        permissionMap.removeAll(key);
    }

    @Override
    public PermissionResolver getPermissionResolver() {
        return _permissionResolver;
    }

    public void reset() {
        _principalPermissionMap.clear();
        _rolePermissionMap.clear();
    }
}
