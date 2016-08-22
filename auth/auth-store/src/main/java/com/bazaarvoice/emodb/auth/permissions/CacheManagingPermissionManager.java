package com.bazaarvoice.emodb.auth.permissions;

import com.bazaarvoice.emodb.auth.shiro.InvalidatableCacheManager;
import org.apache.shiro.authz.Permission;
import org.apache.shiro.authz.permission.PermissionResolver;

import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * {@link com.bazaarvoice.emodb.auth.permissions.PermissionManager} which can explicitly provide some permissions and defer all others to another instance.
 */
public class CacheManagingPermissionManager implements PermissionManager {

    private final PermissionManager _manager;
    private final InvalidatableCacheManager _cacheManager;

    public CacheManagingPermissionManager(PermissionManager manager, InvalidatableCacheManager cacheManager) {
        _manager = checkNotNull(manager, "manager");
        _cacheManager = checkNotNull(cacheManager);
    }

    @Override
    public Set<Permission> getAllForRole(String role) {
        checkNotNull(role, "role");
        return _manager.getAllForRole(role);
    }

    @Override
    public void updateForRole(String role, PermissionUpdateRequest updates) {
        _manager.updateForRole(role, updates);
        _cacheManager.invalidateAll();
    }

    @Override
    public void revokeAllForRole(String role) {
        _manager.revokeAllForRole(role);
        _cacheManager.invalidateAll();
    }

    @Override
    public Iterable<Map.Entry<String, Set<Permission>>> getAll() {
        return _manager.getAll();
    }

    @Override
    public PermissionResolver getPermissionResolver() {
        return _manager.getPermissionResolver();
    }
}
