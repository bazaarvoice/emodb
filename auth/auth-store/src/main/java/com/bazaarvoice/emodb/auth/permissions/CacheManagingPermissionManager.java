package com.bazaarvoice.emodb.auth.permissions;

import com.bazaarvoice.emodb.auth.shiro.InvalidatableCacheManager;
import org.apache.shiro.authz.Permission;
import org.apache.shiro.authz.permission.PermissionResolver;

import java.util.Map;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * {@link com.bazaarvoice.emodb.auth.permissions.PermissionManager} which can explicitly provide some permissions and defer all others to another instance.
 */
public class CacheManagingPermissionManager implements PermissionManager {

    private final PermissionManager _manager;
    private final InvalidatableCacheManager _cacheManager;

    public CacheManagingPermissionManager(PermissionManager manager, InvalidatableCacheManager cacheManager) {
        _manager = requireNonNull(manager, "manager");
        _cacheManager = requireNonNull(cacheManager);
    }

    @Override
    public Set<Permission> getPermissions(String id) {
        requireNonNull(id, "id");
        return _manager.getPermissions(id);
    }

    @Override
    public void updatePermissions(String id, PermissionUpdateRequest updates) {
        // If the request doesn't contain any modifications then still forward to the delegate but don't invalidate
        // the cache.
        _manager.updatePermissions(id, updates);
        if (updates.mayModifyPermissions()) {
            _cacheManager.invalidateAll();
        }
    }

    @Override
    public void revokePermissions(String id) {
        _manager.revokePermissions(id);
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
