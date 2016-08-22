package com.bazaarvoice.emodb.auth.identity;

import com.bazaarvoice.emodb.auth.shiro.InvalidatableCacheManager;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * {@link com.bazaarvoice.emodb.auth.identity.AuthIdentityManager} which can manage a cache e.g. invalidate when an identity is changed
 */
public class CacheManagingAuthIdentityManager<T extends AuthIdentity> implements AuthIdentityManager<T> {

    private final AuthIdentityManager<T> _manager;
    private final InvalidatableCacheManager _cacheManager;

    public CacheManagingAuthIdentityManager(AuthIdentityManager<T> manager, InvalidatableCacheManager cacheManager) {
        _manager = checkNotNull(manager);
        _cacheManager = checkNotNull(cacheManager);
    }

    @Override
    public T getIdentity(String id) {
        checkNotNull(id, "id");
        return _manager.getIdentity(id);
    }

    @Override
    public void updateIdentity(T identity) {
        checkNotNull(identity);
        _manager.updateIdentity(identity);
        _cacheManager.invalidateAll();
    }

    @Override
    public void deleteIdentity(String id) {
        checkNotNull(id);
        _manager.deleteIdentity(id);
        _cacheManager.invalidateAll();
    }
}
