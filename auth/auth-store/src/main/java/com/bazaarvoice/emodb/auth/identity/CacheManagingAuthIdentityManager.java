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
    public void migrateIdentity(String existingId, String newId) {
        checkNotNull(existingId);
        checkNotNull(newId);
        _manager.migrateIdentity(existingId, newId);
        _cacheManager.invalidateAll();
    }

    @Override
    public void deleteIdentityUnsafe(String id) {
        checkNotNull(id, "id");
        _manager.deleteIdentityUnsafe(id);
        _cacheManager.invalidateAll();
    }

    @Override
    public InternalIdentity getInternalIdentity(String internalId) {
        checkNotNull(internalId, "internalId");
        return _manager.getInternalIdentity(internalId);
    }
}
