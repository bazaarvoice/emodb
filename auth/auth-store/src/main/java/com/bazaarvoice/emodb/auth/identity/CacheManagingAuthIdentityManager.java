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
    public T getIdentityByAuthenticationId(String authenticationId) {
        return _manager.getIdentityByAuthenticationId(authenticationId);
    }

    @Override
    public String createIdentity(String authenticationId, AuthIdentityModification<T> modification)
            throws IdentityExistsException {
        checkNotNull(authenticationId, "authenticationId");
        checkNotNull(modification, "modification");
        String id = _manager.createIdentity(authenticationId, modification);
        _cacheManager.invalidateAll();
        return id;
    }

    @Override
    public void updateIdentity(String id, AuthIdentityModification<T> modification)
            throws IdentityNotFoundException {
        checkNotNull(id, "id");
        checkNotNull(modification, "modification");
        _manager.updateIdentity(id, modification);
        _cacheManager.invalidateAll();
    }

    @Override
    public void migrateIdentity(String id, String newAuthenticationId)
            throws IdentityNotFoundException, IdentityExistsException {
        checkNotNull(id);
        checkNotNull(newAuthenticationId);
        _manager.migrateIdentity(id, newAuthenticationId);
        _cacheManager.invalidateAll();
    }

    @Override
    public void deleteIdentity(String id) {
        checkNotNull(id);
        _manager.deleteIdentity(id);
        _cacheManager.invalidateAll();
    }
}
