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
    public T getIdentity(String internalId) {
        checkNotNull(internalId, "internalId");
        return _manager.getIdentity(internalId);
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
        String internalId = _manager.createIdentity(authenticationId, modification);
        _cacheManager.invalidateAll();
        return internalId;
    }

    @Override
    public void updateIdentity(String internalId, AuthIdentityModification<T> modification)
            throws IdentityNotFoundException {
        checkNotNull(internalId, "internalId");
        checkNotNull(modification, "modification");
        _manager.updateIdentity(internalId, modification);
        _cacheManager.invalidateAll();
    }

    @Override
    public void migrateIdentity(String internalId, String newAuthenticationId)
            throws IdentityNotFoundException, IdentityExistsException {
        checkNotNull(internalId);
        checkNotNull(newAuthenticationId);
        _manager.migrateIdentity(internalId, newAuthenticationId);
        _cacheManager.invalidateAll();
    }

    @Override
    public void deleteIdentity(String internalId) {
        checkNotNull(internalId);
        _manager.deleteIdentity(internalId);
        _cacheManager.invalidateAll();
    }
}
