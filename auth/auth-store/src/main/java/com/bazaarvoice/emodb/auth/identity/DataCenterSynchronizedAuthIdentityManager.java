package com.bazaarvoice.emodb.auth.identity;

import com.bazaarvoice.emodb.auth.DataCenterSynchronizer;
import org.apache.curator.framework.CuratorFramework;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Identity manager implementation which synchronizes mutative identity updates in the current data center to prevent
 * possible inconsistencies caused by concurrent updates.  Note that other protections are required to prevent global
 * concurrent updates across all data centers.
 */
public class DataCenterSynchronizedAuthIdentityManager<T extends AuthIdentity> implements AuthIdentityManager<T> {
    private final static String LOCK_PATH = "/lock/identities";

    private final AuthIdentityManager<T> _delegate;
    private final DataCenterSynchronizer _synchronizer;

    public DataCenterSynchronizedAuthIdentityManager(AuthIdentityManager<T> delegate, CuratorFramework curator) {
        _delegate = checkNotNull(delegate, "delegate");
        _synchronizer = new DataCenterSynchronizer(curator, LOCK_PATH);
    }

    @Override
    public T getIdentity(String internalId) {
        return _delegate.getIdentity(internalId);
    }

    @Override
    public T getIdentityByAuthenticationId(String authenticationId) {
        return _delegate.getIdentityByAuthenticationId(authenticationId);
    }

    @Override
    public String createIdentity(String authenticationId, AuthIdentityModification<T> modification)
            throws IdentityExistsException {
        return _synchronizer.inMutex(() -> _delegate.createIdentity(authenticationId, modification));
    }

    @Override
    public void updateIdentity(String internalId, AuthIdentityModification<T> modification)
            throws IdentityNotFoundException {
        _synchronizer.inMutex(() -> _delegate.updateIdentity(internalId, modification));
    }

    @Override
    public void migrateIdentity(String internalId, String newAuthenticationId)
            throws IdentityNotFoundException, IdentityExistsException {
        _synchronizer.inMutex(() -> _delegate.migrateIdentity(internalId, newAuthenticationId));
    }

    @Override
    public void deleteIdentity(String internalId) {
        _synchronizer.inMutex(() -> _delegate.deleteIdentity(internalId));
    }
}
