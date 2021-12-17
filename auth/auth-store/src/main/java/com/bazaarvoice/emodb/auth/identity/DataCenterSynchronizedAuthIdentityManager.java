package com.bazaarvoice.emodb.auth.identity;

import com.bazaarvoice.emodb.auth.DataCenterSynchronizer;
import org.apache.curator.framework.CuratorFramework;

import static java.util.Objects.requireNonNull;

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
        _delegate = requireNonNull(delegate, "delegate");
        _synchronizer = new DataCenterSynchronizer(curator, LOCK_PATH);
    }

    @Override
    public T getIdentity(String id) {
        return _delegate.getIdentity(id);
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
    public void updateIdentity(String id, AuthIdentityModification<T> modification)
            throws IdentityNotFoundException {
        _synchronizer.inMutex(() -> _delegate.updateIdentity(id, modification));
    }

    @Override
    public void migrateIdentity(String id, String newAuthenticationId)
            throws IdentityNotFoundException, IdentityExistsException {
        _synchronizer.inMutex(() -> _delegate.migrateIdentity(id, newAuthenticationId));
    }

    @Override
    public void deleteIdentity(String id) {
        _synchronizer.inMutex(() -> _delegate.deleteIdentity(id));
    }
}
