package com.bazaarvoice.emodb.auth.identity;

import com.google.common.collect.ImmutableMap;

import javax.annotation.Nullable;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * {@link AuthIdentityManager} which can explicitly provide some identities and then defer any others
 * to another instance.
 */
public class DeferringAuthIdentityManager<T extends AuthIdentity> implements AuthIdentityManager<T> {

    private final AuthIdentityManager<T> _manager;
    private final Map<String, T> _identitiesByAuthenticationId;
    private final Map<String, T> _identitiesByInternalId;

    public DeferringAuthIdentityManager(AuthIdentityManager<T> manager, @Nullable Map<String, T> identities) {
        _manager = checkNotNull(manager);
        if (identities == null) {
            _identitiesByAuthenticationId = ImmutableMap.of();
            _identitiesByInternalId = ImmutableMap.of();
        } else {
            ImmutableMap.Builder<String, T> authIdMapBuilder = ImmutableMap.builder();
            ImmutableMap.Builder<String, T> internalIdMapBuilder = ImmutableMap.builder();

            for (Map.Entry<String, T> entry : identities.entrySet()) {
                String authenticationId = entry.getKey();
                T identity = entry.getValue();

                authIdMapBuilder.put(authenticationId, identity);
                internalIdMapBuilder.put(identity.getInternalId(), identity);
            }

            _identitiesByAuthenticationId = authIdMapBuilder.build();
            _identitiesByInternalId = internalIdMapBuilder.build();
        }
    }

    @Override
    public T getIdentity(String internalId) {
        checkNotNull(internalId, "internalId");

        T identity = _identitiesByInternalId.get(internalId);
        if (identity == null) {
            identity = _manager.getIdentity(internalId);
        }
        return identity;
    }

    @Override
    public T getIdentityByAuthenticationId(String authenticationId) {
        checkNotNull(authenticationId, "authenticationId");

        T identity = _identitiesByAuthenticationId.get(authenticationId);
        if (identity == null) {
            identity = _manager.getIdentityByAuthenticationId(authenticationId);
        }
        return identity;
    }

    @Override
    public String createIdentity(String authenticationId, AuthIdentityModification<T> modification)
            throws IdentityExistsException {
        checkNotNull(authenticationId);
        checkNotNull(modification);
        checkArgument(!_identitiesByAuthenticationId.containsKey(authenticationId), "Cannot update static identity: %s", authenticationId);
        String internalId = _manager.createIdentity(authenticationId, modification);
        assert !_identitiesByInternalId.containsKey(internalId) : "Delegate should never return an static internal ID";
        return internalId;
    }

    @Override
    public void updateIdentity(String internalId, AuthIdentityModification<T> modification)
            throws IdentityNotFoundException {
        checkNotNull(internalId);
        checkNotNull(modification);
        checkArgument(!_identitiesByInternalId.containsKey(internalId), "Cannot use internal ID from static identity: %s", internalId);
        _manager.updateIdentity(internalId, modification);
    }

    @Override
    public void migrateIdentity(String internalId, String newAuthenticationId)
            throws IdentityNotFoundException, IdentityExistsException {
        checkNotNull(internalId);
        checkNotNull(newAuthenticationId);
        checkArgument(!_identitiesByAuthenticationId.containsKey(newAuthenticationId), "Cannot update static identity: %s", newAuthenticationId);
        checkArgument(!_identitiesByInternalId.containsKey(internalId), "Cannot use internal ID from static identity: %s", internalId);
        _manager.migrateIdentity(internalId, newAuthenticationId);
    }

    @Override
    public void deleteIdentity(String id) {
        checkNotNull(id);
        checkArgument(!_identitiesByAuthenticationId.containsKey(id), "Cannot delete static identity: %s", id);
        _manager.deleteIdentity(id);
    }
}
