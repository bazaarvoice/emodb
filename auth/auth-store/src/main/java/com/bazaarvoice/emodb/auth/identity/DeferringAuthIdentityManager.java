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
    private final Map<String, T> _identitiesById;

    public DeferringAuthIdentityManager(AuthIdentityManager<T> manager, @Nullable Map<String, T> identities) {
        _manager = checkNotNull(manager);
        if (identities == null) {
            _identitiesByAuthenticationId = ImmutableMap.of();
            _identitiesById = ImmutableMap.of();
        } else {
            ImmutableMap.Builder<String, T> authIdMapBuilder = ImmutableMap.builder();
            ImmutableMap.Builder<String, T> idMapBuilder = ImmutableMap.builder();

            for (Map.Entry<String, T> entry : identities.entrySet()) {
                String authenticationId = entry.getKey();
                T identity = entry.getValue();

                authIdMapBuilder.put(authenticationId, identity);
                idMapBuilder.put(identity.getId(), identity);
            }

            _identitiesByAuthenticationId = authIdMapBuilder.build();
            _identitiesById = idMapBuilder.build();
        }
    }

    @Override
    public T getIdentity(String id) {
        checkNotNull(id, "id");

        T identity = _identitiesById.get(id);
        if (identity == null) {
            identity = _manager.getIdentity(id);
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
        String id = _manager.createIdentity(authenticationId, modification);
        assert !_identitiesById.containsKey(id) : "Delegate should never return an static ID";
        return id;
    }

    @Override
    public void updateIdentity(String id, AuthIdentityModification<T> modification)
            throws IdentityNotFoundException {
        checkNotNull(id);
        checkNotNull(modification);
        checkArgument(!_identitiesById.containsKey(id), "Cannot use ID from static identity: %s", id);
        _manager.updateIdentity(id, modification);
    }

    @Override
    public void migrateIdentity(String id, String newAuthenticationId)
            throws IdentityNotFoundException, IdentityExistsException {
        checkNotNull(id);
        checkNotNull(newAuthenticationId);
        checkArgument(!_identitiesByAuthenticationId.containsKey(newAuthenticationId), "Cannot update static identity: %s", newAuthenticationId);
        checkArgument(!_identitiesById.containsKey(id), "Cannot use ID from static identity: %s", id);
        _manager.migrateIdentity(id, newAuthenticationId);
    }

    @Override
    public void deleteIdentity(String id) {
        checkNotNull(id);
        checkArgument(!_identitiesByAuthenticationId.containsKey(id), "Cannot delete static identity: %s", id);
        _manager.deleteIdentity(id);
    }
}
