package com.bazaarvoice.emodb.auth.identity;

import com.bazaarvoice.emodb.common.json.JsonHelper;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.Maps;

import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Simple in-memory implementation of an {@link AuthIdentityManager}.
 */
public class InMemoryAuthIdentityManager<T extends AuthIdentity> implements AuthIdentityManager<T> {

    private final Class<T> _identityClass;
    private final Map<String, T> _identityMap = Maps.newConcurrentMap();

    public InMemoryAuthIdentityManager(Class<T> identityClass) {
        _identityClass = checkNotNull(identityClass, "identityClass");
    }

    @Override
    public T getIdentity(String id) {
        checkNotNull(id, "id");
        return _identityMap.get(id);
    }

    @Override
    public void updateIdentity(T identity) {
        checkNotNull(identity, "identity");
        checkNotNull(identity.getId(), "id");
        _identityMap.put(identity.getId(), identity);
    }

    @Override
    public void migrateIdentity(String existingId, String newId) {
        T identity = getIdentity(existingId);
        if (identity != null) {
            // Use JSON serialization to create a copy.
            Map<String, Object> newIdentityMap = JsonHelper.convert(identity, new TypeReference<Map<String, Object>>() {});
            // Change the ID to the new ID.
            newIdentityMap.put("id", newId);
            T newIdentity = JsonHelper.convert(newIdentityMap, _identityClass);
            _identityMap.put(newId, newIdentity);

            // Change the state of the existing identity to migrated
            identity.setState(IdentityState.MIGRATED);
        }
    }

    @Override
    public void deleteIdentityUnsafe(String id) {
        checkNotNull(id, "id");
        _identityMap.remove(id);
    }

    @Override
    public InternalIdentity getInternalIdentity(String internalId) {
        checkNotNull(internalId, "internalId");
        for (T identity : _identityMap.values()) {
            if (internalId.equals(identity.getInternalId())) {
                return identity;
            }
        }
        return null;
    }

    public void reset() {
        _identityMap.clear();
    }
}
