package com.bazaarvoice.emodb.auth.identity;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Simple in-memory implementation of an {@link AuthIdentityManager}.
 */
public class InMemoryAuthIdentityManager<T extends AuthIdentity> implements AuthIdentityManager<T> {

    private final Map<String, T> _identityMap = Maps.newConcurrentMap();

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
    public void deleteIdentity(String id) {
        checkNotNull(id, "id");
        _identityMap.remove(id);
    }

    @Override
    public Set<String> getRolesByInternalId(String internalId) {
        checkNotNull(internalId, "internalId");
        for (T identity : _identityMap.values()) {
            if (internalId.equals(identity.getInternalId())) {
                return identity.getRoles();
            }
        }
        return null;
    }

    public void reset() {
        _identityMap.clear();
    }

    public List<T> getAllIdentities() {
        return ImmutableList.copyOf(_identityMap.values());
    }
}
