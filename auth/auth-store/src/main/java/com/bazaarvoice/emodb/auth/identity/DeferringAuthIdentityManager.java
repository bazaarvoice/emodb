package com.bazaarvoice.emodb.auth.identity;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * {@link AuthIdentityManager} which can explicitly provide some identities and then defer any others
 * to another instance.
 */
public class DeferringAuthIdentityManager<T extends AuthIdentity> implements AuthIdentityManager<T> {

    private final AuthIdentityManager<T> _manager;
    private final Map<String, T> _identityMap;
    private final Map<String, T> _internalIdMap;

    public DeferringAuthIdentityManager(AuthIdentityManager<T> manager, @Nullable List<T> identities) {
        _manager = checkNotNull(manager);
        if (identities == null) {
            _identityMap = ImmutableMap.of();
            _internalIdMap = ImmutableMap.of();
        } else {
            ImmutableMap.Builder<String, T> identityMapBuilder = ImmutableMap.builder();
            ImmutableMap.Builder<String, T> internalIdMapBuilder = ImmutableMap.builder();

            for (T identity : identities) {
                identityMapBuilder.put(identity.getId(), identity);
                internalIdMapBuilder.put(identity.getInternalId(), identity);
            }

            _identityMap = identityMapBuilder.build();
            _internalIdMap = internalIdMapBuilder.build();
        }
    }

    @Override
    public T getIdentity(String id) {
        checkNotNull(id, "id");

        T identity = _identityMap.get(id);
        if (identity == null) {
            identity = _manager.getIdentity(id);
        }
        return identity;
    }

    @Override
    public void updateIdentity(T identity) {
        checkNotNull(identity);
        String id = checkNotNull(identity.getId());
        String internalId = checkNotNull(identity.getInternalId());
        checkArgument(!_identityMap.containsKey(id), "Cannot update static identity: %s", id);
        checkArgument(!_internalIdMap.containsKey(internalId), "Cannot use internal ID from static identity: %s", internalId);
        _manager.updateIdentity(identity);
    }

    @Override
    public void deleteIdentity(String id) {
        checkNotNull(id);
        checkArgument(!_identityMap.containsKey(id), "Cannot delete static identity: %s", id);
        _manager.deleteIdentity(id);
    }

    @Override
    public Set<String> getRolesByInternalId(String internalId) {
        checkNotNull(internalId, "internalId");

        T identity = _internalIdMap.get(internalId);
        if (identity != null) {
            return identity.getRoles();
        }
        return _manager.getRolesByInternalId(internalId);
    }
}
