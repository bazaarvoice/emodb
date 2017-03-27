package com.bazaarvoice.emodb.auth.identity;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Simple in-memory implementation of an {@link AuthIdentityManager}.
 */
public class InMemoryAuthIdentityManager<T extends AuthIdentity> implements AuthIdentityManager<T> {

    private final Map<String, String> _authenticationToIdMap = Maps.newConcurrentMap();
    private final Map<String, T> _identityMap = Maps.newConcurrentMap();
    private final Supplier<String> _uniqueIdSupplier;

    /**
     * Default constructor assigns incremental integer IDs.  For more control over ID generation
     * use {@link #InMemoryAuthIdentityManager(Supplier)}.
     */
    public InMemoryAuthIdentityManager() {
        this(new Supplier<String>() {
            private final AtomicInteger nextId = new AtomicInteger(0);
            
            @Override
            public String get() {
                return String.valueOf(nextId.getAndIncrement());
            }
        });
    }

    public InMemoryAuthIdentityManager(Supplier<String> uniqueIdSupplier) {
        _uniqueIdSupplier = uniqueIdSupplier;
    }

    @Override
    synchronized public String createIdentity(String authenticationId, AuthIdentityModification<T> modification)
            throws IdentityExistsException {
        if (_authenticationToIdMap.containsKey(authenticationId)) {
            throw new IdentityExistsException();
        }
        String id = _uniqueIdSupplier.get();
        T identity = modification.buildNew(id);
        identity.setIssued(new Date());
        _authenticationToIdMap.put(authenticationId, id);
        _identityMap.put(id, identity);
        return id;
    }

    @Override
    public T getIdentityByAuthenticationId(String authenticationId) {
        String id = _authenticationToIdMap.get(authenticationId);
        if (id == null) {
            return null;
        }
        return _identityMap.get(id);
    }

    @Override
    public T getIdentity(String id) {
        checkNotNull(id, "id");
        return _identityMap.get(id);
    }

    @Override
    synchronized public void updateIdentity(String id, AuthIdentityModification<T> modification)
            throws IdentityNotFoundException {
        T existing = _identityMap.get(id);
        if (existing == null) {
            throw new IdentityNotFoundException();
        }
        _identityMap.put(id, modification.buildFrom(existing));
    }

    @Override
    synchronized public void migrateIdentity(String id, String newAuthenticationId) {
        if (_authenticationToIdMap.containsKey(newAuthenticationId)) {
            throw new IdentityExistsException();
        }
        if (!_identityMap.containsKey(id)) {
            throw new IdentityNotFoundException();
        }
        deleteAuthenticationReferenceToId(id);
        _authenticationToIdMap.put(newAuthenticationId, id);
    }

    @Override
    synchronized public void deleteIdentity(String id) {
        checkNotNull(id, "id");
        _identityMap.remove(id);
        deleteAuthenticationReferenceToId(id);
    }

    private void deleteAuthenticationReferenceToId(String id) {
        for (Map.Entry<String, String> entry : _authenticationToIdMap.entrySet()) {
            if (entry.getValue().equals(id)) {
                _authenticationToIdMap.remove(entry.getKey());
                return;
            }
        }
    }

    public void reset() {
        _identityMap.clear();
    }

    public List<T> getAllIdentities() {
        return ImmutableList.copyOf(_identityMap.values());
    }
}
