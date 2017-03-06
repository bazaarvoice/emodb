package com.bazaarvoice.emodb.auth.identity;

import com.bazaarvoice.emodb.common.json.JsonHelper;
import com.bazaarvoice.emodb.common.uuid.TimeUUIDs;
import com.bazaarvoice.emodb.sor.api.Audit;
import com.bazaarvoice.emodb.sor.api.AuditBuilder;
import com.bazaarvoice.emodb.sor.api.DataStore;
import com.bazaarvoice.emodb.sor.api.Intrinsic;
import com.bazaarvoice.emodb.sor.api.ReadConsistency;
import com.bazaarvoice.emodb.sor.api.TableOptionsBuilder;
import com.bazaarvoice.emodb.sor.api.Update;
import com.bazaarvoice.emodb.sor.api.WriteConsistency;
import com.bazaarvoice.emodb.sor.condition.Conditions;
import com.bazaarvoice.emodb.sor.delta.Delta;
import com.bazaarvoice.emodb.sor.delta.Deltas;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.hash.HashFunction;

import javax.annotation.Nullable;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * AuthIdentity manager that uses an EmoDB table to store identities.
 *
 * This implementation uses two tables:
 *
 * <ol>
 *     <li>
 *         "identityTable" stores the identity document and is keyed by a hash of the authentication ID.
 *     </li>
 *     <li>
 *         "internalIdIndexTable" stores the hash of the authentication ID and is keyed by the internal ID.
 *     </li>
 * </ol>
 *
 * This may seem backwards to most CRUD systems, since looking up the identity by its immutable key, internal ID, requires
 * two lookups, while looking it by a mutable attribute, authentication ID, requires only one.  This was done to optimize
 * the most common use case.  API keys are created and updated infrequently.  However, they are frequently looked up by
 * authentication ID for authentication and resolving the roles associated with the identity.  Therefore the table
 * structures are optimized for fast lookup by authentication ID.
 *
 * With circular logic more deadly to a robot than saying "this statement is false" the application must have
 * permission to perform operations on the table in order to use this manager.
 */
public class TableAuthIdentityManagerDAO<T extends AuthIdentity> implements AuthIdentityManager<T> {

    private final static String INTERNAL_ID = "internalId";
    private final static String HASHED_ID = "hashedId";

    private final Class<T> _authIdentityClass;
    private final DataStore _dataStore;
    // Table keyed by a hash of the authentication ID which contains the identity
    private final String _identityTableName;
    // Table keyed by the internal ID which contains the authentication ID hash for lookup up the identity
    // in the _identityTableName table.
    private final String _internalIdIndexTableName;
    private final String _placement;
    // Supplier which returns a globally unique internal ID on every call.
    private final Supplier<String> _internalIdSupplier;
    private final HashFunction _hash;
    private volatile boolean _tablesValidated;

    public TableAuthIdentityManagerDAO(Class<T> authIdentityClass, DataStore dataStore, String identityTableName,
                                       String internalIdIndexTableName, String placement,
                                       Supplier<String> internalIdSupplier) {
        this(authIdentityClass, dataStore, identityTableName, internalIdIndexTableName, placement, internalIdSupplier,null);
    }

    public TableAuthIdentityManagerDAO(Class<T> authIdentityClass, DataStore dataStore, String identityTableName,
                                       String internalIdIndexTableName, String placement, Supplier<String> internalIdSupplier,
                                       @Nullable HashFunction hash) {
        _authIdentityClass = checkNotNull(authIdentityClass, "authIdentityClass");
        _dataStore = checkNotNull(dataStore, "client");
        _identityTableName = checkNotNull(identityTableName, "identityTableName");
        _internalIdIndexTableName = checkNotNull(internalIdIndexTableName, "internalIdIndexTableName");
        _placement = checkNotNull(placement, "placement");
        _internalIdSupplier = checkNotNull(internalIdSupplier, "internalIdSupplier");
        _hash = hash;

        checkArgument(!_identityTableName.equals(internalIdIndexTableName), "Identity and internal ID index tables must be distinct");
    }

    @Override
    public T getIdentityByAuthenticationId(String authenticationId) {
        checkNotNull(authenticationId, "authenticationId");
        validateTables();

        String hashedAuthenticationId = hash(authenticationId);
        Map<String, Object> map = _dataStore.get(_identityTableName, hashedAuthenticationId);
        return convertDataStoreEntryToIdentity(map);
    }

    private T convertDataStoreEntryToIdentity(Map<String, Object> map) {
        if (map == null || Intrinsic.isDeleted(map)) {
            return null;
        }

        // Make a copy of the map to avoid mutating the method parameter as a side-effect
        Map<String, Object> identityMap = Maps.newHashMap(map);

        // Identities have been in use since before internal IDs were introduced.  To grandfather in those keys we'll
        // use the hash of the identity's authentication ID as the internal ID.
        if (!identityMap.containsKey(INTERNAL_ID)) {
            identityMap.put(INTERNAL_ID, Intrinsic.getId(map));
        }

        // Remove all intrinsics
        identityMap.keySet().removeAll(Intrinsic.DATA_FIELDS);

        return JsonHelper.convert(identityMap, _authIdentityClass);
    }

    @Override
    public T getIdentity(String internalId) {
        ResolvedIdentity resolvedIdentity = resolveIdentityByInternalId(internalId);
        if (resolvedIdentity == null) {
            return null;
        }
        return resolvedIdentity.identity;
    }

    @Override
    public String createIdentity(String authenticationId, AuthIdentityModification<T> modification)
            throws IdentityExistsException {
        checkNotNull(authenticationId, "authenticationId");
        checkNotNull(modification, "modification");
        validateTables();

        // Check whether the authentication ID conflicts with an existing identity.  Note that we can't protect from a
        // race condition here; we rely on this method being run inside a global synchronization lock.

        if (getIdentityByAuthenticationId(authenticationId) != null) {
            throw new IdentityExistsException();
        }
        String internalId = _internalIdSupplier.get();
        String hashedAuthenticationId = hash(authenticationId);
        UUID changeId = TimeUUIDs.newUUID();
        Audit audit = new AuditBuilder().setLocalHost().setComment("create identity").build();

        T identity = modification.buildNew(internalId);
        // Ignore whatever masked ID was set; mask it now
        identity.setMaskedId(mask(authenticationId));
        identity.setIssued(new Date());

        Map<String, Object> map = JsonHelper.convert(identity, new TypeReference<Map<String, Object>>(){});

        Update identityUpdate = new Update(_identityTableName, hashedAuthenticationId, changeId, Deltas.literal(map),
                audit, WriteConsistency.GLOBAL);

        map = ImmutableMap.<String, Object>of(HASHED_ID, hashedAuthenticationId);
        Update internalIdUpdate = new Update(_internalIdIndexTableName, internalId, changeId, Deltas.literal(map),
                audit, WriteConsistency.GLOBAL);

        // Update the identity and internal ID index in a single update
        _dataStore.updateAll(ImmutableList.of(identityUpdate, internalIdUpdate));

        return internalId;
    }

    @Override
    public void updateIdentity(String internalId, AuthIdentityModification<T> modification)
            throws IdentityNotFoundException {
        checkNotNull(internalId, "internalId");
        checkNotNull(modification, "modification");
        validateTables();

        // Load the existing identity, both to verify it exists and to use for performing partial modifications.
        // Note that we can't protect from a race condition here; we rely on this method being run inside a global
        // synchronization lock.

        ResolvedIdentity resolvedIdentity = resolveIdentityByInternalId(internalId);
        if (resolvedIdentity == null) {
            throw new IdentityNotFoundException();
        }

        T identity = modification.buildFrom(resolvedIdentity.identity);
        
        UUID changeId = TimeUUIDs.newUUID();
        Audit audit = new AuditBuilder().setLocalHost().setComment("update identity").build();

        String hashedAuthenticationId = resolvedIdentity.hashedAuthenticationId;
        Map<String, Object> map = JsonHelper.convert(identity, new TypeReference<Map<String,Object>>(){});

        // Only need to update the identity table; the internal ID table is unchanged.
        _dataStore.update(_identityTableName, hashedAuthenticationId, changeId, Deltas.literal(map),
                audit, WriteConsistency.GLOBAL);
    }

    @Override
    public void migrateIdentity(String internalId, String newAuthenticationId)
            throws IdentityNotFoundException, IdentityExistsException {
        checkNotNull(internalId, "internalId");
        checkNotNull(newAuthenticationId, newAuthenticationId);

        // Check the new authentication ID conflicts with an existing identity.  Note that we can't protect from a race
        // condition here; we rely on this method being run inside a global synchronization lock.

        if (getIdentityByAuthenticationId(newAuthenticationId) != null) {
            throw new IdentityExistsException();
        }

        ResolvedIdentity resolvedIdentity = resolveIdentityByInternalId(internalId);
        if (resolvedIdentity == null) {
            throw new IdentityNotFoundException();
        }

        T identity = resolvedIdentity.identity;
        String oldHashedAuthenticationId = resolvedIdentity.hashedAuthenticationId;
        String newHashedAuthenticationId = hash(newAuthenticationId);
        UUID changeId = TimeUUIDs.newUUID();
        Audit audit = new AuditBuilder().setLocalHost().setComment("migrate identity").build();

        // Change the masked ID to reflect the new value
        identity.setMaskedId(mask(newAuthenticationId));

        Map<String, Object> map = JsonHelper.convert(identity, new TypeReference<Map<String, Object>>(){});

        // Store the new identity
        Update newIdentityCreate = new Update(_identityTableName, newHashedAuthenticationId, changeId, Deltas.literal(map),
                audit, WriteConsistency.GLOBAL);

        // Delete the old identity
        Update oldIdentityDelete = new Update(_identityTableName, oldHashedAuthenticationId, changeId, Deltas.delete(),
                audit, WriteConsistency.GLOBAL);

        // Update the internal ID index table
        map = ImmutableMap.<String, Object>of(HASHED_ID, newHashedAuthenticationId);
        Update internalIdUpdate = new Update(_internalIdIndexTableName, internalId, changeId, Deltas.literal(map),
                audit, WriteConsistency.GLOBAL);

        _dataStore.updateAll(ImmutableList.of(newIdentityCreate, oldIdentityDelete, internalIdUpdate));
    }

    @Override
    public void deleteIdentity(String internalId) {
        checkNotNull(internalId, "internalId");
        validateTables();

        ResolvedIdentity resolvedIdentity = resolveIdentityByInternalId(internalId);
        if (resolvedIdentity == null) {
            // Don't raise an exception, just return taking no action
            return;
        }

        _dataStore.update(
                _identityTableName,
                resolvedIdentity.hashedAuthenticationId,
                TimeUUIDs.newUUID(),
                Deltas.delete(),
                new AuditBuilder().setLocalHost().setComment("delete identity").build(),
                WriteConsistency.GLOBAL);

        // Don't delete the entry from the internal ID index table; it will be lazily deleted next time it is used.
        // Otherwise there may be a race condition when an API key is migrated.
    }

    private ResolvedIdentity resolveIdentityByInternalId(String internalId) {
        checkNotNull(internalId, "internalId");
        validateTables();
        
        String hashedAuthenticationId = null;
        T identity = null;

        // First try using the index table to determine the hashed ID.
        Map<String, Object> internalIdRecord = _dataStore.get(_internalIdIndexTableName, internalId);
        if (!Intrinsic.isDeleted(internalIdRecord)) {
            hashedAuthenticationId = (String) internalIdRecord.get(HASHED_ID);
            Map<String, Object> identityEntry = _dataStore.get(_identityTableName, hashedAuthenticationId);
            identity = convertDataStoreEntryToIdentity(identityEntry);

            if (identity == null || !identity.getInternalId().equals(internalId)) {
                // The internal ID index table entry was stale.  Delete it.
                Delta deleteIndexRecord = Deltas.conditional(
                        Conditions.mapBuilder()
                                .matches(HASHED_ID, Conditions.equal(hashedAuthenticationId))
                                .build(),
                        Deltas.delete());

                _dataStore.update(_internalIdIndexTableName, internalId, TimeUUIDs.newUUID(), deleteIndexRecord,
                        new AuditBuilder().setLocalHost().setComment("delete stale identity").build());

                // Disregard the value
                hashedAuthenticationId = null;
                identity = null;
            }
        }

        if (identity == null) {
            // This should be rare, but if the record was not found or was stale in the index table then scan for it.

            Iterator<Map<String, Object>> entries = _dataStore.scan(_identityTableName, null, Long.MAX_VALUE, ReadConsistency.STRONG);
            while (entries.hasNext() && identity == null) {
                Map<String, Object> entry = entries.next();
                T potentialIdentity = convertDataStoreEntryToIdentity(entry);
                if (potentialIdentity != null && internalId.equals(potentialIdentity.getInternalId())) {
                    // We found the identity
                    hashedAuthenticationId = Intrinsic.getId(entry);
                    identity = potentialIdentity;

                    // Update the internal ID index.  There is a possible race condition if the identity is being
                    // migrated concurrent to this update.  If that happens, however, the next time it is read the
                    // index will be incorrect and it will be lazily updated at that time.
                    Delta updateIndexRecord = Deltas.literal(ImmutableMap.of(HASHED_ID, hashedAuthenticationId));
                    _dataStore.update(_internalIdIndexTableName, internalId, TimeUUIDs.newUUID(), updateIndexRecord,
                            new AuditBuilder().setLocalHost().setComment("update identity").build());
                }
            }
        }

        if (identity != null) {
            return new ResolvedIdentity(hashedAuthenticationId, identity);
        }

        // Identity not found, return null
        return null;
    }

    private void validateTables() {
        if (_tablesValidated) {
            return;
        }

        synchronized(this) {
            if (!_dataStore.getTableExists(_identityTableName)) {
                _dataStore.createTable(
                        _identityTableName,
                        new TableOptionsBuilder().setPlacement(_placement).build(),
                        ImmutableMap.<String, Object>of(),
                        new AuditBuilder().setLocalHost().setComment("create identity table").build());
            }

            if (!_dataStore.getTableExists(_internalIdIndexTableName)) {
                _dataStore.createTable(
                        _internalIdIndexTableName,
                        new TableOptionsBuilder().setPlacement(_placement).build(),
                        ImmutableMap.<String, Object>of(),
                        new AuditBuilder().setLocalHost().setComment("create internal ID table").build());
            }

            _tablesValidated = true;
        }
    }

    private String hash(String id) {
        if (_hash != null) {
            return _hash.hashUnencodedChars(id).toString();
        }
        return id;
    }

    private String mask(String id) {
        if (id.length() <= 8) {
            return Strings.repeat("*", id.length());
        }
        return id.substring(0, 4) + Strings.repeat("*", id.length() - 8) + id.substring(id.length() - 4);
    }

    /**
     * Helper class for resolving an identity by its internal ID.
     */
    private class ResolvedIdentity {
        String hashedAuthenticationId;
        T identity;

        ResolvedIdentity(String hashedAuthenticationId, T identity) {
            this.hashedAuthenticationId = hashedAuthenticationId;
            this.identity = identity;
        }
    }
}
