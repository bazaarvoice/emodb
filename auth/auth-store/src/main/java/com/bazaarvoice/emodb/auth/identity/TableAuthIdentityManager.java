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
import com.bazaarvoice.emodb.sor.condition.Condition;
import com.bazaarvoice.emodb.sor.condition.Conditions;
import com.bazaarvoice.emodb.sor.delta.Delta;
import com.bazaarvoice.emodb.sor.delta.Deltas;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.hash.HashFunction;

import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * AuthIdentity manager that uses an EmoDB table to store identities.
 *
 * With circular logic more deadly to a robot than saying "this statement is false" the application must have
 * permission to perform operations on the table in order to use this manager.
 */
public class TableAuthIdentityManager<T extends AuthIdentity> implements AuthIdentityManager<T> {

    private final static String ID = "id";
    private final static String INTERNAL_ID = "internalId";
    private final static String MASKED_ID = "maskedId";
    private final static String HASHED_ID = "hashedId";
    private final static String STATE = "state";

    private final Class<T> _authIdentityClass;
    private final DataStore _dataStore;
    private final String _identityTableName;
    private final String _internalIdIndexTableName;
    private final String _placement;
    private final HashFunction _hash;
    private volatile boolean _tablesValidated;

    public TableAuthIdentityManager(Class<T> authIdentityClass, DataStore dataStore, String identityTableName,
                                    String internalIdIndexTableName, String placement) {
        this(authIdentityClass, dataStore, identityTableName, internalIdIndexTableName, placement, null);
    }

    public TableAuthIdentityManager(Class<T> authIdentityClass, DataStore dataStore, String identityTableName,
                                    String internalIdIndexTableName, String placement, @Nullable HashFunction hash) {
        _authIdentityClass = checkNotNull(authIdentityClass, "authIdentityClass");
        _dataStore = checkNotNull(dataStore, "client");
        _identityTableName = checkNotNull(identityTableName, "identityTableName");
        _internalIdIndexTableName = checkNotNull(internalIdIndexTableName, "internalIdIndexTableName");
        _placement = checkNotNull(placement, "placement");
        _hash = hash;

        checkArgument(!_identityTableName.equals(internalIdIndexTableName), "Identity and internal ID index tables must be distinct");
    }

    @Override
    public T getIdentity(String id) {
        checkNotNull(id, "id");
        validateTables();

        String hashedId = hash(id);
        Map<String, Object> map = _dataStore.get(_identityTableName, hashedId);
        return convertDataStoreEntryToIdentity(id, map);
    }

    private T convertDataStoreEntryToIdentity(String id, Map<String, Object> map) {
        // If the record does not exist or has been deleted then return null
        if (map == null || Intrinsic.isDeleted(map)) {
            return null;
        }

        // Make a copy of the map to avoid mutating the method parameter as a side-effect
        Map<String, Object> identityMap = Maps.newHashMap(map);

        // Identities have been in use since before internal IDs were introduced.  To grandfather in those keys we'll
        // use the hash of the identity's ID as the internal ID.
        if (!identityMap.containsKey(INTERNAL_ID)) {
            identityMap.put(INTERNAL_ID, Intrinsic.getId(map));
        }
        // Similarly grandfather in identities with no state attribute as active
        if (!identityMap.containsKey(STATE)) {
            identityMap.put(STATE, IdentityState.ACTIVE.toString());
        }

        // Remove all intrinsics
        identityMap.keySet().removeAll(Intrinsic.DATA_FIELDS);

        // The entry is stored without the original ID, so add it back
        identityMap.remove(MASKED_ID);
        identityMap.put(ID, id);

        return JsonHelper.convert(identityMap, _authIdentityClass);
    }

    @Override
    public void updateIdentity(T identity) {
        updateOrMigrateIdentity(identity, null);
    }

    @Override
    public void migrateIdentity(String existingId, String newId) {
        checkNotNull(existingId, "existingId");
        checkNotNull(newId, "newId");

        if (existingId.equals(newId)) {
            // Trivial case.  Don't throw an exception, just return without performing any actual updates.
            return;
        }

        T identity = getIdentity(existingId);
        checkNotNull(identity, "Unknown identity: %s", existingId);

        updateOrMigrateIdentity(identity, newId);
    }

    private void updateOrMigrateIdentity(T identity, @Nullable String newId) {
        boolean isUpdate = newId == null;

        checkNotNull(identity, "identity");
        String id = checkNotNull(identity.getId(), "id");
        String internalId = checkNotNull(identity.getInternalId(), "internalId");
        checkArgument(identity.getState().isActive() || isUpdate, "Cannot migrate %s identity", identity.getState());

        validateTables();

        List<Update> updates = Lists.newArrayListWithCapacity(3);

        UUID changeId = TimeUUIDs.newUUID();
        Audit audit = new AuditBuilder()
                .setLocalHost()
                .setComment(String.format("%s identity", isUpdate ? "update" : "migrate"))
                .build();

        String hashedId = hash(id);
        String maskedId;
        Map<String, Object> identityMap = JsonHelper.convert(identity, new TypeReference<Map<String,Object>>(){});

        if (isUpdate) {
            maskedId = mask(id);
        } else {
            // Change the state for the existing identity to migrated.  Use JSON serialization to create a clone
            // of the original identity
            T oldIdentity = JsonHelper.convert(identityMap, _authIdentityClass);
            // Set the state to migrated
            oldIdentity.setState(IdentityState.MIGRATED);
            // Convert the old identity back to a delta Map representation
            Map<String, Object> oldIdentityMap = JsonHelper.convert(oldIdentity, new TypeReference<Map<String,Object>>(){});
            // Strip the ID so it doesn't get persisted, then update the remaining attributes
            oldIdentityMap.remove(ID);
            Delta delta = Deltas.mapBuilder().putAll(oldIdentityMap).build();
            updates.add(new Update(_identityTableName, hashedId, changeId, delta, audit, WriteConsistency.GLOBAL));

            // Update the ID and masked ID to match the new identity
            hashedId = hash(newId);
            maskedId = mask(newId);
        }

        // Strip the ID and replace it with a masked version
        identityMap.remove(ID);
        identityMap.put(MASKED_ID, maskedId);

        updates.add(new Update(_identityTableName, hashedId, changeId, Deltas.literal(identityMap), audit, WriteConsistency.GLOBAL));

        Map<String, Object> internalIdMap = ImmutableMap.of(HASHED_ID, hashedId);
        updates.add(new Update(_internalIdIndexTableName, internalId, changeId, Deltas.literal(internalIdMap), audit, WriteConsistency.GLOBAL));

        // Update the identity record(s) and internal ID index in a single update
        _dataStore.updateAll(updates);
    }

    @Override
    public void deleteIdentityUnsafe(String id) {
        checkNotNull(id, "id");

        T identity = getIdentity(id);
        if (identity == null) {
            // Identity did not exist.  Don't raise an exception, just silently return with no action.
            return;
        }

        // Create two updates; one to delete from the identity table and one to delete from the internal ID
        // index table

        UUID changeId = TimeUUIDs.newUUID();
        Audit audit = new AuditBuilder().setComment("delete identity").setLocalHost().build();

        List<Update> updates = ImmutableList.of(
                new Update(_identityTableName, hash(id), changeId, Deltas.delete(), audit, WriteConsistency.GLOBAL),
                new Update(_internalIdIndexTableName, identity.getInternalId(), changeId, Deltas.delete(), audit, WriteConsistency.GLOBAL));

        _dataStore.updateAll(updates);
    }

    @Override
    public InternalIdentity getInternalIdentity(String internalId) {
        checkNotNull(internalId, "internalId");

        // The actual ID is stored using a one-way hash so it is not recoverable.  Use a dummy value to satisfy
        // the requirement for constructing the identity; we only need the roles from the identity anyway.
        final String STUB_ID = "ignore";

        T identity = null;
        String staleHashedId = null;

        // First try using the index table to determine the hashed ID.
        Map<String, Object> internalIdRecord = _dataStore.get(_internalIdIndexTableName, internalId);
        if (!Intrinsic.isDeleted(internalIdRecord)) {
            String hashedId = (String) internalIdRecord.get(HASHED_ID);
            Map<String, Object> identityEntry = _dataStore.get(_identityTableName, hashedId);
            identity = convertDataStoreEntryToIdentity(STUB_ID, identityEntry);

            if (identity == null || !identity.getInternalId().equals(internalId)) {
                // Disregard the value
                identity = null;

                // The internal ID index table entry was stale.  Save the ID for later update or deletion.
                staleHashedId = hashedId;
            }
        }

        if (identity == null) {
            // This should be rare, but if the record was not found or was stale in the index table then scan for it.
            String hashedId = null;

            Iterator<Map<String, Object>> entries = _dataStore.scan(_identityTableName, null, Long.MAX_VALUE, ReadConsistency.STRONG);
            while (entries.hasNext() && (identity == null || identity.getState() == IdentityState.MIGRATED)) {
                Map<String, Object> entry = entries.next();
                T potentialIdentity = convertDataStoreEntryToIdentity(STUB_ID, entry);
                if (potentialIdentity != null && internalId.equals(potentialIdentity.getInternalId())) {
                    // We potentially found the identity.  If it is migrated then we'll keep searching for an identity
                    // that is not migrated.
                    identity = potentialIdentity;
                    hashedId = Intrinsic.getId(entry);
                }
            }

            if (hashedId != null || staleHashedId != null) {
                // Update the internal ID index.  There is a possible race condition if the identity is being
                // migrated concurrent to this update.  For that reason the index update will be predicated on the
                // condition that the value is the same as what was previously read.

                Condition condition;
                Delta delta;

                if (staleHashedId == null) {
                    // Only update the index if there is still no index record
                    condition = Conditions.isUndefined();
                } else {
                    // Only update the index if it still has the same stale value
                    condition = Conditions.mapBuilder()
                            .matches(HASHED_ID, Conditions.equal(staleHashedId))
                            .build();
                }

                if (hashedId == null) {
                    // Delete the index record if it doesn't map to an identity
                    delta = Deltas.delete();
                } else {
                    // Update the index record with the hashed ID of the identity
                    delta = Deltas.literal(ImmutableMap.of(HASHED_ID, hashedId));
                }

                _dataStore.update(_internalIdIndexTableName, internalId, TimeUUIDs.newUUID(),
                        Deltas.conditional(condition, delta),
                        new AuditBuilder().setLocalHost().setComment("update identity").build(), WriteConsistency.GLOBAL);
            }
        }

        // If this identity was found this will be not null, otherwise it returns null signalling it was not found.
        return identity;
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
}
