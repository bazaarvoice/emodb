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
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
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

        // Remove all intrinsics
        identityMap.keySet().removeAll(Intrinsic.DATA_FIELDS);

        // The entry is stored without the original ID, so add it back
        identityMap.remove(MASKED_ID);
        identityMap.put(ID, id);

        return JsonHelper.convert(identityMap, _authIdentityClass);
    }

    @Override
    public void updateIdentity(T identity) {
        checkNotNull(identity, "identity");
        String id = checkNotNull(identity.getId(), "id");
        String internalId = checkNotNull(identity.getInternalId(), "internalId");
        validateTables();

        UUID changeId = TimeUUIDs.newUUID();
        Audit audit = new AuditBuilder().setLocalHost().setComment("update identity").build();

        String hashedId = hash(id);
        Map<String, Object> map = JsonHelper.convert(identity, new TypeReference<Map<String,Object>>(){});

        // Strip the ID and replace it with a masked version
        map.remove(ID);
        map.put(MASKED_ID, mask(id));

        Update identityUpdate = new Update(_identityTableName, hashedId, changeId, Deltas.literal(map), audit, WriteConsistency.GLOBAL);

        map = ImmutableMap.<String, Object>of(HASHED_ID, hashedId);
        Update internalIdUpdate = new Update(_internalIdIndexTableName, internalId, changeId, Deltas.literal(map), audit, WriteConsistency.GLOBAL);

        // Update the identity and internal ID index in a single update
        _dataStore.updateAll(ImmutableList.of(identityUpdate, internalIdUpdate));
    }

    @Override
    public void deleteIdentity(String id) {
        checkNotNull(id, "id");
        validateTables();

        String hashedId = hash(id);

        _dataStore.update(
                _identityTableName,
                hashedId,
                TimeUUIDs.newUUID(),
                Deltas.delete(),
                new AuditBuilder().setLocalHost().setComment("delete identity").build(),
                WriteConsistency.GLOBAL);

        // Don't delete the entry from the internal ID index table; it will be lazily deleted next time it is used.
        // Otherwise there may be a race condition when an API key is migrated.
    }

    @Override
    public Set<String> getRolesByInternalId(String internalId) {
        checkNotNull(internalId, "internalId");

        // The actual ID is stored using a one-way hash so it is not recoverable.  Use a dummy value to satisfy
        // the requirement for constructing the identity; we only need the roles from the identity anyway.
        final String STUB_ID = "ignore";

        T identity = null;

        // First try using the index table to determine the hashed ID.
        Map<String, Object> internalIdRecord = _dataStore.get(_internalIdIndexTableName, internalId);
        if (!Intrinsic.isDeleted(internalIdRecord)) {
            String hashedId = (String) internalIdRecord.get(HASHED_ID);
            Map<String, Object> identityEntry = _dataStore.get(_identityTableName, hashedId);
            identity = convertDataStoreEntryToIdentity(STUB_ID, identityEntry);

            if (identity == null || !identity.getInternalId().equals(internalId)) {
                // Disregard the value
                identity = null;

                // The internal ID index table entry was stale.  Delete it.
                Delta deleteIndexRecord = Deltas.conditional(
                        Conditions.mapBuilder()
                                .matches(HASHED_ID, Conditions.equal(hashedId))
                                .build(),
                        Deltas.delete());

                _dataStore.update(_internalIdIndexTableName, internalId, TimeUUIDs.newUUID(), deleteIndexRecord,
                        new AuditBuilder().setLocalHost().setComment("delete stale identity").build());
            }
        }

        if (identity == null) {
            // This should be rare, but if the record was not found or was stale in the index table then scan for it.

            Iterator<Map<String, Object>> entries = _dataStore.scan(_identityTableName, null, Long.MAX_VALUE, ReadConsistency.STRONG);
            while (entries.hasNext() && identity == null) {
                Map<String, Object> entry = entries.next();
                T potentialIdentity = convertDataStoreEntryToIdentity(STUB_ID, entry);
                if (potentialIdentity != null && internalId.equals(potentialIdentity.getInternalId())) {
                    // We found the identity
                    identity = potentialIdentity;

                    // Update the internal ID index.  There is a possible race condition if the identity is being
                    // migrated concurrent to this update.  If that happens, however, the next time it is read the
                    // index will be incorrect and it will be lazily updated at that time.
                    Delta updateIndexRecord = Deltas.literal(ImmutableMap.of(HASHED_ID, Intrinsic.getId(entry)));
                    _dataStore.update(_internalIdIndexTableName, internalId, TimeUUIDs.newUUID(), updateIndexRecord,
                            new AuditBuilder().setLocalHost().setComment("update identity").build());
                }
            }
        }

        if (identity != null) {
            return identity.getRoles();
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
}
