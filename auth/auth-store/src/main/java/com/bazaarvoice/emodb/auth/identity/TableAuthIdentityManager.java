package com.bazaarvoice.emodb.auth.identity;

import com.bazaarvoice.emodb.common.json.JsonHelper;
import com.bazaarvoice.emodb.common.uuid.TimeUUIDs;
import com.bazaarvoice.emodb.sor.api.AuditBuilder;
import com.bazaarvoice.emodb.sor.api.DataStore;
import com.bazaarvoice.emodb.sor.api.Intrinsic;
import com.bazaarvoice.emodb.sor.api.TableOptionsBuilder;
import com.bazaarvoice.emodb.sor.api.WriteConsistency;
import com.bazaarvoice.emodb.sor.delta.Deltas;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.hash.HashFunction;

import javax.annotation.Nullable;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * AuthIdentity manager that uses an EmoDB table to store identities.
 *
 * With circular logic more deadly to a robot than saying "this statement is false" the application must have
 * permission to perform operations on the table in order to use this manager.
 */
public class TableAuthIdentityManager<T extends AuthIdentity> implements AuthIdentityManager<T> {

    private final Class<T> _authIdentityClass;
    private final DataStore _dataStore;
    private final String _tableName;
    private final String _placement;
    private final HashFunction _hash;
    private volatile boolean _tableValidated;

    public TableAuthIdentityManager(Class<T> authIdentityClass, DataStore dataStore, String tableName, String placement) {
        this(authIdentityClass, dataStore, tableName, placement, null);
    }

    public TableAuthIdentityManager(Class<T> authIdentityClass, DataStore dataStore, String tableName, String placement,
                                    @Nullable HashFunction hash) {
        _authIdentityClass = checkNotNull(authIdentityClass, "authIdentityClass");
        _dataStore = checkNotNull(dataStore, "client");
        _tableName = checkNotNull(tableName, "tableName");
        _placement = checkNotNull(placement, "placement");
        _hash = hash;
    }

    @Override
    public T getIdentity(String id) {
        checkNotNull(id, "id");
        validateTable();

        String hashedId = hash(id);
        Map<String, Object> map = _dataStore.get(_tableName, hashedId);
        if (Intrinsic.isDeleted(map)) {
            return null;
        }

        // The entry is stored without the original ID, so add it back
        map.keySet().removeAll(Intrinsic.DATA_FIELDS);
        map.remove("maskedId");
        map.put("id", id);

        return JsonHelper.convert(map, _authIdentityClass);
    }

    @Override
    public void updateIdentity(T identity) {
        checkNotNull(identity, "identity");
        String id = checkNotNull(identity.getId(), "id");
        validateTable();

        String hashedId = hash(id);
        Map<String, Object> map = JsonHelper.convert(identity, new TypeReference<Map<String,Object>>(){});

        // Strip the ID and replace it with a masked version
        map.remove("id");
        map.put("maskedId", mask(id));

        _dataStore.update(
                _tableName,
                hashedId,
                TimeUUIDs.newUUID(),
                Deltas.literal(map),
                new AuditBuilder().setLocalHost().setComment("update identity").build(),
                WriteConsistency.GLOBAL);
    }

    @Override
    public void deleteIdentity(String id) {
        checkNotNull(id, "id");
        validateTable();

        String hashedId = hash(id);

        _dataStore.update(
                _tableName,
                hashedId,
                TimeUUIDs.newUUID(),
                Deltas.delete(),
                new AuditBuilder().setLocalHost().setComment("delete identity").build(),
                WriteConsistency.GLOBAL);
    }

    private void validateTable() {
        if (_tableValidated) {
            return;
        }

        synchronized(this) {
            if (!_dataStore.getTableExists(_tableName)) {
                _dataStore.createTable(
                        _tableName,
                        new TableOptionsBuilder().setPlacement(_placement).build(),
                        ImmutableMap.<String, Object>of(),
                        new AuditBuilder().setLocalHost().setComment("create identity table").build());
            }

            _tableValidated = true;
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
