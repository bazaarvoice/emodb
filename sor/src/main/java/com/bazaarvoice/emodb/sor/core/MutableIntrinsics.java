package com.bazaarvoice.emodb.sor.core;

import com.bazaarvoice.emodb.common.json.JsonHelper;
import com.bazaarvoice.emodb.common.uuid.TimeUUIDs;
import com.bazaarvoice.emodb.sor.db.Key;
import com.bazaarvoice.emodb.sor.delta.eval.Intrinsics;
import com.bazaarvoice.emodb.table.db.Table;
import com.google.common.hash.HashCode;

import java.util.Map;
import java.util.UUID;

class MutableIntrinsics implements Intrinsics {
    private String _id;
    private Table _table;
    private long _version;
    private HashCode _signature;
    private boolean _deleted;
    private UUID _firstUpdateAt;
    private UUID _lastUpdateAt;
    private UUID _lastMutateAt;

    static MutableIntrinsics create(Key key) {
        return new MutableIntrinsics(key);
    }

    private MutableIntrinsics(Key key) {
        _table = key.getTable();
        _id = key.getKey();
    }

    @Override
    public String getId() {
        return _id;
    }

    void setId(String id) {
        _id = id;
    }

    @Override
    public String getTable() {
        return _table.getName();
    }

    void setTable(Table table) {
        _table = table;
    }

    Map<String, Object> getTemplate() {
        return _table.getAttributes();
    }

    long getVersion() {
        return _version;
    }

    void setVersion(long version) {
        _version = version;
    }

    @Override
    public String getSignature() {
        return (_signature != null) ? _signature.toString() : null;
    }

    HashCode getSignatureHashCode() {
        return _signature;
    }

    void setSignature(HashCode signature) {
        _signature = signature;
    }

    @Override
    public boolean isDeleted() {
        return _deleted;
    }

    void setDeleted(boolean deleted) {
        _deleted = deleted;
    }

    @Override
    public String getFirstUpdateAt() {
        return asISOTimestamp(_firstUpdateAt);
    }

    UUID getFirstUpdateAtUuid() {
        return _firstUpdateAt;
    }

    void setFirstUpdateAt(UUID firstUpdateAt) {
        _firstUpdateAt = firstUpdateAt;
    }

    @Override
    public String getLastUpdateAt() {
        return asISOTimestamp(_lastUpdateAt);
    }

    UUID getLastMutateAtUuid() {
        return _lastMutateAt;
    }

    void setLastMutateAt(UUID lastMutateAt) {
        _lastMutateAt = lastMutateAt;
    }

    @Override
    public String getLastMutateAt() {
        return asISOTimestamp(_lastMutateAt);
    }

    @Override
    public String getTablePlacement() {
        return _table.getOptions().getPlacement();
    }

    UUID getLastUpdateAtUuid() {
        return _lastUpdateAt;
    }

    void setLastUpdateAt(UUID lastUpdateAt) {
        _lastUpdateAt = lastUpdateAt;
    }

    private String asISOTimestamp(UUID timeUuid) {
        return (timeUuid != null) ? JsonHelper.formatTimestamp(TimeUUIDs.getTimeMillis(timeUuid)) : null;
    }
}
