package com.bazaarvoice.emodb.sor.db;

import com.bazaarvoice.emodb.sor.api.Audit;
import com.bazaarvoice.emodb.sor.api.WriteConsistency;
import com.bazaarvoice.emodb.sor.delta.Delta;
import com.bazaarvoice.emodb.table.db.Table;

import java.util.Set;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkNotNull;

public final class RecordUpdate {
    private final Table _table;
    private final String _key;
    private final UUID _changeId;
    private final Delta _delta;
    private final Audit _audit;
    private final WriteConsistency _consistency;
    private final Set<String> _tags;

    public RecordUpdate(Table table, String key, UUID changeId, Delta delta, Audit audit, Set<String> tags, WriteConsistency consistency) {
        _table = checkNotNull(table, "table");
        _key = checkNotNull(key, "key");
        _changeId = checkNotNull(changeId, "changeId");
        _delta = checkNotNull(delta, "delta");
        _audit = checkNotNull(audit, "audit");
        _consistency = checkNotNull(consistency, "consistency");
        _tags = checkNotNull(tags, "tags");
    }

    public Table getTable() {
        return _table;
    }

    public String getKey() {
        return _key;
    }

    public UUID getChangeId() {
        return _changeId;
    }

    public Delta getDelta() {
        return _delta;
    }

    public Audit getAudit() {
        return _audit;
    }

    public WriteConsistency getConsistency() {
        return _consistency;
    }

    public Set<String> getTags() {
        return _tags;
    }
}
