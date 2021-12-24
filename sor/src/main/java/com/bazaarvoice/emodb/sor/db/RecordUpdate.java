package com.bazaarvoice.emodb.sor.db;

import com.bazaarvoice.emodb.sor.api.Audit;
import com.bazaarvoice.emodb.sor.api.WriteConsistency;
import com.bazaarvoice.emodb.sor.delta.Delta;
import com.bazaarvoice.emodb.table.db.Table;

import java.util.Set;
import java.util.UUID;

import static java.util.Objects.requireNonNull;

public final class RecordUpdate {
    private final Table _table;
    private final String _key;
    private final UUID _changeId;
    private final Delta _delta;
    private final Audit _audit;
    private final WriteConsistency _consistency;
    private final Set<String> _tags;

    public RecordUpdate(Table table, String key, UUID changeId, Delta delta, Audit audit, Set<String> tags, WriteConsistency consistency) {
        _table = requireNonNull(table, "table");
        _key = requireNonNull(key, "key");
        _changeId = requireNonNull(changeId, "changeId");
        _delta = requireNonNull(delta, "delta");
        _audit = requireNonNull(audit, "audit");
        _consistency = requireNonNull(consistency, "consistency");
        _tags = requireNonNull(tags, "tags");
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