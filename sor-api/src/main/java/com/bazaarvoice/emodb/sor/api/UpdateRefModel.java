package com.bazaarvoice.emodb.sor.api;

import java.util.Objects;
import java.util.Set;
import java.util.UUID;

import static java.util.Objects.hash;
import static java.util.Objects.requireNonNull;

/**
 * Reference to a System of Record update.
 */
public final class UpdateRefModel {
    public static final String TAGS_NAME = "~tags";
    private final String _table;
    private final String _key;
    private final UUID _changeId;
    private final Set<String> _tags;

    public UpdateRefModel(String table, String key, UUID changeId, Set<String> tags) {
        _table = requireNonNull(table, "table");
        _key = requireNonNull(key, "key");
        _changeId = requireNonNull(changeId, "changeId");
        _tags = requireNonNull(tags, "tags");
    }

    public String getTable() {
        return _table;
    }

    public String getKey() {
        return _key;
    }

    public UUID getChangeId() {
        return _changeId;
    }

    public Set<String> getTags() {
        return _tags;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof UpdateRefModel)) {
            return false;
        }
        UpdateRefModel that = (UpdateRefModel) o;
        return Objects.equals(_table, that._table) &&
                Objects.equals(_key, that._key) &&
                Objects.equals(_changeId, that._changeId) &&
                Objects.equals(_tags, that._tags);
    }

    @Override
    public int hashCode() {
        return hash(_table, _key, _changeId, _tags);
    }

}
