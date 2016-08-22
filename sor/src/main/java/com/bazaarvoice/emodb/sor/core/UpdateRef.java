package com.bazaarvoice.emodb.sor.core;

import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.collect.Sets;

import javax.annotation.Nullable;
import java.util.Set;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Reference to a System of Record update.
 */
public final class UpdateRef {
    public static final String TAGS_NAME = "~tags";
    private final String _table;
    private final String _key;
    private final UUID _changeId;
    private final Set<String> _tags;

    public UpdateRef(String table, String key, UUID changeId, Set<String> tags) {
        _table = checkNotNull(table, "table");
        _key = checkNotNull(key, "key");
        _changeId = checkNotNull(changeId, "changeId");
        _tags = checkNotNull(tags, "tags");
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
        if (!(o instanceof UpdateRef)) {
            return false;
        }
        UpdateRef that = (UpdateRef) o;
        return Objects.equal(_table, that._table) &&
                Objects.equal(_key, that._key) &&
                Objects.equal(_changeId, that._changeId) &&
                Objects.equal(_tags, that._tags);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(_table, _key, _changeId, _tags);
    }

}
