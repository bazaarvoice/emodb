package com.bazaarvoice.emodb.sor.core;

import com.google.common.base.Objects;

import java.util.Date;
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
    private final Boolean _isDroppedUpdate;
    private final Long _version;
    private final Date _lastMutateAt;
   // TODO: We may need all the intrinsics - to populate the fake delta in POLL. that's for later.

    public UpdateRef(String table, String key, UUID changeId, Set<String> tags) {
        _table = checkNotNull(table, "table");
        _key = checkNotNull(key, "key");
        _changeId = checkNotNull(changeId, "changeId");
        _tags = checkNotNull(tags, "tags");
        _isDroppedUpdate = Boolean.FALSE;
        _version = null;
        _lastMutateAt = null;
    }

    public UpdateRef(String table, String key, UUID changeId, Set<String> tags,
                     Boolean isDroppedUpdate, Long version, Date lastMutateAt) {
        _table = checkNotNull(table, "table");
        _key = checkNotNull(key, "key");
        _changeId = checkNotNull(changeId, "changeId");
        _tags = checkNotNull(tags, "tags");
        _isDroppedUpdate = checkNotNull(isDroppedUpdate, "isDroppedUpdate");
        _version = checkNotNull(version, "version");
        _lastMutateAt = checkNotNull(lastMutateAt, "lastMutateAt");
        // TODO: ALL INTRINSICS
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

    public Boolean getIsDroppedUpdate() {
        return _isDroppedUpdate;
    }

    public Long getVersion() {
        return _version;
    }

    public Date getLastMutateAt() {
        return _lastMutateAt;
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
                Objects.equal(_tags, that._tags) &&
                Objects.equal(_isDroppedUpdate, that._isDroppedUpdate) &&
                Objects.equal(_version, that._version) &&
                Objects.equal(_lastMutateAt, that._lastMutateAt);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(_table, _key, _changeId, _tags, _isDroppedUpdate, _version, _lastMutateAt);
    }

}
