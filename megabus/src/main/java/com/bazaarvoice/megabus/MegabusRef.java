package com.bazaarvoice.megabus;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;

import javax.annotation.Nullable;
import java.time.Instant;
import java.util.UUID;

import static java.util.Objects.requireNonNull;


/**
 * Reference to a System of Record update. Designed to be similar to {@link com.bazaarvoice.emodb.sor.core.UpdateRef},
 * but with more flexibility to add new fields without breaking serialization.
 *
 * Note that changes do need to be backward-compatible, as the outgoing Megabus will have inevitably written
 * some records as the new Megabus release is coming online.
 */

@JsonIgnoreProperties(ignoreUnknown = true)
public class MegabusRef {
    private final String _table;
    private final String _key;
    private final UUID _changeId;
    private final Instant _readTime;
    private final boolean _deleted;

    @JsonCreator
    public MegabusRef(@JsonProperty("table") String table, @JsonProperty("key") String key,
                     @JsonProperty("changeId") UUID changeId,@JsonProperty("readTime") @Nullable Instant readTime,
                      @JsonProperty("deleted") boolean deleted) {
        _table = requireNonNull(table, "table");
        _key = requireNonNull(key, "key");
        _changeId = requireNonNull(changeId, "changeId");
        _readTime = readTime;
        _deleted = deleted;
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

    public boolean isDeleted() {
        return _deleted;
    }

    @Nullable
    public Instant getReadTime() {
        return _readTime;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof MegabusRef)) {
            return false;
        }
        MegabusRef that = (MegabusRef) o;
        return Objects.equal(_table, that._table) &&
                Objects.equal(_key, that._key) &&
                Objects.equal(_changeId, that._changeId) &&
                Objects.equal(_readTime, that._readTime);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(_table, _key, _changeId, _readTime);
    }
}
