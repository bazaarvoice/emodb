package com.bazaarvoice.megabus;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

import static java.util.Objects.hash;
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

    public enum RefType {
        NORMAL,
        TOUCH,
        DELETED
    }

    private final String _table;
    private final String _key;
    private final UUID _changeId;
    private final Instant _readTime;
    private final RefType _refType;

    @JsonCreator
    public MegabusRef(@JsonProperty("table") String table, @JsonProperty("key") String key,
                     @JsonProperty("changeId") UUID changeId, @JsonProperty("readTime") @Nullable Instant readTime,
                      @Nullable @JsonProperty("refType") RefType refType) {
        _table = requireNonNull(table, "table");
        _key = requireNonNull(key, "key");
        _changeId = requireNonNull(changeId, "changeId");
        _readTime = readTime;
        _refType = Optional.ofNullable(refType).orElse(RefType.NORMAL);

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

    public RefType getRefType() {
        return _refType;
    }

    @Nullable
    public Instant getReadTime() {
        return _readTime;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MegabusRef that = (MegabusRef) o;
        return getTable().equals(that.getTable()) &&
                getKey().equals(that.getKey()) &&
                getChangeId().equals(that.getChangeId()) &&
                java.util.Objects.equals(getReadTime(), that.getReadTime()) &&
                getRefType() == that.getRefType();
    }

    @Override
    public String toString() {
        return "MegabusRef{" +
                "_table='" + _table + '\'' +
                ", _key='" + _key + '\'' +
                ", _changeId=" + _changeId +
                ", _readTime=" + _readTime +
                ", _refType=" + _refType +
                '}';
    }

    @Override
    public int hashCode() {
        return hash(_table, _key, _changeId, _readTime);
    }
}
