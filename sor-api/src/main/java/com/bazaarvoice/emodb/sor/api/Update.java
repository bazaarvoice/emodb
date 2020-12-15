package com.bazaarvoice.emodb.sor.api;

import com.bazaarvoice.emodb.sor.delta.Delta;
import com.bazaarvoice.emodb.sor.uuid.TimeUUIDs;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

import static java.util.Objects.requireNonNull;


public final class Update {
    private final String _table;
    private final String _key;
    private final UUID _changeId;
    private final Delta _delta;
    private final Audit _audit;
    private final WriteConsistency _consistency;

    public Update(String table, String key, UUID changeId, Delta delta, Audit audit) {
        this(table, key, changeId, delta, audit, WriteConsistency.STRONG);
    }

    @JsonCreator
    public Update(@JsonProperty("table") String table, @JsonProperty("key") String key,
                  @JsonProperty("changeId") @Nullable UUID changeId, @JsonProperty("delta") Delta delta,
                  @JsonProperty("audit") Audit audit, @JsonProperty("consistency") @Nullable WriteConsistency consistency) {
        _table = requireNonNull(table, "table");
        if (!Names.isLegalTableName(table)) {
            throw new IllegalArgumentException(
                    "Table name must be a lowercase ASCII string between 1 and 255 characters in length. " +
                            "Allowed punctuation characters are -.:@_ and the table name may not start with a single underscore character. " +
                            "An example of a valid table name would be 'review:testcustomer'.");
        }
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("key must be a non-empty string");
        }
        _key = key;
        _changeId = Optional.ofNullable(changeId).orElse(TimeUUIDs.newUUID());
        if (_changeId.version() != 1) {
            throw new IllegalArgumentException("The changeId must be an RFC 4122 version 1 UUID (a time UUID).");
        }
        _delta = requireNonNull(delta, "delta");
        _audit = requireNonNull(audit, "audit");
        _consistency = Optional.ofNullable(consistency).orElse(WriteConsistency.STRONG);
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

    public Delta getDelta() {
        return _delta;
    }

    public Audit getAudit() {
        return _audit;
    }

    public WriteConsistency getConsistency() {
        return _consistency;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Update)) {
            return false;
        }
        Update update = (Update) o;
        return _table.equals(update.getTable()) &&
                _key.equals(update.getKey()) &&
                _changeId.equals(update.getChangeId()) &&
                _delta.equals(update.getDelta()) &&
                _audit.equals(update.getAudit()) &&
                _consistency == update.getConsistency();
    }

    @Override
    public int hashCode() {
        return Objects.hash(_table, _key, _changeId, _delta, _audit, _consistency);
    }

    @Override
    public String toString() {
        return _table + "/" + _key + "=" + _delta; // for debugging
    }
}
