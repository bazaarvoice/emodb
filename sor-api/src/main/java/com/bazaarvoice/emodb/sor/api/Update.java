package com.bazaarvoice.emodb.sor.api;

import com.bazaarvoice.emodb.sor.delta.Delta;
import com.bazaarvoice.emodb.sor.uuid.TimeUUIDs;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import com.google.common.base.Strings;

import javax.annotation.Nullable;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

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
        _table = checkNotNull(table, "table");
        checkArgument(Names.isLegalTableName(table),
                "Table name must be a lowercase ASCII string between 1 and 255 characters in length. " +
                        "Allowed punctuation characters are -.:@_ and the table name may not start with a single underscore character. " +
                        "An example of a valid table name would be 'review:testcustomer'.");
        checkArgument(!Strings.isNullOrEmpty(key), "key must be a non-empty string");
        _key = key;
        _changeId = Objects.firstNonNull(changeId, TimeUUIDs.newUUID());
        checkArgument(_changeId.version() == 1, "The changeId must be an RFC 4122 version 1 UUID (a time UUID).");
        _delta = checkNotNull(delta, "delta");
        _audit = checkNotNull(audit, "audit");
        _consistency = Objects.firstNonNull(consistency, WriteConsistency.STRONG);
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
        return Objects.hashCode(_table, _key, _changeId, _delta, _audit, _consistency);
    }

    @Override
    public String toString() {
        return _table + "/" + _key + "=" + _delta; // for debugging
    }
}
