package com.bazaarvoice.emodb.common.stash;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import static java.util.Objects.hash;

/**
 * POJO for metadata about a single table in Stash.
 */
public class StashTable {
    private final String _bucket;
    private final String _prefix;
    private final String _tableName;

    @JsonCreator
    public StashTable(@JsonProperty("bucket") String bucket,
                      @JsonProperty("prefix") String prefix,
                      @JsonProperty("tableName") String tableName) {
        _bucket = bucket;
        _prefix = prefix;
        _tableName = tableName;
    }

    public String getBucket() {
        return _bucket;
    }

    public String getPrefix() {
        return _prefix;
    }

    public String getTableName() {
        return _tableName;
    }

    public String toString() {
        return String.format("%s (s3://%s/%s)", _tableName, _bucket, _prefix);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof StashTable)) {
            return false;
        }

        StashTable that = (StashTable) o;

        return _bucket.equals(that._bucket) &&
                _prefix.equals(that._prefix) &&
                _tableName.equals(that._tableName);
    }

    @Override
    public int hashCode() {
        return hash(_bucket, _prefix);
    }
}
