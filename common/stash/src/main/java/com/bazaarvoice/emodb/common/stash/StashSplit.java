package com.bazaarvoice.emodb.common.stash;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Charsets;
import com.google.common.base.Objects;
import com.google.common.io.BaseEncoding;

/**
 * POJO to maintain attributes of a Stash split.
 */
public class StashSplit {
    @JsonProperty ("table")
    private final String _table;
    @JsonProperty ("key")
    private final String _key;
    @JsonProperty ("size")
    private final long _size;

    @JsonCreator
    StashSplit(@JsonProperty ("table") String table, @JsonProperty ("key") String key,
               @JsonProperty ("size") long size) {
        _table = table;
        _key = key;
        _size = size;
    }

    public static StashSplit fromString(String split) {
        String unencoded = new String(BaseEncoding.base64Url().omitPadding().decode(split), Charsets.UTF_8);
        String[] parts = new StringBuilder(unencoded).reverse().toString().split("\n");
        String table = parts[0];
        String key = parts[1];
        long size = Long.parseLong(parts[2]);
        return new StashSplit(table, key, size);
    }

    public String getTable() {
        return _table;
    }

    public String getFile() {
        return _key.substring(_key.lastIndexOf('/') + 1);
    }

    public long getSize() {
        return _size;
    }

    // Package access only, callers should not directly access the S3 key.
    String getKey() {
        return _key;
    }

    @Override
    public String toString() {
        return BaseEncoding.base64Url().omitPadding().encode(
                new StringBuilder(_table.length() + _key.length() + 10)
                        .append(_table).append("\n")
                        .append(_key).append("\n")
                        .append(_size)
                        .reverse()
                        .toString()
                        .getBytes(Charsets.UTF_8));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof StashSplit)) {
            return false;
        }

        StashSplit that = (StashSplit) o;

        return Objects.equal(_table, that._table) &&
                Objects.equal(_key, that._key) &&
                _size == that._size;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(_table, _key);
    }
}
