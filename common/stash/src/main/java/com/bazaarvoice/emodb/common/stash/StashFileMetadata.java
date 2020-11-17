package com.bazaarvoice.emodb.common.stash;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

/**
 * POJO for metadata about a single file in Stash.
 */
public class StashFileMetadata {
    private final String _bucket;
    private final String _key;
    private final long _size;

    @JsonCreator
    public StashFileMetadata(@JsonProperty("bucket") String bucket,
                             @JsonProperty("key") String key,
                             @JsonProperty("size") long size) {
        _bucket = bucket;
        _key = key;
        _size = size;
    }

    public String getBucket() {
        return _bucket;
    }

    public String getKey() {
        return _key;
    }

    public long getSize() {
        return _size;
    }

    public String toString() {
        return String.format("s3://%s/%s", _bucket, _key);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof StashFileMetadata)) {
            return false;
        }

        StashFileMetadata that = (StashFileMetadata) o;

        return _bucket.equals(that._bucket) &&
                _key.equals(that._key) &&
                _size == that._size;
    }

    @Override
    public int hashCode() {
        return Objects.hash(_bucket, _key);
    }
}
