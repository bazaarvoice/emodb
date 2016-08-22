package com.bazaarvoice.emodb.common.stash;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * POJO for metadata about a single table in Stash and its files.
 */
public class StashTableMetadata extends StashTable {
    private final List<StashFileMetadata> _files;

    @JsonCreator
    public StashTableMetadata(@JsonProperty("bucket") String bucket,
                              @JsonProperty("prefix") String prefix,
                              @JsonProperty("tableName") String tableName,
                              @JsonProperty("files") List<StashFileMetadata> files) {
        super(bucket, prefix, tableName);
        _files = files;
    }

    public List<StashFileMetadata> getFiles() {
        return _files;
    }

    public long getSize() {
        long size = 0;
        for (StashFileMetadata file : _files) {
            size += file.getSize();
        }
        return size;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof StashTableMetadata)) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }

        StashTableMetadata that = (StashTableMetadata) o;

        return _files.equals(that._files);
    }

    @Override
    public int hashCode() {
        return 31 * super.hashCode() + _files.hashCode();
    }
}
