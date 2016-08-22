package com.bazaarvoice.emodb.sor.db;

public class RecordEntryRawMetadata {
    private int _size;
    private long _timestamp;

    public int getSize() {
        return _size;
    }

    public void setSize(int size) {
        _size = size;
    }

    public RecordEntryRawMetadata withSize(int size) {
        setSize(size);
        return this;
    }

    public long getTimestamp() {
        return _timestamp;
    }

    public void setTimestamp(long timestamp) {
        _timestamp = timestamp;
    }

    public RecordEntryRawMetadata withTimestamp(long timestamp) {
        setTimestamp(timestamp);
        return this;
    }
}
