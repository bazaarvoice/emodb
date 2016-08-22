package com.bazaarvoice.emodb.blob.db;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

public class StorageSummary {
    private final long _length;
    private final int _chunkCount;
    private final int _chunkSize;
    private String _md5;
    private String _sha1;
    private final Map<String, String> _attributes;
    private final long _timestamp;

    public StorageSummary(@JsonProperty("length") long length,
                          @JsonProperty("chunkCount") int chunkCount,
                          @JsonProperty("chunkSize") int chunkSize,
                          @JsonProperty("md5") String md5,
                          @JsonProperty("sha1") String sha1,
                          @JsonProperty("attributes") Map<String, String> attributes,
                          @JsonProperty("timestamp") long timestamp) {
        _length = length;
        _chunkCount = chunkCount;
        _chunkSize = chunkSize;
        _md5 = md5;
        _sha1 = sha1;
        _attributes = attributes;
        _timestamp = timestamp;
    }

    public long getLength() {
        return _length;
    }

    public int getChunkCount() {
        return _chunkCount;
    }

    public int getChunkSize() {
        return _chunkSize;
    }

    @JsonProperty("md5")
    public String getMD5() {
        return _md5;
    }

    @JsonProperty("sha1")
    public String getSHA1() {
        return _sha1;
    }

    public Map<String, String> getAttributes() {
        return _attributes;
    }

    /** Returns the date/time this blob was uploaded, in microseconds. */
    public long getTimestamp() {
        return _timestamp;
    }
}
