package com.bazaarvoice.emodb.blob.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.Date;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public class DefaultBlobMetadata implements BlobMetadata {
    private final String _id;
    private final Date _timestamp;
    private final long _length;
    private final String _md5;
    private final String _sha1;
    private final Map<String, String> _attributes;

    public DefaultBlobMetadata(BlobMetadata md) {
        this(md.getId(), md.getTimestamp(), md.getLength(), md.getMD5(), md.getSHA1(), md.getAttributes());
    }

    @JsonCreator
    public DefaultBlobMetadata(@JsonProperty("id") String id, @JsonProperty("timestamp") Date timestamp,
                               @JsonProperty("length") long length,
                               @JsonProperty("md5") String md5, @JsonProperty("sha1") String sha1,
                               @JsonProperty("attributes") Map<String, String> attributes) {
        _id = checkNotNull(id, "id");
        _timestamp = checkNotNull(timestamp, "timestamp");
        _length = length;
        _md5 = md5;
        _sha1 = sha1;
        _attributes = checkNotNull(attributes, "attributes");
    }

    @Override
    public String getId() {
        return _id;
    }

    @Override
    public Date getTimestamp() {
        return _timestamp;
    }

    @Override
    public long getLength() {
        return _length;
    }

    @Override
    public String getMD5() {
        return _md5;
    }

    @Override
    public String getSHA1() {
        return _sha1;
    }

    @Override
    public Map<String, String> getAttributes() {
        return Collections.unmodifiableMap(_attributes);
    }

    @Override
    public String toString() {
        return _id;  // for debugging
    }
}
