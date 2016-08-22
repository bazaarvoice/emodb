package com.bazaarvoice.emodb.blob.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties ({"cause", "localizedMessage", "stackTrace"})
public class RangeNotSatisfiableException extends RuntimeException {
    private final long _offset;
    private final long _length;

    @JsonCreator
    public RangeNotSatisfiableException(@JsonProperty("message") String message, @JsonProperty("offset") long offset,
                                        @JsonProperty("length") long length) {
        super(message);
        _offset = offset;
        _length = length;
    }

    public long getOffset() {
        return _offset;
    }

    public long getLength() {
        return _length;
    }
}
