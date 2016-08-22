package com.bazaarvoice.emodb.blob.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties ({"cause", "localizedMessage", "stackTrace"})
public class BlobNotFoundException extends RuntimeException {
    private final String _blobId;

    public BlobNotFoundException() {
        _blobId = null;
    }

    public BlobNotFoundException(String blobId) {
        super(blobId);
        _blobId = blobId;
    }

    @JsonCreator
    public BlobNotFoundException(@JsonProperty("message") String message, @JsonProperty("blobId") String blobId) {
        super(message);
        _blobId = blobId;
    }

    public BlobNotFoundException(String blobId, Throwable cause) {
        super(blobId, cause);
        _blobId = blobId;
    }

    public String getBlobId() {
        return _blobId;
    }
}
