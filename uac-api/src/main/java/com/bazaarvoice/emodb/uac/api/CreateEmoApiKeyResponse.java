package com.bazaarvoice.emodb.uac.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import static java.util.Objects.requireNonNull;

/**
 * Response object when creating an API key.  This object contains the private key and the ID for the new API key.
 */
public class CreateEmoApiKeyResponse {

    private final String _key;
    private final String _id;

    @JsonCreator
    public CreateEmoApiKeyResponse(@JsonProperty("key") String key, @JsonProperty("id") String id) {
        _key = requireNonNull(key, "key");
        _id = requireNonNull(id, "id");
    }

    public String getKey() {
        return _key;
    }

    public String getId() {
        return _id;
    }
}
