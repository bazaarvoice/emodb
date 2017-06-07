package com.bazaarvoice.emodb.uac.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Response object when creating an API key.  This object contains the private key and the ID for the new API key.
 */
public class CreateEmoApiKeyResponse {

    private final String _key;
    private final String _id;

    @JsonCreator
    public CreateEmoApiKeyResponse(@JsonProperty("key") String key, @JsonProperty("id") String id) {
        _key = checkNotNull(key, "key");
        _id = checkNotNull(id, "id");
    }

    public String getKey() {
        return _key;
    }

    public String getId() {
        return _id;
    }
}
