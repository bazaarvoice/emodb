package com.bazaarvoice.emodb.uac.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;


/**
 * Request object for migrating API keys.  In most cases only the single required attribute, "id", should be set.
 */
public class MigrateEmoApiKeyRequest extends UserAccessControlRequest {

    private String _id;

    @JsonCreator
    public MigrateEmoApiKeyRequest() {
        // empty
    }
    
    public MigrateEmoApiKeyRequest(String id) {
        setId(id);
    }

    public MigrateEmoApiKeyRequest setId(String id) {
        if (id == null || id.isEmpty()) {
            throw new IllegalArgumentException(("Id must not be null nor empty"));
        }
        _id = id;
        return this;
    }

    @JsonIgnore
    public String getId() {
        return _id;
    }
}
