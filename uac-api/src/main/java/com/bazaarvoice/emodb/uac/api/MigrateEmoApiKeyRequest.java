package com.bazaarvoice.emodb.uac.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Strings;

import static com.google.common.base.Preconditions.checkArgument;

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
        checkArgument(!Strings.isNullOrEmpty(id), "id");
        _id = id;
        return this;
    }

    @JsonIgnore
    public String getId() {
        return _id;
    }
}
