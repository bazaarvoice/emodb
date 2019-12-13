package com.bazaarvoice.emodb.uac.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;

/**
 * Exception thrown when attempting to create a role which already exists.
 */
@JsonIgnoreProperties({"cause", "localizedMessage", "stackTrace"})
public class EmoRoleExistsException extends RuntimeException {
    private final String _group;
    private final String _id;

    public EmoRoleExistsException() {
        this("unknown", "unknown");
    }

    public EmoRoleExistsException(String group, String id) {
        this(group, id, "Role exists");
    }

    @JsonCreator
    public EmoRoleExistsException(@JsonProperty("group") String group, @JsonProperty("id") String id, @JsonProperty("message") String message) {
        super(message);
        _group = MoreObjects.firstNonNull(group, EmoRoleKey.NO_GROUP);
        _id = id;
    }

    public String getGroup() {
        return _group;
    }

    public String getId() {
        return _id;
    }
}