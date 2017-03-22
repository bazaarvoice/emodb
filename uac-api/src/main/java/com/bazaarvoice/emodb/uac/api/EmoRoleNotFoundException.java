package com.bazaarvoice.emodb.uac.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;

/**
 * Exception thrown when attempting to modify a role which does not exist.
 */
@JsonIgnoreProperties({"cause", "localizedMessage", "stackTrace"})
public class EmoRoleNotFoundException extends RuntimeException {
    private final String _group;
    private final String _id;

    public EmoRoleNotFoundException() {
        this("unknown", "unknown");
    }

    @JsonCreator
    public EmoRoleNotFoundException(@JsonProperty("group") String group, @JsonProperty("id") String id) {
        super("Role not found");
        _group = Objects.firstNonNull(group, EmoRoleKey.NO_GROUP);
        _id = id;
    }

    public String getGroup() {
        return _group;
    }

    public String getId() {
        return _id;
    }
}