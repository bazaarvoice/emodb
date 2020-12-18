package com.bazaarvoice.emodb.uac.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Optional;

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

    @JsonCreator
    public EmoRoleExistsException(@JsonProperty("group") String group, @JsonProperty("id") String id) {
        super("Role exists");
        _group = Optional.ofNullable(group).orElse(EmoRoleKey.NO_GROUP);
        _id = id;
    }

    public String getGroup() {
        return _group;
    }

    public String getId() {
        return _id;
    }
}