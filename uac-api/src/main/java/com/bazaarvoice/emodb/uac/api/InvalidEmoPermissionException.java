package com.bazaarvoice.emodb.uac.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Exception thrown when a permission string in a request was rejected by Emo.
 */
@JsonIgnoreProperties({"cause", "localizedMessage", "stackTrace"})
public class InvalidEmoPermissionException extends RuntimeException {

    private String _permission;

    public InvalidEmoPermissionException() {
    }

    @JsonCreator
    public InvalidEmoPermissionException(@JsonProperty("message") String message, @JsonProperty("permission") String permission) {
        super(message);
        _permission = permission;
    }

    public String getPermission() {
        return _permission;
    }
}
