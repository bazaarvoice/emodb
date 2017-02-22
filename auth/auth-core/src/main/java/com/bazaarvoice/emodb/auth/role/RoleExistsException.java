package com.bazaarvoice.emodb.auth.role;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * Exception thrown when attempting to create a role which already exists.
 */
@JsonIgnoreProperties({"cause", "localizedMessage", "stackTrace"})
public class RoleExistsException extends RuntimeException {
    private final String _group;
    private final String _id;

    public RoleExistsException(String group, String id) {
        super("Role not found");
        _group = group;
        _id = id;
    }
    
    public String getGroup() {
        return _group;
    }

    public String getId() {
        return _id;
    }
}
