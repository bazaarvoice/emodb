package com.bazaarvoice.emodb.auth.role;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * Exception thrown when attempting to utilize a role which does not exist.
 */
@JsonIgnoreProperties({"cause", "localizedMessage", "stackTrace"})
public class RoleNotFoundException extends RuntimeException {
    private final String _group;
    private final String _name;

    public RoleNotFoundException(String group, String name) {
        super("Role not found");
        _group = group;
        _name = name;
    }

    public String getGroup() {
        return _group;
    }

    public String getName() {
        return _name;
    }
}
