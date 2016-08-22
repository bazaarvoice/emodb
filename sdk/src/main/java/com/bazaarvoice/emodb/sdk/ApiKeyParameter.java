package com.bazaarvoice.emodb.sdk;

/**
 * POJO for holding API key configuration.  This consists of a single value, the API key, and an optional list
 * of roles associated with the key.
 */
public class ApiKeyParameter {

    private String value;
    private String[] roles;

    public String getValue() {
        return value;
    }

    public String[] getRoles() {
        if (roles == null) {
            roles = new String[0];
        }
        return roles;
    }
}
