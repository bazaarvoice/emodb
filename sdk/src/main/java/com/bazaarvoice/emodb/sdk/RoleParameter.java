package com.bazaarvoice.emodb.sdk;

/**
 * POJO for holding role configuration.  This consists of a single value, role name, and an optional list
 * of permissions granted to the role.
 */
public class RoleParameter {

    private String name;
    private String[] permissions;

    public String getName() {
        return name;
    }

    public String[] getPermissions() {
        if (permissions == null) {
            permissions = new String[0];
        }
        return permissions;
    }
}
