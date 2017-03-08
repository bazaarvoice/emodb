package com.bazaarvoice.emodb.auth.permissions;

import com.bazaarvoice.emodb.auth.role.RoleIdentifier;

/**
 * Static helper class for converting type-specific permissions to flat namespaced Strings used by
 * {@link PermissionReader#getPermissions(String)}.  While currently only roles can have permissions attached to them
 * this convention allows for future additional permission types without need for a separate permission management
 * system.
 */
public final class PermissionIDs {

    public static String forRole(RoleIdentifier id) {
        return forRole(id.toString());
    }

    public static String forRole(String role) {
        return "role:" + role;
    }
}
