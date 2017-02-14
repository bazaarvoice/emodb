package com.bazaarvoice.emodb.auth.permissions;

import org.apache.shiro.authz.Permission;

import java.util.Map;
import java.util.Set;

/**
 * Manager for permissions.
 */
public interface PermissionManager extends PermissionReader {
    
    /**
     * Updates the permissions associated with an ID, such as a role.
     */
    void updatePermissions(String id, PermissionUpdateRequest updates);

    /**
     * Revokes all permissions associated with an ID, such as a role.
     */
    void revokePermissions(String id);

    /**
     * Gets all IDs with permissions attached.  Each record returned by the iterable maps an ID to the set
     * of permissions that would be returned by calling {@link #getPermissions(String)} for that role.
     * Note that if a role is in use but has no permissions attached then it is up to the implementation whether
     * to include a record with an empty set of permissions or omit the record entirely.
     */
    Iterable<Map.Entry<String, Set<Permission>>> getAll();
}
