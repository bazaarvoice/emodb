package com.bazaarvoice.emodb.auth.permissions;

import org.apache.shiro.authz.Permission;
import org.apache.shiro.authz.permission.PermissionResolver;

import java.util.Map;
import java.util.Set;

/**
 * Manager for permissions.
 */
public interface PermissionManager {

    /**
     * Gets all permissions granted to this role.  If none, returns an empty set, not null,
     * just like {@link com.google.common.collect.Multimap#get(Object)}
     */
    Set<Permission> getAllForRole(String role);

    /**
     * Updates the permissions for a role.
     */
    void updateForRole(String role, PermissionUpdateRequest updates);

    /**
     * Revokes all permissions for a role.
     */
    void revokeAllForRole(String role);

    /**
     * Gets all roles with permissions attached.  Each record returned by the iterable maps a role name to the set
     * of permissions that would be returned by calling {@link #getAllForRole(String)} for that role.
     * Note that if a role is in use but has no permissions attached then it is up to the implementation whether
     * to include a record with an empty set of permissions or omit the record entirely.
     */
    Iterable<Map.Entry<String, Set<Permission>>> getAll();

    /**
     * Gets the permission resolver for this manager.
     */
    PermissionResolver getPermissionResolver();
}
