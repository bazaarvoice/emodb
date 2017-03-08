package com.bazaarvoice.emodb.auth.permissions;

import org.apache.shiro.authz.Permission;
import org.apache.shiro.authz.permission.PermissionResolver;

import java.util.Set;

/**
 * Minimal interface for read-only access to permissions.  The convention for determining permission IDs is orthogonal
 * to this interface and is managed using {@link PermissionIDs}.
 */
public interface PermissionReader {

    /**
     * Gets all permissions granted to this ID, such as a role.  If none, returns an empty set, not null,
     * just like {@link com.google.common.collect.Multimap#get(Object)}
     */
    Set<Permission> getPermissions(String id);

    /**
     * Gets the permission resolver for this reader.
     */
    PermissionResolver getPermissionResolver();
}
