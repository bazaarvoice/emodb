package com.bazaarvoice.emodb.auth.shiro;

import org.apache.shiro.authz.Permission;

import java.util.Set;

/**
 * Interface for a set of role permissions.  Used instead of <code>Set&lt;Permission&gt;</code> to allow for a
 * type-safe self-validating implementation used by validating caches.
 */
public interface RolePermissionSet {

    public Set<Permission> permissions();
}
