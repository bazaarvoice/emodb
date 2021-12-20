package com.bazaarvoice.emodb.auth.shiro;


import org.apache.shiro.authz.Permission;

import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * Trivial implementation of {@link RolePermissionSet} using a Set.
 */
public class SimpleRolePermissionSet implements RolePermissionSet {

    private Set<Permission> _permissions;

    public SimpleRolePermissionSet(Set<Permission> permissions) {
        _permissions = requireNonNull(permissions, "permissions");
    }

    public Set<Permission> permissions() {
        return _permissions;
    }

    @Override
    public boolean equals(Object o) {
        return this == o || (o instanceof RolePermissionSet && ((RolePermissionSet) o).permissions().equals(_permissions));
    }

    @Override
    public int hashCode() {
        return _permissions.hashCode();
    }
}
