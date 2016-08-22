package com.bazaarvoice.emodb.auth.jersey;

import com.bazaarvoice.emodb.auth.shiro.PrincipalWithRoles;
import org.apache.shiro.authz.Permission;
import org.apache.shiro.mgt.SecurityManager;
import org.apache.shiro.subject.PrincipalCollection;

import java.util.Arrays;

/**
 * Injectable for the authenticated subject.
 */
public class Subject {

    private final SecurityManager _securityManager;
    private final PrincipalCollection _principals;

    public Subject(SecurityManager securityManager, PrincipalCollection principals) {
        _securityManager = securityManager;
        _principals = principals;
    }

    public String getId() {
        return ((PrincipalWithRoles) _principals.getPrimaryPrincipal()).getName();
    }

    public boolean hasRole(String role) {
        return _securityManager.hasRole(_principals, role);
    }

    public boolean hasPermission(String permission) {
        return _securityManager.isPermitted(_principals, permission);
    }

    public boolean hasPermission(Permission permission) {
        return _securityManager.isPermitted(_principals, permission);
    }

    public boolean hasPermissions(String... permissions) {
        return _securityManager.isPermittedAll(_principals, permissions);
    }

    public boolean hasPermissions(Permission... permissions) {
        return _securityManager.isPermittedAll(_principals, Arrays.<Permission>asList(permissions));
    }
}
