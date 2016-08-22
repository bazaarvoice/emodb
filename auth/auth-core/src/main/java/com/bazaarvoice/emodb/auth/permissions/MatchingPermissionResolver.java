package com.bazaarvoice.emodb.auth.permissions;

import org.apache.shiro.authz.permission.PermissionResolver;

public class MatchingPermissionResolver implements PermissionResolver {

    @Override
    public MatchingPermission resolvePermission(String permissionString) {
        return new MatchingPermission(permissionString);
    }
}
