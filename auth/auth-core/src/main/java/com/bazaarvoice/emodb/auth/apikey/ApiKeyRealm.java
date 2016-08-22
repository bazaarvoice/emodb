package com.bazaarvoice.emodb.auth.apikey;

import com.bazaarvoice.emodb.auth.identity.AuthIdentityManager;
import com.bazaarvoice.emodb.auth.permissions.PermissionManager;
import com.bazaarvoice.emodb.auth.shiro.AnonymousCredentialsMatcher;
import com.bazaarvoice.emodb.auth.shiro.AnonymousToken;
import com.bazaarvoice.emodb.auth.shiro.PrincipalWithRoles;
import org.apache.shiro.authc.AuthenticationException;
import org.apache.shiro.authc.AuthenticationInfo;
import org.apache.shiro.authc.AuthenticationToken;
import org.apache.shiro.authc.credential.SimpleCredentialsMatcher;
import org.apache.shiro.authz.AuthorizationInfo;
import org.apache.shiro.authz.Permission;
import org.apache.shiro.authz.SimpleAuthorizationInfo;
import org.apache.shiro.authz.permission.RolePermissionResolver;
import org.apache.shiro.cache.Cache;
import org.apache.shiro.cache.CacheManager;
import org.apache.shiro.realm.AuthorizingRealm;
import org.apache.shiro.subject.PrincipalCollection;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

public class ApiKeyRealm extends AuthorizingRealm {

    private final AuthIdentityManager<ApiKey> _authIdentityManager;
    private final PermissionManager _permissionManager;
    private final String _anonymousId;

    private static final String DEFAULT_ROLES_CACHE_SUFFIX = ".rolesCache";
    private Cache<String, Set<Permission>> _rolesCache;
    private String _rolesCacheName;

    public ApiKeyRealm(String name, CacheManager cacheManager, AuthIdentityManager<ApiKey> authIdentityManager,
                       PermissionManager permissionManager, @Nullable String anonymousId) {
        super(cacheManager, AnonymousCredentialsMatcher.anonymousOrMatchUsing(new SimpleCredentialsMatcher()));

        _authIdentityManager = checkNotNull(authIdentityManager, "authIdentityManager");
        _permissionManager = checkNotNull(permissionManager, "permissionManager");
        _anonymousId = anonymousId;

        setName(checkNotNull(name, "name"));
        setAuthenticationTokenClass(ApiKeyAuthenticationToken.class);
        setPermissionResolver(permissionManager.getPermissionResolver());
        setRolePermissionResolver(createRolePermissionResolver());
        setAuthenticationCachingEnabled(true);
        setAuthorizationCachingEnabled(true);
    }

    @Override
    protected void onInit() {
        super.onInit();
        // Force creation of the roles cache on initialization
        getAvailableRolesCache();
    }

    /**
     * Override the parent method to also accept anonymous tokens
     */
    @Override
    public boolean supports(AuthenticationToken token) {
        return super.supports(token) || (_anonymousId != null && AnonymousToken.isAnonymous(token));
    }

    /**
     * Gets the AuthenticationInfo that matches a token.  This method is only called if the info is not already
     * cached by the realm, so this method does not need to perform any further caching.
     */
    @SuppressWarnings("unchecked")
    @Override
    protected AuthenticationInfo doGetAuthenticationInfo(AuthenticationToken token)
            throws AuthenticationException {
        String id;

        if (AnonymousToken.isAnonymous(token)) {
            // Only continue if an anonymous identity has been set
            if (_anonymousId != null) {
                id = _anonymousId;
            } else {
                return null;
            }
        } else {
            id = ((ApiKeyAuthenticationToken) token).getPrincipal();
        }

        ApiKey apiKey = _authIdentityManager.getIdentity(id);
        if (apiKey == null) {
            return null;
        }

        return new ApiKeyAuthenticationInfo(apiKey, getName());
    }

    /**
     * Gets the AuthorizationInfo that matches a token.  This method is only called if the info is not already
     * cached by the realm, so this method does not need to perform any further caching.
     */
    @SuppressWarnings("unchecked")
    @Override
    protected AuthorizationInfo doGetAuthorizationInfo(PrincipalCollection principals) {
        SimpleAuthorizationInfo authInfo = new SimpleAuthorizationInfo();

        // Realm always returns PrincipalWithRoles for principals
        Collection<PrincipalWithRoles> realmPrincipals = (Collection<PrincipalWithRoles>) principals.fromRealm(getName());

        for (PrincipalWithRoles principal : realmPrincipals) {
            authInfo.addRoles(principal.getRoles());
        }

        return authInfo;
    }

    @Override
    public void setName(String name) {
        super.setName(name);
        _rolesCacheName = name + DEFAULT_ROLES_CACHE_SUFFIX;
        getAvailableRolesCache();
    }

    public String getRolesCacheName() {
        return _rolesCacheName;
    }

    protected Cache<String, Set<Permission>> getAvailableRolesCache() {
        if(getCacheManager() == null) {
            return null;
        }

        if (_rolesCache == null) {
            String cacheName = getRolesCacheName();
            _rolesCache = getCacheManager().getCache(cacheName);
        }
        return _rolesCache;
    }

    private RolePermissionResolver createRolePermissionResolver() {
        return new RolePermissionResolver () {
            @Override
            public Collection<Permission> resolvePermissionsInRole(String role) {
                return getPermissions(role);
            }
        };
    }

    protected Collection<Permission> getPermissions(String role) {
        if (role == null) {
            return null;
        }
        Cache<String, Set<Permission>> cache = getAvailableRolesCache();

        if (cache == null) {
            return _permissionManager.getAllForRole(role);
        }

        Set<Permission> cachedPermissions, permissions = cachedPermissions = cache.get(role);
        while (cachedPermissions == null || ! cachedPermissions.equals(permissions)) {
            if(permissions != null) {
                cache.put(role, permissions);
            }
            permissions = _permissionManager.getAllForRole(role);
            cachedPermissions = cache.get(role);
        }
        return permissions;
    }
}