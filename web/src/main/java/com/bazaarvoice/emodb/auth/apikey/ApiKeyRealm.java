package com.bazaarvoice.emodb.auth.apikey;

import com.bazaarvoice.emodb.auth.identity.AuthIdentityReader;
import com.bazaarvoice.emodb.auth.permissions.PermissionIDs;
import com.bazaarvoice.emodb.auth.permissions.PermissionReader;
import com.bazaarvoice.emodb.auth.shiro.AnonymousCredentialsMatcher;
import com.bazaarvoice.emodb.auth.shiro.AnonymousToken;
import com.bazaarvoice.emodb.auth.shiro.InvalidatableCacheManager;
import com.bazaarvoice.emodb.auth.shiro.PrincipalWithRoles;
import com.bazaarvoice.emodb.auth.shiro.RolePermissionSet;
import com.bazaarvoice.emodb.auth.shiro.SimpleRolePermissionSet;
import com.bazaarvoice.emodb.auth.shiro.ValidatingCacheManager;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

public class ApiKeyRealm extends AuthorizingRealm {

    private static final String DEFAULT_ROLES_CACHE_SUFFIX = ".rolesCache";
    private static final String DEFAULT_ID_AUTHORIZATION_CACHE_SUFFIX = ".idAuthorizationCache";

    private final Logger _log = LoggerFactory.getLogger(getClass());

    private final AuthIdentityReader<ApiKey> _authIdentityReader;
    private final PermissionReader _permissionReader;
    private final String _anonymousId;
    private final boolean _clearCaches;

    /**
     * Reserved AuthorizationInfo instance used to cache when an ID does not map to a user.
     * Necessary because our cache implementation cannot store nulls.  This instance has no roles or permissions.
     */
    private final AuthorizationInfo _nullAuthorizationInfo = new SimpleAuthorizationInfo(ImmutableSet.<String>of());

    // Cache for permissions by role.
    private Cache<String, RolePermissionSet> _rolesCache;
    // Cache for authorization info by user's ID.
    private Cache<String, AuthorizationInfo> _idAuthorizationCache;

    private String _rolesCacheName;
    private String _idAuthorizationCacheName;

    public ApiKeyRealm(String name, CacheManager cacheManager, AuthIdentityReader<ApiKey> authIdentityReader,
                       PermissionReader permissionReader, @Nullable String anonymousId) {
        super(null, AnonymousCredentialsMatcher.anonymousOrMatchUsing(new SimpleCredentialsMatcher()));


        _authIdentityReader = checkNotNull(authIdentityReader, "authIdentityReader");
        _permissionReader = checkNotNull(permissionReader, "permissionReader");
        _anonymousId = anonymousId;

        setName(checkNotNull(name, "name"));
        setAuthenticationTokenClass(ApiKeyAuthenticationToken.class);
        setPermissionResolver(permissionReader.getPermissionResolver());
        setRolePermissionResolver(createRolePermissionResolver());
        setCacheManager(prepareCacheManager(cacheManager));
        setAuthenticationCachingEnabled(true);
        setAuthorizationCachingEnabled(true);

        // By default Shiro calls clearCache() for each user when they are logged out in order to prevent stale
        // credentials from being cached.  However, if the cache manager implements InvalidatingCacheManager then it has
        // its own internal listeners that will invalidate the cache on any updates, making this behavior unnecessarily
        // expensive.
        _clearCaches = cacheManager != null && !(cacheManager instanceof InvalidatableCacheManager);
        _log.debug("Clearing of caches for realm {} is {}", name, _clearCaches ? "enabled" : "disabled");
    }

    /**
     * If necessary, wraps the raw cache manager with a validating facade.
     */
    private CacheManager prepareCacheManager(CacheManager cacheManager) {
        if (cacheManager == null || !(cacheManager instanceof InvalidatableCacheManager)) {
            return cacheManager;
        }

        return new ValidatingCacheManager(cacheManager) {
            @Nullable
            @Override
            protected CacheValidator<?, ?> getCacheValidatorForCache(String name) {
                String cacheName = getAuthenticationCacheName();
                if (cacheName != null && name.equals(cacheName)) {
                    return new ValidatingCacheManager.CacheValidator<Object, AuthenticationInfo>(Object.class, AuthenticationInfo.class) {
                        @Override
                        public boolean isCurrentValue(Object key, AuthenticationInfo value) {
                            String id;
                            if (AnonymousToken.isAnonymousPrincipal(key)) {
                                if (_anonymousId == null) {
                                    return false;
                                }
                                id = _anonymousId;
                            } else {
                                // For all non-anonymous users "key" is an API key
                                id = (String) key;
                            }

                            AuthenticationInfo authenticationInfo = getUncachedAuthenticationInfoForKey(id);
                            return Objects.equal(authenticationInfo, value);
                        }
                    };
                }

                cacheName = getAuthorizationCacheName();
                if (cacheName != null && name.equals(cacheName)) {
                    return new ValidatingCacheManager.CacheValidator<Object, AuthorizationInfo>(Object.class, AuthorizationInfo.class) {
                        @Override
                        public boolean isCurrentValue(Object key, AuthorizationInfo value) {
                            // Key is always a principal collection
                            PrincipalCollection principalCollection = (PrincipalCollection) key;
                            AuthorizationInfo authorizationInfo = getUncachedAuthorizationInfoFromPrincipals(principalCollection);
                            // Only the roles are used for authorization
                            return authorizationInfo != null && authorizationInfo.getRoles().equals(value.getRoles());
                        }
                    };
                }

                cacheName = getIdAuthorizationCacheName();
                if (cacheName != null && name.equals(cacheName)) {
                    return new ValidatingCacheManager.CacheValidator<String, AuthorizationInfo>(String.class, AuthorizationInfo.class) {
                        @Override
                        public boolean isCurrentValue(String key, AuthorizationInfo value) {
                            // Key is the identity's ID
                            AuthorizationInfo authorizationInfo = getUncachedAuthorizationInfoById(key);
                            // Only the roles are used for authorization
                            return authorizationInfo != null && authorizationInfo.getRoles().equals(value.getRoles());
                        }
                    };
                }

                cacheName = getRolesCacheName();
                if (cacheName != null && name.equals(cacheName)) {
                    return new ValidatingCacheManager.CacheValidator<String, RolePermissionSet>(String.class, RolePermissionSet.class) {
                        @Override
                        public boolean isCurrentValue(String key, RolePermissionSet value) {
                            // The key is the role name
                            Set<Permission> currentPermissions = _permissionReader.getPermissions(PermissionIDs.forRole(key));
                            return value.permissions().equals(currentPermissions);
                        }
                    };
                }

                return null;
            }
        };
    }

    @Override
    protected void onInit() {
        super.onInit();
        // Force creation of the roles cache on initialization
        getAvailableRolesCache();
        // Create a cache for IDs
        getAvailableIdAuthorizationCache();
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

        return getUncachedAuthenticationInfoForKey(id);
    }

    /**
     * Gets the authentication info for an API key from the source (not from cache).
     */
    private AuthenticationInfo getUncachedAuthenticationInfoForKey(String authenicationId) {
        ApiKey apiKey = _authIdentityReader.getIdentityByAuthenticationId(authenicationId);
        if (apiKey == null) {
            return null;
        }

        return createAuthenticationInfo(authenicationId, apiKey);
    }

    /**
     * Simple method to build and AuthenticationInfo instance from an API key.
     */
    private ApiKeyAuthenticationInfo createAuthenticationInfo(String authenticationId, ApiKey apiKey) {
        return new ApiKeyAuthenticationInfo(authenticationId, apiKey, getName());
    }

    /**
     * Gets the AuthorizationInfo that matches a token.  This method is only called if the info is not already
     * cached by the realm, so this method does not need to perform any further caching.
     */
    @Override
    protected AuthorizationInfo doGetAuthorizationInfo(PrincipalCollection principals) {

        AuthorizationInfo authorizationInfo = getUncachedAuthorizationInfoFromPrincipals(principals);

        Cache<String, AuthorizationInfo> idAuthorizationCache = getAvailableIdAuthorizationCache();
        if (idAuthorizationCache != null) {
            // Proactively cache any ID authorization info not already in cache
            for (PrincipalWithRoles principal : getPrincipalsFromPrincipalCollection(principals)) {
                if (idAuthorizationCache.get(principal.getId()) == null) {
                    cacheAuthorizationInfoById(principal.getId(), authorizationInfo);
                }
            }
        }

        return authorizationInfo;
    }

    @SuppressWarnings("unchecked")
    private Collection<PrincipalWithRoles> getPrincipalsFromPrincipalCollection(PrincipalCollection principals) {
        // Realm always returns PrincipalWithRoles for principals
        return (Collection<PrincipalWithRoles>) principals.fromRealm(getName());
    }

    /**
     * Gets the authorization info for an API key's principals from the source (not from cache).
     */
    private AuthorizationInfo getUncachedAuthorizationInfoFromPrincipals(PrincipalCollection principals) {
        SimpleAuthorizationInfo authInfo = new SimpleAuthorizationInfo();

        for (PrincipalWithRoles principal : getPrincipalsFromPrincipalCollection(principals)) {
            authInfo.addRoles(principal.getRoles());
        }

        return authInfo;
    }

    @Override
    public void setName(String name) {
        super.setName(name);
        // Set reasonable defaults for the role and ID authorization caches.
        _rolesCacheName = name + DEFAULT_ROLES_CACHE_SUFFIX;
        _idAuthorizationCacheName = name + DEFAULT_ID_AUTHORIZATION_CACHE_SUFFIX;
    }

    public String getRolesCacheName() {
        return _rolesCacheName;
    }

    public void setIdAuthorizationCacheName(String name) {
        _idAuthorizationCacheName = name + DEFAULT_ID_AUTHORIZATION_CACHE_SUFFIX;
        getAvailableIdAuthorizationCache();
    }

    public String getIdAuthorizationCacheName() {
        return _idAuthorizationCacheName;
    }

    protected Cache<String, RolePermissionSet> getAvailableRolesCache() {
        if(getCacheManager() == null) {
            return null;
        }

        if (_rolesCache == null) {
            String cacheName = getRolesCacheName();
            _rolesCache = getCacheManager().getCache(cacheName);
        }
        return _rolesCache;
    }

    public Cache<String, AuthorizationInfo> getIdAuthorizationCache() {
        return _idAuthorizationCache;
    }

    protected Cache<String, AuthorizationInfo> getAvailableIdAuthorizationCache() {
        if (getCacheManager() == null) {
            return null;
        }

        if (_idAuthorizationCache == null) {
            String cacheName = getIdAuthorizationCacheName();
            _idAuthorizationCache = getCacheManager().getCache(cacheName);
        }
        return _idAuthorizationCache;
    }

    private RolePermissionResolver createRolePermissionResolver() {
        return new RolePermissionResolver () {
            @Override
            public Collection<Permission> resolvePermissionsInRole(String role) {
                return getRolePermissions(role);
            }
        };
    }

    /**
     * Gets the permissions for a role.  If possible the permissions are cached for efficiency.
     */
    protected Collection<Permission> getRolePermissions(String role) {
        if (role == null) {
            return null;
        }
        Cache<String, RolePermissionSet> cache = getAvailableRolesCache();

        if (cache == null) {
            return _permissionReader.getPermissions(PermissionIDs.forRole(role));
        }

        RolePermissionSet rolePermissionSet = cache.get(role);

        if (rolePermissionSet == null) {
            Set<Permission> permissions = _permissionReader.getPermissions(PermissionIDs.forRole(role));
            rolePermissionSet = new SimpleRolePermissionSet(permissions);
            cache.put(role, rolePermissionSet);
        }

        return rolePermissionSet.permissions();
    }

    /**
     * Override default behavior to only clear cached authentication info if enabled.
     */
    @Override
    protected void clearCachedAuthenticationInfo(PrincipalCollection principals) {
        if (_clearCaches) {
            super.clearCachedAuthenticationInfo(principals);
        }
    }

    /**
     * Override default behavior to only clear cached authorization info if enabled.
     */
    @Override
    protected void clearCachedAuthorizationInfo(PrincipalCollection principals) {
        if (_clearCaches) {
            super.clearCachedAuthorizationInfo(principals);
        }
    }

    /**
     * Gets the authorization info for a user by their ID.  If possible the value is cached for efficient lookup.
     */
    @Nullable
    private AuthorizationInfo getAuthorizationInfoById(String id) {
        AuthorizationInfo authorizationInfo;

        // Search the cache first
        Cache<String, AuthorizationInfo> idAuthorizationCache = getAvailableIdAuthorizationCache();

        if (idAuthorizationCache != null) {
            authorizationInfo = idAuthorizationCache.get(id);

            if (authorizationInfo != null) {
                // Check whether it is the stand-in "null" cached value
                if (authorizationInfo != _nullAuthorizationInfo) {
                    _log.debug("Authorization info found cached for id {}", id);
                    return authorizationInfo;
                } else {
                    _log.debug("Authorization info previously cached as not found for id {}", id);
                    return null;
                }
            }
        }

        authorizationInfo = getUncachedAuthorizationInfoById(id);
        cacheAuthorizationInfoById(id, authorizationInfo);
        return authorizationInfo;
    }

    /**
     * If possible, this method caches the authorization info for an API key by its ID.  This may be called
     * either by an explicit call to get the authorization info by ID or as a side effect of loading the
     * authorization info by API key and proactive caching by ID.
     */
    private void cacheAuthorizationInfoById(String id, AuthorizationInfo authorizationInfo) {
        Cache<String, AuthorizationInfo> idAuthorizationCache = getAvailableIdAuthorizationCache();

        if (idAuthorizationCache != null) {
            idAuthorizationCache.put(id, authorizationInfo);
        }
    }

    /**
     * Gets the authorization info for an API key's ID from the source (not from cache).
     */
    private AuthorizationInfo getUncachedAuthorizationInfoById(String id) {
        // Retrieve the API key by ID
        ApiKey apiKey = _authIdentityReader.getIdentity(id);
        if (apiKey == null) {
            _log.debug("Authorization info requested for non-existent id {}", id);
            return _nullAuthorizationInfo;
        }

        return new SimpleAuthorizationInfo(ImmutableSet.copyOf(apiKey.getRoles()));
    }

    /**
     * Test for whether an API key has a specific permission using its ID.
     */
    public boolean hasPermissionById(String id, String permission) {
        Permission resolvedPermission = getPermissionResolver().resolvePermission(permission);
        return hasPermissionById(id, resolvedPermission);
    }

    /**
     * Test for whether an API key has a specific permission using its ID.
     */
    public boolean hasPermissionById(String id, Permission permission) {
        return hasPermissionsById(id, ImmutableList.of(permission));
    }

    /**
     * Test for whether an API key has specific permissions using its ID.
     */
    public boolean hasPermissionsById(String id, String... permissions) {
        List<Permission> resolvedPermissions = Lists.newArrayListWithCapacity(permissions.length);
        for (String permission : permissions) {
            resolvedPermissions.add(getPermissionResolver().resolvePermission(permission));
        }
        return hasPermissionsById(id, resolvedPermissions);
    }

    /**
     * Test for whether an API key has specific permissions using its ID.
     */
    public boolean hasPermissionsById(String id, Permission... permissions) {
        return hasPermissionsById(id, Arrays.asList(permissions));
    }

    /**
     * Test for whether an API key has specific permissions using its ID.
     */
    public boolean hasPermissionsById(String id, Collection<Permission> permissions) {
        AuthorizationInfo authorizationInfo = getAuthorizationInfoById(id);
        return authorizationInfo != null && isPermittedAll(permissions, authorizationInfo);
    }
}