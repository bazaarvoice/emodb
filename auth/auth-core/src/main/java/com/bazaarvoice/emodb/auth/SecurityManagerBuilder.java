package com.bazaarvoice.emodb.auth;

import com.bazaarvoice.emodb.auth.apikey.ApiKey;
import com.bazaarvoice.emodb.auth.apikey.ApiKeyRealm;
import com.bazaarvoice.emodb.auth.apikey.ApiKeySecurityManager;
import com.bazaarvoice.emodb.auth.identity.AuthIdentityReader;
import com.bazaarvoice.emodb.auth.permissions.PermissionReader;
import com.bazaarvoice.emodb.auth.shiro.GuavaCacheManager;
import org.apache.shiro.cache.CacheManager;
import org.apache.shiro.mgt.SecurityManager;
import org.apache.shiro.util.LifecycleUtils;

import javax.annotation.Nullable;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Creates a {@link SecurityManager} which can be used for performing authentication and authorization.
 */
public class SecurityManagerBuilder {

    private final static String DEFAULT_REALM_NAME = "DefaultRealm";

    protected String _realmName = DEFAULT_REALM_NAME;
    protected AuthIdentityReader<ApiKey> _authIdentityReader;
    protected PermissionReader _permissionReader;
    protected String _anonymousId;
    protected CacheManager _cacheManager;

    public SecurityManagerBuilder() {
        // empty
    }

    /**
     * Starts a new configuration builder.
     */
    public static SecurityManagerBuilder create() {
        return new SecurityManagerBuilder();
    }

    public SecurityManagerBuilder withRealmName(String realmName) {
        _realmName = checkNotNull(realmName, "realmName");
        return this;
    }

    public SecurityManagerBuilder withAuthIdentityReader(AuthIdentityReader<ApiKey> authIdentityReader) {
        _authIdentityReader = checkNotNull(authIdentityReader, "authIdentityReader");
        return this;
    }

    public SecurityManagerBuilder withPermissionReader(PermissionReader permissionReader) {
        _permissionReader = checkNotNull(permissionReader, "permissionReader");
        return this;
    }

    public SecurityManagerBuilder withCacheManager(CacheManager cacheManager) {
        _cacheManager = checkNotNull(cacheManager, "cacheManager");
        return this;
    }

    /**
     * If a request comes in with no authentication information at all (such as will no headers) then an ID
     * can be associated with these requests to provide controlled access to anonymous requests.  If the anonymous
     * ID is null (which is the default) then any request that requires authentication will be forbidden.
     */
    public SecurityManagerBuilder withAnonymousAccessAs(@Nullable String id) {
        _anonymousId = id;
        return this;
    }

    public EmoSecurityManager build() {
        checkNotNull(_authIdentityReader, "authIdentityManager not set");
        checkNotNull(_permissionReader, "permissionManager not set");
        if(_cacheManager == null) { // intended for test use
            _cacheManager = new GuavaCacheManager(null);
        }
        ApiKeyRealm realm = new ApiKeyRealm(_realmName, _cacheManager, _authIdentityReader, _permissionReader, _anonymousId);
        LifecycleUtils.init(realm);

        return new ApiKeySecurityManager(realm);
    }
}
