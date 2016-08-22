package com.bazaarvoice.emodb.auth.apikey;

import com.bazaarvoice.emodb.auth.shiro.PrincipalWithRoles;
import org.apache.shiro.authc.AuthenticationInfo;
import org.apache.shiro.subject.PrincipalCollection;
import org.apache.shiro.subject.SimplePrincipalCollection;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.String.format;

/**
 * {@link AuthenticationInfo} implementation for ApiKeys.  Since API keys do not have credentials (the presence
 * of a valid ApiKey in the request constitutes authentication) the principal's ID is also used as the credentials.
 */
public class ApiKeyAuthenticationInfo implements AuthenticationInfo {

    private final PrincipalCollection _principals;
    private final String _credentials;

    public ApiKeyAuthenticationInfo(ApiKey apiKey, String realm) {
        checkNotNull(apiKey, "apiKey");
        checkNotNull(realm, "realm");
        PrincipalWithRoles principal = new PrincipalWithRoles(apiKey.getId(), apiKey.getRoles());
        _principals = new SimplePrincipalCollection(principal, realm);
        // Use the API key as the credentials
        _credentials = apiKey.getId();
    }

    @Override
    public PrincipalCollection getPrincipals() {
        return _principals;
    }

    @Override
    public String getCredentials() {
        return _credentials;
    }

    @Override
    public String toString() {
        return format("%s{%s}", getClass().getSimpleName(), ((PrincipalWithRoles) _principals.getPrimaryPrincipal()).getName());
    }
}
