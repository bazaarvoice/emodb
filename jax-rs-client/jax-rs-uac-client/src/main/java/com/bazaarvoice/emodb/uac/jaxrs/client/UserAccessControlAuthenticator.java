package com.bazaarvoice.emodb.uac.jaxrs.client;

import com.bazaarvoice.emodb.auth.InvalidCredentialException;
import com.bazaarvoice.emodb.auth.proxy.CachingAuthenticatingProxy;
import com.bazaarvoice.emodb.uac.api.AuthUserAccessControl;
import com.bazaarvoice.emodb.uac.api.UserAccessControl;

/**
 * Utility class for replacing a {@link AuthUserAccessControl} instance with the simpler {@link UserAccessControl} interface.
 * @see CachingAuthenticatingProxy
 */
public class UserAccessControlAuthenticator extends CachingAuthenticatingProxy<UserAccessControl, String> {

    private final AuthUserAccessControl _authUserAccessControl;

    public static UserAccessControlAuthenticator proxied(AuthUserAccessControl authUserAccessControl) {
        return new UserAccessControlAuthenticator(authUserAccessControl);
    }

    private UserAccessControlAuthenticator(AuthUserAccessControl authUserAccessControl) {
        _authUserAccessControl = authUserAccessControl;
    }

    @Override
    protected String validateCredentials(String apiKey) throws InvalidCredentialException {
        if (apiKey.isEmpty()) {
            throw new InvalidCredentialException("API key cannot be empty");
        }
        return apiKey;
    }

    @Override
    protected UserAccessControl createInstanceWithCredentials(String apiKey) {
        return new UserAccessControlAuthenticatorProxy(_authUserAccessControl, apiKey);
    }
}
