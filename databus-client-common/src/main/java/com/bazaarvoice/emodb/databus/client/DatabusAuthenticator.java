package com.bazaarvoice.emodb.databus.client;

import com.bazaarvoice.emodb.auth.InvalidCredentialException;
import com.bazaarvoice.emodb.auth.proxy.CachingAuthenticatingProxy;
import com.bazaarvoice.emodb.databus.api.AuthDatabus;
import com.bazaarvoice.emodb.databus.api.Databus;

public class DatabusAuthenticator extends CachingAuthenticatingProxy<Databus, String> {

    private final AuthDatabus _authDatabus;

    public static DatabusAuthenticator proxied(AuthDatabus authDatabus) {
        return new DatabusAuthenticator(authDatabus);
    }

    private DatabusAuthenticator(AuthDatabus authDatabus) {
        _authDatabus = authDatabus;
    }

    @Override
    protected String validateCredentials(String apiKey) throws InvalidCredentialException {
        if (apiKey.isEmpty()) {
            throw new InvalidCredentialException("API key cannot be empty");
        }
        return apiKey;
    }

    @Override
    protected Databus createInstanceWithCredentials(String apiKey) {
        return new DatabusAuthenticatorProxy(_authDatabus, apiKey);
    }
}
