package com.bazaarvoice.emodb.sor.client;

import com.bazaarvoice.emodb.auth.InvalidCredentialException;
import com.bazaarvoice.emodb.auth.proxy.CachingAuthenticatingProxy;
import com.bazaarvoice.emodb.sor.api.AuthDataStore;
import com.bazaarvoice.emodb.sor.api.DataStore;

public class DataStoreAuthenticator extends CachingAuthenticatingProxy<DataStore, String> {

    private final AuthDataStore _authDataStore;

    public static DataStoreAuthenticator proxied(AuthDataStore authDataStore) {
        return new DataStoreAuthenticator(authDataStore);
    }

    private DataStoreAuthenticator(AuthDataStore authDataStore) {
        _authDataStore = authDataStore;
    }

    @Override
    protected String validateCredentials(String apiKey) throws InvalidCredentialException {
        if (apiKey.isEmpty()) {
            throw new InvalidCredentialException("API key cannot be empty");
        }
        return apiKey;
    }

    @Override
    protected DataStore createInstanceWithCredentials(String apiKey) {
        return new DataStoreAuthenticatorProxy(_authDataStore, apiKey);
    }
}
