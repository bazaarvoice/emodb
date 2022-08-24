package com.bazaarvoice.emodb.sor.client;

import com.bazaarvoice.emodb.auth.InvalidCredentialException;
import com.bazaarvoice.emodb.auth.proxy.CachingAuthenticatingProxy;
import com.bazaarvoice.emodb.auth.util.CredentialEncrypter;
import com.bazaarvoice.emodb.sor.api.AuthDataStore;
import com.bazaarvoice.emodb.sor.api.DataStore;

import static java.util.Objects.requireNonNull;

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
        return validateApiKey(apiKey);
    }

    @Override
    protected DataStore createInstanceWithCredentials(String apiKey) {
        return new DataStoreAuthenticatorProxy(_authDataStore, apiKey);
    }

    private static String validateApiKey(String apiKey) throws InvalidCredentialException {
        requireNonNull(apiKey, "API key is required");
        if (apiKey.isEmpty()) {
            throw new InvalidCredentialException("API key cannot be empty");
        }
        if (CredentialEncrypter.isPotentiallyEncryptedString(apiKey)) {
            throw new InvalidCredentialException("API Key is encrypted, please decrypt it");
        }
        return apiKey;
    }
}
