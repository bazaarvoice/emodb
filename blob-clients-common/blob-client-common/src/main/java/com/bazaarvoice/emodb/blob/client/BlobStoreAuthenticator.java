package com.bazaarvoice.emodb.blob.client;

import com.bazaarvoice.emodb.auth.InvalidCredentialException;
import com.bazaarvoice.emodb.auth.proxy.CachingAuthenticatingProxy;
import com.bazaarvoice.emodb.blob.api.AuthBlobStore;
import com.bazaarvoice.emodb.blob.api.BlobStore;

/**
 *
 */
public class BlobStoreAuthenticator extends CachingAuthenticatingProxy<BlobStore, String> {

    private final AuthBlobStore _authBlobStore;

    public static BlobStoreAuthenticator proxied(AuthBlobStore secureDatabus) {
        return new BlobStoreAuthenticator(secureDatabus);
    }

    private BlobStoreAuthenticator(AuthBlobStore authBlobStore) {
        _authBlobStore = authBlobStore;
    }

    @Override
    protected String validateCredentials(String apiKey) throws InvalidCredentialException {
        if (apiKey.isEmpty()) {
            throw new InvalidCredentialException("API key cannot be empty");
        }
        return apiKey;
    }

    @Override
    protected BlobStore createInstanceWithCredentials(String apiKey) {
        return new BlobStoreAuthenticatorProxy(_authBlobStore, apiKey);
    }
}

