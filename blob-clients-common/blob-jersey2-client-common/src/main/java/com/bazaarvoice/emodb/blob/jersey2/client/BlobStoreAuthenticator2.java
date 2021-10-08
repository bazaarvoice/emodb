package com.bazaarvoice.emodb.blob.jersey2.client;

import com.bazaarvoice.emodb.auth.InvalidCredentialException;
import com.bazaarvoice.emodb.auth.proxy.CachingAuthenticatingProxy;
import com.bazaarvoice.emodb.blob.api.AuthBlobStore;
import com.bazaarvoice.emodb.blob.api.BlobStore;

/**
 *
 */
public class BlobStoreAuthenticator2 extends CachingAuthenticatingProxy<BlobStore, String> {

    private final AuthBlobStore _authBlobStore;

    public static BlobStoreAuthenticator2 proxied(AuthBlobStore secureDatabus) {
        return new BlobStoreAuthenticator2(secureDatabus);
    }

    private BlobStoreAuthenticator2(AuthBlobStore authBlobStore) {
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
        return new BlobStoreJersey2AuthenticatorProxy(_authBlobStore, apiKey);
    }
}

