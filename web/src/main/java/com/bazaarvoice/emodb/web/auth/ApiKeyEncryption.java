package com.bazaarvoice.emodb.web.auth;

import com.bazaarvoice.emodb.auth.util.CredentialEncrypter;
import com.bazaarvoice.emodb.common.dropwizard.guice.ServerCluster;
import com.google.inject.Inject;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * This class is used to encrypt and decrypt API keys which must be stored in the system's config.yaml file.
 * It serves as a thin injectable wrapper around a {@link CredentialEncrypter}.
 */
public class ApiKeyEncryption {

    private final CredentialEncrypter _credentialEncrypter;

    @Inject
    public ApiKeyEncryption(@ServerCluster String cluster) {
        _credentialEncrypter = new CredentialEncrypter(checkNotNull(cluster, "cluster"));
    }

    public String encrypt(String apiKey) {
        return _credentialEncrypter.encryptString(apiKey);
    }

    public String decrypt(String encryptedApiKey) {
        return _credentialEncrypter.decryptString(encryptedApiKey);
    }

    public static boolean isPotentiallyEncryptedApiKey(String encryptedApiKey) {
        return CredentialEncrypter.isPotentiallyEncryptedString(encryptedApiKey);
    }
}
