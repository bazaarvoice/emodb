package com.bazaarvoice.emodb.auth.util;

import com.bazaarvoice.emodb.common.dropwizard.guice.ServerCluster;
import com.google.inject.Inject;

import static java.util.Objects.requireNonNull;

/**
 * This class is used to encrypt and decrypt API keys which must be stored in the system's config.yaml file.
 * It serves as a thin injectable wrapper around a {@link CredentialEncrypter}.
 */
public class ApiKeyEncryption {

    private final CredentialEncrypter _credentialEncrypter;

    @Inject
    public ApiKeyEncryption(@ServerCluster String cluster) {
        _credentialEncrypter = new CredentialEncrypter(requireNonNull(cluster, "cluster"));
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
