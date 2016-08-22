package com.bazaarvoice.emodb.auth.util;

import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.hash.Hashing;
import com.google.common.io.BaseEncoding;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Arrays;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Utility class for encrypting and decrypting credentials.  This is useful when credentials need to be explicitly
 * stored somewhere but for security reasons should not be in plain-text, such as in configuration files.
 */
public class CredentialEncrypter {

    private final static String ALGORITHM = "AES";
    private final static String CIPHER = "AES/CBC/PKCS5Padding";
    private final static String SEED_PRNG = "SHA1PRNG";
    private final static int MINIMUM_LENGTH = 48;

    // Seed used to create the secret key.  This isn't very secure, but it's better than nothing.
    private final static String SEED = "XrOo5o4lQGlZxKU/e4BWZVil3ot8bkBSTl1f6BJ5QPX1yDVw7hQc9S+9HQxKvJWqa2I6cd7ZopqFkCnc";

    private transient SecretKey _key;
    private IvParameterSpec _iv;

    /**
     * Creates a new instance initialized from a byte array.  Two instances created with the same initialization bytes
     * will always produce the same encrypted values when given the same input.
     */
    public CredentialEncrypter(byte[] initializationBytes) {
        checkNotNull(initializationBytes, "initializationBytes");

        _key = createKey(initializationBytes);
        _iv = createInitializationVector(initializationBytes);
    }

    /**
     * Convenience constructor to initialize from the a string converted to a UTF-8 byte array.
     */
    public CredentialEncrypter(String initializationString) {
        this(checkNotNull(initializationString, "initializationString").getBytes(Charsets.UTF_8));
    }

    private SecretKey createKey(byte[] initializationBytes) {
        try {
            // Create the encryption key from a SecureRandom seeded with the secret SEED value and initialization bytes.
            // This way different initialization values will generate different keys.
            SecureRandom random = SecureRandom.getInstance(SEED_PRNG);
            random.setSeed(BaseEncoding.base64().decode(SEED));
            random.setSeed(initializationBytes);

            byte[] keyBytes = new byte[16];
            random.nextBytes(keyBytes);

            return new SecretKeySpec(keyBytes, ALGORITHM);
        } catch (NoSuchAlgorithmException e) {
            // This shouldn't happen since SHA1PRNG is supported by all JVMs.
            throw Throwables.propagate(e);
        }
    }

    private IvParameterSpec createInitializationVector(byte[] initializationBytes) {
        // Create an initialization vector from the byte array
        byte[] hash = Hashing.sha256().hashBytes(initializationBytes).asBytes();
        // Reduce hash from 256 to 128 bits
        for (int i=0; i < 16; i++) {
            hash[i] = (byte) (hash[i] ^ hash[i+16]);
        }
        return new IvParameterSpec(Arrays.copyOfRange(hash, 0, 16));
    }

    /**
     * Encrypts a byte array and returns a string representation of the encrypted value.  The original byte array
     * can be recovered using {@link #decryptBytes(String)}.
     */
    public String encryptBytes(byte[] credentials) {
        checkNotNull(credentials, "credentials");

        try {
            int length = credentials.length;
            byte[] plaintextBytes = ByteBuffer.wrap(new byte[Math.max(length + 4, MINIMUM_LENGTH)])
                    .putInt(credentials.length)
                    .put(credentials)
                    .array();

            Cipher cipher = Cipher.getInstance(CIPHER);
            cipher.init(Cipher.ENCRYPT_MODE, _key, _iv);
            byte[] encryptedApiKey = cipher.doFinal(plaintextBytes);
            return BaseEncoding.base64().omitPadding().encode(encryptedApiKey);
        } catch (Throwable t) {
            // This shouldn't happen since AES is supported by all JVMs.
            throw Throwables.propagate(t);
        }
    }

    /**
     * Encrypts a String and returns a string representation of the encrypted value.  The original String
     * can be recovered using {@link #decryptString(String)}.
     */
    public String encryptString(String credentials) {
        byte[] credentialBytes = checkNotNull(credentials, "credentials").getBytes(Charsets.UTF_8);
        return encryptBytes(credentialBytes);
    }

    /**
     * Recovers the plaintext bytes from the encrypted value returned by {@link #encryptBytes(byte[])}.
     */
    public byte[] decryptBytes(String encryptedCredentials) {
        checkNotNull(encryptedCredentials, "encryptedCredentials");

        try {
            byte[] encryptedBytes = BaseEncoding.base64().omitPadding().decode(encryptedCredentials);
            Cipher cipher = Cipher.getInstance(CIPHER);
            cipher.init(Cipher.DECRYPT_MODE, _key, _iv);
            ByteBuffer plaintextBytes = ByteBuffer.wrap(cipher.doFinal(encryptedBytes));
            int length = plaintextBytes.getInt();
            byte[] apiKey = new byte[length];
            plaintextBytes.get(apiKey);
            return apiKey;
        } catch (Throwable t) {
            // This shouldn't happen since AES is supported by all JVMs.
            throw Throwables.propagate(t);
        }
    }

    /**
     * Recovers the plaintext String from the encrypted value returned by {@link #encryptString(String)}.
     */
    public String decryptString(String encryptedCredentials) {
        byte[] plaintextBytes = decryptBytes(encryptedCredentials);
        return new String(plaintextBytes, Charsets.UTF_8);
    }

    /**
     * Returns true if the provided bytes _could_ be encrypted credentials, even if they can't be decrypted
     * by a specific instance.
     */
    public static boolean isPotentiallyEncryptedBytes(byte[] bytes) {
        checkNotNull(bytes, "bytes");

        // The number of bytes is a non-zero multiple of the block size.
        try {
            return bytes.length != 0 && bytes.length % Cipher.getInstance(CIPHER).getBlockSize() == 0;
        } catch (Throwable t) {
            // This shouldn't happen since AES is supported by all JVMs.
            throw Throwables.propagate(t);
        }
    }

    /**
     * Returns true if the provided String _could_ be encrypted credentials, even if it can't be decrypted
     * by a specific instance.
     */
    public static boolean isPotentiallyEncryptedString(String string) {
        checkNotNull(string, "string");

        // String is base64 encoded
        byte[] encryptedBytes;
        try {
            encryptedBytes = BaseEncoding.base64().omitPadding().decode(string);
        } catch (IllegalArgumentException e) {
            return false;
        }

        return isPotentiallyEncryptedBytes(encryptedBytes);
    }
}
