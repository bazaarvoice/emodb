package com.bazaarvoice.emodb.auth.util;

import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;

public class ApiKeyEncryptionTest {

    private final static String CLUSTER = "test_cluster";

    private BiMap<String, String> _rawToEncryptedKeyMap;

    @BeforeClass
    public void createRawToEncryptedKeyMap() {
        // The following map has been pre-computed using "test_cluster" as the cluster name
        _rawToEncryptedKeyMap = ImmutableBiMap.<String, String>builder()
                .put("key-0", "UmInSDEtplM3CRWwdEYs4A/K/JXbvgwxzjvIujtj5u00fEr02GazYhSDU/a8psxDv9izuhsLllOgRJYqSVpJSw")
                .put("key-1", "6vZNCZtWgFu0Kc//drkUDLvXh6UBFbofRh2wiCi5c08uHiHx8dKkTp2d53hA4k7+2msznOEuhvth4gaVpK/AJw")
                .put("key-2", "e8QVbTrXAfI4GyMn/cc1hNh98XLDX2P6PrTZaHU6FU3tYk93dHo+a2zPhaCYdVCNScRDwylC2yiNh6euPqfj5w")
                .put("key-3", "QVRET2/mqrXY3w/6BK4kFLocgSxahBOekeCrj/cx6D8YAZ1Fno/rtev6mX1HCUtutii7LyXl+fvYE5JMJj17UQ")
                .put("key-4", "co8sivg9e+WID5uH4+06rqCXrV80aALfLufHK/xDIa1QoUPsVKNeERPSWtX0WEmEEWrKwASxlWfepLRWdH3Obg")
                .build();
    }

    @Test
    public void testEncryptKey() {
        ApiKeyEncryption apiKeyEncryption = new ApiKeyEncryption(CLUSTER);

        for (Map.Entry<String,String> entry : _rawToEncryptedKeyMap.entrySet()) {
            String encrypted = apiKeyEncryption.encrypt(entry.getKey());
            assertEquals(encrypted, entry.getValue());
        }
    }

    @Test
    public void testDecryptKey() {
        ApiKeyEncryption apiKeyEncryption = new ApiKeyEncryption(CLUSTER);

        for (Map.Entry<String,String> entry : _rawToEncryptedKeyMap.inverse().entrySet()) {
            String decrypted = apiKeyEncryption.decrypt(entry.getKey());
            assertEquals(decrypted, entry.getValue());
        }
    }

    @Test
    public void testUniqueKeyPerCluster() {
        ApiKeyEncryption apiKeyEncryption = new ApiKeyEncryption("different_cluster");

        for (Map.Entry<String,String> entry : _rawToEncryptedKeyMap.entrySet()) {
            String encrypted = apiKeyEncryption.encrypt(entry.getKey());
            assertNotEquals(encrypted, entry.getValue());
        }
    }

    @Test
    public void testPotentiallyEncryptedApiKeys() {
        assertTrue(ApiKeyEncryption.isPotentiallyEncryptedApiKey("UmInSDEtplM3CRWwdEYs4A/K/JXbvgwxzjvIujtj5u00fEr02GazYhSDU/a8psxDv9izuhsLllOgRJYqSVpJSw"));
        // Not Base64 encoded
        assertFalse(ApiKeyEncryption.isPotentiallyEncryptedApiKey("plaintext"));
        // Base64 decodes to an invalid length
        assertFalse(ApiKeyEncryption.isPotentiallyEncryptedApiKey("SDhR0kBddiXAevqIPCt3iSuEvtg"));
    }
}
