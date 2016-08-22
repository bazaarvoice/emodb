package com.bazaarvoice.emodb.sor.api.deser;

import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.KeyDeserializer;

import java.io.IOException;
import java.util.UUID;

/**
 * Jackson adapter for deserializing JSON maps that use {@link UUID} objects for keys.
 */
public final class UUIDKeyDeserializer extends KeyDeserializer {

    @Override
    public Object deserializeKey(String key, DeserializationContext ctxt) throws IOException {
        if (key == null) {
            return null;
        }
        try {
            return UUID.fromString(key);
        } catch (Exception e) {
            throw ctxt.weirdKeyException(UUID.class, key, "not a valid representation: " + e);
        }
    }
}
