package com.bazaarvoice.emodb.common.json.deferred;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;

/**
 * Serializer for {@link LazyJsonMap}.  The serialization logic is contained in {@link LazyJsonMap#writeTo(JsonGenerator)};
 * this object merely satisfies the {@link JsonSerializer} class hierarchy.
 */
public class LazyJsonMapSerializer extends JsonSerializer<LazyJsonMap> {
    @Override
    public void serialize(LazyJsonMap lazyJsonMap, JsonGenerator generator, SerializerProvider provider)
            throws IOException, JsonProcessingException {
        lazyJsonMap.writeTo(generator);
    }
}
