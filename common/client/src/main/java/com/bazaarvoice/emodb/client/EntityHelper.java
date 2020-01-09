package com.bazaarvoice.emodb.client;

import com.bazaarvoice.emodb.common.json.JsonHelper;
import com.bazaarvoice.emodb.common.json.JsonStreamingArrayParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.PeekingIterator;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Iterator;

/**
 * Helper class for reading entity responses from the EmoDB API.
 */
abstract public class EntityHelper {

    /**
     * Reads the entity input stream and deserializes the JSON content to the given class.
     */
    public static <T> T getEntity(InputStream in, Class<T> clazz) {
        if (clazz == InputStream.class) {
            //noinspection unchecked
            return (T) clazz;
        }

        try {
            return JsonHelper.readJson(in, clazz);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Reads the entity input stream and deserializes the JSON content to the given type reference.  If the
     * type reference is for an iterator then a {@link JsonStreamingArrayParser} will be returned to stream
     * the deserialized contents to the caller.
     */
    @SuppressWarnings("unchecked")
    public static <T> T getEntity(InputStream in, TypeReference<T> reference) {
        // If the entity type is an iterator then return a streaming iterator to prevent Jackson instantiating
        // the entire response in memory.
        Type type = reference.getType();
        if (type instanceof ParameterizedType &&
                Iterator.class.isAssignableFrom((Class<?>) ((ParameterizedType) type).getRawType())) {
            Type elementType = ((ParameterizedType) type).getActualTypeArguments()[0];
            return (T) streamingIterator(in, typeReferenceFrom(elementType));
        }

        // Use Jackson to deserialize the input stream.
        try {
            return JsonHelper.readJson(in, reference);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Internal method for creating a TypeReference from a Java Type.
     */
    private static <T> TypeReference<T> typeReferenceFrom(final Type type) {
        return new TypeReference<T>() {
            @Override
            public Type getType() {
                return type;
            }
        };
    }

    private static <T> Iterator<T> streamingIterator(InputStream in, TypeReference<T> typeReference) {
        PeekingIterator<T> iter = new JsonStreamingArrayParser<>(in, typeReference);

        // Fetch the first element in the result stream immediately, while still wrapped by the Ostrich retry logic.
        // If we can't get the first element then Ostrich should retry immediately.  If we fail to get subsequent
        // elements then clients must catch JsonStreamingEOFException and deal with it themselves.  They are highly
        // encouraged to use the DataStoreStreaming class which handles the restart logic automatically.
        if (iter.hasNext()) {
            iter.peek();
        }

        return iter;
    }
}
