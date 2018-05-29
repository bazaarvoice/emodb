package com.bazaarvoice.emodb.common.json;

import com.bazaarvoice.emodb.streaming.AbstractSpliterator;
import com.bazaarvoice.emodb.streaming.SpliteratorIterator;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Type;
import java.util.Spliterator;

/**
 * Incrementally parses an JSON array of objects, returning the results as an iterator.  Allows parsing an
 * arbitrary amount of data, potentially more than could fit in memory at one time.
 */
public class JsonStreamingArrayParser<T> extends SpliteratorIterator<T> implements Closeable {
    private final JsonParser _jp;
    private final ObjectReader _reader;
    private final Class<? extends T> _type;

    public JsonStreamingArrayParser(InputStream in, Class<? extends T> elementType) {
        this(in, CustomJsonObjectMapperFactory.build(), elementType);
    }

    public JsonStreamingArrayParser(InputStream in, ObjectMapper mapper, Class<? extends T> elementType) {
        this(in, mapper, (Type) elementType);
    }

    public JsonStreamingArrayParser(InputStream in, TypeReference<? extends T> elementType) {
        this(in, CustomJsonObjectMapperFactory.build(), elementType.getType());
    }

    public JsonStreamingArrayParser(InputStream in, ObjectMapper mapper, TypeReference<? extends T> elementType) {
        this(in, mapper, elementType.getType());
    }

    private JsonStreamingArrayParser(InputStream in, ObjectMapper mapper, Type elementType) {
        try {
            JavaType javaType = mapper.constructType(elementType);
            //noinspection unchecked
            _type = (Class<? extends T>) javaType.getRawClass();
            _jp = mapper.getFactory().createJsonParser(in);
            _reader = mapper.reader(javaType);

            // Parse at least the first byte of the response to make sure the input stream is valid.
            if (_jp.nextToken() != JsonToken.START_ARRAY) {
                throw new JsonParseException("Invalid JSON response, expected content to start with '[': " +
                        _jp.getCurrentToken(), _jp.getTokenLocation());
            }
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected Spliterator<T> getSpliterator() {
        return new AbstractSpliterator<T>() {
            @Override
            protected T computeNext() {
                try {
                    if (_jp.nextToken() == JsonToken.END_ARRAY) {
                        if (_jp.nextToken() != null) {
                            throw new IllegalStateException("Expected EOF in JavaScript input stream.");
                        }
                        _jp.close();
                        return endOfStream();
                    }
                    return _type.cast(_reader.readValue(_jp));
                } catch (IOException e) {
                    // If the root cause is a JsonProcessingException then throw it as a JsonStreamProcessingException.
                    if (isJsonProcessingException(e)) {
                        throw new JsonStreamProcessingException(e);
                    }

                    // We already parsed the first few bytes in the constructor and verified that the InputStream looked valid
                    // so if there's an unexpected end of input here it likely means we lost the connection to the server.
                    // In practice this a JsonStreamingEOFException or a TruncatedChunkException or something similar.
                    throw new JsonStreamingEOFException(e);
                }
            }
        };
    }

    /**
     * Returns true if the exception is a JsonProcessingException whose root cause is not a premature end of data,
     * since this is more likely caused by a connection error.
     */
    private boolean isJsonProcessingException(Exception e) {
        // Unfortunately Jackson doesn't have specific subclasses for each parse exception, so we have to use the
        // more brittle approach of checking the exception message.
        return e instanceof JsonProcessingException &&
                !(e instanceof JsonParseException &&
                        e.getMessage() != null &&
                        e.getMessage().startsWith("Unexpected end-of-input"));
    }

    @Override
    public void close() throws IOException {
        _jp.close();
    }
}
