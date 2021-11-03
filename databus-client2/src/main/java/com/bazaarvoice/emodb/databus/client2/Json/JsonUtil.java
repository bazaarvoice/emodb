package com.bazaarvoice.emodb.databus.client2.Json;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.base.Throwables;

import com.google.common.collect.AbstractIterator;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

public class JsonUtil {

    private final static ObjectMapper OBJECT_MAPPER = new ObjectMapper()
            .registerModule(new Jdk8Module())
            .registerModule(new JavaTimeModule());

    public static <T> T parseJson(String json, Class<T> type) {
        try {
            return OBJECT_MAPPER.readValue(json, type);
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    public static <T> T parseJson(byte[] json, Class<T> type) {
        try {
            return OBJECT_MAPPER.readValue(json, type);
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    public static <T> T parseJson(InputStream json, Class<T> type) {
        try {
            return OBJECT_MAPPER.readValue(json, type);
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    public static <T> T parseJson(String json, TypeReference<T> type) {
        try {
            return OBJECT_MAPPER.readValue(json, type);
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    public static <T> T parseJson(byte[] json, TypeReference<T> type) {
        try {
            return OBJECT_MAPPER.readValue(json, type);
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    public static <T> T parseJson(InputStream json, TypeReference<T> type) {
        try {
            return OBJECT_MAPPER.readValue(json, type);
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    public static String toJsonString(Object value) {
        try {
            return OBJECT_MAPPER.writeValueAsString(value);
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    public static <T> Iterator<T> deserializeStream(InputStream json, Class<T> elementType) {
        return deserializeStream(json, OBJECT_MAPPER.getTypeFactory().constructType(elementType));
    }

    public static <T> Iterator<T> deserializeStream(InputStream json, TypeReference<T> elementType) {
        return deserializeStream(json, OBJECT_MAPPER.getTypeFactory().constructType(elementType));
    }

    private static <T> Iterator<T> deserializeStream(InputStream json, JavaType elementType) {
        try {
            final JsonParser parser = OBJECT_MAPPER.getFactory().createParser(json);
            if (parser.nextToken() != JsonToken.START_ARRAY) {
                throw new JsonParseException("Start array not found", parser.getTokenLocation());
            }

            return new AbstractIterator<T>() {
                @Override
                protected T computeNext() {
                    try {
                        if (parser.nextToken() == JsonToken.END_ARRAY) {
                            parser.close();
                            return endOfData();
                        } else {
                            return OBJECT_MAPPER.readValue(parser, elementType);
                        }
                    } catch (Exception e) {
                        throw Throwables.propagate(e);
                    }
                }
            };
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }
}
