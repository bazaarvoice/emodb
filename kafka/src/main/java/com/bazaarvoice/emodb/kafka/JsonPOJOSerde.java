package com.bazaarvoice.emodb.kafka;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class JsonPOJOSerde<T> implements Serde<T> {

    private static final ObjectMapper _mapper = new ObjectMapper();
    private final Class<T> _cls;
    private final TypeReference<T> _typeReference;

    static {
        _mapper.registerModule(new JavaTimeModule());
    }

    public JsonPOJOSerde(Class<T> cls) {
        this._cls = cls;
        _typeReference = null;
    }

    public JsonPOJOSerde(TypeReference<T> t) {
        _typeReference = t;
        _cls = null;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public void close() {

    }

    @Override
    public Serializer<T> serializer() {
        return new Serializer<T>() {

            @Override
            public void configure(Map<String, ?> configs, boolean isKey) {

            }

            @Override
            public byte[] serialize(String topic, T data) {
                if (data == null) {
                    return null;
                }
                try {
                    return _mapper.writeValueAsBytes(data);
                } catch (Exception e) {
                    throw new SerializationException("Error serializing JSON message", e);
                }
            }

            @Override
            public void close() {

            }
        };

    }

    @Override
    public Deserializer<T> deserializer() {
        return new Deserializer<T>() {
            @Override
            public void configure(Map<String, ?> configs, boolean isKey) {

            }

            @Override
            public T deserialize(String topic, byte[] data) {

                if (data == null) {
                    return null;
                }

                T result;
                try {
                    result = _typeReference != null ? _mapper.readValue(data, _typeReference) : _mapper.readValue(data, _cls);
                } catch (Exception e) {
                    throw new SerializationException(e);
                }

                return result;
            }

            @Override
            public void close() {

            }
        };
    }
}
