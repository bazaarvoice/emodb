package com.bazaarvoice.emodb.common.json.deferred;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.core.util.VersionUtil;
import com.fasterxml.jackson.databind.BeanDescription;
import com.fasterxml.jackson.databind.BeanProperty;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.SerializationConfig;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.BeanSerializerModifier;
import com.fasterxml.jackson.databind.ser.ContextualSerializer;
import com.fasterxml.jackson.databind.type.MapType;
import com.fasterxml.jackson.databind.type.SimpleType;

import java.io.IOException;
import java.util.Map;

/**
 * Module for efficiently serializing {@link LazyJsonMap}.  Since LazyJsonMap implements {@link Map} the standard
 * JSON serializer will serialize it by iterating over its {@link Map#entrySet()}.  This forces the LazyJsonMap to
 * be deserialized, which bypasses the entire purpose of not requiring unnecessary deserilization for serialization.
 *
 * This module overrides the standard {@link com.fasterxml.jackson.databind.ser.std.MapSerializer} to use the
 * lazy implementation when the input Map is a LazyJsonMap instance, otherwise to use the standard Map serializer.
 */
public class LazyJsonModule extends Module {

    private final static JsonSerializer<LazyJsonMap> _lazyMapSerializer = new LazyJsonMapSerializer();

    @Override
    public String getModuleName() {
        return "LazyJson";
    }

    @Override
    public Version version() {
        return VersionUtil.parseVersion("1.0", "com.bazaarvoice.emodb.common.json", "lazy-json");
    }

    @Override
    public void setupModule(SetupContext context) {
        // Modify the Map serializer to the delegate if it matches Map<String, ?>
        context.addBeanSerializerModifier(new BeanSerializerModifier() {
            @Override
            public JsonSerializer<?> modifyMapSerializer(SerializationConfig config, MapType valueType, BeanDescription beanDesc,
                                                         JsonSerializer<?> serializer) {
                if (valueType.getKeyType().equals(SimpleType.construct(String.class))) {
                    return new DelegatingMapSerializer(serializer);
                }
                return serializer;
            }
        });
    }

    /**
     * Map serializer implementation which uses a {@link LazyJsonMapSerializer} for LazyJsonMap instances and delegates
     * to a default serializer for all other Map classes.
     */
    private static class DelegatingMapSerializer extends JsonSerializer<Map<String, Object>> implements ContextualSerializer {

        private final JsonSerializer<Map<String, Object>> _delegateSerializer;

        DelegatingMapSerializer(JsonSerializer<?> delegateSerializer) {
            //noinspection unchecked
            _delegateSerializer = (JsonSerializer<Map<String, Object>>) delegateSerializer;
        }

        @Override
        public void serialize(Map<String, Object> value, JsonGenerator jgen, SerializerProvider provider)
                throws IOException, JsonProcessingException {
            if (value instanceof LazyJsonMap) {
                _lazyMapSerializer.serialize((LazyJsonMap) value, jgen, provider);
            } else {
                _delegateSerializer.serialize(value, jgen, provider);
            }
        }

        /**
         * Override to preserve the delegating behavior when a contextualized serializer is created.
         */
        @Override
        public JsonSerializer<?> createContextual(SerializerProvider prov, BeanProperty property)
                throws JsonMappingException {
            if (_delegateSerializer instanceof ContextualSerializer) {
                JsonSerializer<?> contextualDelegate = ((ContextualSerializer) _delegateSerializer).createContextual(prov, property);
                // Check for different instance
                if (contextualDelegate != _delegateSerializer) {
                    return new DelegatingMapSerializer(contextualDelegate);
                }
            }
            return this;
        }
    }
}
