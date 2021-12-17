package com.bazaarvoice.emodb.common.json.deferred;

import com.bazaarvoice.emodb.common.json.JsonHelper;
import com.bazaarvoice.emodb.common.json.OrderedJson;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.collect.Maps;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

/**
 * This object takes a string which contains a JSON object and lazily provides a Map&lt;String, Object&gt; interface
 * around it.  A sequence similar to the following is performed frequently by Emo:
 *
 * <ol>
 *     <li>
 *         A request for a document comes in from the API, such as a DataStore request for a single record.
 *     </li>
 *     <li>
 *         The document is retrieved from the backend and contains a single, large compacted JSON object.
 *     </li>
 *     <li>
 *         The JSON object is converted into a Java map.  Usually a few more attributes are added to the map,
 *         such as intrinsics and table attributes.
 *     </li>
 *     <li>
 *         The object is returned to the web application layer, which in turn converts the Java map back into
 *         a JSON stream.
 *     </li>
 * </ol>
 *
 * With large records and in aggregate over many records tests have shown that it is significantly more efficient to
 * avoid generating the Java map as much as possible and stream the original JSON back to the web application.  In
 * the best case the server responds more efficiently.  In those cases where an object representation is needed
 * the implementation lazily deserializes the JSON into a <code>Map</code>, which in the end doesn't perform any additional
 * work beyond what would have been done were the JSON deserialized to a <code>Map</code> in the first place.
 *
 * This implementation is not thread safe on updates such as {@link #put(String, Object)}.  Because the underlying map
 * may be lazily deserialized a race condition could result in updates being lost if the instance is updated concurrently
 * by multiple threads.
 *
 * CAUTION!
 *
 * The JSON string is itself lazily resolved.  Passing in a string which is not valid JSON or is not a JSON object
 * will result in deferred errors when interacting with the map.  This class should only be used in situations where
 * the input string is known to contain a valid JSON object.
 */
@JsonSerialize(using = LazyJsonMapSerializer.class)
public class LazyJsonMap implements Map<String, Object> {

    private final AtomicReference<DeserializationState> _deserState;

    @JsonCreator
    public LazyJsonMap(String json) {
        this(new DeserializationState(requireNonNull(json, "json")));
    }

    private LazyJsonMap(DeserializationState deserState) {
        _deserState = new AtomicReference<>(deserState);
    }

    /**
     * At any time the map could be in one of two states:
     * <ol>
     *     <li>JSON string, in which case the state consists of the original JSON string and a map of any updates
     *         which have been performed on the map, and</li>
     *     <li>Deserialized, in which case the state consists of the Java Map object representation.</li>
     * </ol>
     */
    private static class DeserializationState {
        // Initial JSON string attributes
        private final String json;
        private final Map<String, Object> overrides;
        // Deserialized attributes
        private final Map<String, Object> deserialized;

        DeserializationState(String json) {
            this.json = json;
            this.overrides = Maps.newHashMap();
            this.deserialized = null;
        }

        DeserializationState(Map<String, Object> deserialized) {
            this.deserialized = deserialized;
            this.json = null;
            this.overrides = null;
        }

        boolean isDeserialized() {
            return deserialized != null;
        }

        DeserializationState copy() {
            DeserializationState copy;
            if (deserialized != null) {
                copy = new DeserializationState(Maps.newHashMap(deserialized));
            } else {
                copy = new DeserializationState(json);
                copy.overrides.putAll(overrides);
            }
            return copy;
        }
    }

    /**
     * Returns the JSON as a Map.  If necessary the JSON is converted to a Map as a result of this call.
     */
    private Map<String, Object> deserialized() {
        // Written as a loop to prevent the need for locking
        DeserializationState deserState;
        while (!(deserState = _deserState.get()).isDeserialized()) {
            Map<String, Object> deserialized = JsonHelper.fromJson(deserState.json, new TypeReference<Map<String, Object>>() {});
            deserialized.putAll(deserState.overrides);
            DeserializationState newDeserState = new DeserializationState(deserialized);
            _deserState.compareAndSet(deserState, newDeserState);
        }
        return deserState.deserialized;
    }

    @Override
    public boolean isEmpty() {
        DeserializationState deserializationState = _deserState.get();
        if (deserializationState.isDeserialized()) {
            return deserializationState.deserialized.isEmpty();
        }
        // If the JSON is empty it will contain only '{', '}', and possibly white space.  If it is not empty it must
        // contain at least one '"' to open the first field name string.  So a shortcut to test emptiness is to check
        // whether '"' does not exist in the string.
        return deserializationState.overrides.isEmpty() && deserializationState.json.indexOf('"') == -1;
    }

    @Override
    public boolean containsKey(Object key) {
        DeserializationState deserializationState = _deserState.get();
        if (deserializationState.isDeserialized()) {
            return deserializationState.deserialized.containsKey(key);
        }
        // If the overrides contains the key then we can still hold off on deserializing the map
        return deserializationState.overrides.containsKey(key) || deserialized().containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        DeserializationState deserializationState = _deserState.get();
        if (deserializationState.isDeserialized()) {
            return deserializationState.deserialized.containsValue(value);
        }
        // If the overrides contains the value then we can still hold off on deserializing the map
        return deserializationState.overrides.containsValue(value) || deserialized().containsValue(value);
    }

    @Override
    public Object get(Object key) {
        DeserializationState deserializationState = _deserState.get();
        if (deserializationState.isDeserialized()) {
            return deserializationState.deserialized.get(key);
        }
        // If the overrides contains the key then we can still hold off on deserializing the map
        if (deserializationState.overrides.containsKey(key)) {
            return deserializationState.overrides.get(key);
        }
        return deserialized().get(key);
    }

    /**
     * For efficiency this method breaks the contract that the old value is returned.  Otherwise common operations such
     * as adding intrinsics and template attributes would require deserializing the object.
     */
    @Override
    public Object put(String key, Object value) {
        DeserializationState deserializationState = _deserState.get();
        if (deserializationState.isDeserialized()) {
            return deserializationState.deserialized.put(key, value);
        }
        return deserializationState.overrides.put(key, value);
    }

    @Override
    public Object remove(Object key) {
        // It's possible to handle removals without deserializing by creating a special "removed" object in the
        // overrides.  However, removing attributes from literals is uncommon and slows down other operations,
        // so always deserialize on remove.
        return deserialized().remove(key);
    }

    @Override
    public void putAll(Map<? extends String, ?> map) {
        DeserializationState deserializationState = _deserState.get();
        if (deserializationState.isDeserialized()) {
            deserializationState.deserialized.putAll(map);
        } else {
            deserializationState.overrides.putAll(map);
        }
    }

    @Override
    public void clear() {
        // No need to deserialize if not already deserialized
        _deserState.set(new DeserializationState(Maps.newHashMap()));
    }

    @Override
    public Set<String> keySet() {
        // Must deserialize to get all keys
        return deserialized().keySet();
    }

    @Override
    public Collection<Object> values() {
        // Must deserialize to get all values
        return deserialized().values();
    }

    @Override
    public Set<Entry<String, Object>> entrySet() {
        // Must deserialize to get all entries
        return deserialized().entrySet();
    }

    @Override
    public int size() {
        // Must deserialize to get size
        return deserialized().size();
    }

    /**
     * Writes this record to the provided generator in the most efficient manner possible in the current state.
     */
    void writeTo(JsonGenerator generator) throws IOException {
        DeserializationState deserState = _deserState.get();

        if (deserState.isDeserialized()) {
            // Object has already been deserialized, use standard writer
            generator.writeObject(deserState.deserialized);
            return;
        }

        if (deserState.overrides.isEmpty()) {
            // With no overrides the most efficient action is to copy the original JSON verbatim.
            try {
                generator.writeRaw(deserState.json);
                return;
            } catch (UnsupportedOperationException e) {
                // Not all parsers are guaranteed to support this.  If this is one then use the default
                // generator implementation which follows.
            }
        }

        ObjectCodec codec = generator.getCodec();
        if (codec == null) {
            // No codec, defer to generator
            generator.writeObject(deserialized());
            return;
        }

        try (JsonParser parser = codec.getFactory().createParser(deserState.json)) {
            checkState(parser.nextToken() == JsonToken.START_OBJECT, "JSON did not contain an object");
            generator.writeStartObject();

            // Typically the JSON string has been pre-sorted.  Insert the overrides in order.  If it turns out the
            // JSON wasn't sorted then we'll just dump the remaining overrides at the end; it's valid JSON
            // one way or the other.

            //noinspection unchecked
            Iterator<Map.Entry<String, Object>> sortedOverrides =
                    ((Map<String, Object>) OrderedJson.ordered(deserState.overrides)).entrySet().iterator();

            Map.Entry<String, Object> nextOverride = sortedOverrides.hasNext() ? sortedOverrides.next() : null;

            JsonToken token;
            while ((token = parser.nextToken()) != JsonToken.END_OBJECT) {
                assert token == JsonToken.FIELD_NAME;

                String field = parser.getText();
                if (deserState.overrides.containsKey(field)) {
                    // There's an override for this entry; skip it
                    token = parser.nextToken();
                    if (token.isStructStart()) {
                        parser.skipChildren();
                    }
                } else {
                    // Write all overrides which sort prior to this field
                    while (nextOverride != null && OrderedJson.KEY_COMPARATOR.compare(nextOverride.getKey(), field) < 0) {
                        generator.writeFieldName(nextOverride.getKey());
                        generator.writeObject(nextOverride.getValue());
                        nextOverride = sortedOverrides.hasNext() ? sortedOverrides.next() : null;
                    }

                    // Copy this field name and value to the generator
                    generator.copyCurrentStructure(parser);
                }
                // Both of the above operations leave the current token immediately prior to the next
                // field or the end object token.
            }

            // Write any remaining overrides
            while (nextOverride != null) {
                generator.writeFieldName(nextOverride.getKey());
                generator.writeObject(nextOverride.getValue());
                nextOverride = sortedOverrides.hasNext() ? sortedOverrides.next() : null;
            }

            generator.writeEndObject();
        }
    }

    public LazyJsonMap lazyCopy() {
        return new LazyJsonMap(_deserState.get().copy());
    }

    public boolean isDeserialized() {
        return _deserState.get().isDeserialized();
    }

    /**
     * Returns the overrides on the map if it has not been deserialized, or null if the map is deserialized.
     */
    @Nullable
    public Map<String, Object> getOverrides() {
        return _deserState.get().overrides;
    }

    @Override
    public String toString() {
        return JsonHelper.asJson(this);
    }

    @Override
    public int hashCode() {
        // For consistency must use the deserialized map
        return deserialized().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        // Delay deserialization if we can rule out equality by class
        if (!(obj instanceof Map)) {
            return false;
        }
        // For consistency must use the deserialized map
        return deserialized().equals(obj);
    }
}
