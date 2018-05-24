package com.bazaarvoice.emodb.common.json.deferred;

import com.bazaarvoice.emodb.common.json.CustomJsonObjectMapperFactory;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import org.testng.annotations.Test;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class LazyJsonMapTest {

    @Test
    public void testGet() {
        LazyJsonMap map = new LazyJsonMap("{\"key\":[\"value\"]}");
        Object actual = map.get("key");
        assertEquals(actual, ImmutableList.of("value"));
        assertTrue(map.isDeserialized());
    }

    @Test
    public void testKeySet() {
        LazyJsonMap map = new LazyJsonMap("{\"k1\":\"v1\",\"k2\":\"v2\"}");
        Set<String> actual = map.keySet();
        assertEquals(actual, ImmutableSet.of("k1", "k2"));
        assertTrue(map.isDeserialized());
    }

    @Test
    public void testValues() {
        LazyJsonMap map = new LazyJsonMap("{\"k1\":\"v1\",\"k2\":\"v2\"}");
        Collection<Object> actual = map.values();
        assertEquals(actual.size(), 2);
        assertEquals(ImmutableSet.copyOf(actual), ImmutableSet.of("v1", "v2"));
        assertTrue(map.isDeserialized());
    }

    @Test
    public void testEntrySet() {
        LazyJsonMap map = new LazyJsonMap("{\"k1\":\"v1\",\"k2\":\"v2\"}");
        Map<String, Object> expected = Maps.newHashMapWithExpectedSize(2);
        expected.put("k1", "v1");
        expected.put("k2", "v2");
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            assertEquals(entry.getValue(), expected.remove(entry.getKey()));
        }
        assertTrue(expected.isEmpty());
        assertTrue(map.isDeserialized());
    }

    @Test
    public void testGetSize() {
        LazyJsonMap map = new LazyJsonMap("{\"k1\":\"v1\",\"k2\":\"v2\"}");
        assertEquals(map.size(), 2);
        assertTrue(map.isDeserialized());
    }

    @Test
    public void testGetWithOverrides() {
        LazyJsonMap map = new LazyJsonMap("{\"k1\":1}");
        map.put("k1", 100);
        map.put("k2", 200);
        assertEquals(map.get("k1"), 100);
        assertEquals(map.get("k2"), 200);
        assertFalse(map.isDeserialized());
    }

    @Test
    public void testKeySetWithOverride() {
        LazyJsonMap map = new LazyJsonMap("{\"k1\":\"v1\",\"k2\":\"v2\"}");
        map.put("k2", "v22");
        map.put("k3", "v3");
        Set<String> actual = map.keySet();
        assertEquals(actual, ImmutableSet.of("k1", "k2", "k3"));
        assertTrue(map.isDeserialized());
    }

    @Test
    public void testValuesWithOverride() {
        LazyJsonMap map = new LazyJsonMap("{\"k1\":\"v1\",\"k2\":\"v2\"}");
        map.put("k2", "v22");
        map.put("k3", "v3");
        Collection<Object> actual = map.values();
        assertEquals(actual.size(), 3);
        assertEquals(ImmutableSet.copyOf(actual), ImmutableSet.of("v1", "v22", "v3"));
        assertTrue(map.isDeserialized());
    }

    @Test
    public void testEntrySetWithOverride() {
        LazyJsonMap map = new LazyJsonMap("{\"k1\":\"v1\",\"k2\":\"v2\"}");
        map.put("k2", "v22");
        map.put("k3", "v3");
        Map<String, Object> expected = Maps.newHashMapWithExpectedSize(3);
        expected.put("k1", "v1");
        expected.put("k2", "v22");
        expected.put("k3", "v3");
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            assertEquals(entry.getValue(), expected.remove(entry.getKey()));
        }
        assertTrue(expected.isEmpty());
        assertTrue(map.isDeserialized());
    }

    @Test
    public void testGetSizeWithOverride() {
        LazyJsonMap map = new LazyJsonMap("{\"k1\":\"v1\",\"k2\":\"v2\"}");
        map.put("k2", "v22");
        map.put("k3", "v3");
        assertEquals(map.size(), 3);
        assertTrue(map.isDeserialized());
    }

    @Test
    public void testContainsKey() {
        LazyJsonMap map = new LazyJsonMap("{\"k1\":\"v1\",\"k2\":\"v2\"}");
        map.put("k2", "v22");
        map.put("k3", "v3");

        assertTrue(map.containsKey("k2"));
        assertTrue(map.containsKey("k3"));
        assertFalse(map.isDeserialized());
        // Checking for any other key requires deserializing the map
        assertTrue(map.containsKey("k1"));
        assertFalse(map.containsKey("k999"));
        assertTrue(map.isDeserialized());
    }

    @Test
    public void testContainsValue() {
        LazyJsonMap map = new LazyJsonMap("{\"k1\":\"v1\",\"k2\":\"v2\"}");
        map.put("k2", "v22");
        map.put("k3", "v3");

        assertTrue(map.containsValue("v22"));
        assertTrue(map.containsValue("v3"));
        assertFalse(map.isDeserialized());
        // Checking for any other value requires deserializing the map
        assertTrue(map.containsValue("v1"));
        assertFalse(map.containsValue("v999"));
        assertTrue(map.isDeserialized());
    }

    @Test
    public void testIsEmptyWithEmptyJson() {
        // Empty JSON, no overrides
        LazyJsonMap map = new LazyJsonMap("{}");
        assertTrue(map.isEmpty());
        // Empty check shouldn't have serialized
        assertFalse(map.isDeserialized());
        // Add an override
        map.put("k1", "v1");
        assertFalse(map.isEmpty());
        // Still shouldn't have serialized
        assertFalse(map.isDeserialized());
    }

    @Test
    public void testIsEmptyWithNonEmptyJson() {
        LazyJsonMap map = new LazyJsonMap("{\"k1\":\"v1\"}");
        assertFalse(map.isEmpty());
        // Empty check shouldn't have serialized
        assertFalse(map.isDeserialized());
    }

    @Test
    public void testJsonSerializeNoDeserialization() throws Exception {
        LazyJsonMap map = new LazyJsonMap("{\"k1\":\"v1\",\"k2\":\"v2\"}");
        map.put("k2", "v22");
        map.put("k3", "v3");

        ObjectMapper objectMapper = CustomJsonObjectMapperFactory.build();
        objectMapper.registerModule(new LazyJsonModule());

        // Write to JSON, then read back
        String asJson = objectMapper.writeValueAsString(map);
        Map<String, Object> actual = objectMapper.readValue(asJson, new TypeReference<Map<String, Object>>() {});
        Map<String, Object> expected = ImmutableMap.of("k1", "v1", "k2", "v22", "k3", "v3");
        assertEquals(actual, expected);
        // Serialization should not have forced deserialize
        assertFalse(map.isDeserialized());
    }

    @Test
    public void testJsonSerializeAfterDeserialization() throws Exception {
        LazyJsonMap map = new LazyJsonMap("{\"k1\":\"v1\",\"k2\":\"v2\"}");
        map.put("k2", "v22");
        map.put("k3", "v3");
        // Perform a size call which should force the map to deserialize
        map.size();
        assertTrue(map.isDeserialized());

        ObjectMapper objectMapper = CustomJsonObjectMapperFactory.build();
        objectMapper.registerModule(new LazyJsonModule());

        // Write to JSON, then read back
        String asJson = objectMapper.writeValueAsString(map);
        Map<String, Object> actual = objectMapper.readValue(asJson, new TypeReference<Map<String, Object>>() {});
        Map<String, Object> expected = ImmutableMap.of("k1", "v1", "k2", "v22", "k3", "v3");
        assertEquals(actual, expected);
    }
}
