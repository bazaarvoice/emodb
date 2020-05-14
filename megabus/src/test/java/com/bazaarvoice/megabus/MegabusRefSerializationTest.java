package com.bazaarvoice.megabus;

import com.bazaarvoice.emodb.common.json.ISO8601DateFormat;
import com.bazaarvoice.emodb.common.uuid.TimeUUIDs;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import java.io.IOException;
import java.time.Instant;

import static org.testng.Assert.assertEquals;

public class MegabusRefSerializationTest {

    private ObjectMapper _mapper = new ObjectMapper();
    private Serializer<JsonNode> _serializer = new JsonSerializer();

    private static final String TEST_TABLE_NAME = "tableA";
    private static final String TEST_KEY = "abc123";

    @BeforeSuite
    public void setup() {
        _mapper
                .setDateFormat(new ISO8601DateFormat())
                .registerModule(new JavaTimeModule());
    }

    @Test
    public void testSerialization() throws JsonProcessingException {
        MegabusRef ref = new MegabusRef(TEST_TABLE_NAME, TEST_KEY, TimeUUIDs.minimumUuid(), Instant.EPOCH, MegabusRef.RefType.NORMAL);
        assertEquals(_mapper.writeValueAsString(ref), "{\"table\":\"tableA\",\"key\":\"abc123\",\"changeId\":\"00000000-0000-1000-8000-000000000000\",\"readTime\":\"1970-01-01T00:00:00Z\",\"refType\":\"NORMAL\"}");
    }

    @Test
    public void testDeserialization() throws IOException {
        String ref = "{\"table\":\"tableA\",\"key\":\"abc123\",\"changeId\":\"00000000-0000-1000-8000-000000000000\",\"readTime\":\"1970-01-01T00:00:00Z\",\"refType\":\"DELETED\"}";
        MegabusRef refDeserialized = _mapper.readValue(ref, MegabusRef.class);
        MegabusRef expectedRef = new MegabusRef(TEST_TABLE_NAME, TEST_KEY, TimeUUIDs.minimumUuid(), Instant.EPOCH, MegabusRef.RefType.DELETED);
        assertEquals(refDeserialized, expectedRef);
    }

    @Test
    public void testLegacyDeserialization() throws IOException {
        String ref = "{\"table\":\"tableA\",\"key\":\"abc123\",\"changeId\":\"00000000-0000-1000-8000-000000000000\",\"readTime\":\"1970-01-01T00:00:00Z\"}";
        MegabusRef refDeserialized = _mapper.readValue(ref, MegabusRef.class);
        MegabusRef expectedRef = new MegabusRef(TEST_TABLE_NAME, TEST_KEY, TimeUUIDs.minimumUuid(), Instant.EPOCH, MegabusRef.RefType.NORMAL);
        assertEquals(refDeserialized, expectedRef);
    }
}
