package com.bazaarvoice.emodb.kafka;

import com.fasterxml.jackson.core.type.TypeReference;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

public class JsonPOJOSerdeTest {

    @Test
    public void testPOJO() {
        JsonPOJOSerde<TestPOJO> jsonPOJOSerde = new JsonPOJOSerde<>(TestPOJO.class);

        TestPOJO testPOJO = new TestPOJO(1, "hello");

        byte[] serializedResult = jsonPOJOSerde.serializer().serialize("", testPOJO);
        TestPOJO deserializedResult = jsonPOJOSerde.deserializer().deserialize("", serializedResult);

        assertEquals(new String(serializedResult), "{\"val1\":1,\"val2\":\"hello\"}");
        assertEquals(deserializedResult, testPOJO);
    }

    @Test
    public void testList() {
        JsonPOJOSerde<List<TestPOJO>> jsonPOJOSerde = new JsonPOJOSerde<>(new TypeReference<List<TestPOJO>>() {});
        List<TestPOJO> testPOJOList = Collections.singletonList(new TestPOJO(1, "hello"));
        byte[] serializedResult = jsonPOJOSerde.serializer().serialize("", testPOJOList);
        List<TestPOJO> deserializedResult = jsonPOJOSerde.deserializer().deserialize("", serializedResult);
        assertEquals(new String(serializedResult), "[{\"val1\":1,\"val2\":\"hello\"}]");
        assertEquals(deserializedResult, testPOJOList);
    }

    @Test
    public void testNull() {
        JsonPOJOSerde<TestPOJO> jsonPOJOSerde = new JsonPOJOSerde<>(TestPOJO.class);
        TestPOJO nullTestPOJO = null;

        byte[] serializedResult = jsonPOJOSerde.serializer().serialize("", null);
        TestPOJO deserializedResult = jsonPOJOSerde.deserializer().deserialize("", serializedResult);

        assertNull(serializedResult);
        assertNull(deserializedResult);
        assertEquals(serializedResult, deserializedResult);
    }

    private static class TestPOJO {

        private int val1;
        private String val2;

        public TestPOJO(int val1, String val2) {
            this.val1 = val1;
            this.val2 = val2;
        }

        public TestPOJO() {
        }

        public int getVal1() {
            return val1;
        }

        public String getVal2() {
            return val2;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof TestPOJO)) return false;
            TestPOJO testPOJO = (TestPOJO) o;
            return getVal1() == testPOJO.getVal1() &&
                    Objects.equals(getVal2(), testPOJO.getVal2());
        }
    }
}
